#![cfg(unix)]

use amux::{client::Client, config::Config};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use std::{
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use tokio_tungstenite::tungstenite::Message;

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct OwnedDaemon(Child);

impl Drop for OwnedDaemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn single_process_stop_remove_and_recreate_preserves_event_order() {
    use amux::model::{
        CreateWorkspaceRequest, IoMode, ProcessEvent, ProcessEventKind, ProcessSnapshot,
        RunRequest, StopRequest,
    };
    use amux::protocol::Response;
    use http::Method;

    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let _daemon = OwnedDaemon(daemon(&state, &socket));
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket,
    });
    client
        .do_json(
            Method::POST,
            "/v1/workspaces",
            Some(&CreateWorkspaceRequest {
                name: "dev".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let snapshot: ProcessSnapshot = serde_json::from_slice(
        &client
            .do_empty(Method::GET, "/v1/processes/snapshot")
            .await
            .unwrap(),
    )
    .unwrap();
    let mut events = client
        .stream(&format!(
            "/v1/processes/events?after={}&instance={}",
            snapshot.sequence, snapshot.daemon_instance_id
        ))
        .await
        .unwrap();
    let run = |index| RunRequest {
        name: "worker".into(),
        command: vec!["/bin/sleep".into(), "30".into()],
        io_mode: if index % 2 == 0 {
            IoMode::Tty
        } else {
            IoMode::Pipe
        },
        ..Default::default()
    };
    let mut current: Response = serde_json::from_slice(
        &client
            .do_json(Method::POST, "/v1/workspaces/dev/processes", Some(&run(0)))
            .await
            .unwrap(),
    )
    .unwrap();
    let mut pending = Vec::new();
    let mut seen = Vec::<ProcessEvent>::new();
    for index in 0..6 {
        let process = current.process.as_ref().unwrap().process.clone();
        if index == 0 {
            assert!(
                client
                    .do_json(Method::POST, "/v1/workspaces/dev/processes", Some(&run(0)))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("name_conflict")
            );
        }
        client
            .do_json(
                Method::POST,
                "/v1/processes/dev/worker/stop",
                Some(&StopRequest { grace_millis: 100 }),
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            client.do_empty(Method::GET, "/v1/processes/dev/worker/wait"),
        )
        .await
        .unwrap()
        .unwrap();
        if index == 0 {
            assert!(
                client
                    .do_json(Method::POST, "/v1/workspaces/dev/processes", Some(&run(0)))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("name_conflict")
            );
        }
        client
            .do_empty(Method::DELETE, "/v1/processes/dev/worker")
            .await
            .unwrap();
        // Recreate immediately, before reading queued lifecycle events. No keeper process.
        current = serde_json::from_slice(
            &client
                .do_json(
                    Method::POST,
                    "/v1/workspaces/dev/processes",
                    Some(&run(index + 1)),
                )
                .await
                .unwrap(),
        )
        .unwrap();
        let next_id = &current.process.as_ref().unwrap().process.id;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !seen.iter().any(|event| &event.process_id == next_id) {
            let frame = tokio::time::timeout_at(deadline, events.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            pending.extend_from_slice(&frame.into_data().unwrap_or_default());
            while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                let line: Vec<_> = pending.drain(..=end).collect();
                seen.push(serde_json::from_slice(&line).unwrap());
            }
        }
        let lifecycle: Vec<_> = seen
            .iter()
            .filter(|event| event.process_id == process.id)
            .map(|event| event.kind.clone())
            .collect();
        assert_eq!(lifecycle.first(), Some(&ProcessEventKind::ProcessCreated));
        assert!(lifecycle.contains(&ProcessEventKind::ProcessStarted));
        assert_eq!(
            lifecycle
                .iter()
                .filter(|kind| **kind == ProcessEventKind::ProcessExited)
                .count(),
            1
        );
        assert_eq!(lifecycle.last(), Some(&ProcessEventKind::ProcessRemoved));
        assert!(
            !state
                .join("workspaces")
                .join(&process.workspace_id)
                .join("processes")
                .join(&process.id)
                .exists()
        );
    }
    assert!(
        seen.windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    // Workspace deletion must also finish when its last supervisor has unloaded.
    tokio::time::timeout(
        Duration::from_secs(5),
        client.do_empty(Method::DELETE, "/v1/workspaces/dev"),
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot: ProcessSnapshot = serde_json::from_slice(
        &client
            .do_empty(Method::GET, "/v1/processes/snapshot")
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(snapshot.processes.is_empty());
    client
        .do_empty(Method::POST, "/v1/admin/stop")
        .await
        .unwrap();
}

fn daemon(state: &Path, socket: &Path) -> Child {
    daemon_with_env(state, socket, &[])
}

fn daemon_with_env(state: &Path, socket: &Path, env: &[(&str, &str)]) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_amux"));
    command
        .env("AMUX_STATE_DIR", state)
        .env("AMUX_SOCKET", socket)
        .envs(env.iter().copied())
        .args(["daemon", "serve"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::os::unix::net::UnixStream::connect(socket).is_err() {
        assert!(Instant::now() < deadline, "daemon socket was not ready");
        thread::sleep(Duration::from_millis(20));
    }
    child
}

fn named_amux(state: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_amux"))
        .env_remove("AMUX_SOCKET")
        .env_remove("AMUX_DAEMON")
        .env("AMUX_STATE_DIR", state)
        .args(args)
        .output()
        .unwrap()
}

fn amux(state: &Path, socket: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_STATE_DIR", state)
        .env("AMUX_SOCKET", socket)
        .args(args)
        .output()
        .unwrap()
}

fn ok(state: &Path, socket: &Path, args: &[&str]) -> String {
    let output = amux(state, socket, args);
    assert!(
        output.status.success(),
        "{:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn ok_in_workspace(state: &Path, socket: &Path, workspace: &str, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_STATE_DIR", state)
        .env("AMUX_SOCKET", socket)
        .env("AMUX_WORKSPACE", workspace)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

async fn next_text(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    kind: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            if let Message::Text(text) = message.unwrap()
                && text.contains(&format!("\"type\":\"{kind}\""))
            {
                return text.to_string();
            }
        }
        String::new()
    })
    .await
    .unwrap()
}

fn sequenced_output(message: Message) -> Option<(u64, Vec<u8>)> {
    let Message::Binary(frame) = message else {
        return None;
    };
    if frame.len() < 8 {
        return None;
    }
    let sequence = u64::from_be_bytes(frame[..8].try_into().unwrap());
    Some((sequence, frame[8..].to_vec()))
}

#[tokio::test]
async fn pipe_lifecycle_and_daemon_recovery() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut first = OwnedDaemon(daemon(&state, &socket));

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok_in_workspace(
        &state,
        &socket,
        "dev",
        &["run", "--name", "defaulted", "--", "/usr/bin/true"],
    );
    ok(&state, &socket, &["wait", "dev/defaulted"]);
    ok(&state, &socket, &["rm", "dev/defaulted"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "io",
            "--",
            "/bin/sh",
            "-c",
            "printf $AMUX:$AMUX_WORKSPACE:$AMUX_PROCESS; printf err >&2; exit 7",
        ],
    );
    let waited = ok(&state, &socket, &["--json", "wait", "dev/io"]);
    assert!(waited.contains("\"exit_code\":7"));
    assert!(waited.contains("\"state\":\"exited\""));
    let waited_json: serde_json::Value = serde_json::from_str(&waited).unwrap();
    let workspace_id = waited_json["process"]["workspace_id"].as_str().unwrap();
    let process_id = waited_json["process"]["id"].as_str().unwrap();
    let event_index = std::fs::read_to_string(
        state
            .join("workspaces")
            .join(workspace_id)
            .join("processes")
            .join(process_id)
            .join("output")
            .join("current")
            .join("events.jsonl"),
    )
    .unwrap();
    assert!(
        !event_index.contains("\"data\""),
        "event index must reference authoritative raw logs"
    );
    let compatibility_client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let encoded = compatibility_client
        .do_empty(http::Method::GET, "/v1/processes/dev%2Fio")
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&encoded).contains("\"target\":\"dev/io\""));
    let encoded_logs = compatibility_client
        .do_empty(http::Method::GET, "/v1/processes/dev%2Fio/logs?after=0")
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&encoded_logs).contains("1:dev:io"));
    let logs = ok(&state, &socket, &["logs", "dev/io", "--jsonl"]);
    assert!(logs.contains("\"stream\":\"stdout\""));
    assert!(logs.contains("\"stream\":\"stderr\""));
    assert!(logs.contains("1:dev:io"));
    ok(&state, &socket, &["restart", "dev/io"]);
    let restarted = ok(&state, &socket, &["--json", "wait", "dev/io"]);
    assert!(restarted.contains("\"execution\":2"));
    let restarted_logs = ok(&state, &socket, &["logs", "dev/io", "--jsonl"]);
    assert!(restarted_logs.contains("\"execution\":2"));

    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "survivor",
            "--",
            "/bin/sh",
            "-c",
            "while true; do sleep 1; done",
        ],
    );
    let before = ok(&state, &socket, &["--json", "inspect", "dev/survivor"]);
    let pid = serde_json::from_str::<serde_json::Value>(&before).unwrap()["process"]["pid"]
        .as_i64()
        .unwrap();
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while unsafe { libc::kill(pid as i32, 0) } == 0 {
        assert!(
            Instant::now() < deadline,
            "business process survived daemon death"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let _second = OwnedDaemon(daemon(&state, &socket));
    // PID disappearance precedes the shim's final metadata write. The new daemon
    // must recover that terminal state even without the old lifecycle bridge.
    let after = tokio::time::timeout(
        Duration::from_secs(5),
        compatibility_client.do_empty(http::Method::GET, "/v1/processes/dev/survivor/wait"),
    )
    .await
    .unwrap()
    .unwrap();
    let after = std::str::from_utf8(&after).unwrap();
    assert!(after.contains("\"state\":\"exited\"") || after.contains("\"state\":\"failed\""));
    ok(&state, &socket, &["rm", "dev/survivor"]);
    ok(&state, &socket, &["rm", "dev/io"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
}

#[tokio::test]
async fn inherited_tty_default_accepts_ctrl_c_and_allows_pipe_override() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    struct RuntimeGuard {
        child: Child,
    }
    impl Drop for RuntimeGuard {
        fn drop(&mut self) {
            // This fixture uses an explicit socket, not a named daemon registration.
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
            let _ = self.child.wait();
        }
    }
    let _runtime = RuntimeGuard {
        child: daemon(&state, &socket),
    };
    ok(&state, &socket, &["workspace", "create", "dev"]);
    let launch = |name: &str, mode: Option<&str>, script: &str| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_amux"));
        command
            .env("AMUX_STATE_DIR", &state)
            .env("AMUX_SOCKET", &socket)
            .env("AMUX_WORKSPACE", "dev")
            .env("AMUX_IO_MODE", "tty")
            .args(["--json", "run", "--name", name]);
        if let Some(mode) = mode {
            command.args(["--io-mode", mode]);
        }
        let output = command
            .args(["--", "/bin/sh", "-c", script])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    let process = launch(
        "interactive",
        None,
        "test -t 0 && test -t 1 || exit 9; trap 'exit 42' INT; printf READY; while :; do read value; done",
    );
    assert_eq!(process["process"]["io_mode"], "tty");
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let mut terminal = client
        .attach("/v1/processes/dev/interactive/attach?replay_bytes=8192")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut output = Vec::new();
        while let Some(message) = terminal.next().await {
            if let Message::Binary(bytes) = message.unwrap() {
                output.extend_from_slice(&bytes);
                if output.windows(5).any(|bytes| bytes == b"READY") {
                    return;
                }
            }
        }
        panic!("TTY closed before the command was ready");
    })
    .await
    .unwrap();
    terminal
        .send(Message::Binary(vec![3].into()))
        .await
        .unwrap();
    let exit = next_text(&mut terminal, "exit").await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&exit).unwrap()["exit_code"],
        42,
        "Ctrl+C must reach the command's SIGINT trap"
    );
    terminal.close(None).await.ok();

    let process = launch("piped", Some("pipe"), "test ! -t 0 && test ! -t 1");
    assert_eq!(process["process"]["io_mode"], "pipe");
    let waited = ok(&state, &socket, &["--json", "wait", "dev/piped"]);
    assert!(waited.contains("\"exit_code\":0"));
}

#[tokio::test]
async fn tty_attach_and_controller_ownership() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "tty",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "read value; printf 'TTY:%s\\n' \"$value\"",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let mut first = client
        .attach("/v1/processes/dev/tty/attach?replay_bytes=0")
        .await
        .unwrap();
    assert!(
        client
            .attach("/v1/processes/dev%2Ftty/attach?replay_bytes=0")
            .await
            .is_err()
    );
    first
        .send(Message::Binary(b"hello\n".to_vec().into()))
        .await
        .unwrap();

    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = first.next().await {
            match message.unwrap() {
                Message::Binary(value) => {
                    bytes.extend_from_slice(&value);
                    if bytes.windows(9).any(|v| v == b"TTY:hello") {
                        break;
                    }
                }
                Message::Text(value) if value.contains("\"type\":\"exit\"") => break,
                _ => {}
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("TTY:hello"));
    drop(first);

    let waited = ok(&state, &socket, &["--json", "wait", "dev/tty"]);
    assert!(waited.contains("\"exit_code\":0"));
    ok(&state, &socket, &["rm", "dev/tty"]);

    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "follow",
            "--",
            "/bin/sh",
            "-c",
            "printf first; sleep 0.2; printf second",
        ],
    );
    let mut stream = client
        .stream("/v1/processes/dev/follow/logs?after=0&follow=true")
        .await
        .unwrap();
    let followed = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(frame) = stream.frame().await {
            bytes.extend_from_slice(&frame.unwrap().into_data().unwrap_or_default());
            if bytes.windows(6).any(|value| value == b"second") {
                break;
            }
        }
        bytes
    })
    .await
    .unwrap();
    let followed = String::from_utf8_lossy(&followed);
    assert!(followed.contains("first") && followed.contains("second"));
    ok(&state, &socket, &["wait", "dev/follow"]);
    ok(&state, &socket, &["rm", "dev/follow"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn acknowledged_tty_input_survives_reconnect_without_duplicates() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "reliable",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "IFS= read -r first; IFS= read -r second; printf 'TTY:%s:%s\\n' \"$first\" \"$second\"",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let path = "/v1/processes/dev/reliable/attach?takeover=true&replay_bytes=0&input_protocol=acked-v1&input_client_id=browser-1";
    let mut first = client.attach(path).await.unwrap();
    first
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"alpha\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = first.next().await {
            if let Message::Text(text) = message.unwrap()
                && text.contains("\"type\":\"input_ack\"")
                && text.contains("\"sequence\":1")
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(first);

    let mut second = client.attach(path).await.unwrap();
    for (sequence, data) in [(1, b"alpha\n".as_slice()), (2, b"beta\n".as_slice())] {
        second
            .send(Message::Text(
                serde_json::json!({"type":"input","sequence":sequence,"data":BASE64.encode(data)})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = second.next().await {
            match message.unwrap() {
                Message::Binary(value) => {
                    bytes.extend_from_slice(&value);
                    if bytes.windows(14).any(|value| value == b"TTY:alpha:beta") {
                        break;
                    }
                }
                Message::Text(value) if value.contains("\"type\":\"exit\"") => break,
                _ => {}
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("TTY:alpha:beta"));

    let waited = ok(&state, &socket, &["--json", "wait", "dev/reliable"]);
    assert!(waited.contains("\"exit_code\":0"));
    ok(&state, &socket, &["rm", "dev/reliable"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn acknowledged_tty_input_rejects_a_displaced_controller() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "takeover",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "IFS= read -r value; printf 'TTY:%s\\n' \"$value\"",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let mut displaced = client
        .attach("/v1/processes/dev/takeover/attach?takeover=true&replay_bytes=0&input_protocol=acked-v1&input_client_id=old")
        .await
        .unwrap();
    let mut controller = client
        .attach("/v1/processes/dev/takeover/attach?takeover=true&replay_bytes=0&input_protocol=acked-v1&input_client_id=new")
        .await
        .unwrap();
    displaced
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"lost\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let rejected = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = displaced.next().await {
            if let Message::Text(text) = message.unwrap()
                && text.contains("\"type\":\"input_rejected\"")
            {
                return text.to_string();
            }
        }
        String::new()
    })
    .await
    .unwrap();
    assert!(rejected.contains("\"code\":\"ownership_lost\""));

    controller
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"kept\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = controller.next().await {
            match message.unwrap() {
                Message::Binary(value) => {
                    bytes.extend_from_slice(&value);
                    if bytes.windows(8).any(|value| value == b"TTY:kept") {
                        break;
                    }
                }
                Message::Text(value) if value.contains("\"type\":\"exit\"") => break,
                _ => {}
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("TTY:kept"));

    let _ = ok(&state, &socket, &["--json", "wait", "dev/takeover"]);
    ok(&state, &socket, &["rm", "dev/takeover"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn preferred_control_recovers_same_window_and_keeps_other_windows_readonly() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "preferred",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "IFS= read -r value; printf 'TTY:%s\\n' \"$value\"",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let path_a = "/v1/processes/dev/preferred/attach?control=preferred&controller_client_id=window-a&replay_bytes=0&input_protocol=acked-v1&input_client_id=queue-a&output_protocol=sequenced-v1";
    let mut original = client.attach(path_a).await.unwrap();
    assert!(
        next_text(&mut original, "ownership_state")
            .await
            .contains("\"state\":\"controller\"")
    );

    let mut recovered = client.attach(path_a).await.unwrap();
    assert!(
        next_text(&mut recovered, "ownership_state")
            .await
            .contains("\"state\":\"controller\"")
    );
    let mut observer = client.attach("/v1/processes/dev/preferred/attach?control=preferred&controller_client_id=window-b&replay_bytes=0&input_protocol=acked-v1&input_client_id=queue-b&output_protocol=sequenced-v1").await.unwrap();
    assert!(
        next_text(&mut observer, "ownership_state")
            .await
            .contains("\"state\":\"readonly\"")
    );
    observer
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"ignored\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        next_text(&mut observer, "input_rejected")
            .await
            .contains("\"code\":\"read_only\"")
    );

    drop(recovered);
    tokio::time::sleep(Duration::from_millis(50)).await;
    observer.send(Message::Text(serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"still-ignored\n")}).to_string().into())).await.unwrap();
    assert!(
        next_text(&mut observer, "input_rejected")
            .await
            .contains("\"code\":\"read_only\"")
    );

    let mut final_controller = client.attach(path_a).await.unwrap();
    assert!(
        next_text(&mut final_controller, "ownership_state")
            .await
            .contains("\"state\":\"controller\"")
    );
    final_controller
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"kept\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        next_text(&mut final_controller, "input_ack")
            .await
            .contains("\"sequence\":1")
    );
    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = final_controller.next().await {
            if let Some((_, data)) = sequenced_output(message.unwrap()) {
                bytes.extend(data);
                if bytes.windows(8).any(|value| value == b"TTY:kept") {
                    break;
                }
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("TTY:kept"));

    let _ = ok(&state, &socket, &["--json", "wait", "dev/preferred"]);
    ok(&state, &socket, &["rm", "dev/preferred"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn sequenced_output_reconnect_resumes_strictly_after_cursor() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);
    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "cursor",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "printf FIRST; IFS= read -r value; printf SECOND",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let base = "/v1/processes/dev/cursor/attach?control=preferred&controller_client_id=window-a&replay_bytes=65536&input_protocol=acked-v1&input_client_id=queue-a&output_protocol=sequenced-v1";
    let mut first = client.attach(base).await.unwrap();
    let mut cursor = 0;
    let first_bytes = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = first.next().await {
            if let Some((sequence, data)) = sequenced_output(message.unwrap()) {
                assert!(sequence > cursor);
                cursor = sequence;
                bytes.extend(data);
                if bytes.windows(5).any(|value| value == b"FIRST") {
                    break;
                }
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&first_bytes).contains("FIRST"));
    drop(first);

    let path = format!("{base}&after_output_sequence={cursor}");
    let mut resumed = client.attach(&path).await.unwrap();
    assert!(
        next_text(&mut resumed, "ownership_state")
            .await
            .contains("\"state\":\"controller\"")
    );
    resumed
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"go\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let resumed_bytes = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = resumed.next().await {
            if let Some((sequence, data)) = sequenced_output(message.unwrap()) {
                assert!(
                    sequence > cursor,
                    "reconnect repeated output sequence {sequence}"
                );
                cursor = sequence;
                bytes.extend(data);
                if bytes.windows(6).any(|value| value == b"SECOND") {
                    break;
                }
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(!String::from_utf8_lossy(&resumed_bytes).contains("FIRST"));
    assert!(String::from_utf8_lossy(&resumed_bytes).contains("SECOND"));

    let _ = ok(&state, &socket, &["--json", "wait", "dev/cursor"]);
    ok(&state, &socket, &["rm", "dev/cursor"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn sequenced_output_reports_rotated_history_gap() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon_with_env(
        &state,
        &socket,
        &[
            ("AMUX_LOG_ROTATE_SIZE", "512"),
            ("AMUX_LOG_BACKUP_COUNT", "1"),
        ],
    );
    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "gap",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "i=0; while [ $i -lt 80 ]; do printf 'output-%04d-abcdefghijklmnopqrstuvwxyz\\n' $i; i=$((i+1)); sleep 0.01; done; IFS= read -r done",
        ],
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let mut terminal = client.attach("/v1/processes/dev/gap/attach?control=preferred&controller_client_id=window-a&replay_bytes=65536&input_protocol=acked-v1&input_client_id=queue-a&output_protocol=sequenced-v1&after_output_sequence=1").await.unwrap();
    let gap = next_text(&mut terminal, "output_gap").await;
    assert!(gap.contains("\"requested_after\":\"1\""));
    terminal
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":1,"data":BASE64.encode(b"done\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let _ = ok(&state, &socket, &["--json", "wait", "dev/gap"]);
    ok(&state, &socket, &["rm", "dev/gap"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn acknowledged_tty_input_preserves_a_rapid_key_burst() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);

    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "burst",
            "--tty",
            "--",
            "/bin/sh",
            "-c",
            "IFS= read -r value; printf 'COUNT:%s\\n' \"${#value}\"",
        ],
    );
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let mut terminal = client
        .attach("/v1/processes/dev/burst/attach?takeover=true&replay_bytes=0&input_protocol=acked-v1&input_client_id=burst")
        .await
        .unwrap();
    for sequence in 1..=500 {
        terminal
            .send(Message::Text(
                serde_json::json!({"type":"input","sequence":sequence,"data":BASE64.encode(b"x")})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    terminal
        .send(Message::Text(
            serde_json::json!({"type":"input","sequence":501,"data":BASE64.encode(b"\n")})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(message) = terminal.next().await {
            match message.unwrap() {
                Message::Binary(value) => {
                    bytes.extend_from_slice(&value);
                    if bytes.windows(9).any(|value| value == b"COUNT:500") {
                        break;
                    }
                }
                Message::Text(value) if value.contains("\"type\":\"exit\"") => break,
                _ => {}
            }
        }
        bytes
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("COUNT:500"));

    let _ = ok(&state, &socket, &["--json", "wait", "dev/burst"]);
    ok(&state, &socket, &["rm", "dev/burst"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn group_reuse_parent_links_and_process_events() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon(&state, &socket);
    ok(&state, &socket, &["workspace", "create", "dev"]);
    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    let snapshot: amux::model::ProcessSnapshot = serde_json::from_slice(
        &client
            .do_empty(http::Method::GET, "/v1/processes/snapshot")
            .await
            .unwrap(),
    )
    .unwrap();
    let mut events = client
        .stream(&format!(
            "/v1/processes/events?after={}&instance={}",
            snapshot.sequence, snapshot.daemon_instance_id
        ))
        .await
        .unwrap();
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "parent",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let parent: serde_json::Value =
        serde_json::from_str(&ok(&state, &socket, &["--json", "inspect", "dev/parent"])).unwrap();
    let parent_id = parent["process"]["id"].as_str().unwrap().to_owned();
    let child_output = Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_STATE_DIR", &state)
        .env("AMUX_SOCKET", &socket)
        .env("AMUX_PROCESS_ID", &parent_id)
        .env("AMUX_WORKSPACE", "dev")
        .args(["run", "--name", "child", "--", "/bin/sh", "-c", "sleep 30"])
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "{}",
        String::from_utf8_lossy(&child_output.stderr)
    );
    let child: serde_json::Value =
        serde_json::from_str(&ok(&state, &socket, &["--json", "inspect", "dev/child"])).unwrap();
    let child_id = child["process"]["id"].as_str().unwrap().to_owned();
    assert_eq!(child["process"]["parent_process_id"], parent_id);
    assert_eq!(child["process"]["group_id"], parent["process"]["group_id"]);
    assert!(child["process"]["runtime_handle"].is_null());

    let mut observed = Vec::new();
    let mut pending = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while observed.len() < 4 && tokio::time::Instant::now() < deadline {
        let Ok(Some(frame)) = tokio::time::timeout_at(deadline, events.frame()).await else {
            break;
        };
        let bytes = frame.unwrap().into_data().unwrap_or_default();
        pending.extend_from_slice(&bytes);
        while let Some(index) = pending.iter().position(|v| *v == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            if let Ok(event) = serde_json::from_slice::<amux::model::ProcessEvent>(&line) {
                observed.push(event);
            }
        }
    }
    assert!(
        observed
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert!(observed.iter().any(|event| event.process_id == parent_id
        && matches!(event.kind, amux::model::ProcessEventKind::ProcessCreated)));
    assert!(
        observed
            .iter()
            .any(|event| event.parent_process_id.as_deref() == Some(&parent_id))
    );

    ok(&state, &socket, &["stop", "--time", "100ms", "dev/parent"]);
    ok(&state, &socket, &["wait", "dev/parent"]);
    let still_running = ok(&state, &socket, &["--json", "inspect", "dev/child"]);
    assert!(still_running.contains("\"state\":\"running\""));
    ok(&state, &socket, &["stop", "--time", "100ms", "dev/child"]);
    ok(&state, &socket, &["wait", "dev/child"]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ![&parent_id, &child_id].iter().all(|id| {
        observed.iter().any(|event| {
            &event.process_id == *id
                && matches!(event.kind, amux::model::ProcessEventKind::ProcessExited)
        })
    }) && tokio::time::Instant::now() < deadline
    {
        let Ok(Some(frame)) = tokio::time::timeout_at(deadline, events.frame()).await else {
            break;
        };
        pending.extend_from_slice(&frame.unwrap().into_data().unwrap_or_default());
        while let Some(index) = pending.iter().position(|value| *value == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            if let Ok(event) = serde_json::from_slice::<amux::model::ProcessEvent>(&line) {
                observed.push(event);
            }
        }
    }
    assert!([&parent_id, &child_id].iter().all(|id| {
        observed.iter().any(|event| {
            &event.process_id == *id
                && matches!(event.kind, amux::model::ProcessEventKind::ProcessExited)
        })
    }));
    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[tokio::test]
async fn default_daemons_in_different_state_roots_are_isolated() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let first_root = temp.path().join("first");
    let second_root = temp.path().join("second");
    let first = Config::named(&first_root, "default").unwrap();
    let second = Config::named(&second_root, "default").unwrap();
    assert_ne!(first.socket, second.socket);
    let _first = OwnedDaemon(daemon_with_env(
        &first.state_dir,
        &first.socket,
        &[("AMUX_DAEMON", "default")],
    ));
    let _second = OwnedDaemon(daemon_with_env(
        &second.state_dir,
        &second.socket,
        &[("AMUX_DAEMON", "default")],
    ));
    for (root, workspace) in [(&first_root, "first-only"), (&second_root, "second-only")] {
        let output = named_amux(root, &["workspace", "create", workspace]);
        assert!(output.status.success(), "{output:?}");
        let listing = named_amux(root, &["--json", "workspace", "ls"]);
        assert!(listing.status.success(), "{listing:?}");
        let listing = String::from_utf8(listing.stdout).unwrap();
        assert!(listing.contains(workspace));
        assert!(!listing.contains(if workspace == "first-only" {
            "second-only"
        } else {
            "first-only"
        }));
    }
    let stopped = named_amux(&first_root, &["daemon", "stop"]);
    assert!(stopped.status.success(), "{stopped:?}");
    let removed = named_amux(&first_root, &["daemon", "rm", "default"]);
    assert!(removed.status.success(), "{removed:?}");
    assert!(!first.state_dir.exists());
    assert!(!first.socket.exists());
    let status = named_amux(&second_root, &["daemon", "status"]);
    assert!(status.status.success(), "{status:?}");
    let listing = named_amux(&second_root, &["--json", "workspace", "ls"]);
    assert!(listing.status.success(), "{listing:?}");
    assert!(
        String::from_utf8(listing.stdout)
            .unwrap()
            .contains("second-only")
    );
    let stopped = named_amux(&second_root, &["daemon", "stop"]);
    assert!(stopped.status.success(), "{stopped:?}");
}

#[tokio::test]
async fn socket_address_change_requires_stopping_the_existing_state_owner() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("state");
    let config = Config::named(&root, "default").unwrap();
    // Simulate an older daemon holding the same state at a different address.
    let old_socket = temp.path().join("old.sock");
    let mut old = OwnedDaemon(daemon_with_env(
        &config.state_dir,
        &old_socket,
        &[("AMUX_DAEMON", "default")],
    ));
    ok(
        &config.state_dir,
        &old_socket,
        &["workspace", "create", "retained"],
    );
    let attempt = named_amux(&root, &["workspace", "ls"]);
    assert!(!attempt.status.success(), "{attempt:?}");
    assert!(!config.socket.exists());
    assert!(old.0.try_wait().unwrap().is_none());
    assert!(
        ok(
            &config.state_dir,
            &old_socket,
            &["--json", "workspace", "ls"],
        )
        .contains("retained")
    );
    Client::new(Config {
        state_dir: config.state_dir.clone(),
        socket: old_socket,
    })
    .do_empty(http::Method::POST, "/v1/admin/stop")
    .await
    .unwrap();
    assert!(old.0.wait().unwrap().success());

    let _new = OwnedDaemon(daemon_with_env(
        &config.state_dir,
        &config.socket,
        &[("AMUX_DAEMON", "default")],
    ));
    let listing = named_amux(&root, &["--json", "workspace", "ls"]);
    assert!(listing.status.success(), "{listing:?}");
    assert!(
        String::from_utf8(listing.stdout)
            .unwrap()
            .contains("retained")
    );
    let stopped = named_amux(&root, &["daemon", "stop"]);
    assert!(stopped.status.success(), "{stopped:?}");
}

#[tokio::test]
async fn daemon_remove_is_locked_idempotent_and_isolated() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");

    for name in ["alpha", "beta"] {
        let output = named_amux(&state, &["--json", "daemon", "start", name]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let alpha = Config::named(&state, "alpha").unwrap();
    let beta = Config::named(&state, "beta").unwrap();
    assert!(alpha.state_dir.exists() && beta.state_dir.exists());

    let running = named_amux(&state, &["--json", "daemon", "rm", "alpha"]);
    assert!(!running.status.success());
    assert!(String::from_utf8_lossy(&running.stdout).contains("daemon_running"));

    assert!(
        named_amux(&state, &["daemon", "stop", "alpha"])
            .status
            .success()
    );
    let removed = named_amux(&state, &["--json", "daemon", "rm", "alpha"]);
    assert!(removed.status.success());
    assert!(String::from_utf8_lossy(&removed.stdout).contains("\"removed\":true"));
    assert!(!alpha.state_dir.exists());
    assert!(!alpha.socket.exists());
    assert!(beta.state_dir.exists());
    assert!(
        named_amux(&state, &["daemon", "status", "beta"])
            .status
            .success()
    );

    let repeated = named_amux(&state, &["--json", "daemon", "rm", "alpha"]);
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("\"removed\":false"));

    assert!(
        named_amux(&state, &["daemon", "stop", "beta"])
            .status
            .success()
    );
    assert!(
        named_amux(&state, &["daemon", "rm", "beta"])
            .status
            .success()
    );
}

#[tokio::test]
async fn output_rotation_preserves_generations_and_reports_gap() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon_with_env(
        &state,
        &socket,
        &[
            ("AMUX_LOG_ROTATE_SIZE", "512"),
            ("AMUX_LOG_BACKUP_COUNT", "2"),
        ],
    );
    ok(&state, &socket, &["workspace", "create", "dev"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "dev",
            "--name",
            "rotating",
            "--",
            "/bin/sh",
            "-c",
            "i=0; while [ $i -lt 30 ]; do printf 'abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789'; sleep 0.02; i=$((i+1)); done; printf '\\377'",
        ],
    );
    let waited = ok(&state, &socket, &["--json", "wait", "dev/rotating"]);
    let value: serde_json::Value = serde_json::from_str(&waited).unwrap();
    let process_dir = state
        .join("workspaces")
        .join(value["process"]["workspace_id"].as_str().unwrap())
        .join("processes")
        .join(value["process"]["id"].as_str().unwrap());
    let output_dir = process_dir.join("output");
    let mut generations = std::fs::read_dir(&output_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    generations.sort();
    assert_eq!(
        generations
            .iter()
            .filter(|path| path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".bak"))
            .count(),
        2
    );
    let mut sequences = Vec::new();
    for generation in &generations {
        let index = std::fs::read_to_string(generation.join("events.jsonl")).unwrap();
        assert!(generation.join("stdout.log").exists());
        for line in index.lines() {
            let event: amux::model::OutputEvent = serde_json::from_str(line).unwrap();
            sequences.push(event.sequence);
        }
    }
    sequences.sort_unstable();
    assert!(sequences.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert!(sequences[0] > 1, "old generations should have been pruned");

    let json_logs = amux(&state, &socket, &["logs", "dev/rotating", "--jsonl"]);
    assert!(json_logs.status.success());
    let stdout = String::from_utf8_lossy(&json_logs.stdout);
    assert!(stdout.lines().next().unwrap().contains("\"type\":\"gap\""));
    assert!(stdout.contains("\"encoding\":\"base64\""));
    let plain_logs = amux(&state, &socket, &["logs", "dev/rotating"]);
    assert!(plain_logs.status.success());
    assert!(String::from_utf8_lossy(&plain_logs.stderr).contains("log gap"));

    daemon_process.kill().unwrap();
    let _ = daemon_process.wait();
}

#[test]
fn invalid_log_configuration_rejects_daemon_and_shim_startup() {
    let temp = tempfile::tempdir().unwrap();
    let daemon_output = Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_STATE_DIR", temp.path().join("daemon-state"))
        .env("AMUX_SOCKET", temp.path().join("daemon.sock"))
        .env("AMUX_LOG_ROTATE_SIZE", "0")
        .args(["daemon", "serve"])
        .output()
        .unwrap();
    assert!(!daemon_output.status.success());
    assert!(String::from_utf8_lossy(&daemon_output.stderr).contains("AMUX_LOG_ROTATE_SIZE"));

    let shim_output = Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_LOG_BACKUP_COUNT", "17")
        .args([
            "daemon",
            "shim",
            "--workspace-dir",
            temp.path().to_str().unwrap(),
            "--socket",
            temp.path().join("shim.sock").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!shim_output.status.success());
    assert!(String::from_utf8_lossy(&shim_output.stderr).contains("AMUX_LOG_BACKUP_COUNT"));
}

#[tokio::test]
async fn tracing_diagnostics_rotate_without_copying_process_output() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut daemon_process = daemon_with_env(
        &state,
        &socket,
        &[
            ("AMUX_LOG", "amux=debug"),
            ("AMUX_LOG_ROTATE_SIZE", "384"),
            ("AMUX_LOG_BACKUP_COUNT", "2"),
        ],
    );
    ok(&state, &socket, &["workspace", "create", "diag"]);
    ok(
        &state,
        &socket,
        &[
            "run",
            "--workspace",
            "diag",
            "--name",
            "keeper",
            "--",
            "/bin/sh",
            "-c",
            "sleep 10",
        ],
    );
    for index in 0..5 {
        let name = format!("short-{index}");
        ok(
            &state,
            &socket,
            &[
                "run",
                "--workspace",
                "diag",
                "--name",
                &name,
                "--",
                "/bin/sh",
                "-c",
                "printf 'PROCESS_OUTPUT_SECRET'",
            ],
        );
        ok(&state, &socket, &["wait", &format!("diag/{name}")]);
    }
    let inspect = ok(&state, &socket, &["--json", "inspect", "diag/keeper"]);
    let value: serde_json::Value = serde_json::from_str(&inspect).unwrap();
    let workspace_id = value["process"]["workspace_id"].as_str().unwrap();
    let group_id = value["process"]["group_id"].as_str().unwrap();
    ok(&state, &socket, &["stop", "--time", "100ms", "diag/keeper"]);
    ok(&state, &socket, &["wait", "diag/keeper"]);
    let shim_socket = Path::new("/tmp")
        .join(format!("amux-{}", unsafe { libc::getuid() }))
        .join("shims")
        .join(format!("{group_id}.sock"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while shim_socket.exists() {
        assert!(Instant::now() < deadline, "group shim did not stop");
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(100));

    let client = Client::new(Config {
        state_dir: state.clone(),
        socket: socket.clone(),
    });
    client
        .do_empty(http::Method::POST, "/v1/admin/stop")
        .await
        .unwrap();
    assert!(daemon_process.wait().unwrap().success());

    let daemon_logs = read_rotating_logs(&state, "amuxd.log");
    let workspace_dir = state.join("workspaces").join(workspace_id);
    let shim_logs = read_rotating_logs(&workspace_dir, "shim.log");
    assert!(daemon_logs.contains("process lifecycle"));
    assert!(shim_logs.contains("process exited") || shim_logs.contains("process started"));
    assert!(!daemon_logs.contains("PROCESS_OUTPUT_SECRET"));
    assert!(!shim_logs.contains("PROCESS_OUTPUT_SECRET"));
    let daemon_backups = rotating_backup_count(&state, "amuxd.log");
    let shim_backups = rotating_backup_count(&workspace_dir, "shim.log");
    assert!(daemon_backups <= 2, "daemon backups: {daemon_backups}");
    assert!(shim_backups <= 2, "shim backups: {shim_backups}");
    assert!(daemon_backups > 0 || shim_backups > 0);
}

fn read_rotating_logs(dir: &Path, active: &str) -> String {
    let prefix = format!("{active}.");
    let mut paths = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name == active || (name.starts_with(&prefix) && name.ends_with(".bak"))
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .collect::<Vec<_>>()
        .join("\n")
}

fn rotating_backup_count(dir: &Path, active: &str) -> usize {
    let prefix = format!("{active}.");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(&prefix) && name.ends_with(".bak")
        })
        .count()
}
