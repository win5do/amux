#![cfg(unix)]

use amux::{client::Client, config::Config};
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

fn daemon(state: &Path, socket: &Path) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_amux"))
        .env("AMUX_STATE_DIR", state)
        .env("AMUX_SOCKET", socket)
        .args(["daemon", "serve"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::os::unix::net::UnixStream::connect(socket).is_err() {
        assert!(Instant::now() < deadline, "daemon socket was not ready");
        thread::sleep(Duration::from_millis(20));
    }
    child
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

#[tokio::test]
async fn pipe_lifecycle_and_daemon_recovery() {
    let _guard = E2E_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let socket = temp.path().join("amux.sock");
    let mut first = daemon(&state, &socket);

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
    first.kill().unwrap();
    first.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while unsafe { libc::kill(pid as i32, 0) } == 0 {
        assert!(
            Instant::now() < deadline,
            "business process survived daemon death"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let mut second = daemon(&state, &socket);
    let after = ok(&state, &socket, &["--json", "inspect", "dev/survivor"]);
    assert!(after.contains("\"state\":\"exited\"") || after.contains("\"state\":\"failed\""));
    ok(&state, &socket, &["rm", "dev/survivor"]);
    ok(&state, &socket, &["rm", "dev/io"]);
    ok(&state, &socket, &["workspace", "rm", "dev"]);
    second.kill().unwrap();
    let _ = second.wait();
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
