use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    fs,
    future::IntoFuture,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path as AxumPath, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde::Deserialize;
use serde_json::json;
use tokio::{net::UnixListener, sync::broadcast};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    logging::{LogConfig, install_process_log},
    model::{IoMode, Process, ProcessState, SignalRequest, StopRequest},
    output::{OutputLog, OutputState, RecordReader},
    store::{atomic_json, read_json},
};

const REPLAY_LIMIT: usize = 4 * 1024 * 1024;

struct Inner {
    process: Process,
    input: Option<Box<dyn Write + Send>>,
    pty: Option<Box<dyn MasterPty + Send>>,
    output: OutputState,
    replay: VecDeque<u8>,
    controller: Option<u64>,
    next_attach: u64,
}

#[derive(Clone)]
enum WireMessage {
    Binary(Vec<u8>),
    Text(String),
}

#[derive(Clone)]
pub struct Shim {
    dir: PathBuf,
    socket: PathBuf,
    output_log: OutputLog,
    inner: Arc<Mutex<Inner>>,
    output: broadcast::Sender<WireMessage>,
    lifecycle: broadcast::Sender<Process>,
    readers: Arc<(Mutex<usize>, Condvar)>,
    shutdown: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
pub struct GroupShim {
    workspace_dir: PathBuf,
    socket: PathBuf,
    processes: Arc<Mutex<BTreeMap<String, Shim>>>,
    lifecycle: broadcast::Sender<Process>,
    shutdown: Arc<tokio::sync::Notify>,
    log_config: LogConfig,
}

#[derive(serde::Deserialize)]
struct StartProcessRequest {
    process_dir: PathBuf,
}

impl GroupShim {
    pub fn load(workspace_dir: impl Into<PathBuf>, socket: impl Into<PathBuf>) -> Result<Self> {
        let log_config = LogConfig::from_env()?;
        let (lifecycle, _) = broadcast::channel(256);
        Ok(Self {
            workspace_dir: workspace_dir.into(),
            socket: socket.into(),
            processes: Arc::new(Mutex::new(BTreeMap::new())),
            lifecycle,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            log_config,
        })
    }

    pub async fn serve(self, ready_fd: Option<i32>, lifecycle_fd: Option<i32>) -> Result<()> {
        if lifecycle_fd.is_some() {
            install_process_log(self.workspace_dir.join("shim.log"), self.log_config)?;
        }
        if let Some(parent) = self.socket.parent() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let _ = fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        fs::set_permissions(&self.socket, fs::Permissions::from_mode(0o600))?;
        if let Some(fd) = lifecycle_fd {
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
            let group = self.clone();
            std::thread::spawn(move || {
                let mut byte = [0u8; 1];
                let _ = unsafe { libc::read(fd, byte.as_mut_ptr().cast(), 1) };
                unsafe {
                    libc::close(fd);
                }
                group.terminate_all();
                group.shutdown.notify_waiters();
            });
        }
        if let Some(fd) = ready_fd {
            write_ready(fd);
        }
        let idle_group = self.clone();
        let mut lifecycle = self.lifecycle.subscribe();
        tokio::spawn(async move {
            loop {
                match lifecycle.recv().await {
                    Ok(process) => {
                        if process.state.terminal() {
                            let mut processes = idle_group.processes.lock().unwrap();
                            processes.remove(&process.id);
                            if processes.is_empty() {
                                drop(processes);
                                idle_group.shutdown.notify_waiters();
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let mut processes = idle_group.processes.lock().unwrap();
                        processes
                            .retain(|_, shim| !shim.inner.lock().unwrap().process.state.terminal());
                        if processes.is_empty() {
                            drop(processes);
                            idle_group.shutdown.notify_waiters();
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        let app = Router::new()
            .route("/v1/processes", post(group_start))
            .route("/v1/events", get(group_events))
            .route("/v1/processes/{id}/status", get(group_status))
            .route("/v1/processes/{id}/signal", post(group_signal))
            .route("/v1/processes/{id}/stop", post(group_stop))
            .route("/v1/processes/{id}/logs", get(group_logs))
            .route("/v1/processes/{id}/attach", any(group_attach))
            .route("/v1/shutdown", post(group_shutdown))
            .with_state(self.clone());
        let stopping = self.shutdown.clone();
        let server = axum::serve(listener, app).into_future();
        tokio::pin!(server);
        let result = tokio::select! {
            value = &mut server => value,
            _ = stopping.notified() => Ok(()),
        };
        let _ = fs::remove_file(&self.socket);
        result?;
        Ok(())
    }

    fn get(&self, id: &str) -> Option<Shim> {
        self.processes.lock().unwrap().get(id).cloned()
    }

    fn terminate_all(&self) {
        let shims = self
            .processes
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shim in &shims {
            let pgid = shim.inner.lock().unwrap().process.process_group_id;
            if pgid > 0 && !shim.inner.lock().unwrap().process.state.terminal() {
                let _ = killpg(Pid::from_raw(pgid), Signal::SIGTERM);
            }
        }
        std::thread::sleep(Duration::from_secs(10));
        for shim in shims {
            let inner = shim.inner.lock().unwrap();
            if inner.process.process_group_id > 0 && !inner.process.state.terminal() {
                let _ = killpg(
                    Pid::from_raw(inner.process.process_group_id),
                    Signal::SIGKILL,
                );
            }
        }
    }
}

async fn group_start(
    State(group): State<GroupShim>,
    Json(request): Json<StartProcessRequest>,
) -> Response {
    if !request.process_dir.starts_with(&group.workspace_dir) {
        return (StatusCode::BAD_REQUEST, "invalid process directory").into_response();
    }
    let shim = match Shim::load(
        &request.process_dir,
        group.socket.with_extension("unused"),
        group.lifecycle.clone(),
        group.log_config,
    ) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let id = shim.inner.lock().unwrap().process.id.clone();
    {
        let mut processes = group.processes.lock().unwrap();
        if let Some(existing) = processes.get(&id)
            && !existing.inner.lock().unwrap().process.state.terminal()
        {
            return Json(json!({"ok":true})).into_response();
        }
        processes.remove(&id);
    }
    group
        .processes
        .lock()
        .unwrap()
        .insert(id.clone(), shim.clone());
    if let Err(error) = shim.start_process() {
        shim.fail_wait(&error);
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }
    Json(json!({"ok":true})).into_response()
}

async fn group_events(State(group): State<GroupShim>) -> Response {
    let receiver = group.lifecycle.subscribe();
    let initial = group
        .processes
        .lock()
        .unwrap()
        .values()
        .filter_map(|shim| {
            let process = shim.inner.lock().unwrap().process.clone();
            serde_json::to_vec(&process).ok().map(|mut line| {
                line.push(b'\n');
                Ok::<_, Infallible>(Bytes::from(line))
            })
        })
        .collect::<Vec<_>>();
    let follow = tokio_stream::StreamExt::filter_map(BroadcastStream::new(receiver), |item| {
        let process = item.ok()?;
        let mut line = serde_json::to_vec(&process).ok()?;
        line.push(b'\n');
        Some(Ok::<_, Infallible>(Bytes::from(line)))
    });
    Body::from_stream(tokio_stream::StreamExt::chain(
        tokio_stream::iter(initial),
        follow,
    ))
    .into_response()
}

async fn group_status(State(group): State<GroupShim>, AxumPath(id): AxumPath<String>) -> Response {
    match group.get(&id) {
        Some(shim) => status(State(shim)).await.into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn group_signal(
    State(group): State<GroupShim>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<SignalRequest>,
) -> Response {
    match group.get(&id) {
        Some(shim) => signal(State(shim), Json(req)).await,
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn group_stop(
    State(group): State<GroupShim>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<StopRequest>,
) -> Response {
    match group.get(&id) {
        Some(shim) => stop(State(shim), Json(req)).await,
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn group_logs(
    State(group): State<GroupShim>,
    AxumPath(id): AxumPath<String>,
    query: Query<LogsQuery>,
) -> Response {
    match group.get(&id) {
        Some(shim) => logs(State(shim), query).await,
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn group_attach(
    State(group): State<GroupShim>,
    AxumPath(id): AxumPath<String>,
    query: Query<AttachQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    match group.get(&id) {
        Some(shim) => attach(State(shim), query, ws).await,
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn group_shutdown(State(group): State<GroupShim>) -> Response {
    if group
        .processes
        .lock()
        .unwrap()
        .values()
        .any(|s| !s.inner.lock().unwrap().process.state.terminal())
    {
        return (StatusCode::CONFLICT, "group still has running processes").into_response();
    }
    group.shutdown.notify_waiters();
    Json(json!({"ok":true})).into_response()
}

impl Shim {
    pub fn load(
        dir: impl Into<PathBuf>,
        socket: impl Into<PathBuf>,
        lifecycle: broadcast::Sender<Process>,
        log_config: LogConfig,
    ) -> Result<Self> {
        let dir = dir.into();
        let socket = socket.into();
        let process: Process = read_json(dir.join("metadata.json"))?;
        let (output_log, output_state) = OutputLog::open(&dir, log_config)?;
        let replay = output_log.replay_tail("tty", REPLAY_LIMIT)?;
        let (output, _) = broadcast::channel(256);
        Ok(Self {
            dir,
            socket,
            output_log,
            inner: Arc::new(Mutex::new(Inner {
                process,
                input: None,
                pty: None,
                output: output_state,
                replay,
                controller: None,
                next_attach: 0,
            })),
            output,
            lifecycle,
            readers: Arc::new((Mutex::new(0), Condvar::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn publish_lifecycle(&self, process: Process) {
        let _ = self.lifecycle.send(process);
    }

    pub async fn serve(self, ready_fd: Option<i32>) -> Result<()> {
        if let Some(parent) = self.socket.parent() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let _ = fs::remove_file(&self.socket);
        if let Err(error) = self.start_process() {
            let mut inner = self.inner.lock().unwrap();
            inner.process.state = ProcessState::Failed;
            inner.process.error = error.to_string();
            inner.process.finished_at = Some(Utc::now());
            let _ = atomic_json(&self.dir.join("metadata.json"), &inner.process);
            return Err(error);
        }
        let listener = UnixListener::bind(&self.socket)?;
        fs::set_permissions(&self.socket, fs::Permissions::from_mode(0o600))?;
        if let Some(fd) = ready_fd {
            write_ready(fd);
        }

        let app = Router::new()
            .route("/v1/status", get(status))
            .route("/v1/signal", post(signal))
            .route("/v1/stop", post(stop))
            .route("/v1/logs", get(logs))
            .route("/v1/attach", any(attach))
            .route("/v1/shutdown", post(shutdown))
            .with_state(self.clone());
        let stopping = self.shutdown.clone();
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(async move { stopping.notified().await })
            .await;
        let _ = fs::remove_file(&self.socket);
        result?;
        Ok(())
    }

    fn start_process(&self) -> Result<()> {
        let (process, env): (Process, BTreeMap<String, String>) = {
            let inner = self.inner.lock().unwrap();
            let env = read_json(self.dir.join("runtime-env.json"))
                .unwrap_or_else(|_| inner.process.env.clone());
            (inner.process.clone(), env)
        };
        if process.command.is_empty() {
            bail!("empty command")
        }
        match process.io_mode {
            IoMode::Pipe => self.start_pipe(process, env),
            IoMode::Tty => self.start_tty(process, env),
        }
    }

    fn start_pipe(&self, process: Process, env: BTreeMap<String, String>) -> Result<()> {
        let mut command = Command::new(&process.command[0]);
        command
            .args(&process.command[1..])
            .env_clear()
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !process.cwd.is_empty() {
            command.current_dir(&process.cwd);
        }
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().context("spawn process")?;
        let pid = child.id() as i32;
        let stdin = child
            .stdin
            .take()
            .map(|v| Box::new(v) as Box<dyn Write + Send>);
        if let Some(stdout) = child.stdout.take() {
            self.spawn_reader("stdout", stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.spawn_reader("stderr", stderr);
        }
        let running = {
            let mut inner = self.inner.lock().unwrap();
            inner.input = stdin;
            inner.process.pid = pid;
            inner.process.process_group_id = pid;
            inner.process.state = ProcessState::Running;
            inner.process.started_at = Some(Utc::now());
            atomic_json(&self.dir.join("metadata.json"), &inner.process)?;
            inner.process.clone()
        };
        self.publish_lifecycle(running);
        self.spawn_wait_pipe(child);
        Ok(())
    }

    fn start_tty(&self, process: Process, env: BTreeMap<String, String>) -> Result<()> {
        let pair = native_pty_system().openpty(PtySize {
            rows: process.initial_rows,
            cols: process.initial_cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        let mut command = CommandBuilder::new(&process.command[0]);
        command.args(&process.command[1..]);
        if !process.cwd.is_empty() {
            command.cwd(&process.cwd);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        let child = pair.slave.spawn_command(command)?;
        let pid = child.process_id().unwrap_or(0) as i32;
        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;
        let running = {
            let mut inner = self.inner.lock().unwrap();
            inner.input = Some(writer);
            inner.pty = Some(pair.master);
            inner.process.pid = pid;
            inner.process.process_group_id = pid;
            inner.process.state = ProcessState::Running;
            inner.process.started_at = Some(Utc::now());
            atomic_json(&self.dir.join("metadata.json"), &inner.process)?;
            inner.process.clone()
        };
        self.publish_lifecycle(running);
        self.spawn_reader("tty", reader);
        self.spawn_wait_pty(child);
        Ok(())
    }

    fn spawn_reader(&self, stream: &'static str, mut reader: impl Read + Send + 'static) {
        let shim = self.clone();
        *shim.readers.0.lock().unwrap() += 1;
        std::thread::spawn(move || {
            let mut buffer = [0u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => shim.emit(stream, &buffer[..count]),
                }
            }
            let mut count = shim.readers.0.lock().unwrap();
            *count -= 1;
            shim.readers.1.notify_all();
        });
    }

    fn emit(&self, stream: &str, bytes: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        let execution = inner.process.execution;
        if self
            .output_log
            .append(&mut inner.output, stream, bytes, execution)
            .is_err()
        {
            return;
        }
        inner.replay.extend(bytes);
        while inner.replay.len() > REPLAY_LIMIT {
            inner.replay.pop_front();
        }
        drop(inner);
        let _ = self.output.send(WireMessage::Binary(bytes.to_vec()));
    }

    fn spawn_wait_pipe(&self, mut child: Child) {
        let shim = self.clone();
        std::thread::spawn(move || match child.wait() {
            Ok(status) => shim.finish(
                status.code(),
                std::os::unix::process::ExitStatusExt::signal(&status),
            ),
            Err(error) => shim.fail_wait(error),
        });
    }

    fn spawn_wait_pty(&self, mut child: Box<dyn portable_pty::Child + Send + Sync>) {
        let shim = self.clone();
        std::thread::spawn(move || match child.wait() {
            Ok(status) => shim.finish(Some(status.exit_code() as i32), None),
            Err(error) => shim.fail_wait(error),
        });
    }

    fn finish(&self, code: Option<i32>, signal: Option<i32>) {
        let (lock, ready) = &*self.readers;
        let mut count = lock.lock().unwrap();
        while *count != 0 {
            count = ready.wait(count).unwrap();
        }
        drop(count);
        let (process, message) = {
            let mut inner = self.inner.lock().unwrap();
            inner.process.exit_code = Some(code.unwrap_or(-1));
            inner.process.exit_signal = signal.and_then(signal_name).unwrap_or_default();
            inner.process.state = ProcessState::Exited;
            inner.process.finished_at = Some(Utc::now());
            inner.input = None;
            let _ = atomic_json(&self.dir.join("metadata.json"), &inner.process);
            let message = serde_json::to_string(&json!({"type":"exit","exit_code":inner.process.exit_code,"signal":inner.process.exit_signal})).unwrap_or_default();
            (inner.process.clone(), message)
        };
        self.publish_lifecycle(process);
        let _ = self.output.send(WireMessage::Text(message));
    }

    fn fail_wait(&self, error: impl std::fmt::Display) {
        let process = {
            let mut inner = self.inner.lock().unwrap();
            inner.process.state = ProcessState::Failed;
            inner.process.error = error.to_string();
            inner.process.finished_at = Some(Utc::now());
            let _ = atomic_json(&self.dir.join("metadata.json"), &inner.process);
            inner.process.clone()
        };
        self.publish_lifecycle(process);
        let _ = self.output.send(WireMessage::Text(
            serde_json::to_string(&json!({"type":"exit","error":"process wait failed"}))
                .unwrap_or_default(),
        ));
    }
}

async fn status(State(shim): State<Shim>) -> Json<Process> {
    Json(shim.inner.lock().unwrap().process.clone())
}

async fn signal(State(shim): State<Shim>, Json(request): Json<SignalRequest>) -> Response {
    let pgid = shim.inner.lock().unwrap().process.process_group_id;
    match parse_signal(&request.signal)
        .and_then(|sig| killpg(Pid::from_raw(pgid), sig).map_err(anyhow::Error::from))
    {
        Ok(()) => Json(json!({"ok":true})).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn stop(State(shim): State<Shim>, Json(request): Json<StopRequest>) -> Response {
    let (pgid, process) = {
        let mut inner = shim.inner.lock().unwrap();
        if inner.process.state.terminal() {
            return Json(json!({"ok":true})).into_response();
        }
        inner.process.state = ProcessState::Stopping;
        let _ = atomic_json(&shim.dir.join("metadata.json"), &inner.process);
        (inner.process.process_group_id, inner.process.clone())
    };
    shim.publish_lifecycle(process);
    let _ = killpg(Pid::from_raw(pgid), Signal::SIGTERM);
    let delayed = shim.clone();
    tokio::spawn(async move {
        let grace = if request.grace_millis <= 0 {
            10_000
        } else {
            request.grace_millis
        };
        tokio::time::sleep(Duration::from_millis(grace as u64)).await;
        if !delayed.inner.lock().unwrap().process.state.terminal() {
            let _ = killpg(Pid::from_raw(pgid), Signal::SIGKILL);
        }
    });
    Json(json!({"ok":true})).into_response()
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default)]
    after: u64,
    #[serde(default)]
    follow: bool,
}

async fn logs(State(shim): State<Shim>, Query(query): Query<LogsQuery>) -> Response {
    if !query.follow {
        let boundary = shim.inner.lock().unwrap().output.sequence;
        let dir = shim.dir.clone();
        let stream = async_stream::stream! {
            if let Ok(records) = RecordReader::open(&dir, query.after, Some(boundary)) {
                for record in records.flatten() {
                    if let Ok(line) = record.to_json_line() {
                        yield Ok::<_, Infallible>(Bytes::from(line));
                    }
                }
            }
        };
        return Body::from_stream(stream).into_response();
    }
    let mut receiver = shim.output.subscribe();
    let boundary = shim.inner.lock().unwrap().output.sequence;
    let stream = async_stream::stream! {
        if let Ok(records) = RecordReader::open(&shim.dir, query.after, Some(boundary)) {
            for record in records.flatten() {
                if let Ok(line) = record.to_json_line() {
                    yield Ok::<_, Infallible>(Bytes::from(line));
                }
            }
        }
        let mut cursor = boundary.max(query.after);
        loop {
            match receiver.recv().await {
                Ok(WireMessage::Text(text)) => {
                    if serde_json::from_str::<serde_json::Value>(&text)
                        .ok()
                        .and_then(|value| value.get("type").and_then(|kind| kind.as_str()).map(str::to_owned))
                        .as_deref() == Some("exit")
                    {
                        let latest = shim.inner.lock().unwrap().output.sequence;
                        if let Ok(records) = RecordReader::open(&shim.dir, cursor, Some(latest)) {
                            for record in records.flatten() {
                                if let Ok(line) = record.to_json_line() {
                                    yield Ok::<_, Infallible>(Bytes::from(line));
                                }
                            }
                        }
                        break;
                    }
                },
                Ok(WireMessage::Binary(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    let latest = shim.inner.lock().unwrap().output.sequence;
                    if let Ok(records) = RecordReader::open(&shim.dir, cursor, Some(latest)) {
                        for record in records.flatten() {
                            if let Ok(line) = record.to_json_line() {
                                yield Ok::<_, Infallible>(Bytes::from(line));
                            }
                        }
                    }
                    cursor = latest;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Body::from_stream(stream).into_response()
}

#[derive(Deserialize)]
struct AttachQuery {
    #[serde(default)]
    readonly: bool,
    #[serde(default)]
    takeover: bool,
    #[serde(default = "default_replay")]
    replay_bytes: usize,
}
fn default_replay() -> usize {
    8192
}

async fn attach(
    State(shim): State<Shim>,
    Query(query): Query<AttachQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    if shim.inner.lock().unwrap().process.io_mode != IoMode::Tty {
        return (StatusCode::CONFLICT, "process has no TTY").into_response();
    }
    let id = {
        let mut inner = shim.inner.lock().unwrap();
        inner.next_attach += 1;
        let id = inner.next_attach;
        if !query.readonly {
            if inner.controller.is_some() && !query.takeover {
                return (StatusCode::CONFLICT, "controller already exists").into_response();
            }
            inner.controller = Some(id);
        }
        id
    };
    ws.on_upgrade(move |socket| attach_socket(shim, socket, id, query))
}

async fn attach_socket(shim: Shim, socket: WebSocket, id: u64, query: AttachQuery) {
    let (mut sink, mut source) = socket.split();
    let replay = {
        let inner = shim.inner.lock().unwrap();
        inner
            .replay
            .iter()
            .skip(inner.replay.len().saturating_sub(query.replay_bytes))
            .copied()
            .collect::<Vec<_>>()
    };
    if !replay.is_empty() && sink.send(Message::Binary(replay.into())).await.is_err() {
        release(&shim, id);
        return;
    }
    let mut output = shim.output.subscribe();
    if !query.readonly {
        let notice = serde_json::to_string(
            &json!({"type":"ownership_changed","controller":id,"takeover":query.takeover}),
        )
        .unwrap();
        let _ = shim.output.send(WireMessage::Text(notice));
    }
    let terminal = {
        let inner = shim.inner.lock().unwrap();
        if inner.process.state.terminal() {
            Some(serde_json::to_string(&json!({"type":"exit","exit_code":inner.process.exit_code.unwrap_or(0),"signal":inner.process.exit_signal})).unwrap())
        } else {
            None
        }
    };
    if let Some(message) = terminal
        && sink.send(Message::Text(message.into())).await.is_err()
    {
        release(&shim, id);
        return;
    }
    loop {
        tokio::select! {
            received = output.recv() => match received {
                Ok(WireMessage::Binary(data)) => if sink.send(Message::Binary(data.into())).await.is_err() { break; },
                Ok(WireMessage::Text(text)) => if sink.send(Message::Text(text.into())).await.is_err() { break; },
                Err(_) => break,
            },
            message = futures_util::StreamExt::next(&mut source) => match message {
                Some(Ok(Message::Binary(data))) if !query.readonly => {
                    let failed = {
                        let mut inner = shim.inner.lock().unwrap();
                        inner.controller == Some(id)
                            && inner.input.as_mut().is_some_and(|input| {
                                input.write_all(&data).and_then(|_| input.flush()).is_err()
                            })
                    };
                    if failed {
                        let message = serde_json::to_string(&json!({"type":"input_error","code":"pty_write_failed"})).unwrap_or_default();
                        let _ = sink.send(Message::Text(message.into())).await;
                        break;
                    }
                }
                Some(Ok(Message::Text(text))) if !query.readonly => handle_control(&shim, id, &text),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    }
    release(&shim, id);
}

fn handle_control(shim: &Shim, id: u64, text: &str) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    let inner = shim.inner.lock().unwrap();
    if inner.controller != Some(id) {
        return;
    }
    match value.get("type").and_then(|v| v.as_str()) {
        Some("resize") => {
            let rows = value.get("rows").and_then(|v| v.as_u64()).unwrap_or(40) as u16;
            let cols = value.get("cols").and_then(|v| v.as_u64()).unwrap_or(120) as u16;
            if let Some(pty) = inner.pty.as_ref() {
                let _ = pty.resize(PtySize {
                    rows,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                });
            }
        }
        Some("signal") => {
            if let Some(name) = value.get("signal").and_then(|v| v.as_str())
                && let Ok(sig) = parse_signal(name)
            {
                let _ = killpg(Pid::from_raw(inner.process.process_group_id), sig);
            }
        }
        _ => {}
    }
}

fn release(shim: &Shim, id: u64) {
    let mut inner = shim.inner.lock().unwrap();
    if inner.controller == Some(id) {
        inner.controller = None;
    }
}

async fn shutdown(State(shim): State<Shim>) -> Response {
    if !shim.inner.lock().unwrap().process.state.terminal() {
        return (StatusCode::CONFLICT, "process still running").into_response();
    }
    shim.shutdown.notify_one();
    Json(json!({"ok":true})).into_response()
}

fn parse_signal(value: &str) -> Result<Signal> {
    match value
        .trim_start_matches("SIG")
        .to_ascii_uppercase()
        .as_str()
    {
        "INT" => Ok(Signal::SIGINT),
        "TERM" => Ok(Signal::SIGTERM),
        "KILL" => Ok(Signal::SIGKILL),
        "HUP" => Ok(Signal::SIGHUP),
        "QUIT" => Ok(Signal::SIGQUIT),
        "USR1" => Ok(Signal::SIGUSR1),
        "USR2" => Ok(Signal::SIGUSR2),
        "STOP" => Ok(Signal::SIGSTOP),
        "CONT" => Ok(Signal::SIGCONT),
        other => bail!("unsupported signal {other}"),
    }
}

fn signal_name(value: i32) -> Option<String> {
    Signal::try_from(value)
        .ok()
        .map(|s| format!("{s:?}").trim_start_matches("SIG").to_owned())
}

fn write_ready(fd: i32) {
    unsafe {
        let byte = [1u8];
        libc::write(fd, byte.as_ptr().cast(), 1);
        libc::close(fd);
    }
}
