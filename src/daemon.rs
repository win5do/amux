use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    future::IntoFuture,
    os::{
        fd::{FromRawFd, RawFd},
        unix::{
            fs::{OpenOptionsExt, PermissionsExt},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{
        FromRequestParts, Path as AxumPath, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
    routing::{any, get, post},
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use serde_json::json;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::{client_async, tungstenite};
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::{
    config::{Config, boot_id},
    logging::{LogConfig, install_process_log},
    model::*,
    observability::{self, ProcessRole},
    output::RecordReader,
    protocol::Response,
    store::Store,
    unix_http,
};

#[derive(Clone)]
pub struct Daemon {
    config: Config,
    store: Arc<Store>,
    shim_path: Arc<PathBuf>,
    shim_args: Arc<Vec<String>>,
    embedded_shims: bool,
    instance_id: Arc<String>,
    events: Arc<Mutex<EventHub>>,
    event_tx: broadcast::Sender<ProcessEvent>,
    shutdown: Arc<Notify>,
    group_lifelines: Arc<Mutex<BTreeMap<String, fs::File>>>,
    group_event_bridges: Arc<Mutex<HashSet<String>>>,
    last_request: Arc<AtomicU64>,
    log_config: LogConfig,
}

const EVENT_BUFFER_LIMIT: usize = 4096;

struct EventHub {
    sequence: u64,
    buffer: VecDeque<ProcessEvent>,
    fingerprints: BTreeMap<String, String>,
}

impl Daemon {
    pub fn new(config: Config) -> Result<Self> {
        let executable = std::env::current_exe().context("locate amux executable")?;
        Self::with_shim_command(config, executable, vec!["daemon".into(), "shim".into()])
    }

    /// Creates a daemon that launches shims through an arbitrary executable.
    ///
    /// Embedders can point this at their own executable and use `shim_args` to
    /// select a hidden shim entry point, avoiding a separately bundled binary.
    pub fn with_shim_command(
        config: Config,
        shim_path: PathBuf,
        shim_args: Vec<String>,
    ) -> Result<Self> {
        let log_config = LogConfig::from_env()?;
        let store = Arc::new(Store::deferred(&config.state_dir));
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER_LIMIT);
        Ok(Self {
            config,
            store,
            shim_path: Arc::new(shim_path),
            shim_args: Arc::new(shim_args),
            embedded_shims: false,
            instance_id: Arc::new(uuid::Uuid::new_v4().to_string()),
            events: Arc::new(Mutex::new(EventHub {
                sequence: 0,
                buffer: VecDeque::new(),
                fingerprints: BTreeMap::new(),
            })),
            event_tx,
            shutdown: Arc::new(Notify::new()),
            group_lifelines: Arc::new(Mutex::new(BTreeMap::new())),
            group_event_bridges: Arc::new(Mutex::new(HashSet::new())),
            last_request: Arc::new(AtomicU64::new(now_seconds())),
            log_config,
        })
    }

    /// Creates a daemon whose shims run as tasks in the daemon process.
    /// Intended for tests; embedded shims do not survive their host process.
    pub fn with_embedded_shims(config: Config) -> Result<Self> {
        let log_config = LogConfig::from_env()?;
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER_LIMIT);
        Ok(Self {
            store: Arc::new(Store::deferred(&config.state_dir)),
            config,
            shim_path: Arc::new(PathBuf::new()),
            shim_args: Arc::new(Vec::new()),
            embedded_shims: true,
            instance_id: Arc::new(uuid::Uuid::new_v4().to_string()),
            events: Arc::new(Mutex::new(EventHub {
                sequence: 0,
                buffer: VecDeque::new(),
                fingerprints: BTreeMap::new(),
            })),
            event_tx,
            shutdown: Arc::new(Notify::new()),
            group_lifelines: Arc::new(Mutex::new(BTreeMap::new())),
            group_event_bridges: Arc::new(Mutex::new(HashSet::new())),
            last_request: Arc::new(AtomicU64::new(now_seconds())),
            log_config,
        })
    }

    pub async fn serve(self, ready_fd: Option<i32>) -> Result<()> {
        let parent = self
            .config
            .socket
            .parent()
            .context("socket has no parent")?;
        let parent_existed = parent.exists();
        fs::create_dir_all(parent)?;
        if !parent_existed {
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let state_parent = self
            .config
            .state_dir
            .parent()
            .context("state directory has no parent")?;
        fs::create_dir_all(state_parent)?;
        let lock_path = self.config.lock_path()?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(lock_path)?;
        if unsafe {
            libc::flock(
                std::os::fd::AsRawFd::as_raw_fd(&lock),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        } != 0
        {
            bail!("another amuxd owns the socket")
        }
        self.store.ensure_schema()?;
        if !self.embedded_shims {
            install_process_log(self.config.state_dir.join("amuxd.log"), self.log_config)?;
        }
        observability::init(ProcessRole::Daemon)?;
        let daemon_name = std::env::var("AMUX_DAEMON").unwrap_or_else(|_| "default".into());
        let span = info_span!(
            "daemon",
            component = "daemon",
            daemon_name,
            daemon_instance_id = self.instance_id.as_str(),
            pid = std::process::id()
        );
        self.serve_runtime(ready_fd, lock).instrument(span).await
    }

    async fn serve_runtime(self, ready_fd: Option<i32>, lock: fs::File) -> Result<()> {
        if self.config.socket.exists() {
            debug!(socket = %self.config.socket.display(), "removing stale daemon socket");
            let _ = fs::remove_file(&self.config.socket);
        }
        let listener = UnixListener::bind(&self.config.socket)?;
        fs::set_permissions(&self.config.socket, fs::Permissions::from_mode(0o600))?;
        if let Some(fd) = ready_fd {
            write_ready(fd);
        }

        self.write_registration()?;
        info!(socket = %self.config.socket.display(), "daemon ready");
        let monitor = self.clone();
        tokio::spawn(async move { monitor.consistency_loop().await });
        let idle = self.clone();
        tokio::spawn(async move { idle.monitor_idle().await });
        let app = Router::new()
            .route("/v1/health", get(health))
            .route("/v1/processes/snapshot", get(process_snapshot))
            .route("/v1/processes/events", get(process_events))
            .route("/v1/admin/stop", post(stop_daemon))
            .route(
                "/v1/workspaces",
                get(list_workspaces).post(create_workspace),
            )
            .route(
                "/v1/workspaces/{target}",
                get(inspect_workspace).delete(remove_workspace),
            )
            .route("/v1/workspaces/{target}/processes", post(run_process))
            .route("/v1/processes", get(list_processes))
            .route("/v1/processes/{target}", any(process_base_encoded))
            .route("/v1/processes/{target}/attach", any(process_attach_encoded))
            .route("/v1/processes/{target}/logs", any(process_logs_encoded))
            .route("/v1/processes/{target}/wait", any(process_wait_encoded))
            .route("/v1/processes/{target}/signal", any(process_signal_encoded))
            .route("/v1/processes/{target}/stop", any(process_stop_encoded))
            .route(
                "/v1/processes/{target}/restart",
                any(process_restart_encoded),
            )
            .route(
                "/v1/processes/{workspace}/{process}",
                any(process_base_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/attach",
                any(process_attach_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/logs",
                any(process_logs_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/wait",
                any(process_wait_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/signal",
                any(process_signal_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/stop",
                any(process_stop_pair),
            )
            .route(
                "/v1/processes/{workspace}/{process}/restart",
                any(process_restart_pair),
            )
            .route("/v1/groups", get(list_groups))
            .route("/v1/environments", get(list_environments))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                track_activity,
            ))
            .with_state(self.clone());
        let stopping = self.shutdown.clone();
        let server = axum::serve(listener, app).into_future();
        tokio::pin!(server);
        let result = tokio::select! {
            value = &mut server => value,
            _ = async move { tokio::select! { _ = shutdown_signal() => {}, _ = stopping.notified() => {} } } => Ok(()),
        };
        info!("daemon shutdown requested");
        self.stop_all_processes().await;
        self.group_lifelines.lock().unwrap().clear();
        let _ = fs::remove_file(&self.config.socket);
        let _ = fs::remove_file(self.config.state_dir.join("daemon.json"));
        drop(lock);
        result?;
        info!("daemon stopped");
        Ok(())
    }

    fn shim_socket(&self, process: &Process) -> PathBuf {
        PathBuf::from(format!(
            "/tmp/amux-{}/shims/{}.sock",
            unsafe { libc::getuid() },
            process.group_id
        ))
    }

    async fn shim_status(&self, process: &Process) -> Result<Process> {
        let value: Process = unix_http::json_request(
            &self.shim_socket(process),
            Method::GET,
            &format!("/v1/processes/{}/status", process.id),
            None,
        )
        .await?;
        Ok(value)
    }

    async fn launch_shim(&self, process: &mut Process) -> Result<()> {
        let socket = self.shim_socket(process);
        process.runtime_handle.clear();
        self.store.save_process(process)?;
        let path = self.shim_path.clone();
        let args = self.shim_args.clone();
        let process_dir = self.store.process_dir(&process.workspace_id, &process.id);
        let workspace_dir = self.store.workspace_dir(&process.workspace_id);
        if !socket.exists() {
            if self.embedded_shims {
                let group = crate::shim::GroupShim::load(&workspace_dir, &socket)?;
                tokio::spawn(async move {
                    if let Err(error) = group.serve(None, None).await {
                        error!(%error, "embedded group shim failed");
                    }
                });
                for _ in 0..100 {
                    if socket.exists() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } else {
                let launch_socket = socket.clone();
                let (group_id, lifeline) = tokio::task::spawn_blocking(move || {
                    launch_detached(&path, &args, &workspace_dir, &launch_socket)
                })
                .await??;
                self.group_lifelines
                    .lock()
                    .unwrap()
                    .insert(group_id, lifeline);
            }
            info!(group_id = %process.group_id, workspace_id = %process.workspace_id, "group shim launched");
        }
        self.ensure_group_event_bridge(process.group_id.clone(), socket.clone());
        unix_http::json_request::<serde_json::Value>(
            &self.shim_socket(process),
            Method::POST,
            "/v1/processes",
            Some(Bytes::from(serde_json::to_vec(
                &json!({"process_dir": process_dir}),
            )?)),
        )
        .await?;
        for _ in 0..100 {
            if let Ok(value) = self.shim_status(process).await {
                *process = value;
                return Ok(());
            }
            if let Ok(value) = self.store.find_process(&process.id)
                && value.state.terminal()
            {
                *process = value;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        *process = self.shim_status(process).await?;
        Ok(())
    }

    fn ensure_group_event_bridge(&self, group_id: String, socket: PathBuf) {
        if !self
            .group_event_bridges
            .lock()
            .unwrap()
            .insert(group_id.clone())
        {
            return;
        }
        let daemon = self.clone();
        tokio::spawn(async move {
            let mut retry_delay = Duration::from_millis(250);
            loop {
                let request = Request::builder()
                    .method(Method::GET)
                    .uri("http://unix/v1/events")
                    .body(Full::new(Bytes::new()));
                if let Ok(request) = request
                    && let Ok(response) = unix_http::request_stream(&socket, request).await
                    && response.status().is_success()
                {
                    debug!(group_id = %group_id, "group event bridge connected");
                    retry_delay = Duration::from_millis(250);
                    let mut body = response.into_body();
                    let mut pending = Vec::new();
                    while let Some(frame) = body.frame().await {
                        let Ok(frame) = frame else { break };
                        pending.extend_from_slice(&frame.into_data().unwrap_or_default());
                        while let Some(index) = pending.iter().position(|value| *value == b'\n') {
                            let line = pending.drain(..=index).collect::<Vec<_>>();
                            if let Ok(process) = serde_json::from_slice::<Process>(&line) {
                                daemon.observe_process(process);
                            }
                        }
                    }
                }
                if !socket.exists() {
                    break;
                }
                warn!(group_id = %group_id, delay_ms = retry_delay.as_millis(), "group event bridge disconnected; retrying");
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(10));
            }
            daemon.group_event_bridges.lock().unwrap().remove(&group_id);
            daemon.group_lifelines.lock().unwrap().remove(&group_id);
            debug!(group_id = %group_id, "group event bridge removed");
        });
    }

    fn observe_process(&self, process: Process) {
        let fingerprint = serde_json::to_string(&process).unwrap_or_default();
        let kind = {
            let mut hub = self.events.lock().unwrap();
            if hub.fingerprints.get(&process.id) == Some(&fingerprint) {
                return;
            }
            let kind = if hub.fingerprints.contains_key(&process.id) {
                if process.state == ProcessState::Exited {
                    ProcessEventKind::ProcessExited
                } else if process.state == ProcessState::Failed {
                    ProcessEventKind::ProcessUpdated
                } else if process.state == ProcessState::Running {
                    ProcessEventKind::ProcessStarted
                } else {
                    ProcessEventKind::ProcessUpdated
                }
            } else {
                ProcessEventKind::ProcessCreated
            };
            hub.fingerprints.insert(process.id.clone(), fingerprint);
            kind
        };
        let terminal = process.state.terminal();
        if let Err(error) = self.store.save_process(&process) {
            error!(%error, process_id = %process.id, "persist process observation failed");
        }
        self.emit(kind, process.clone());
        if terminal {
            self.events.lock().unwrap().fingerprints.remove(&process.id);
        }
    }

    fn view(&self, process: Process) -> ProcessView {
        let workspace_name = self
            .store
            .find_workspace(&process.workspace_id)
            .map(|w| w.name)
            .unwrap_or_default();
        let target = if workspace_name.is_empty() {
            process.id.clone()
        } else {
            format!("{workspace_name}/{}", process.name)
        };
        ProcessView {
            process,
            workspace_name,
            target,
        }
    }

    fn write_registration(&self) -> Result<()> {
        fs::create_dir_all(&self.config.state_dir)?;
        crate::store::atomic_json(
            &self.config.state_dir.join("daemon.json"),
            &json!({
                "schema_version": SCHEMA_VERSION,
                "name": std::env::var("AMUX_DAEMON").unwrap_or_else(|_| "default".into()),
                "instance_id": self.instance_id.as_str(),
                "pid": std::process::id(),
                "socket": self.config.socket,
                "state_dir": self.config.state_dir,
                "version": env!("CARGO_PKG_VERSION"),
                "started_at": chrono::Utc::now(),
                "active_groups": 0,
                "active_processes": 0,
            }),
        )
    }

    fn emit(&self, kind: ProcessEventKind, process: Process) {
        let view = self.view(process.clone());
        let fingerprint = serde_json::to_string(&process).unwrap_or_default();
        let event = {
            let mut hub = self.events.lock().unwrap();
            hub.sequence += 1;
            if kind == ProcessEventKind::ProcessRemoved || process.state.terminal() {
                hub.fingerprints.remove(&process.id);
            } else {
                hub.fingerprints.insert(process.id.clone(), fingerprint);
            }
            let event = ProcessEvent {
                kind: kind.clone(),
                daemon_instance_id: self.instance_id.as_ref().clone(),
                sequence: hub.sequence,
                timestamp: chrono::Utc::now(),
                workspace_id: process.workspace_id.clone(),
                group_id: process.group_id.clone(),
                process_id: process.id.clone(),
                parent_process_id: process.parent_process_id.clone(),
                execution: process.execution,
                process: view,
            };
            hub.buffer.push_back(event.clone());
            while hub.buffer.len() > EVENT_BUFFER_LIMIT {
                hub.buffer.pop_front();
            }
            event
        };
        let _ = self.event_tx.send(event);
        match kind {
            ProcessEventKind::ProcessCreated
            | ProcessEventKind::ProcessStarted
            | ProcessEventKind::ProcessExited
            | ProcessEventKind::ProcessRemoved => info!(
                event = ?kind,
                workspace_id = %process.workspace_id,
                group_id = %process.group_id,
                process_id = %process.id,
                execution = process.execution,
                state = ?process.state,
                "process lifecycle"
            ),
            ProcessEventKind::ProcessUpdated => debug!(
                workspace_id = %process.workspace_id,
                group_id = %process.group_id,
                process_id = %process.id,
                execution = process.execution,
                state = ?process.state,
                "process updated"
            ),
        }
        self.update_registration_counts();
    }

    fn update_registration_counts(&self) {
        let path = self.config.state_dir.join("daemon.json");
        let Ok(mut value) = crate::store::read_json::<serde_json::Value>(&path) else {
            return;
        };
        let active = self
            .store
            .list_processes(None)
            .unwrap_or_default()
            .into_iter()
            .filter(|p| !p.state.terminal())
            .collect::<Vec<_>>();
        let groups = active
            .iter()
            .map(|p| &p.group_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        value["active_processes"] = json!(active.len());
        value["active_groups"] = json!(groups);
        if let Err(error) = crate::store::atomic_json(&path, &value) {
            warn!(%error, "update daemon registration counts failed");
        }
    }

    async fn consistency_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if !self.config.socket.exists() {
                continue;
            }
            let processes = match self.store.list_processes(None) {
                Ok(processes) => processes,
                Err(error) => {
                    warn!(%error, "consistency scan failed");
                    continue;
                }
            };
            for process in processes {
                if !process.state.terminal() {
                    self.observe_process(process);
                }
            }
        }
    }

    async fn monitor_idle(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let active = self
                .store
                .list_processes(None)
                .unwrap_or_default()
                .iter()
                .any(|p| !p.state.terminal());
            if !active
                && now_seconds().saturating_sub(self.last_request.load(Ordering::Relaxed)) >= 300
            {
                self.shutdown.notify_waiters();
                break;
            }
        }
    }

    async fn stop_all_processes(&self) {
        let processes = self.store.list_processes(None).unwrap_or_default();
        for process in &processes {
            if !process.state.terminal() {
                let body = Bytes::from(
                    serde_json::to_vec(&StopRequest {
                        grace_millis: 10_000,
                    })
                    .unwrap(),
                );
                let _ = unix_http::json_request::<serde_json::Value>(
                    &self.shim_socket(process),
                    Method::POST,
                    &format!("/v1/processes/{}/stop", process.id),
                    Some(body),
                )
                .await;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(11);
        while tokio::time::Instant::now() < deadline {
            if processes.iter().all(|p| {
                self.store
                    .find_process(&p.id)
                    .map(|v| v.state.terminal())
                    .unwrap_or(true)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn track_activity(
    State(daemon): State<Daemon>,
    request: Request<Body>,
    next: axum::middleware::Next,
) -> AxumResponse {
    daemon.last_request.store(now_seconds(), Ordering::Relaxed);
    next.run(request).await
}

fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn health(State(daemon): State<Daemon>) -> Json<serde_json::Value> {
    Json(
        json!({"ok": true, "daemon_instance_id": daemon.instance_id.as_str(), "schema_version": SCHEMA_VERSION}),
    )
}

async fn stop_daemon(State(daemon): State<Daemon>) -> Json<serde_json::Value> {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        daemon.shutdown.notify_waiters();
    });
    Json(json!({"ok": true}))
}

async fn process_snapshot(State(daemon): State<Daemon>) -> AxumResponse {
    let hub = daemon.events.lock().unwrap();
    let snapshot = ProcessSnapshot {
        daemon_instance_id: daemon.instance_id.as_ref().clone(),
        sequence: hub.sequence,
        processes: daemon
            .store
            .list_processes(None)
            .unwrap_or_default()
            .into_iter()
            .map(|p| daemon.view(p))
            .collect(),
    };
    Json(snapshot).into_response()
}

#[derive(Default, Deserialize)]
struct EventQuery {
    #[serde(default)]
    after: u64,
    instance: Option<String>,
}

async fn process_events(
    State(daemon): State<Daemon>,
    Query(query): Query<EventQuery>,
) -> AxumResponse {
    let (initial, receiver) = {
        let hub = daemon.events.lock().unwrap();
        if query
            .instance
            .as_deref()
            .is_some_and(|v| v != daemon.instance_id.as_str())
        {
            return api_error(
                StatusCode::CONFLICT,
                "resync_required",
                "daemon instance changed",
            );
        }
        let oldest = hub
            .buffer
            .front()
            .map(|e| e.sequence)
            .unwrap_or(hub.sequence.saturating_add(1));
        if query.after.saturating_add(1) < oldest {
            return api_error(
                StatusCode::CONFLICT,
                "resync_required",
                "event cursor is outside the replay buffer",
            );
        }
        if query.after > hub.sequence {
            return api_error(
                StatusCode::CONFLICT,
                "resync_required",
                "event cursor is ahead of the daemon",
            );
        }
        (
            hub.buffer
                .iter()
                .filter(|e| e.sequence > query.after)
                .cloned()
                .collect::<Vec<_>>(),
            daemon.event_tx.subscribe(),
        )
    };
    let initial = tokio_stream::iter(
        initial
            .into_iter()
            .filter_map(|event| serde_json::to_vec(&event).ok())
            .map(|mut line| {
                line.push(b'\n');
                Ok::<_, std::convert::Infallible>(Bytes::from(line))
            }),
    );
    let follow =
        tokio_stream::StreamExt::filter_map(BroadcastStream::new(receiver), |item| match item {
            Ok(event) => serde_json::to_vec(&event).ok().map(|mut line| {
                line.push(b'\n');
                Ok::<_, std::convert::Infallible>(Bytes::from(line))
            }),
            Err(_) => None,
        });
    Body::from_stream(tokio_stream::StreamExt::chain(initial, follow)).into_response()
}

async fn list_workspaces(State(daemon): State<Daemon>) -> AxumResponse {
    match daemon.store.list_workspaces() {
        Ok(mut workspaces) => {
            for workspace in &mut workspaces {
                let active = daemon
                    .store
                    .list_processes(Some(&workspace.id))
                    .unwrap_or_default()
                    .iter()
                    .any(|p| {
                        matches!(
                            p.state,
                            ProcessState::Running | ProcessState::Starting | ProcessState::Stopping
                        )
                    });
                workspace.state = if active {
                    WorkspaceState::Active
                } else {
                    WorkspaceState::Idle
                };
                if let Err(error) = daemon.store.save_workspace(workspace) {
                    warn!(%error, workspace_id = %workspace.id, "persist workspace state failed");
                }
            }
            let mut response = Response::ok();
            response.workspaces = workspaces;
            Json(response).into_response()
        }
        Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error),
    }
}

async fn create_workspace(
    State(daemon): State<Daemon>,
    Json(request): Json<CreateWorkspaceRequest>,
) -> AxumResponse {
    match daemon.store.create_workspace(request) {
        Ok(workspace) => {
            let mut response = Response::ok();
            response.workspace = Some(workspace);
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(error) => {
            let code = if error.to_string().contains("conflict") {
                "name_conflict"
            } else if error.to_string().contains("supported") {
                "runtime_not_supported"
            } else {
                "invalid_argument"
            };
            api_error(StatusCode::CONFLICT, code, error)
        }
    }
}

async fn inspect_workspace(
    State(daemon): State<Daemon>,
    AxumPath(target): AxumPath<String>,
) -> AxumResponse {
    match daemon.store.find_workspace(&target) {
        Ok(workspace) => match daemon.store.defaults(&workspace.id) {
            Ok((group, environment)) => Json(json!({"ok":true,"workspace":workspace,"groups":[group],"environments":[environment]})).into_response(),
            Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error),
        },
        Err(error) => api_error(StatusCode::NOT_FOUND, "workspace_not_found", error),
    }
}

async fn run_process(
    State(daemon): State<Daemon>,
    AxumPath(target): AxumPath<String>,
    Json(mut request): Json<RunRequest>,
) -> AxumResponse {
    let workspace = match daemon.store.find_workspace(&target) {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::NOT_FOUND, "workspace_not_found", e),
    };
    if request.command.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            anyhow!("name and command are required"),
        );
    }
    if let Some(parent_id) = request.parent_process_id.as_deref()
        && daemon.store.find_process(parent_id).is_err()
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_parent_process",
            "parent process does not belong to this daemon",
        );
    }
    if !request.runtime.is_empty() && request.runtime != workspace.runtime {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            anyhow!("process runtime must match workspace runtime"),
        );
    }
    if request.cwd.is_empty() {
        request.cwd = workspace.root_dir.clone();
    }
    let explicit_env = request.env.clone();
    let mut runtime_env: BTreeMap<String, String> = std::env::vars()
        .filter(|(key, _)| !key.ends_with("_READY_FD"))
        .collect();
    runtime_env.extend(workspace.env.clone());
    runtime_env.extend(explicit_env);
    let mut process = match daemon.store.create_process(&workspace, request, boot_id()) {
        Ok(value) => value,
        Err(error) => {
            return api_error(
                StatusCode::CONFLICT,
                if error.to_string().contains("conflict") {
                    "name_conflict"
                } else {
                    "invalid_argument"
                },
                error,
            );
        }
    };
    daemon.emit(ProcessEventKind::ProcessCreated, process.clone());
    runtime_env.extend([
        ("AMUX".into(), "1".into()),
        ("AMUX_WORKSPACE".into(), workspace.name.clone()),
        ("AMUX_WORKSPACE_ID".into(), workspace.id.clone()),
        ("AMUX_PROCESS".into(), process.name.clone()),
        ("AMUX_PROCESS_ID".into(), process.id.clone()),
        (
            "AMUX_IO_MODE".into(),
            match process.io_mode {
                IoMode::Pipe => "pipe",
                IoMode::Tty => "tty",
            }
            .into(),
        ),
    ]);
    if let Err(error) = daemon.store.save_runtime_env(&process, &runtime_env) {
        error!(%error, process_id = %process.id, "persist process runtime environment failed");
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error);
    }
    if let Err(error) = daemon.launch_shim(&mut process).await {
        error!(%error, process_id = %process.id, group_id = %process.group_id, "launch process supervisor failed");
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "process_start_failed",
            error,
        );
    }
    let mut response = Response::ok();
    response.process = Some(daemon.view(process));
    (StatusCode::CREATED, Json(response)).into_response()
}

#[derive(Default, Deserialize)]
struct ListQuery {
    workspace: Option<String>,
}

async fn list_processes(
    State(daemon): State<Daemon>,
    Query(query): Query<ListQuery>,
) -> AxumResponse {
    let wid = match query.workspace {
        Some(target) => match daemon.store.find_workspace(&target) {
            Ok(w) => Some(w.id),
            Err(e) => return api_error(StatusCode::NOT_FOUND, "workspace_not_found", e),
        },
        None => None,
    };
    let mut processes = match daemon.store.list_processes(wid.as_deref()) {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e),
    };
    for process in &mut processes {
        match daemon.shim_status(process).await {
            Ok(value) => *process = value,
            Err(_)
                if matches!(
                    process.state,
                    ProcessState::Running | ProcessState::Starting | ProcessState::Stopping
                ) =>
            {
                process.state = ProcessState::Unknown;
                process.error = "shim unavailable".into();
                if let Err(error) = daemon.store.save_process(process) {
                    error!(%error, process_id = %process.id, "persist unknown process state failed");
                }
                warn!(process_id = %process.id, group_id = %process.group_id, "live process shim is unavailable");
            }
            Err(_) => {}
        }
    }
    let mut response = Response::ok();
    response.processes = processes.into_iter().map(|p| daemon.view(p)).collect();
    Json(response).into_response()
}

async fn process_route(
    State(daemon): State<Daemon>,
    AxumPath(rest): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    let (target, action) = split_action(&rest);
    let mut process = match daemon.store.find_process(target) {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::NOT_FOUND, "process_not_found", e),
    };
    match action {
        Some("attach") if process.state.terminal() => {
            return api_error(
                StatusCode::CONFLICT,
                "invalid_state",
                "terminal process cannot be attached",
            );
        }
        Some("attach") => return proxy_attach(daemon, process, request).await,
        Some("logs") if process.state.terminal() => {
            return historical_logs(&daemon, &process, request.uri().query()).unwrap_or_else(|e| {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e)
            });
        }
        Some("logs") | Some("signal") | Some("stop") => {
            return proxy_http(&daemon, &process, action.unwrap(), request).await;
        }
        Some("wait") => loop {
            if let Ok(value) = daemon.shim_status(&process).await {
                process = value;
            } else if let Ok(value) = daemon.store.find_process(&process.id) {
                process = value;
            }
            if process.state.terminal() {
                let mut response = Response::ok();
                response.process = Some(daemon.view(process));
                return Json(response).into_response();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        },
        Some("restart") => return restart_process(daemon, process).await,
        _ => {}
    }
    match *request.method() {
        Method::GET => {
            if let Ok(value) = daemon.shim_status(&process).await {
                process = value;
            }
            let mut response = Response::ok();
            response.process = Some(daemon.view(process));
            Json(response).into_response()
        }
        Method::DELETE => {
            if let Ok(value) = daemon.shim_status(&process).await {
                process = value;
            }
            if !process.state.terminal() {
                return api_error(
                    StatusCode::CONFLICT,
                    "invalid_state",
                    anyhow!("running process must be stopped before removal"),
                );
            }
            match daemon.store.remove_process(&process) {
                Ok(()) => {
                    daemon.emit(ProcessEventKind::ProcessRemoved, process);
                    Json(json!({"ok":true})).into_response()
                }
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e),
            }
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn historical_logs(
    daemon: &Daemon,
    process: &Process,
    query: Option<&str>,
) -> Result<AxumResponse> {
    let after = query
        .and_then(|q| q.split('&').find_map(|part| part.strip_prefix("after=")))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let dir = daemon.store.process_dir(&process.workspace_id, &process.id);
    let records = RecordReader::open(&dir, after, None)?;
    let stream = async_stream::stream! {
        for record in records.flatten() {
            if let Ok(line) = record.to_json_line() {
                yield Ok::<_, std::convert::Infallible>(Bytes::from(line));
            }
        }
    };
    Ok(Body::from_stream(stream).into_response())
}

async fn process_base_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(target), request).await
}
async fn process_attach_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/attach")), request).await
}
async fn process_logs_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/logs")), request).await
}
async fn process_wait_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/wait")), request).await
}
async fn process_signal_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/signal")), request).await
}
async fn process_stop_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/stop")), request).await
}
async fn process_restart_encoded(
    state: State<Daemon>,
    AxumPath(target): AxumPath<String>,
    request: Request<Body>,
) -> AxumResponse {
    process_route(state, AxumPath(format!("{target}/restart")), request).await
}

async fn process_base_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "", request).await
}
async fn process_attach_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "attach", request).await
}
async fn process_logs_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "logs", request).await
}
async fn process_wait_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "wait", request).await
}
async fn process_signal_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "signal", request).await
}
async fn process_stop_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "stop", request).await
}
async fn process_restart_pair(
    state: State<Daemon>,
    AxumPath(pair): AxumPath<(String, String)>,
    request: Request<Body>,
) -> AxumResponse {
    process_pair(state, pair, "restart", request).await
}

async fn process_pair(
    state: State<Daemon>,
    pair: (String, String),
    action: &str,
    request: Request<Body>,
) -> AxumResponse {
    let suffix = if action.is_empty() {
        String::new()
    } else {
        format!("/{action}")
    };
    process_route(
        state,
        AxumPath(format!("{}/{}{}", pair.0, pair.1, suffix)),
        request,
    )
    .await
}

async fn proxy_http(
    daemon: &Daemon,
    process: &Process,
    action: &str,
    request: Request<Body>,
) -> AxumResponse {
    let query = request
        .uri()
        .query()
        .map(|v| format!("?{v}"))
        .unwrap_or_default();
    let method = request.method().clone();
    let bytes = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, "invalid_argument", e),
    };
    let req = match Request::builder()
        .method(method)
        .uri(format!(
            "http://unix/v1/processes/{}/{action}{query}",
            process.id
        ))
        .header("content-type", "application/json")
        .body(Full::new(bytes))
    {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e),
    };
    match unix_http::request_stream(&daemon.shim_socket(process), req).await {
        Ok(response) => {
            if !response.status().is_success() {
                let status = response.status();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map(|value| value.to_bytes())
                    .unwrap_or_default();
                let code = if status == StatusCode::BAD_REQUEST {
                    "invalid_argument"
                } else {
                    "process_not_running"
                };
                return api_error(status, code, String::from_utf8_lossy(&body));
            }
            let (parts, body) = response.into_parts();
            let mut output = AxumResponse::new(Body::new(body));
            *output.status_mut() = parts.status;
            *output.headers_mut() = parts.headers;
            output
        }
        Err(error) => api_error(StatusCode::CONFLICT, "process_not_running", error),
    }
}

async fn restart_process(daemon: Daemon, mut process: Process) -> AxumResponse {
    if let Ok(value) = daemon.shim_status(&process).await {
        process = value;
    }
    if !process.state.terminal()
        && !(process.state == ProcessState::Unknown && process.boot_id != boot_id())
    {
        return api_error(
            StatusCode::CONFLICT,
            "invalid_state",
            anyhow!("process cannot be safely restarted"),
        );
    }
    let group_has_other_active_processes = daemon
        .store
        .list_processes(None)
        .unwrap_or_default()
        .into_iter()
        .any(|candidate| {
            candidate.id != process.id
                && candidate.group_id == process.group_id
                && !candidate.state.terminal()
        });
    if !group_has_other_active_processes && daemon.shim_socket(&process).exists() {
        shutdown_shim(&daemon, &process).await;
    }
    process.state = ProcessState::Starting;
    process.execution += 1;
    process.pid = 0;
    process.process_group_id = 0;
    process.exit_code = None;
    process.exit_signal.clear();
    process.error.clear();
    process.boot_id = boot_id();
    process.started_at = None;
    process.finished_at = None;
    if let Err(error) = daemon.store.save_process(&process) {
        error!(%error, process_id = %process.id, "persist process restart state failed");
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error);
    }
    if let Err(error) = daemon.launch_shim(&mut process).await {
        error!(%error, process_id = %process.id, group_id = %process.group_id, "restart process supervisor failed");
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "process_start_failed",
            error,
        );
    }
    let mut response = Response::ok();
    response.process = Some(daemon.view(process));
    Json(response).into_response()
}

async fn remove_workspace(
    State(daemon): State<Daemon>,
    AxumPath(target): AxumPath<String>,
) -> AxumResponse {
    let workspace = match daemon.store.find_workspace(&target) {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::NOT_FOUND, "workspace_not_found", e),
    };
    let mut processes = daemon
        .store
        .list_processes(Some(&workspace.id))
        .unwrap_or_default();
    for process in &mut processes {
        if let Ok(value) = daemon.shim_status(process).await {
            *process = value;
        }
        if !process.state.terminal() {
            let body = serde_json::to_vec(&StopRequest {
                grace_millis: 10_000,
            })
            .unwrap();
            let _ = unix_http::json_request::<serde_json::Value>(
                &daemon.shim_socket(process),
                Method::POST,
                &format!("/v1/processes/{}/stop", process.id),
                Some(Bytes::from(body)),
            )
            .await;
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let mut all = true;
        for process in &mut processes {
            if let Ok(value) = daemon.shim_status(process).await {
                *process = value;
            }
            if !process.state.terminal() {
                all = false;
            }
        }
        if all {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return api_error(
                StatusCode::CONFLICT,
                "invalid_state",
                anyhow!("some processes did not stop"),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for process in &processes {
        shutdown_shim(&daemon, process).await;
    }
    match daemon.store.remove_workspace(&workspace) {
        Ok(()) => Json(json!({"ok":true})).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e),
    }
}

async fn list_groups(State(daemon): State<Daemon>) -> AxumResponse {
    let groups = daemon
        .store
        .list_workspaces()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|w| daemon.store.defaults(&w.id).ok().map(|v| v.0))
        .collect::<Vec<_>>();
    let mut response = Response::ok();
    response.groups = groups;
    Json(response).into_response()
}

async fn list_environments(State(daemon): State<Daemon>) -> AxumResponse {
    let environments = daemon
        .store
        .list_workspaces()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|w| daemon.store.defaults(&w.id).ok().map(|v| v.1))
        .collect::<Vec<_>>();
    let mut response = Response::ok();
    response.environments = environments;
    Json(response).into_response()
}

async fn proxy_attach(daemon: Daemon, process: Process, request: Request<Body>) -> AxumResponse {
    let query = request
        .uri()
        .query()
        .map(|v| format!("?{v}"))
        .unwrap_or_default();
    let (mut parts, _) = request.into_parts();
    let ws = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let socket = daemon.shim_socket(&process);
    let path = format!("ws://unix/v1/processes/{}/attach{query}", process.id);
    let stream = match UnixStream::connect(socket).await {
        Ok(v) => v,
        Err(e) => return api_error(StatusCode::CONFLICT, "process_not_running", e),
    };
    let (back, _) = match client_async(path, stream).await {
        Ok(v) => v,
        Err(e) => {
            return api_error(
                StatusCode::CONFLICT,
                if e.to_string().contains("controller") {
                    "controller_exists"
                } else {
                    "process_not_running"
                },
                e,
            );
        }
    };
    ws.on_upgrade(move |front| bridge_ws(front, back))
}

async fn bridge_ws(front: WebSocket, back: tokio_tungstenite::WebSocketStream<UnixStream>) {
    let (mut front_tx, mut front_rx) = front.split();
    let (mut back_tx, mut back_rx) = back.split();
    loop {
        tokio::select! {
            message = front_rx.next() => match message { Some(Ok(v)) => if back_tx.send(to_tungstenite(v)).await.is_err() { break }, _ => break },
            message = back_rx.next() => match message { Some(Ok(v)) => if let Some(v) = to_axum(v) && front_tx.send(v).await.is_err() { break }, _ => break },
        }
    }
}

fn to_tungstenite(value: Message) -> tungstenite::Message {
    match value {
        Message::Text(v) => tungstenite::Message::Text(v.to_string().into()),
        Message::Binary(v) => tungstenite::Message::Binary(v),
        Message::Ping(v) => tungstenite::Message::Ping(v),
        Message::Pong(v) => tungstenite::Message::Pong(v),
        Message::Close(_) => tungstenite::Message::Close(None),
    }
}
fn to_axum(value: tungstenite::Message) -> Option<Message> {
    match value {
        tungstenite::Message::Text(v) => Some(Message::Text(v.to_string().into())),
        tungstenite::Message::Binary(v) => Some(Message::Binary(v)),
        tungstenite::Message::Ping(v) => Some(Message::Ping(v)),
        tungstenite::Message::Pong(v) => Some(Message::Pong(v)),
        tungstenite::Message::Close(_) => Some(Message::Close(None)),
        tungstenite::Message::Frame(_) => None,
    }
}

async fn shutdown_shim(daemon: &Daemon, process: &Process) {
    let _ = unix_http::json_request::<serde_json::Value>(
        &daemon.shim_socket(process),
        Method::POST,
        "/v1/shutdown",
        None,
    )
    .await;
    for _ in 0..200 {
        if !daemon.shim_socket(process).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn split_action(rest: &str) -> (&str, Option<&str>) {
    for action in ["attach", "logs", "wait", "signal", "stop", "restart"] {
        if let Some(target) = rest.strip_suffix(&format!("/{action}")) {
            return (target, Some(action));
        }
    }
    (rest, None)
}

fn api_error(status: StatusCode, code: &str, error: impl std::fmt::Display) -> AxumResponse {
    (status, Json(Response::fail(code, error.to_string()))).into_response()
}

fn launch_detached(
    shim: &Path,
    shim_args: &[String],
    workspace_dir: &Path,
    socket: &Path,
) -> Result<(String, fs::File)> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut lifecycle = [0i32; 2];
    if unsafe { libc::pipe(lifecycle.as_mut_ptr()) } != 0 {
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        return Err(std::io::Error::last_os_error().into());
    }
    let read_fd = fds[0];
    let write_fd = fds[1];
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(workspace_dir.join("shim.log"))?;
    let stderr = log.try_clone()?;
    let mut command = Command::new(shim);
    command
        .args(shim_args)
        .args([
            "--workspace-dir",
            &workspace_dir.to_string_lossy(),
            "--socket",
            &socket.to_string_lossy(),
        ])
        .env("AMUX_SHIM_READY_FD", write_fd.to_string())
        .env("AMUX_SHIM_LIFECYCLE_FD", lifecycle[0].to_string())
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(stderr);
    unsafe {
        command.pre_exec(move || {
            libc::close(read_fd);
            libc::close(lifecycle[1]);
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    unsafe {
        libc::close(write_fd);
        libc::close(lifecycle[0]);
    }
    let mut byte = [0u8; 1];
    let mut poll = libc::pollfd {
        fd: read_fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let polled = unsafe { libc::poll(&mut poll, 1, 5_000) };
    let count = if polled > 0 {
        unsafe { libc::read(read_fd, byte.as_mut_ptr().cast(), 1) }
    } else {
        0
    };
    unsafe {
        libc::close(read_fd);
    }
    if count != 1 {
        unsafe {
            libc::close(lifecycle[1]);
        }
        let _ = child.kill();
        let _ = child.wait();
        bail!("shim readiness failed")
    }
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    unsafe {
        libc::fcntl(lifecycle[1], libc::F_SETFD, libc::FD_CLOEXEC);
    }
    let lifeline = unsafe { fs::File::from_raw_fd(lifecycle[1]) };
    let group: Group = crate::store::read_json(workspace_dir.join("group.json"))?;
    Ok((group.id, lifeline))
}

fn write_ready(fd: RawFd) {
    unsafe {
        let byte = [1u8];
        libc::write(fd, byte.as_ptr().cast(), 1);
        libc::close(fd);
    }
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
}
