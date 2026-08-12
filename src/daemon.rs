use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    os::{
        fd::RawFd,
        unix::{
            fs::{OpenOptionsExt, PermissionsExt},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
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
use tokio_tungstenite::{client_async, tungstenite};

use crate::{
    config::{Config, boot_id},
    model::*,
    protocol::Response,
    shim::Shim,
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
}

impl Daemon {
    pub fn new(config: Config) -> Result<Self> {
        let shim_path = sibling_executable("amux-shim")?;
        Self::with_shim_command(config, shim_path, Vec::new())
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
        let store = Arc::new(Store::open(&config.state_dir)?);
        Ok(Self {
            config,
            store,
            shim_path: Arc::new(shim_path),
            shim_args: Arc::new(shim_args),
            embedded_shims: false,
        })
    }

    /// Creates a daemon whose shims run as tasks in the daemon process.
    /// Intended for tests; embedded shims do not survive their host process.
    pub fn with_embedded_shims(config: Config) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Store::open(&config.state_dir)?),
            config,
            shim_path: Arc::new(PathBuf::new()),
            shim_args: Arc::new(Vec::new()),
            embedded_shims: true,
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
        let lock_path = parent.join("amuxd.lock");
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
        let _ = fs::remove_file(&self.config.socket);
        let listener = UnixListener::bind(&self.config.socket)?;
        fs::set_permissions(&self.config.socket, fs::Permissions::from_mode(0o600))?;
        if let Some(fd) = ready_fd {
            write_ready(fd);
        }

        let app = Router::new()
            .route("/v1/health", get(|| async { Json(json!({"ok":true})) }))
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
            .with_state(self.clone());
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await;
        let _ = fs::remove_file(&self.config.socket);
        drop(lock);
        result?;
        Ok(())
    }

    fn shim_socket(&self, process: &Process) -> PathBuf {
        if !process.runtime_handle.is_empty() {
            return PathBuf::from(&process.runtime_handle);
        }
        PathBuf::from(format!(
            "/tmp/amux-{}/shims/{}.sock",
            unsafe { libc::getuid() },
            process.id
        ))
    }

    async fn shim_status(&self, process: &Process) -> Result<Process> {
        let value: Process =
            unix_http::json_request(&self.shim_socket(process), Method::GET, "/v1/status", None)
                .await?;
        self.store.save_process(&value)?;
        Ok(value)
    }

    async fn launch_shim(&self, process: &mut Process) -> Result<()> {
        let socket = self.shim_socket(process);
        process.runtime_handle = socket.to_string_lossy().into_owned();
        self.store.save_process(process)?;
        let path = self.shim_path.clone();
        let args = self.shim_args.clone();
        let dir = self.store.process_dir(&process.workspace_id, &process.id);
        if self.embedded_shims {
            let shim = Shim::load(dir, socket)?;
            tokio::spawn(async move {
                let _ = shim.serve(None).await;
            });
        } else {
            tokio::task::spawn_blocking(move || launch_detached(&path, &args, &dir, &socket))
                .await??;
        }
        for _ in 0..100 {
            if self.shim_status(process).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        *process = self.shim_status(process).await?;
        Ok(())
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
                let _ = daemon.store.save_workspace(workspace);
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
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error);
    }
    if let Err(error) = daemon.launch_shim(&mut process).await {
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
                let _ = daemon.store.save_process(process);
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
        Some("attach") => return proxy_attach(daemon, process, request).await,
        Some("logs") | Some("signal") | Some("stop") => {
            return proxy_http(&daemon, &process, action.unwrap(), request).await;
        }
        Some("wait") => loop {
            if let Ok(value) = daemon.shim_status(&process).await {
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
            shutdown_shim(&daemon, &process).await;
            match daemon.store.remove_process(&process) {
                Ok(()) => Json(json!({"ok":true})).into_response(),
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e),
            }
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
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
        .uri(format!("http://unix/v1/{action}{query}"))
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
    shutdown_shim(&daemon, &process).await;
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
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", error);
    }
    if let Err(error) = daemon.launch_shim(&mut process).await {
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
                "/v1/stop",
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
    let path = format!("ws://unix/v1/attach{query}");
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

fn sibling_executable(name: &str) -> Result<PathBuf> {
    if let Ok(current) = std::env::current_exe() {
        let path = current.with_file_name(name);
        if path.is_file() {
            return Ok(path);
        }
    }
    if let Some(path) = std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|p| p.join(name))
            .find(|p| p.is_file())
    }) {
        return Ok(path);
    }
    bail!("find {name}: executable not found")
}

fn launch_detached(shim: &Path, shim_args: &[String], dir: &Path, socket: &Path) -> Result<()> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let read_fd = fds[0];
    let write_fd = fds[1];
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("shim.log"))?;
    let stderr = log.try_clone()?;
    let mut command = Command::new(shim);
    command
        .args(shim_args)
        .args([
            "--process-dir",
            &dir.to_string_lossy(),
            "--socket",
            &socket.to_string_lossy(),
        ])
        .env("AMUX_SHIM_READY_FD", write_fd.to_string())
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(stderr);
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    unsafe {
        libc::close(write_fd);
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
        let _ = child.kill();
        let _ = child.wait();
        bail!("shim readiness failed")
    }
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
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
