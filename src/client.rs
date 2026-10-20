use std::{
    fs::OpenOptions,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http::{Method, Request};
use http_body_util::Full;
use tokio::net::UnixStream;
use tokio_tungstenite::{WebSocketStream, client_async};
use tracing::{debug, warn};

use crate::{config::Config, protocol::Response, unix_http};

#[derive(Clone)]
pub struct Client {
    pub config: Config,
    daemon_name: String,
}

struct StartingDaemon {
    child: std::process::Child,
    ready: bool,
}

impl Drop for StartingDaemon {
    fn drop(&mut self) {
        if !self.ready {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

impl Client {
    pub fn daemon_name(&self) -> &str {
        &self.daemon_name
    }

    pub async fn health(&self) -> crate::lifecycle::Health {
        use crate::lifecycle::{HEALTH_TIMEOUT, Health};
        match tokio::time::timeout(HEALTH_TIMEOUT, self.do_empty(Method::GET, "/v1/health")).await {
            Err(_) => Health::Timeout,
            Ok(Err(error)) => {
                if error.chain().any(|cause| {
                    cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
                        matches!(
                            io.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                        )
                    })
                }) {
                    Health::Unreachable {
                        message: error.to_string(),
                    }
                } else {
                    Health::InvalidResponse {
                        message: error.to_string(),
                    }
                }
            }
            Ok(Ok(bytes)) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(value)
                    if value["ok"] == true
                        && value["daemon_instance_id"]
                            .as_str()
                            .is_some_and(|id| !id.is_empty()) =>
                {
                    Health::Healthy {
                        instance_id: value["daemon_instance_id"].as_str().unwrap().into(),
                    }
                }
                _ => Health::InvalidResponse {
                    message: "Invalid daemon health response".into(),
                },
            },
        }
    }

    pub async fn runtime_status(&self) -> Result<crate::lifecycle::RuntimeStatus> {
        crate::lifecycle::inspect(self).await
    }

    pub async fn recover_daemon_with(
        &self,
        executable: PathBuf,
        arguments: Vec<String>,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(45), async {
            crate::lifecycle::recover(self).await?;
            self.ensure_daemon_with(executable, arguments).await
        })
        .await
        .context("daemon recovery timed out")?
    }
    pub fn new(config: Config) -> Self {
        Self {
            config,
            daemon_name: crate::config::daemon_name(),
        }
    }

    pub fn named(config: Config, daemon_name: impl Into<String>) -> Self {
        Self {
            config,
            daemon_name: daemon_name.into(),
        }
    }

    pub async fn do_json<T: serde::Serialize>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<Bytes> {
        let bytes = match body {
            Some(value) => Bytes::from(serde_json::to_vec(value)?),
            None => Bytes::new(),
        };
        self.do_bytes(method, path, bytes).await
    }

    pub async fn do_empty(&self, method: Method, path: &str) -> Result<Bytes> {
        self.do_bytes(method, path, Bytes::new()).await
    }

    pub async fn do_bytes(&self, method: Method, path: &str, body: Bytes) -> Result<Bytes> {
        debug!(%method, path, daemon_name = %self.daemon_name, "daemon request");
        let request = Request::builder()
            .method(method)
            .uri(format!("http://unix{path}"))
            .header("content-type", "application/json")
            .body(Full::new(body))?;
        let response = unix_http::request(&self.config.socket, request).await?;
        if !response.status().is_success() {
            debug!(status = %response.status(), path, "daemon request rejected");
            if let Ok(value) = serde_json::from_slice::<Response>(response.body())
                && let Some(error) = value.error
            {
                return Err(error.into());
            }
            bail!(
                "amux API {}: {}",
                response.status(),
                String::from_utf8_lossy(response.body())
            );
        }
        Ok(response.into_body())
    }

    pub async fn stream(&self, path: &str) -> Result<hyper::body::Incoming> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("http://unix{path}"))
            .body(Full::new(Bytes::new()))?;
        let response = unix_http::request_stream(&self.config.socket, request).await?;
        if !response.status().is_success() {
            bail!("amux API {}", response.status());
        }
        Ok(response.into_body())
    }

    pub async fn attach(&self, path: &str) -> Result<WebSocketStream<UnixStream>> {
        let stream = UnixStream::connect(&self.config.socket).await?;
        let (socket, _) = client_async(format!("ws://unix{path}"), stream).await?;
        Ok(socket)
    }

    pub async fn ready(&self) -> bool {
        self.health().await.healthy()
    }

    pub async fn ensure_daemon(&self) -> Result<()> {
        let executable = std::env::current_exe().context("locate amux executable")?;
        self.ensure_daemon_with(executable, vec!["daemon".into(), "serve".into()])
            .await
    }

    pub async fn ensure_daemon_with(
        &self,
        executable: PathBuf,
        arguments: Vec<String>,
    ) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.start_daemon(executable, arguments),
        )
        .await
        .context("daemon startup timed out")?
    }

    async fn start_daemon(&self, executable: PathBuf, arguments: Vec<String>) -> Result<()> {
        let status = self.runtime_status().await?;
        if status.state == crate::lifecycle::RuntimeState::Healthy {
            debug!(daemon_name = %self.daemon_name, "daemon already ready");
            return Ok(());
        }
        if status.state != crate::lifecycle::RuntimeState::Stopped {
            return Err(crate::lifecycle::unavailable(&status));
        }
        std::fs::create_dir_all(&self.config.state_dir)?;
        std::fs::set_permissions(
            &self.config.state_dir,
            std::fs::Permissions::from_mode(0o700),
        )?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.config.state_dir.join("amuxd.log"))?;
        let stderr = log.try_clone()?;
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let read_fd = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let write_fd = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        let mut command = Command::new(executable);
        command
            .args(arguments)
            .env("AMUX_DAEMON", &self.daemon_name)
            .env("AMUX_STATE_DIR", &self.config.state_dir)
            .env("AMUX_SOCKET", &self.config.socket)
            .env("AMUX_DAEMON_READY_FD", fds[1].to_string())
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(stderr);
        unsafe {
            command.pre_exec(move || {
                libc::close(fds[0]);
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = StartingDaemon {
            child: command.spawn()?,
            ready: false,
        };
        debug!(daemon_name = %self.daemon_name, pid = child.child.id(), "daemon spawned; waiting for readiness");
        drop(write_fd);
        let ready = tokio::task::spawn_blocking(move || {
            let mut byte = [0u8; 1];
            let mut poll = libc::pollfd {
                fd: read_fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let polled = unsafe { libc::poll(&mut poll, 1, 5_000) };
            let n = if polled > 0 {
                unsafe { libc::read(read_fd.as_raw_fd(), byte.as_mut_ptr().cast(), 1) }
            } else {
                0
            };
            n == 1
        });
        let ok = tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .context("daemon readiness timeout")??;
        if !ok && !self.ready().await {
            warn!(daemon_name = %self.daemon_name, "daemon readiness failed");
            bail!("daemon readiness failed")
        }
        child.ready = true;
        Ok(())
    }
}
