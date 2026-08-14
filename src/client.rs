use std::{
    fs::OpenOptions,
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

use crate::{config::Config, protocol::Response, unix_http};

#[derive(Clone)]
pub struct Client {
    pub config: Config,
    daemon_name: String,
}

impl Client {
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
        let request = Request::builder()
            .method(method)
            .uri(format!("http://unix{path}"))
            .header("content-type", "application/json")
            .body(Full::new(body))?;
        let response = unix_http::request(&self.config.socket, request).await?;
        if !response.status().is_success() {
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
        self.do_empty(Method::GET, "/v1/health").await.is_ok()
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
        if self.ready().await {
            return Ok(());
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
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        unsafe {
            libc::close(fds[1]);
        }
        let read_fd = fds[0];
        let ready = tokio::task::spawn_blocking(move || {
            let mut byte = [0u8; 1];
            let mut poll = libc::pollfd {
                fd: read_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let polled = unsafe { libc::poll(&mut poll, 1, 5_000) };
            let n = if polled > 0 {
                unsafe { libc::read(read_fd, byte.as_mut_ptr().cast(), 1) }
            } else {
                0
            };
            unsafe {
                libc::close(read_fd);
            }
            n == 1
        });
        let ok = tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .context("daemon readiness timeout")??;
        if !ok && !self.ready().await {
            let _ = child.kill();
            let _ = child.wait();
            bail!("daemon readiness failed")
        }
        Ok(())
    }
}
