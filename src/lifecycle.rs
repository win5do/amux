//! Bounded daemon inspection and explicit, state-root-scoped recovery.
use crate::{
    client::Client, config::Config, process_identity::ProcessIdentity, protocol::ApiError,
};
use anyhow::{Context, Result, bail};
use http::Method;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
    path::{Path, PathBuf},
    time::Duration,
};

pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(1);
const STOP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeState {
    Healthy,
    Stopped,
    Unresponsive,
    Legacy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Health {
    Healthy { instance_id: String },
    Unreachable { message: String },
    Timeout,
    InvalidResponse { message: String },
}
impl Health {
    pub fn healthy(&self) -> bool {
        matches!(self, Self::Healthy { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registration {
    pub name: String,
    pub instance_id: String,
    pub pid: i32,
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    #[serde(default)]
    pub process_identity: Option<ProcessIdentity>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LockOwner {
    pub instance_id: String,
    pub process_identity: ProcessIdentity,
}

pub(crate) fn record_lock_owner(file: &mut File, instance_id: &str) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let owner = LockOwner {
        instance_id: instance_id.into(),
        process_identity: ProcessIdentity::current(std::process::id() as i32)?
            .context("cannot identify daemon process")?,
    };
    // Never rename the file holding flock: the locked inode is the authority.
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    serde_json::to_writer(&mut *file, &owner)?;
    file.flush()?;
    Ok(())
}

fn verified_owner(client: &Client, record: &Registration) -> Result<bool> {
    let owner: LockOwner = match crate::store::read_json(client.config.lock_path()?) {
        Ok(owner) => owner,
        Err(_) => return Ok(false),
    };
    Ok(owner.instance_id == record.instance_id
        && record.process_identity.as_ref() == Some(&owner.process_identity)
        && owner.process_identity.pid == record.pid
        && owner.process_identity.uid == unsafe { libc::getuid() }
        && owner.process_identity.alive()?)
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeStatus {
    pub state: RuntimeState,
    pub health: Health,
    pub recovery_available: bool,
    pub registration: Option<Registration>,
}

pub fn recovery_path(config: &Config) -> Result<PathBuf> {
    Ok(config.lock_path()?.with_extension("recovery.lock"))
}

pub fn lock_for_removal(config: &Config) -> Result<File> {
    lock(&recovery_path(config)?, false, true)?
        .ok_or_else(|| ApiError::new("daemon_recovering", "Runtime recovery is in progress").into())
}

pub(crate) fn lock(path: &Path, exclusive: bool, create: bool) -> Result<Option<File>> {
    if create {
        std::fs::create_dir_all(path.parent().context("lock has no parent")?)?;
    }
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if !create && error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Ok(None);
        }
        return Err(error.into());
    }
    Ok(Some(file))
}

pub fn state_available(config: &Config) -> Result<bool> {
    let path = config.lock_path()?;
    if !path.exists() {
        return Ok(true);
    }
    Ok(lock(&path, true, false)?.is_some())
}

fn registration(client: &Client) -> Option<Registration> {
    let record: Registration =
        crate::store::read_json(client.config.state_dir.join("daemon.json")).ok()?;
    if record.name != client.daemon_name()
        || record.state_dir.canonicalize().ok()? != client.config.state_dir.canonicalize().ok()?
        || record.instance_id.is_empty()
    {
        return None;
    }
    Some(record)
}

pub async fn inspect(client: &Client) -> Result<RuntimeStatus> {
    let mut health = client.health().await;
    if !health.healthy() {
        tokio::time::sleep(Duration::from_millis(100)).await;
        health = client.health().await;
    }
    let registration = registration(client);
    if health.healthy() {
        return Ok(RuntimeStatus {
            state: RuntimeState::Healthy,
            health,
            recovery_available: false,
            registration,
        });
    }
    if state_available(&client.config)? {
        // A listening but unhealthy endpoint is not a stale file we can unlink.
        let pending = client
            .config
            .state_dir
            .join("recovery-pending.json")
            .exists();
        let unreachable = matches!(health, Health::Unreachable { .. });
        let state = if unreachable && !pending {
            RuntimeState::Stopped
        } else {
            RuntimeState::Unresponsive
        };
        return Ok(RuntimeStatus {
            state,
            health,
            recovery_available: unreachable && pending,
            registration,
        });
    }
    if let Some(record) = &registration
        && record.socket != client.config.socket
    {
        let old = Client::named(
            Config {
                state_dir: client.config.state_dir.clone(),
                socket: record.socket.clone(),
            },
            client.daemon_name(),
        );
        if let Health::Healthy { instance_id } = old.health().await
            && instance_id == record.instance_id
        {
            return Ok(RuntimeStatus {
                state: RuntimeState::Legacy,
                health,
                recovery_available: true,
                registration,
            });
        }
    }
    let recovery_available = registration
        .as_ref()
        .is_some_and(|record| verified_owner(client, record).unwrap_or(false));
    Ok(RuntimeStatus {
        state: RuntimeState::Unresponsive,
        health,
        recovery_available,
        registration,
    })
}

pub fn unavailable(status: &RuntimeStatus) -> anyhow::Error {
    let (code, message) = match status.state {
        RuntimeState::Legacy => (
            "daemon_legacy",
            "A daemon is still running at its registered old socket; recover the runtime to restart it",
        ),
        _ => (
            "daemon_unresponsive",
            "The daemon is unavailable or its state is still owned; retry the connection or explicitly recover the runtime",
        ),
    };
    ApiError::new(code, message).into()
}

async fn wait_released(config: &Config, timeout: Duration) -> Result<bool> {
    let until = tokio::time::Instant::now() + timeout;
    loop {
        if state_available(config)? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= until {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn verify_record(client: &Client, expected: &Registration) -> Result<()> {
    let current =
        registration(client).context("daemon identity is missing; refusing forced recovery")?;
    if current.instance_id != expected.instance_id
        || current.pid != expected.pid
        || current.process_identity != expected.process_identity
    {
        bail!("daemon identity changed during recovery; retry inspection");
    }
    Ok(())
}

/// A caller must explicitly authorize ending this daemon's managed tasks.
/// Starting daemons take the shared recovery lock before acquiring the state lock.
pub async fn recover(client: &Client) -> Result<()> {
    let _recovery = lock(&recovery_path(&client.config)?, true, true)?.ok_or_else(|| {
        ApiError::new(
            "daemon_recovering",
            "Another runtime recovery is in progress",
        )
    })?;
    let status = inspect(client).await?;
    if status.state == RuntimeState::Healthy {
        return Ok(());
    }
    if status.state == RuntimeState::Stopped {
        crate::store::atomic_json(
            &client.config.state_dir.join("recovery-pending.json"),
            &serde_json::json!({ "name": client.daemon_name() }),
        )?;
        cleanup_groups(&client.config).await?;
        std::fs::remove_file(client.config.state_dir.join("recovery-pending.json"))?;
        return Ok(());
    }
    if !status.recovery_available {
        bail!("daemon process identity cannot be verified; stop the old runtime manually");
    }
    if state_available(&client.config)? {
        cleanup_groups(&client.config).await?;
        std::fs::remove_file(client.config.state_dir.join("recovery-pending.json"))?;
        return Ok(());
    }
    let record = status
        .registration
        .context("daemon identity is missing; refusing recovery")?;
    verify_record(client, &record)?;
    crate::store::atomic_json(
        &client.config.state_dir.join("recovery-pending.json"),
        &record,
    )?;
    let old = Client::named(
        Config {
            state_dir: client.config.state_dir.clone(),
            socket: record.socket.clone(),
        },
        client.daemon_name(),
    );
    let graceful = matches!(old.health().await, Health::Healthy { instance_id } if instance_id == record.instance_id);
    if graceful {
        let _ =
            tokio::time::timeout(STOP_TIMEOUT, old.do_empty(Method::POST, "/v1/admin/stop")).await;
    }
    // A responsive daemon needs time for its normal ten-second task grace period,
    // including legacy daemons without identities suitable for forced signaling.
    let release_timeout = if graceful {
        Duration::from_secs(15)
    } else {
        STOP_TIMEOUT
    };
    if !wait_released(&client.config, release_timeout).await? {
        let identity = record
            .process_identity
            .as_ref()
            .context("old daemon has no verifiable process identity; stop it manually")?;
        if identity.pid != record.pid {
            bail!("daemon PID does not match its process identity");
        }
        verify_record(client, &record)?;
        if !verified_owner(client, &record)? {
            bail!("daemon lock owner identity cannot be verified");
        }
        if !identity.alive()? {
            bail!("registered process no longer owns this identity; refusing to signal its PID");
        }
        tracing::warn!(daemon_name = client.daemon_name(), instance_id = %record.instance_id, pid = record.pid, "terminating unresponsive daemon");
        identity.signal(libc::SIGTERM, false)?;
        if !wait_released(&client.config, STOP_TIMEOUT).await? {
            verify_record(client, &record)?;
            if !verified_owner(client, &record)? {
                bail!("daemon lock owner changed before forced termination");
            }
            tracing::warn!(daemon_name = client.daemon_name(), instance_id = %record.instance_id, pid = record.pid, "forcing daemon termination");
            identity.signal(libc::SIGKILL, false)?;
            if !wait_released(&client.config, STOP_TIMEOUT).await? {
                bail!("daemon state lock was not released after termination");
            }
        }
    }
    // The lifeline thread stops process groups even when the daemon was killed.
    // Verify those groups drained before allowing a new runtime to use this state.
    cleanup_groups(&client.config).await?;
    std::fs::remove_file(client.config.state_dir.join("recovery-pending.json"))?;
    Ok(())
}

async fn cleanup_groups(config: &Config) -> Result<()> {
    let store = crate::store::Store::deferred(&config.state_dir);
    if !store.root().join("workspaces").try_exists()? {
        return Ok(());
    }
    let processes = store.list_processes(None)?;
    let shims: Vec<Option<ProcessIdentity>> = store
        .list_workspaces()?
        .iter()
        .map(|workspace| {
            crate::store::read_json(
                store
                    .workspace_dir(&workspace.id)
                    .join("shim-identity.json"),
            )
            .unwrap_or(None)
        })
        .collect();
    let until = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let mut live = false;
        for process in &processes {
            if process.pid <= 1 {
                continue;
            }
            let identity: Option<ProcessIdentity> = crate::store::read_json(
                store
                    .process_dir(&process.workspace_id, &process.id)
                    .join("identity.json"),
            )
            .unwrap_or(None);
            if let Some(identity) = identity {
                if identity.pid == process.pid && identity.alive()? {
                    live = true;
                    if tokio::time::Instant::now() >= until {
                        identity.signal(libc::SIGKILL, true)?;
                    }
                }
            } else if !process.state.terminal() && ProcessIdentity::current(process.pid)?.is_some()
            {
                live = true;
                if tokio::time::Instant::now() >= until {
                    bail!(
                        "managed process lacks a verifiable identity; refusing to restart over it"
                    );
                }
            }
            if !process.state.terminal()
                && process.boot_id == crate::config::boot_id()
                && process.process_group_id > 1
                && unsafe { libc::kill(-process.process_group_id, 0) } == 0
            {
                live = true;
            }
        }
        // Do not kill a shim before its lifeline thread has escalated signals
        // to the full process groups, including children of exited leaders.
        for identity in shims.iter().flatten() {
            if identity.alive()? {
                live = true;
                if tokio::time::Instant::now() >= until {
                    identity.signal(libc::SIGKILL, false)?;
                }
            }
        }
        if !live {
            break;
        }
        if tokio::time::Instant::now() >= until + STOP_TIMEOUT {
            bail!("managed processes did not exit during recovery");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // All writers and managed processes are gone. Preserve real exit results,
    // but finalize records whose shim died before it could persist an outcome.
    // The replacement daemon builds its first snapshot from these records.
    for mut process in store.list_processes(None)? {
        if !process.state.terminal() {
            process.state = crate::model::ProcessState::Failed;
            process.finished_at = Some(chrono::Utc::now());
            process.error = "runtime recovered before the process exit result was recorded".into();
            store.save_process(&process)?;
        }
    }
    Ok(())
}
