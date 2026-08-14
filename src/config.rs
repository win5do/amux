use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct Config {
    pub state_dir: PathBuf,
    pub socket: PathBuf,
}

impl Config {
    pub fn load() -> Result<Self> {
        let root = state_root()?;
        if let Some(socket) = env::var_os("AMUX_SOCKET") {
            return Ok(Self {
                state_dir: root,
                socket: PathBuf::from(socket),
            });
        }
        Self::named(root, &daemon_name())
    }

    pub fn named(root: impl AsRef<Path>, name: &str) -> Result<Self> {
        validate_daemon_name(name)?;
        let digest = daemon_digest(name);
        let state_dir = root.as_ref().join("daemons").join(&digest);
        let runtime = env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let socket = runtime.join(format!("amux-{}-{digest}.sock", unsafe { libc::getuid() }));
        Ok(Self { state_dir, socket })
    }
}

pub fn state_root() -> Result<PathBuf> {
    Ok(match env::var_os("AMUX_STATE_DIR") {
        Some(path) => PathBuf::from(path),
        None => {
            if let Some(path) = env::var_os("XDG_STATE_HOME") {
                PathBuf::from(path).join("amux")
            } else {
                PathBuf::from(env::var_os("HOME").context("HOME is not set")?)
                    .join(".local/state/amux")
            }
        }
    })
}

pub fn daemon_name() -> String {
    env::var("AMUX_DAEMON")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "default".into())
}

pub fn validate_daemon_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        bail!("invalid daemon name")
    }
    Ok(())
}

pub fn daemon_digest(name: &str) -> String {
    let digest = Sha256::digest(name.as_bytes());
    digest[..12].iter().map(|v| format!("{v:02x}")).collect()
}

pub fn boot_id() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(id) = std::fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        return id.trim().to_owned();
    }
    #[cfg(target_os = "macos")]
    {
        let mut size = std::mem::size_of::<libc::timeval>();
        let mut tv: libc::timeval = unsafe { std::mem::zeroed() };
        let name = b"kern.boottime\0";
        let ok = unsafe {
            libc::sysctlbyname(
                name.as_ptr().cast(),
                (&mut tv as *mut libc::timeval).cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        if ok == 0 {
            return format!("{}:{}", tv.tv_sec, tv.tv_usec);
        }
    }
    "unknown".to_owned()
}
