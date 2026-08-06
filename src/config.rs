use std::{env, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub state_dir: PathBuf,
    pub socket: PathBuf,
}

impl Config {
    pub fn load() -> Result<Self> {
        let state_dir = match env::var_os("AMUX_STATE_DIR") {
            Some(path) => PathBuf::from(path),
            None => {
                if let Some(path) = env::var_os("XDG_STATE_HOME") {
                    PathBuf::from(path).join("amux")
                } else {
                    let home = env::var_os("HOME").context("HOME is not set")?;
                    PathBuf::from(home).join(".local/state/amux")
                }
            }
        };
        let socket = env::var_os("AMUX_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                if let Some(path) = env::var_os("XDG_RUNTIME_DIR") {
                    PathBuf::from(path).join("amux/amuxd.sock")
                } else {
                    std::env::temp_dir()
                        .join(format!("amux-{}/amuxd.sock", unsafe { libc::getuid() }))
                }
            });
        Ok(Self { state_dir, socket })
    }
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
