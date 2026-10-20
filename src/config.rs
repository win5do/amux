use std::{
    env,
    os::unix::ffi::OsStrExt,
    path::{Component, Path, PathBuf},
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
        let state_dir = normalize_state_root(root.as_ref())?
            .join("daemons")
            .join(&digest);
        // Keep the socket short while giving every data root its own namespace.
        // Preserve the existing on-disk daemon directory layout.
        let socket_digest = short_digest(state_dir.as_os_str().as_bytes());
        let runtime = env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let socket = runtime.join(format!("amux-{}-{socket_digest}.sock", unsafe {
            libc::getuid()
        }));
        Ok(Self { state_dir, socket })
    }

    pub fn lock_path(&self) -> Result<PathBuf> {
        let parent = self
            .state_dir
            .parent()
            .context("state directory has no parent")?;
        let name = self
            .state_dir
            .file_name()
            .context("state directory has no name")?
            .to_string_lossy();
        Ok(parent.join(format!(".{name}.lock")))
    }
}

fn normalize_state_root(root: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(root).context("resolve state root")?;
    let mut normalized = PathBuf::new();
    // Resolve existing prefixes, including symlinks, before appending missing
    // directories. The identity must not change when first startup creates them.
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => {
                normalized.push(component.as_os_str());
                match normalized.canonicalize() {
                    Ok(path) => normalized = path,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(error).context("normalize state root");
                    }
                }
            }
        }
    }
    Ok(normalized)
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
    short_digest(name.as_bytes())
}

fn short_digest(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_daemons_are_scoped_to_the_state_root() {
        let temp = tempfile::tempdir().unwrap();
        let first = Config::named(temp.path().join("first"), "default").unwrap();
        let second = Config::named(temp.path().join("second"), "default").unwrap();
        assert_ne!(first.socket, second.socket);
        assert_ne!(
            first.socket,
            Config::named(temp.path().join("first"), "other")
                .unwrap()
                .socket
        );
    }

    #[test]
    fn state_root_identity_survives_creation_and_path_aliases() {
        let temp = tempfile::tempdir_in(env::current_dir().unwrap()).unwrap();
        let root = temp.path().join("missing/state");
        let before = Config::named(&root, "default").unwrap();
        std::fs::create_dir_all(&root).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(temp.path().join("missing"), &alias).unwrap();
        for path in [
            root.clone(),
            root.strip_prefix(env::current_dir().unwrap())
                .unwrap()
                .to_path_buf(),
            alias.join("./state"),
            root.join("../state"),
        ] {
            let config = Config::named(path, "default").unwrap();
            assert_eq!(before.socket, config.socket);
            assert_eq!(before.state_dir, config.state_dir);
        }
    }
}
