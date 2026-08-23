use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;

pub const DEFAULT_ROTATE_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BACKUP_COUNT: usize = 2;

#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    pub rotate_size: u64,
    pub backup_count: usize,
}

impl LogConfig {
    pub fn from_env() -> Result<Self> {
        let rotate_size = match env::var("AMUX_LOG_ROTATE_SIZE") {
            Ok(value) => parse_size(&value).context("invalid AMUX_LOG_ROTATE_SIZE")?,
            Err(env::VarError::NotPresent) => DEFAULT_ROTATE_SIZE,
            Err(error) => return Err(error.into()),
        };
        let backup_count = match env::var("AMUX_LOG_BACKUP_COUNT") {
            Ok(value) => value
                .parse::<usize>()
                .ok()
                .filter(|value| *value <= 16)
                .context("invalid AMUX_LOG_BACKUP_COUNT: expected 0..16")?,
            Err(env::VarError::NotPresent) => DEFAULT_BACKUP_COUNT,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            rotate_size,
            backup_count,
        })
    }
}

pub fn parse_size(value: &str) -> Result<u64> {
    let value = value.trim();
    let (number, multiplier) = [
        ("KiB", 1024_u64),
        ("MiB", 1024_u64.pow(2)),
        ("GiB", 1024_u64.pow(3)),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| value.strip_suffix(suffix).map(|v| (v, multiplier)))
    .unwrap_or((value, 1));
    let number = number
        .parse::<u64>()
        .with_context(|| format!("invalid log size {value:?}"))?;
    let bytes = number
        .checked_mul(multiplier)
        .context("log size is too large")?;
    if bytes == 0 {
        bail!("log size must be greater than zero")
    }
    Ok(bytes)
}

/// Redirects this process's stdout and stderr through a shared rotating writer.
/// The caller's original descriptors may point at the same file; reopening by
/// pathname here is what lets a long-running process follow rotations.
pub fn install_process_log(path: impl Into<PathBuf>, config: LogConfig) -> Result<()> {
    let path = path.into();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let writer = RotatingWriter::open(path, config)?;
    let mut fds = [0_i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let read_fd = fds[0];
    let write_fd = fds[1];
    if unsafe { libc::dup2(write_fd, libc::STDOUT_FILENO) } == -1
        || unsafe { libc::dup2(write_fd, libc::STDERR_FILENO) } == -1
    {
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return Err(std::io::Error::last_os_error().into());
    }
    unsafe {
        libc::close(write_fd);
    }
    std::thread::spawn(move || {
        let mut file = unsafe { File::from_raw_fd(read_fd) };
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            match file.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    let _ = writer.write_all(&buffer[..count]);
                }
            }
        }
    });
    Ok(())
}

use std::os::fd::FromRawFd;

struct RotatingWriter {
    inner: Mutex<RotatingWriterInner>,
}

struct RotatingWriterInner {
    path: PathBuf,
    config: LogConfig,
    file: File,
    size: u64,
}

impl RotatingWriter {
    fn open(path: PathBuf, config: LogConfig) -> Result<Self> {
        let file = open_append(&path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            inner: Mutex::new(RotatingWriterInner {
                path,
                config,
                file,
                size,
            }),
        })
    }

    fn write_all(&self, bytes: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.size > 0
            && inner.size.saturating_add(bytes.len() as u64) > inner.config.rotate_size
        {
            inner.rotate()?;
        }
        inner.file.write_all(bytes)?;
        inner.file.flush()?;
        inner.size = inner.size.saturating_add(bytes.len() as u64);
        Ok(())
    }
}

impl RotatingWriterInner {
    fn rotate(&mut self) -> Result<()> {
        self.file.flush()?;
        if self.config.backup_count == 0 {
            self.file = open_truncate(&self.path)?;
        } else {
            // Make room before publishing the new backup so an abrupt process
            // exit can never leave more files than the configured bound.
            prune_file_backups(&self.path, self.config.backup_count - 1)?;
            let backup = unique_backup_path(&self.path);
            fs::rename(&self.path, backup)?;
            self.file = open_append(&self.path)?;
        }
        self.size = 0;
        Ok(())
    }
}

fn open_append(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?)
}

fn open_truncate(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?)
}

fn unique_backup_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%.9fZ");
    for suffix in 0_u32.. {
        let extra = if suffix == 0 {
            String::new()
        } else {
            format!("-{suffix}")
        };
        let candidate = parent.join(format!("{name}.{timestamp}{extra}.bak"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}

fn prune_file_backups(path: &Path, keep: usize) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let prefix = format!(
        "{}.",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    let mut backups = fs::read_dir(parent)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(&prefix) && name.ends_with(".bak")
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    backups.sort();
    let remove = backups.len().saturating_sub(keep);
    for backup in backups.into_iter().take(remove) {
        fs::remove_file(backup)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sizes_and_limits() {
        assert_eq!(parse_size("1").unwrap(), 1);
        assert_eq!(parse_size("2KiB").unwrap(), 2048);
        assert_eq!(parse_size("3MiB").unwrap(), 3 * 1024 * 1024);
        assert_eq!(parse_size("4GiB").unwrap(), 4 * 1024 * 1024 * 1024);
        assert!(parse_size("0").is_err());
        assert!(parse_size("1MB").is_err());
    }

    #[test]
    fn rotates_a_live_file_and_prunes_backups() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("shim.log");
        let writer = RotatingWriter::open(
            path.clone(),
            LogConfig {
                rotate_size: 5,
                backup_count: 2,
            },
        )
        .unwrap();
        for value in [b"one".as_slice(), b"two", b"three", b"four"] {
            writer.write_all(value).unwrap();
        }
        let backups = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".bak"))
            .count();
        assert_eq!(backups, 2);
        assert_eq!(fs::read(path).unwrap(), b"four");
    }
}
