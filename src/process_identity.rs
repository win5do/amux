//! OS process identities used only for verified recovery, never bare PID signaling.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: i32,
    pub uid: u32,
    pub started: String,
    pub boot_id: String,
    pub group: i32,
}

impl ProcessIdentity {
    pub fn current(pid: i32) -> Result<Option<Self>> {
        if pid <= 1 {
            return Ok(None);
        }
        inspect(pid)
    }

    pub fn alive(&self) -> Result<bool> {
        Ok(Self::current(self.pid)?.as_ref() == Some(self))
    }

    pub fn signal(&self, signal: i32, group: bool) -> Result<()> {
        if self.uid != unsafe { libc::getuid() } || self.pid == std::process::id() as i32 {
            bail!("refusing to signal an unowned process");
        }
        if !self.alive()? {
            return Ok(());
        }
        if group && self.group != self.pid {
            bail!("process is not its group leader");
        }
        let target = if group { -self.group } else { self.pid };
        if unsafe { libc::kill(target, signal) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn inspect(pid: i32) -> Result<Option<ProcessIdentity>> {
    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_pidinfo(
            pid: i32,
            flavor: i32,
            arg: u64,
            buffer: *mut libc::c_void,
            size: i32,
        ) -> i32;
    }
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let read = unsafe {
        proc_pidinfo(
            pid,
            3,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if read == 0 {
        let error = std::io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(libc::ESRCH | libc::ENOENT)) {
            return Ok(None);
        }
        return Err(error).context("inspect process identity");
    }
    if read != size {
        bail!("incomplete process identity");
    }
    if info.pbi_status == 5 {
        return Ok(None);
    } // SZOMB
    Ok(Some(ProcessIdentity {
        pid,
        uid: info.pbi_uid,
        started: format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
        boot_id: crate::config::boot_id(),
        group: info.pbi_pgid as i32,
    }))
}

#[cfg(target_os = "linux")]
fn inspect(pid: i32) -> Result<Option<ProcessIdentity>> {
    use std::os::unix::fs::MetadataExt;
    let path = format!("/proc/{pid}");
    let contents = match std::fs::read_to_string(format!("{path}/stat")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let fields: Vec<_> = contents[contents.rfind(')').context("invalid process stat")? + 1..]
        .split_whitespace()
        .collect();
    if fields.first() == Some(&"Z") {
        return Ok(None);
    }
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(ProcessIdentity {
        pid,
        uid: metadata.uid(),
        started: fields
            .get(19)
            .context("missing process start time")?
            .to_string(),
        boot_id: crate::config::boot_id(),
        group: fields.get(2).context("missing process group")?.parse()?,
    }))
}
