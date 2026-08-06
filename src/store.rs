use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};

use crate::model::*;

pub struct Store {
    root: PathBuf,
    lock: Mutex<()>,
}

impl Store {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("workspaces"))?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            root,
            lock: Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn workspace_dir(&self, id: &str) -> PathBuf {
        self.root.join("workspaces").join(id)
    }
    pub fn process_dir(&self, wid: &str, pid: &str) -> PathBuf {
        self.workspace_dir(wid).join("processes").join(pid)
    }

    pub fn create_workspace(&self, req: CreateWorkspaceRequest) -> Result<Workspace> {
        let _guard = self.lock.lock().unwrap();
        if !valid_name(&req.name) {
            bail!("invalid name")
        }
        if self
            .list_workspaces_unlocked()?
            .iter()
            .any(|w| w.name == req.name)
        {
            bail!("name conflict")
        }
        let runtime = if req.runtime.is_empty() {
            "host".to_owned()
        } else {
            req.runtime
        };
        if runtime != "host" {
            bail!("runtime not supported")
        }
        let now = Utc::now();
        let workspace = Workspace {
            schema_version: SCHEMA_VERSION,
            id: id("ws"),
            name: req.name,
            runtime,
            root_dir: req.root_dir,
            env: req.env,
            state: WorkspaceState::Idle,
            created_at: now,
            updated_at: now,
        };
        let group = Group {
            id: id("grp"),
            workspace_id: workspace.id.clone(),
            name: "default".into(),
            placement: Default::default(),
            created_at: now,
        };
        let environment = Environment {
            id: id("env"),
            workspace_id: workspace.id.clone(),
            group_id: group.id.clone(),
            name: "host".into(),
            image: String::new(),
            env: Default::default(),
            mounts: vec![],
            runtime_data: Default::default(),
            created_at: now,
        };
        let dir = self.workspace_dir(&workspace.id);
        fs::create_dir_all(dir.join("processes"))?;
        atomic_json(&dir.join("metadata.json"), &workspace)?;
        atomic_json(&dir.join("group.json"), &group)?;
        atomic_json(&dir.join("environment.json"), &environment)?;
        Ok(workspace)
    }

    pub fn list_workspaces(&self) -> Result<Vec<Workspace>> {
        let _guard = self.lock.lock().unwrap();
        self.list_workspaces_unlocked()
    }

    fn list_workspaces_unlocked(&self) -> Result<Vec<Workspace>> {
        let mut out: Vec<Workspace> = Vec::new();
        for entry in fs::read_dir(self.root.join("workspaces"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Ok(value) = read_json(entry.path().join("metadata.json"))
            {
                out.push(value);
            }
        }
        out.sort_by_key(|w| w.created_at);
        Ok(out)
    }

    pub fn find_workspace(&self, target: &str) -> Result<Workspace> {
        self.list_workspaces()?
            .into_iter()
            .find(|w| w.id == target || w.name == target)
            .ok_or_else(|| anyhow!("workspace not found"))
    }

    pub fn defaults(&self, wid: &str) -> Result<(Group, Environment)> {
        let dir = self.workspace_dir(wid);
        Ok((
            read_json(dir.join("group.json"))?,
            read_json(dir.join("environment.json"))?,
        ))
    }

    pub fn save_workspace(&self, workspace: &mut Workspace) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        workspace.updated_at = Utc::now();
        atomic_json(
            &self.workspace_dir(&workspace.id).join("metadata.json"),
            workspace,
        )
    }

    pub fn create_process(
        &self,
        workspace: &Workspace,
        req: RunRequest,
        boot_id: String,
    ) -> Result<Process> {
        let _guard = self.lock.lock().unwrap();
        if !valid_name(&req.name) || req.command.is_empty() {
            bail!("invalid process")
        }
        if self
            .list_processes_unlocked(&workspace.id)?
            .iter()
            .any(|p| p.name == req.name)
        {
            bail!("name conflict")
        }
        let (group, env): (Group, Environment) = (
            read_json(self.workspace_dir(&workspace.id).join("group.json"))?,
            read_json(self.workspace_dir(&workspace.id).join("environment.json"))?,
        );
        let now = Utc::now();
        let (rows, cols) = if req.io_mode == IoMode::Tty {
            (
                if req.initial_rows == 0 {
                    40
                } else {
                    req.initial_rows
                },
                if req.initial_cols == 0 {
                    120
                } else {
                    req.initial_cols
                },
            )
        } else {
            (0, 0)
        };
        let process = Process {
            schema_version: SCHEMA_VERSION,
            id: id("proc"),
            workspace_id: workspace.id.clone(),
            group_id: group.id,
            environment_id: env.id,
            name: req.name,
            command: req.command,
            cwd: req.cwd,
            env: req.env,
            io_mode: req.io_mode,
            runtime: "host".into(),
            pid: 0,
            process_group_id: 0,
            runtime_handle: String::new(),
            state: ProcessState::Starting,
            exit_code: None,
            exit_signal: String::new(),
            error: String::new(),
            execution: 1,
            initial_rows: rows,
            initial_cols: cols,
            boot_id,
            created_at: now,
            started_at: None,
            finished_at: None,
        };
        let dir = self.process_dir(&process.workspace_id, &process.id);
        fs::create_dir_all(&dir)?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        atomic_json(&dir.join("metadata.json"), &process)?;
        Ok(process)
    }

    pub fn save_process(&self, process: &Process) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        atomic_json(
            &self
                .process_dir(&process.workspace_id, &process.id)
                .join("metadata.json"),
            process,
        )
    }

    pub fn save_runtime_env(
        &self,
        process: &Process,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        atomic_json(
            &self
                .process_dir(&process.workspace_id, &process.id)
                .join("runtime-env.json"),
            env,
        )
    }

    pub fn list_processes(&self, wid: Option<&str>) -> Result<Vec<Process>> {
        let _guard = self.lock.lock().unwrap();
        if let Some(wid) = wid {
            return self.list_processes_unlocked(wid);
        }
        let mut out = vec![];
        for workspace in self.list_workspaces_unlocked()? {
            out.extend(self.list_processes_unlocked(&workspace.id)?);
        }
        Ok(out)
    }

    fn list_processes_unlocked(&self, wid: &str) -> Result<Vec<Process>> {
        let dir = self.workspace_dir(wid).join("processes");
        let Ok(entries) = fs::read_dir(dir) else {
            return Ok(vec![]);
        };
        let mut out = vec![];
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Ok(process) = read_json(entry.path().join("metadata.json"))
            {
                out.push(process);
            }
        }
        Ok(out)
    }

    pub fn find_process(&self, target: &str) -> Result<Process> {
        let processes = self.list_processes(None)?;
        if let Some(process) = processes.iter().find(|p| p.id == target) {
            return Ok(process.clone());
        }
        if let Some((workspace, name)) = target.split_once('/') {
            let workspace = self.find_workspace(workspace)?;
            if let Some(process) = processes
                .into_iter()
                .find(|p| p.workspace_id == workspace.id && p.name == name)
            {
                return Ok(process);
            }
        }
        bail!("process not found")
    }

    pub fn remove_process(&self, process: &Process) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        fs::remove_dir_all(self.process_dir(&process.workspace_id, &process.id))?;
        Ok(())
    }

    pub fn remove_workspace(&self, workspace: &Workspace) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        fs::remove_dir_all(self.workspace_dir(&workspace.id))?;
        Ok(())
    }
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("path has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    Ok(())
}

pub fn append_line(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

pub fn append_raw(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

pub fn valid_name(value: &str) -> bool {
    Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
        .unwrap()
        .is_match(value)
}

fn id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let workspace = store
            .create_workspace(CreateWorkspaceRequest {
                name: "dev".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(store.find_workspace("dev").unwrap().id, workspace.id);
        let process = store
            .create_process(
                &workspace,
                RunRequest {
                    name: "api".into(),
                    command: vec!["true".into()],
                    ..Default::default()
                },
                "boot".into(),
            )
            .unwrap();
        assert_eq!(store.find_process("dev/api").unwrap().id, process.id);
    }
}
