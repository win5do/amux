use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceState {
    Created,
    Active,
    Idle,
    Removing,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Created,
    Starting,
    Running,
    Stopping,
    Exited,
    Failed,
    Unknown,
}

impl ProcessState {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Exited | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IoMode {
    #[default]
    Pipe,
    Tty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub runtime: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root_dir: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub placement: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountSpec {
    pub source: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: String,
    pub workspace_id: String,
    pub group_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<MountSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub runtime_data: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Process {
    pub schema_version: u32,
    pub id: String,
    pub workspace_id: String,
    pub group_id: String,
    pub environment_id: String,
    pub name: String,
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cwd: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    pub io_mode: IoMode,
    pub runtime: String,
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub pid: i32,
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub process_group_id: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub runtime_handle: String,
    pub state: ProcessState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub exit_signal: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub execution: u64,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub initial_rows: u16,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub initial_cols: u16,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub boot_id: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessView {
    #[serde(flatten)]
    pub process: Process,
    pub workspace_name: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputEvent {
    pub sequence: u64,
    pub execution: u64,
    pub timestamp: DateTime<Utc>,
    pub stream: String,
    pub offset: i64,
    pub length: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<EncodedData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedData {
    pub encoding: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base64: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateWorkspaceRequest {
    pub name: String,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub root_dir: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunRequest {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub io_mode: IoMode,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub initial_rows: u16,
    #[serde(default)]
    pub initial_cols: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRequest {
    pub signal: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StopRequest {
    #[serde(default)]
    pub grace_millis: i64,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}
