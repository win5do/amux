package model

import "time"

const SchemaVersion = 1

type WorkspaceState string

const (
	WorkspaceCreated  WorkspaceState = "created"
	WorkspaceActive   WorkspaceState = "active"
	WorkspaceIdle     WorkspaceState = "idle"
	WorkspaceRemoving WorkspaceState = "removing"
	WorkspaceError    WorkspaceState = "error"
)

type ProcessState string

const (
	ProcessCreated  ProcessState = "created"
	ProcessStarting ProcessState = "starting"
	ProcessRunning  ProcessState = "running"
	ProcessStopping ProcessState = "stopping"
	ProcessExited   ProcessState = "exited"
	ProcessFailed   ProcessState = "failed"
	ProcessUnknown  ProcessState = "unknown"
)

type IOMode string

const (
	IOPipe IOMode = "pipe"
	IOTTY  IOMode = "tty"
)

type Workspace struct {
	SchemaVersion int               `json:"schema_version"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Runtime       string            `json:"runtime"`
	RootDir       string            `json:"root_dir,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	State         WorkspaceState    `json:"state"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}
type Group struct {
	ID          string            `json:"id"`
	WorkspaceID string            `json:"workspace_id"`
	Name        string            `json:"name"`
	Placement   map[string]string `json:"placement,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}
type MountSpec struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only,omitempty"`
}
type Environment struct {
	ID          string            `json:"id"`
	WorkspaceID string            `json:"workspace_id"`
	GroupID     string            `json:"group_id"`
	Name        string            `json:"name"`
	Image       string            `json:"image,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Mounts      []MountSpec       `json:"mounts,omitempty"`
	RuntimeData map[string]string `json:"runtime_data,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

type Process struct {
	SchemaVersion  int               `json:"schema_version"`
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	GroupID        string            `json:"group_id"`
	EnvironmentID  string            `json:"environment_id"`
	Name           string            `json:"name"`
	Command        []string          `json:"command"`
	Cwd            string            `json:"cwd,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	IOMode         IOMode            `json:"io_mode"`
	Runtime        string            `json:"runtime"`
	PID            int               `json:"pid,omitempty"`
	ProcessGroupID int               `json:"process_group_id,omitempty"`
	RuntimeHandle  string            `json:"runtime_handle,omitempty"`
	State          ProcessState      `json:"state"`
	ExitCode       *int              `json:"exit_code,omitempty"`
	ExitSignal     string            `json:"exit_signal,omitempty"`
	Error          string            `json:"error,omitempty"`
	Execution      uint64            `json:"execution"`
	BootID         string            `json:"boot_id,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	FinishedAt     *time.Time        `json:"finished_at,omitempty"`
}
type OutputEvent struct {
	Sequence  uint64       `json:"sequence"`
	Execution uint64       `json:"execution"`
	Timestamp time.Time    `json:"timestamp"`
	Stream    string       `json:"stream"`
	Offset    int64        `json:"offset"`
	Length    int          `json:"length"`
	Data      *EncodedData `json:"data,omitempty"`
}
type EncodedData struct {
	Encoding string `json:"encoding"`
	Text     string `json:"text,omitempty"`
	Base64   string `json:"base64,omitempty"`
}

type CreateWorkspaceRequest struct {
	Name    string            `json:"name"`
	Runtime string            `json:"runtime,omitempty"`
	RootDir string            `json:"root_dir,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
}
type RunRequest struct {
	Name    string            `json:"name"`
	Command []string          `json:"command"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	IOMode  IOMode            `json:"io_mode,omitempty"`
	Runtime string            `json:"runtime,omitempty"`
}
type SignalRequest struct {
	Signal string `json:"signal"`
}
type StopRequest struct {
	GraceMillis int64 `json:"grace_millis,omitempty"`
}

func (p Process) Terminal() bool { return p.State == ProcessExited || p.State == ProcessFailed }
