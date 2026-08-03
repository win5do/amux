package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/win5do/amux/internal/model"
)

// ProcessHandle is opaque to the control plane. Runtime-specific identifiers
// are persisted in Process.RuntimeHandle.
type ProcessHandle struct{ Runtime, ID string }
type Runtime interface {
	Name() string
	PrepareWorkspace(context.Context, model.Workspace) error
	InspectProcess(context.Context, ProcessHandle) (model.Process, error)
	SignalProcess(context.Context, ProcessHandle, string) error
	StopProcess(context.Context, ProcessHandle, time.Duration) error
	RemoveWorkspace(context.Context, model.Workspace) error
}
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Runtime
}

func NewRegistry() *Registry { return &Registry{drivers: map[string]Runtime{}} }
func (r *Registry) Register(v Runtime) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v == nil || v.Name() == "" {
		return fmt.Errorf("runtime must have a name")
	}
	if _, ok := r.drivers[v.Name()]; ok {
		return fmt.Errorf("runtime %q already registered", v.Name())
	}
	r.drivers[v.Name()] = v
	return nil
}
func (r *Registry) Get(name string) (Runtime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.drivers[name]
	return v, ok
}
