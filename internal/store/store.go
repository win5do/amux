package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/win5do/amux/internal/model"
)

var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

func ValidName(s string) bool { return validName.MatchString(s) }
func ID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

type Store struct {
	Root string
	mu   sync.RWMutex
}

func Open(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "workspaces"), 0700); err != nil {
		return nil, err
	}
	_ = os.Chmod(root, 0700)
	return &Store{Root: root}, nil
}
func atomicJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if e != nil {
		return e
	}
	n := f.Name()
	defer os.Remove(n)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(n, path)
}
func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func (s *Store) WorkspaceDir(id string) string { return filepath.Join(s.Root, "workspaces", id) }
func (s *Store) ProcessDir(wid, pid string) string {
	return filepath.Join(s.WorkspaceDir(wid), "processes", pid)
}
func (s *Store) CreateWorkspace(req model.CreateWorkspaceRequest) (model.Workspace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ValidName(req.Name) {
		return model.Workspace{}, fmt.Errorf("invalid name")
	}
	all, _ := s.listWorkspaces()
	for _, w := range all {
		if w.Name == req.Name {
			return model.Workspace{}, fmt.Errorf("name conflict")
		}
	}
	now := time.Now().UTC()
	w := model.Workspace{SchemaVersion: 1, ID: ID("ws"), Name: req.Name, Runtime: req.Runtime, RootDir: req.RootDir, Env: req.Env, State: model.WorkspaceIdle, CreatedAt: now, UpdatedAt: now}
	if w.Runtime == "" {
		w.Runtime = "host"
	}
	if w.Runtime != "host" {
		return model.Workspace{}, fmt.Errorf("runtime not supported")
	}
	d := s.WorkspaceDir(w.ID)
	if e := os.MkdirAll(filepath.Join(d, "processes"), 0700); e != nil {
		return w, e
	}
	g := model.Group{ID: ID("grp"), WorkspaceID: w.ID, Name: "default", CreatedAt: now}
	env := model.Environment{ID: ID("env"), WorkspaceID: w.ID, GroupID: g.ID, Name: "host", CreatedAt: now}
	for p, v := range map[string]any{filepath.Join(d, "metadata.json"): w, filepath.Join(d, "group.json"): g, filepath.Join(d, "environment.json"): env} {
		if e := atomicJSON(p, v); e != nil {
			return w, e
		}
	}
	return w, nil
}
func (s *Store) listWorkspaces() ([]model.Workspace, error) {
	ents, e := os.ReadDir(filepath.Join(s.Root, "workspaces"))
	if e != nil {
		return nil, e
	}
	var out []model.Workspace
	for _, x := range ents {
		if !x.IsDir() {
			continue
		}
		var w model.Workspace
		if readJSON(filepath.Join(s.WorkspaceDir(x.Name()), "metadata.json"), &w) == nil {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func (s *Store) ListWorkspaces() ([]model.Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listWorkspaces()
}
func (s *Store) FindWorkspace(target string) (model.Workspace, error) {
	ws, e := s.ListWorkspaces()
	if e != nil {
		return model.Workspace{}, e
	}
	for _, w := range ws {
		if w.ID == target || w.Name == target {
			return w, nil
		}
	}
	return model.Workspace{}, os.ErrNotExist
}
func (s *Store) Defaults(wid string) (model.Group, model.Environment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var g model.Group
	var e model.Environment
	if x := readJSON(filepath.Join(s.WorkspaceDir(wid), "group.json"), &g); x != nil {
		return g, e, x
	}
	x := readJSON(filepath.Join(s.WorkspaceDir(wid), "environment.json"), &e)
	return g, e, x
}
func (s *Store) SaveWorkspace(w model.Workspace) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.UpdatedAt = time.Now().UTC()
	return atomicJSON(filepath.Join(s.WorkspaceDir(w.ID), "metadata.json"), w)
}
func (s *Store) CreateProcess(w model.Workspace, r model.RunRequest, boot string) (model.Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ValidName(r.Name) || len(r.Command) == 0 {
		return model.Process{}, fmt.Errorf("invalid process")
	}
	ps, _ := s.listProcessesLocked(w.ID)
	for _, p := range ps {
		if p.Name == r.Name {
			return p, fmt.Errorf("name conflict")
		}
	}
	var g model.Group
	var env model.Environment
	if e := readJSON(filepath.Join(s.WorkspaceDir(w.ID), "group.json"), &g); e != nil {
		return model.Process{}, e
	}
	if e := readJSON(filepath.Join(s.WorkspaceDir(w.ID), "environment.json"), &env); e != nil {
		return model.Process{}, e
	}
	now := time.Now().UTC()
	p := model.Process{SchemaVersion: 1, ID: ID("proc"), WorkspaceID: w.ID, GroupID: g.ID, EnvironmentID: env.ID, Name: r.Name, Command: r.Command, Cwd: r.Cwd, Env: r.Env, IOMode: r.IOMode, Runtime: "host", State: model.ProcessStarting, Execution: 1, BootID: boot, CreatedAt: now}
	if p.IOMode == "" {
		p.IOMode = model.IOPipe
	}
	d := s.ProcessDir(w.ID, p.ID)
	if e := os.MkdirAll(d, 0700); e != nil {
		return p, e
	}
	return p, atomicJSON(filepath.Join(d, "metadata.json"), p)
}
func (s *Store) SaveProcess(p model.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicJSON(filepath.Join(s.ProcessDir(p.WorkspaceID, p.ID), "metadata.json"), p)
}
func (s *Store) SaveRuntimeEnv(p model.Process, env map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicJSON(filepath.Join(s.ProcessDir(p.WorkspaceID, p.ID), "runtime-env.json"), env)
}
func (s *Store) listProcessesLocked(wid string) ([]model.Process, error) {
	d := filepath.Join(s.WorkspaceDir(wid), "processes")
	ents, e := os.ReadDir(d)
	if errors.Is(e, os.ErrNotExist) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var out []model.Process
	for _, x := range ents {
		var p model.Process
		if x.IsDir() && readJSON(filepath.Join(d, x.Name(), "metadata.json"), &p) == nil {
			out = append(out, p)
		}
	}
	return out, nil
}
func (s *Store) ListProcesses(wid string) ([]model.Process, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if wid != "" {
		return s.listProcessesLocked(wid)
	}
	ws, e := s.listWorkspaces()
	if e != nil {
		return nil, e
	}
	var out []model.Process
	for _, w := range ws {
		p, _ := s.listProcessesLocked(w.ID)
		out = append(out, p...)
	}
	return out, nil
}
func (s *Store) FindProcess(target string) (model.Process, error) {
	ps, e := s.ListProcesses("")
	if e != nil {
		return model.Process{}, e
	}
	for _, p := range ps {
		if p.ID == target {
			return p, nil
		}
	}
	parts := regexp.MustCompile(`/`).Split(target, 2)
	if len(parts) == 2 {
		w, e := s.FindWorkspace(parts[0])
		if e == nil {
			for _, p := range ps {
				if p.WorkspaceID == w.ID && p.Name == parts[1] {
					return p, nil
				}
			}
		}
	}
	return model.Process{}, os.ErrNotExist
}
func (s *Store) RemoveProcess(p model.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.RemoveAll(s.ProcessDir(p.WorkspaceID, p.ID))
}
func (s *Store) RemoveWorkspace(w model.Workspace) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.RemoveAll(s.WorkspaceDir(w.ID))
}
