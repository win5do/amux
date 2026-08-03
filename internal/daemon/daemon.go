//go:build unix

package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/win5do/amux/internal/config"
	"github.com/win5do/amux/internal/model"
	"github.com/win5do/amux/internal/protocol"
	hostruntime "github.com/win5do/amux/internal/runtime/host"
	"github.com/win5do/amux/internal/store"
)

type Daemon struct {
	cfg      config.Config
	st       *store.Store
	server   *http.Server
	mu       sync.Mutex
	shimPath string
	lockFile *os.File
}

func New(cfg config.Config) (*Daemon, error) {
	st, e := store.Open(cfg.StateDir)
	if e != nil {
		return nil, e
	}
	return &Daemon{cfg: cfg, st: st}, nil
}
func (d *Daemon) Serve(ctx context.Context, ready *os.File) error {
	if e := os.MkdirAll(filepath.Dir(d.cfg.Socket), 0700); e != nil {
		return e
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(d.cfg.Socket), "amuxd.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return fmt.Errorf("another amuxd owns the socket: %w", e)
	}
	d.lockFile = lock
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); _ = lock.Close() }()
	_ = os.Remove(d.cfg.Socket)
	ln, e := net.Listen("unix", d.cfg.Socket)
	if e != nil {
		return e
	}
	_ = os.Chmod(d.cfg.Socket, 0600)
	m := http.NewServeMux()
	m.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]any{"ok": true}) })
	m.HandleFunc("/v1/workspaces", d.workspaces)
	m.HandleFunc("/v1/workspaces/", d.workspace)
	m.HandleFunc("/v1/processes", d.processes)
	m.HandleFunc("/v1/processes/", d.process)
	m.HandleFunc("/v1/groups", d.groups)
	m.HandleFunc("/v1/environments", d.environments)
	d.server = &http.Server{Handler: m}
	if ready != nil {
		_, _ = ready.Write([]byte{1})
		_ = ready.Close()
	}
	go func() { <-ctx.Done(); _ = d.server.Shutdown(context.Background()) }()
	e = d.server.Serve(ln)
	_ = os.Remove(d.cfg.Socket)
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func (d *Daemon) workspaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ws, e := d.st.ListWorkspaces()
		if e != nil {
			fail(w, 500, "internal_error", e.Error())
			return
		}
		for i := range ws {
			d.updateWorkspace(&ws[i])
		}
		write(w, 200, protocol.Response{OK: true, Workspaces: ws})
	case http.MethodPost:
		var q model.CreateWorkspaceRequest
		if decode(r, &q) != nil {
			fail(w, 400, "invalid_argument", "invalid JSON")
			return
		}
		x, e := d.st.CreateWorkspace(q)
		if e != nil {
			code := "invalid_argument"
			if strings.Contains(e.Error(), "conflict") {
				code = "name_conflict"
			}
			if strings.Contains(e.Error(), "supported") {
				code = "runtime_not_supported"
			}
			fail(w, 409, code, e.Error())
			return
		}
		write(w, 201, protocol.Response{OK: true, Workspace: x})
	default:
		w.WriteHeader(405)
	}
}
func (d *Daemon) workspace(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/workspaces/")
	parts := strings.Split(rest, "/")
	ws, e := d.st.FindWorkspace(parts[0])
	if e != nil {
		fail(w, 404, "workspace_not_found", "workspace was not found")
		return
	}
	if len(parts) == 2 && parts[1] == "processes" && r.Method == http.MethodPost {
		d.run(w, r, ws)
		return
	}
	switch r.Method {
	case http.MethodGet:
		d.updateWorkspace(&ws)
		g, env, _ := d.st.Defaults(ws.ID)
		write(w, 200, map[string]any{"ok": true, "workspace": ws, "groups": []model.Group{g}, "environments": []model.Environment{env}})
	case http.MethodDelete:
		d.removeWorkspace(w, r, ws)
	default:
		w.WriteHeader(405)
	}
}
func (d *Daemon) groups(w http.ResponseWriter, r *http.Request) {
	ws, _ := d.st.ListWorkspaces()
	var out []model.Group
	for _, x := range ws {
		g, _, e := d.st.Defaults(x.ID)
		if e == nil {
			out = append(out, g)
		}
	}
	write(w, 200, protocol.Response{OK: true, Groups: out})
}
func (d *Daemon) environments(w http.ResponseWriter, r *http.Request) {
	ws, _ := d.st.ListWorkspaces()
	var out []model.Environment
	for _, x := range ws {
		_, e1, e := d.st.Defaults(x.ID)
		if e == nil {
			out = append(out, e1)
		}
	}
	write(w, 200, protocol.Response{OK: true, Environments: out})
}
func (d *Daemon) run(w http.ResponseWriter, r *http.Request, ws model.Workspace) {
	var q model.RunRequest
	if decode(r, &q) != nil || len(q.Command) == 0 {
		fail(w, 400, "invalid_argument", "name and command are required")
		return
	}
	if q.Runtime != "" && q.Runtime != ws.Runtime {
		fail(w, 400, "invalid_argument", "process runtime must match workspace runtime")
		return
	}
	explicitEnv := q.Env
	runtimeEnv := mergeEnv(os.Environ(), ws.Env, explicitEnv)
	q.Env = explicitEnv
	if q.Cwd == "" {
		q.Cwd = ws.RootDir
	}
	p, e := d.st.CreateProcess(ws, q, hostruntime.BootID())
	if e != nil {
		code := "invalid_argument"
		if strings.Contains(e.Error(), "conflict") {
			code = "name_conflict"
		}
		fail(w, 409, code, e.Error())
		return
	}
	if e = d.st.SaveRuntimeEnv(p, runtimeEnv); e != nil {
		fail(w, 500, "internal_error", e.Error())
		return
	}
	if e = d.launchShim(&p); e != nil {
		fail(w, 500, "process_start_failed", e.Error())
		return
	}
	ws.State = model.WorkspaceActive
	_ = d.st.SaveWorkspace(ws)
	write(w, 201, protocol.Response{OK: true, Process: p})
}
func mergeEnv(base []string, maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, v := range base {
		if i := strings.IndexByte(v, '='); i > 0 {
			if strings.HasSuffix(v[:i], "_READY_FD") {
				continue
			}
			out[v[:i]] = v[i+1:]
		}
	}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}
func (d *Daemon) shimExecutable() (string, error) {
	if d.shimPath != "" {
		return d.shimPath, nil
	}
	self, e := os.Executable()
	if e == nil {
		p := filepath.Join(filepath.Dir(self), "amux-shim")
		if x := exec.Command(p).Path; fileExists(x) {
			d.shimPath = p
			return p, nil
		}
	}
	p, e := exec.LookPath("amux-shim")
	if e == nil {
		d.shimPath = p
	}
	return p, e
}
func fileExists(p string) bool { s, e := os.Stat(p); return e == nil && !s.IsDir() }
func (d *Daemon) launchShim(p *model.Process) error {
	bin, e := d.shimExecutable()
	if e != nil {
		return fmt.Errorf("find amux-shim: %w", e)
	}
	rd, wr, e := os.Pipe()
	if e != nil {
		return e
	}
	defer rd.Close()
	logf, e := os.OpenFile(filepath.Join(d.st.ProcessDir(p.WorkspaceID, p.ID), "shim.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		wr.Close()
		return e
	}
	defer logf.Close()
	sock := d.shimSocket(*p)
	p.RuntimeHandle = sock
	if e := d.st.SaveProcess(*p); e != nil {
		wr.Close()
		return e
	}
	cmd := exec.Command(bin, "--process-dir", d.st.ProcessDir(p.WorkspaceID, p.ID), "--socket", sock)
	cmd.Env = append(os.Environ(), "AMUX_SHIM_READY_FD=3")
	cmd.ExtraFiles = []*os.File{wr}
	cmd.Stdin = nil
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if e = cmd.Start(); e != nil {
		wr.Close()
		return e
	}
	wr.Close()
	done := make(chan error, 1)
	go func() { b := make([]byte, 1); _, x := io.ReadFull(rd, b); done <- x }()
	select {
	case e = <-done:
	case <-time.After(5 * time.Second):
		e = fmt.Errorf("shim readiness timeout")
	}
	if e != nil {
		_ = cmd.Process.Kill()
		return e
	}
	_ = cmd.Process.Release()
	fresh, e := d.shimStatus(*p)
	if e == nil {
		*p = fresh
	}
	return e
}
func (d *Daemon) shimClient(p model.Process) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", d.shimSocket(p))
	}}}
}
func (d *Daemon) shimSocket(p model.Process) string {
	if p.RuntimeHandle != "" {
		return p.RuntimeHandle
	}
	// Darwin's sockaddr_un path limit is only 104 bytes. Keep per-process
	// control sockets in a short, private runtime directory rather than below
	// the persistent state tree.
	return filepath.Join("/tmp", fmt.Sprintf("amux-%d", os.Getuid()), "shims", p.ID+".sock")
}
func (d *Daemon) shimStatus(p model.Process) (model.Process, error) {
	resp, e := d.shimClient(p).Get("http://unix/v1/status")
	if e != nil {
		return p, e
	}
	defer resp.Body.Close()
	if e = json.NewDecoder(resp.Body).Decode(&p); e == nil {
		_ = d.st.SaveProcess(p)
	}
	return p, e
}
func (d *Daemon) processes(w http.ResponseWriter, r *http.Request) {
	wid := ""
	if x := r.URL.Query().Get("workspace"); x != "" {
		ws, e := d.st.FindWorkspace(x)
		if e != nil {
			fail(w, 404, "workspace_not_found", "workspace was not found")
			return
		}
		wid = ws.ID
	}
	ps, e := d.st.ListProcesses(wid)
	if e != nil {
		fail(w, 500, "internal_error", e.Error())
		return
	}
	for i := range ps {
		if x, e := d.shimStatus(ps[i]); e == nil {
			ps[i] = x
		} else if ps[i].State == model.ProcessRunning || ps[i].State == model.ProcessStarting || ps[i].State == model.ProcessStopping {
			ps[i].State = model.ProcessUnknown
			ps[i].Error = "shim unavailable"
			_ = d.st.SaveProcess(ps[i])
		}
	}
	write(w, 200, protocol.Response{OK: true, Processes: ps})
}
func (d *Daemon) process(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/processes/")
	parts := strings.Split(rest, "/")
	action := ""
	targetEsc := rest
	if len(parts) > 1 {
		last := parts[len(parts)-1]
		switch last {
		case "attach", "logs", "wait", "signal", "stop", "restart":
			action = last
			targetEsc = strings.Join(parts[:len(parts)-1], "/")
		}
	}
	target, e := url.PathUnescape(targetEsc)
	if e != nil {
		fail(w, 400, "invalid_argument", "invalid target encoding")
		return
	}
	p, e := d.st.FindProcess(target)
	if e != nil {
		fail(w, 404, "process_not_found", "process was not found")
		return
	}
	if action == "attach" {
		d.proxyAttach(w, r, p)
		return
	}
	if action == "logs" {
		d.proxy(w, r, p, "/v1/logs")
		return
	}
	if action == "wait" {
		d.wait(w, r, p)
		return
	}
	if action == "signal" {
		d.proxy(w, r, p, "/v1/signal")
		return
	}
	if action == "stop" {
		d.proxy(w, r, p, "/v1/stop")
		return
	}
	if action == "restart" {
		d.restart(w, r, p)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if x, e := d.shimStatus(p); e == nil {
			p = x
		}
		write(w, 200, protocol.Response{OK: true, Process: p})
	case http.MethodDelete:
		if x, e := d.shimStatus(p); e == nil {
			p = x
		}
		if !p.Terminal() {
			fail(w, 409, "invalid_state", "running process must be stopped before removal")
			return
		}
		d.shutdownShim(p)
		if e = d.st.RemoveProcess(p); e != nil {
			fail(w, 500, "internal_error", e.Error())
			return
		}
		write(w, 200, map[string]any{"ok": true})
	default:
		w.WriteHeader(405)
	}
}
func (d *Daemon) proxy(w http.ResponseWriter, r *http.Request, p model.Process, path string) {
	u := "http://unix" + path
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, e := http.NewRequestWithContext(r.Context(), r.Method, u, r.Body)
	if e != nil {
		fail(w, 500, "internal_error", e.Error())
		return
	}
	resp, e := d.shimClient(p).Do(req)
	if e != nil {
		fail(w, 409, "process_not_running", e.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		code := "process_not_running"
		if resp.StatusCode == 400 {
			code = "invalid_argument"
		}
		fail(w, resp.StatusCode, code, strings.TrimSpace(string(b)))
		return
	}
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
func (d *Daemon) wait(w http.ResponseWriter, r *http.Request, p model.Process) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if x, e := d.shimStatus(p); e == nil {
			p = x
		}
		if p.Terminal() {
			write(w, 200, protocol.Response{OK: true, Process: p})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}
func (d *Daemon) restart(w http.ResponseWriter, r *http.Request, p model.Process) {
	if x, e := d.shimStatus(p); e == nil {
		p = x
	}
	if !p.Terminal() && !(p.State == model.ProcessUnknown && p.BootID != hostruntime.BootID()) {
		fail(w, 409, "invalid_state", "process cannot be safely restarted")
		return
	}
	d.shutdownShim(p)
	p.State = model.ProcessStarting
	p.Execution++
	p.PID = 0
	p.ProcessGroupID = 0
	p.ExitCode = nil
	p.ExitSignal = ""
	p.Error = ""
	p.BootID = hostruntime.BootID()
	p.StartedAt = nil
	p.FinishedAt = nil
	if e := d.st.SaveProcess(p); e != nil {
		fail(w, 500, "internal_error", e.Error())
		return
	}
	if e := d.launchShim(&p); e != nil {
		fail(w, 500, "process_start_failed", e.Error())
		return
	}
	write(w, 200, protocol.Response{OK: true, Process: p})
}
func (d *Daemon) removeWorkspace(w http.ResponseWriter, r *http.Request, ws model.Workspace) {
	ps, _ := d.st.ListProcesses(ws.ID)
	for _, p := range ps {
		if x, e := d.shimStatus(p); e == nil {
			p = x
		}
		if !p.Terminal() {
			b, _ := json.Marshal(model.StopRequest{GraceMillis: 10000})
			req, _ := http.NewRequest(http.MethodPost, "http://unix/v1/stop", bytes.NewReader(b))
			_, _ = d.shimClient(p).Do(req)
		}
	}
	deadline := time.NewTimer(12 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		all := true
		for _, p := range ps {
			if x, e := d.shimStatus(p); e == nil && !x.Terminal() {
				all = false
			}
		}
		if all {
			break
		}
		select {
		case <-deadline.C:
			fail(w, 409, "invalid_state", "some processes did not stop")
			return
		case <-tick.C:
		}
	}
	for _, p := range ps {
		d.shutdownShim(p)
	}
	if e := d.st.RemoveWorkspace(ws); e != nil {
		fail(w, 500, "internal_error", e.Error())
		return
	}
	write(w, 200, map[string]any{"ok": true})
}
func (d *Daemon) shutdownShim(p model.Process) {
	req, _ := http.NewRequest(http.MethodPost, "http://unix/v1/shutdown", nil)
	if resp, e := d.shimClient(p).Do(req); e == nil {
		_ = resp.Body.Close()
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, e := os.Stat(d.shimSocket(p)); errors.Is(e, os.ErrNotExist) {
			return
		}
		select {
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}
func (d *Daemon) updateWorkspace(w *model.Workspace) {
	ps, _ := d.st.ListProcesses(w.ID)
	active := false
	for _, p := range ps {
		if p.State == model.ProcessRunning || p.State == model.ProcessStarting || p.State == model.ProcessStopping {
			active = true
		}
	}
	if active {
		w.State = model.WorkspaceActive
	} else {
		w.State = model.WorkspaceIdle
	}
	_ = d.st.SaveWorkspace(*w)
}
func (d *Daemon) proxyAttach(w http.ResponseWriter, r *http.Request, p model.Process) {
	dialer := websocket.Dialer{NetDial: func(network, address string) (net.Conn, error) {
		return net.Dial("unix", d.shimSocket(p))
	}}
	u := "ws://unix/v1/attach"
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	back, resp, e := dialer.Dial(u, nil)
	if e != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		fail(w, 409, "process_not_running", e.Error())
		return
	}
	defer back.Close()
	front, e := websocket.Upgrade(w, r, nil, 1024, 1024)
	if e != nil {
		return
	}
	defer front.Close()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	copyWS := func(dst, src *websocket.Conn) {
		for {
			t, b, e := src.ReadMessage()
			if e != nil {
				cancel()
				return
			}
			if dst.WriteMessage(t, b) != nil {
				cancel()
				return
			}
		}
	}
	go copyWS(back, front)
	copyWS(front, back)
	<-ctx.Done()
}
func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, code, msg string) {
	write(w, status, protocol.Response{OK: false, Error: protocol.E(code, "%s", msg)})
}
func (d *Daemon) Config() config.Config { return d.cfg }
func parseInt(s string) int             { n, _ := strconv.Atoi(s); return n }
