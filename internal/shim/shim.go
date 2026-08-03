//go:build unix

package shim

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"github.com/win5do/amux/internal/model"
	hostruntime "github.com/win5do/amux/internal/runtime/host"
)

type message struct {
	kind int
	data []byte
}
type Shim struct {
	dir        string
	socket     string
	mu         sync.Mutex
	proc       model.Process
	runEnv     map[string]string
	cmd        *exec.Cmd
	input      io.WriteCloser
	inputQueue chan []byte
	tty        *os.File
	seq        uint64
	offsets    map[string]int64
	events     []model.OutputEvent
	subs       map[uint64]chan message
	nextSub    uint64
	controller uint64
	server     *http.Server
	readers    sync.WaitGroup
}

func Load(dir, socket string) (*Shim, error) {
	var p model.Process
	b, e := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(b, &p); e != nil {
		return nil, e
	}
	runEnv := p.Env
	if b, e := os.ReadFile(filepath.Join(dir, "runtime-env.json")); e == nil {
		_ = json.Unmarshal(b, &runEnv)
	}
	if socket == "" {
		socket = filepath.Join(dir, "shim.sock")
	}
	s := &Shim{dir: dir, socket: socket, proc: p, runEnv: runEnv, offsets: map[string]int64{}, subs: map[uint64]chan message{}}
	s.loadEvents()
	return s, nil
}
func (s *Shim) loadEvents() {
	f, e := os.Open(filepath.Join(s.dir, "events.jsonl"))
	if e != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var v model.OutputEvent
		if json.Unmarshal(sc.Bytes(), &v) == nil {
			s.events = append(s.events, v)
			if v.Sequence > s.seq {
				s.seq = v.Sequence
			}
			if end := v.Offset + int64(v.Length); end > s.offsets[v.Stream] {
				s.offsets[v.Stream] = end
			}
		}
	}
}
func (s *Shim) saveStatus() error {
	b, e := json.MarshalIndent(s.proc, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	f, e := os.CreateTemp(s.dir, ".status-")
	if e != nil {
		return e
	}
	n := f.Name()
	defer os.Remove(n)
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(n, filepath.Join(s.dir, "metadata.json"))
	}
	return e
}
func (s *Shim) Run(ctx context.Context, ready *os.File) error {
	sock := s.socket
	if e := os.MkdirAll(filepath.Dir(sock), 0700); e != nil {
		return e
	}
	_ = os.Remove(sock)
	defer os.Remove(sock)
	ln, e := net.Listen("unix", sock)
	if e != nil {
		return e
	}
	_ = os.Chmod(sock, 0600)
	if e = s.start(); e != nil {
		s.mu.Lock()
		s.proc.State = model.ProcessFailed
		s.proc.Error = e.Error()
		now := time.Now().UTC()
		s.proc.FinishedAt = &now
		_ = s.saveStatus()
		s.mu.Unlock()
		return e
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", s.status)
	mux.HandleFunc("/v1/signal", s.signal)
	mux.HandleFunc("/v1/stop", s.stop)
	mux.HandleFunc("/v1/logs", s.logs)
	mux.HandleFunc("/v1/attach", s.attach)
	mux.HandleFunc("/v1/shutdown", s.shutdown)
	s.server = &http.Server{Handler: mux}
	if ready != nil {
		_, _ = ready.Write([]byte{1})
		_ = ready.Close()
	}
	go func() { <-ctx.Done(); _ = s.server.Shutdown(context.Background()) }()
	e = s.server.Serve(ln)
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func (s *Shim) start() error {
	p := &s.proc
	cmd := exec.Command(p.Command[0], p.Command[1:]...)
	cmd.Dir = p.Cwd
	cmd.Env = mapEnv(s.runEnv)
	if p.IOMode == model.IOTTY {
		f, e := pty.Start(cmd)
		if e != nil {
			return e
		}
		s.tty = f
		s.input = f
		s.cmd = cmd
		p.PID = cmd.Process.Pid
		p.ProcessGroupID = cmd.Process.Pid
		s.readers.Add(1)
		go func() { defer s.readers.Done(); s.read("tty", f) }()
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		out, outw, e := os.Pipe()
		if e != nil {
			return e
		}
		errout, errw, e := os.Pipe()
		if e != nil {
			out.Close()
			outw.Close()
			return e
		}
		cmd.Stdout = outw
		cmd.Stderr = errw
		in, e := cmd.StdinPipe()
		if e != nil {
			out.Close()
			outw.Close()
			errout.Close()
			errw.Close()
			return e
		}
		if e = cmd.Start(); e != nil {
			out.Close()
			outw.Close()
			errout.Close()
			errw.Close()
			in.Close()
			return e
		}
		_ = outw.Close()
		_ = errw.Close()
		s.cmd = cmd
		s.input = in
		p.PID = cmd.Process.Pid
		p.ProcessGroupID = cmd.Process.Pid
		s.readers.Add(2)
		go func() { defer s.readers.Done(); s.read("stdout", out) }()
		go func() { defer s.readers.Done(); s.read("stderr", errout) }()
	}
	now := time.Now().UTC()
	p.StartedAt = &now
	p.FinishedAt = nil
	p.State = model.ProcessRunning
	p.Error = ""
	if e := s.saveStatus(); e != nil {
		return e
	}
	s.inputQueue = make(chan []byte, 128)
	go s.inputLoop()
	go s.wait()
	return nil
}
func (s *Shim) inputLoop() {
	for b := range s.inputQueue {
		s.mu.Lock()
		in := s.input
		s.mu.Unlock()
		if in != nil {
			if _, e := in.Write(b); e != nil {
				return
			}
		}
	}
}
func mapEnv(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}
func (s *Shim) read(stream string, r io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, e := r.Read(buf)
		if n > 0 {
			s.emit(stream, append([]byte(nil), buf[:n]...))
		}
		if e != nil {
			if stream == "tty" && errors.Is(e, syscall.EIO) {
				return
			}
			return
		}
	}
}
func (s *Shim) emit(stream string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, e := os.OpenFile(filepath.Join(s.dir, stream+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return
	}
	off := s.offsets[stream]
	_, e = f.Write(data)
	if e == nil {
		e = f.Sync()
	}
	_ = f.Close()
	if e != nil {
		return
	}
	s.seq++
	ev := model.OutputEvent{Sequence: s.seq, Execution: s.proc.Execution, Timestamp: time.Now().UTC(), Stream: stream, Offset: off, Length: len(data)}
	line, _ := json.Marshal(ev)
	ef, e := os.OpenFile(filepath.Join(s.dir, "events.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return
	}
	_, e = ef.Write(append(line, '\n'))
	if e == nil {
		e = ef.Sync()
	}
	_ = ef.Close()
	if e != nil {
		return
	}
	s.offsets[stream] += int64(len(data))
	s.events = append(s.events, ev)
	for id, ch := range s.subs {
		select {
		case ch <- message{websocket.BinaryMessage, data}:
		default:
			close(ch)
			delete(s.subs, id)
			if s.controller == id {
				s.controller = 0
			}
		}
	}
}
func (s *Shim) wait() {
	e := s.cmd.Wait()
	s.readers.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.proc.FinishedAt = &now
	s.proc.State = model.ProcessExited
	code := 0
	if e != nil {
		if ee, ok := e.(*exec.ExitError); ok {
			code = ee.ExitCode()
			if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				s.proc.ExitSignal = ws.Signal().String()
			}
		} else {
			s.proc.State = model.ProcessFailed
			s.proc.Error = e.Error()
			code = -1
		}
	}
	s.proc.ExitCode = &code
	_ = s.saveStatus()
	b, _ := json.Marshal(map[string]any{"type": "exit", "exit_code": code, "signal": s.proc.ExitSignal})
	for id, ch := range s.subs {
		select {
		case ch <- message{websocket.TextMessage, b}:
		default:
			close(ch)
			delete(s.subs, id)
		}
	}
}
func (s *Shim) status(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, s.proc)
}
func (s *Shim) signal(w http.ResponseWriter, r *http.Request) {
	var q model.SignalRequest
	if json.NewDecoder(r.Body).Decode(&q) != nil {
		http.Error(w, "bad request", 400)
		return
	}
	sig, e := hostruntime.ParseSignal(strings.ToUpper(strings.TrimPrefix(q.Signal, "SIG")))
	if e == nil {
		e = hostruntime.SignalGroup(s.proc.ProcessGroupID, sig)
	}
	if e != nil {
		http.Error(w, e.Error(), 409)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}
func (s *Shim) stop(w http.ResponseWriter, r *http.Request) {
	var q model.StopRequest
	_ = json.NewDecoder(r.Body).Decode(&q)
	if q.GraceMillis <= 0 {
		q.GraceMillis = 10000
	}
	s.mu.Lock()
	if s.proc.State != model.ProcessRunning {
		s.mu.Unlock()
		http.Error(w, "not running", 409)
		return
	}
	s.proc.State = model.ProcessStopping
	_ = s.saveStatus()
	pg := s.proc.ProcessGroupID
	s.mu.Unlock()
	_ = hostruntime.SignalGroup(pg, syscall.SIGTERM)
	go func() {
		t := time.NewTimer(time.Duration(q.GraceMillis) * time.Millisecond)
		defer t.Stop()
		<-t.C
		s.mu.Lock()
		running := s.proc.State == model.ProcessStopping
		s.mu.Unlock()
		if running {
			_ = hostruntime.SignalGroup(pg, syscall.SIGKILL)
		}
	}()
	writeJSON(w, map[string]bool{"ok": true})
}
func (s *Shim) shutdown(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	terminal := s.proc.Terminal()
	s.mu.Unlock()
	if !terminal {
		http.Error(w, "process still running", 409)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
	go s.server.Shutdown(context.Background())
}
func (s *Shim) logs(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	follow := r.URL.Query().Get("follow") == "true"
	s.mu.Lock()
	id := s.nextSub
	s.nextSub++
	var ch chan message
	if follow {
		ch = make(chan message, 128)
		s.subs[id] = ch
	}
	snapshot := append([]model.OutputEvent(nil), s.events...)
	s.mu.Unlock()
	if follow {
		defer func() {
			s.mu.Lock()
			if c, ok := s.subs[id]; ok {
				delete(s.subs, id)
				close(c)
			}
			s.mu.Unlock()
		}()
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	fl, _ := w.(http.Flusher)
	for _, ev := range snapshot {
		if ev.Sequence > after {
			if writeEvent(w, s.dir, ev) != nil {
				return
			}
			after = ev.Sequence
		}
	}
	if fl != nil {
		fl.Flush()
	}
	if !follow {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			s.mu.Lock()
			events := append([]model.OutputEvent(nil), s.events...)
			s.mu.Unlock()
			for _, ev := range events {
				if ev.Sequence > after {
					if writeEvent(w, s.dir, ev) != nil {
						return
					}
					after = ev.Sequence
				}
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}
}
func writeEvent(w io.Writer, dir string, ev model.OutputEvent) error {
	f, e := os.Open(filepath.Join(dir, ev.Stream+".log"))
	if e != nil {
		return e
	}
	defer f.Close()
	b := make([]byte, ev.Length)
	_, e = f.ReadAt(b, ev.Offset)
	if e != nil {
		return e
	}
	if utf8.Valid(b) {
		ev.Data = &model.EncodedData{Encoding: "utf8", Text: string(b)}
	} else {
		ev.Data = &model.EncodedData{Encoding: "base64", Base64: base64.StdEncoding.EncodeToString(b)}
	}
	return json.NewEncoder(w).Encode(ev)
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *Shim) attach(w http.ResponseWriter, r *http.Request) {
	if s.proc.IOMode != model.IOTTY {
		http.Error(w, "tty required", 409)
		return
	}
	readonly := r.URL.Query().Get("readonly") == "true"
	takeover := r.URL.Query().Get("takeover") == "true"
	replayBytes, _ := strconv.Atoi(r.URL.Query().Get("replay_bytes"))
	c, e := upgrader.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
	s.mu.Lock()
	id := s.nextSub
	s.nextSub++
	if !readonly && s.controller != 0 && !takeover {
		s.mu.Unlock()
		_ = c.WriteJSON(map[string]any{"type": "error", "code": "controller_exists"})
		return
	}
	if !readonly {
		s.controller = id
	}
	ch := make(chan message, 128)
	s.subs[id] = ch
	if replayBytes > 0 {
		if b, e := tailFile(filepath.Join(s.dir, "tty.log"), replayBytes); e == nil && len(b) > 0 {
			ch <- message{websocket.BinaryMessage, b}
		}
	}
	if !readonly {
		b, _ := json.Marshal(map[string]any{"type": "ownership_changed", "controller": id, "takeover": takeover})
		for _, sub := range s.subs {
			select {
			case sub <- message{websocket.TextMessage, b}:
			default:
			}
		}
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if sub, ok := s.subs[id]; ok {
			delete(s.subs, id)
			close(sub)
		}
		if s.controller == id {
			s.controller = 0
		}
		s.mu.Unlock()
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range ch {
			if c.WriteMessage(m.kind, m.data) != nil {
				return
			}
		}
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "slow client"), time.Now().Add(time.Second))
	}()
	for {
		kind, b, e := c.ReadMessage()
		if e != nil {
			return
		}
		if kind == websocket.BinaryMessage {
			s.mu.Lock()
			ctrl := s.controller == id
			inputQueue := s.inputQueue
			s.mu.Unlock()
			if ctrl && inputQueue != nil {
				select {
				case inputQueue <- append([]byte(nil), b...):
				default:
					return
				}
			}
		} else {
			var q struct {
				Type   string `json:"type"`
				Rows   uint16 `json:"rows"`
				Cols   uint16 `json:"cols"`
				Signal string `json:"signal"`
			}
			if json.Unmarshal(b, &q) != nil {
				continue
			}
			s.mu.Lock()
			ctrl := s.controller == id
			tty := s.tty
			pg := s.proc.ProcessGroupID
			s.mu.Unlock()
			if !ctrl {
				continue
			}
			if q.Type == "resize" && tty != nil {
				_ = pty.Setsize(tty, &pty.Winsize{Rows: q.Rows, Cols: q.Cols})
			}
			if q.Type == "signal" {
				if sig, x := hostruntime.ParseSignal(strings.ToUpper(strings.TrimPrefix(q.Signal, "SIG"))); x == nil {
					_ = hostruntime.SignalGroup(pg, sig)
				}
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}
func tailFile(path string, n int) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if int64(n) > st.Size() {
		n = int(st.Size())
	}
	b := make([]byte, n)
	_, e = f.ReadAt(b, st.Size()-int64(n))
	if e == io.EOF {
		e = nil
	}
	return b, e
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Shim) String() string { return fmt.Sprintf("shim(%s)", s.proc.ID) }
