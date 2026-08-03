//go:build unix

package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"github.com/win5do/amux/internal/model"
	"github.com/win5do/amux/internal/protocol"
	"github.com/win5do/amux/pkg/amuxclient"
	"golang.org/x/term"
)

type app struct {
	c           amuxclient.Client
	json, jsonl bool
}

func main() {
	a := &app{}
	c, e := amuxclient.New()
	if e != nil {
		fatal(e)
	}
	a.c = c
	root := a.root()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	e = a.c.EnsureDaemon(ctx)
	cancel()
	if e != nil {
		fatal(e)
	}
	if e = root.Execute(); e != nil {
		a.reportError(e)
		os.Exit(1)
	}
}
func (a *app) reportError(e error) {
	pe, ok := e.(*protocol.Error)
	if !ok {
		pe = protocol.E("internal_error", "%s", e)
	}
	if a.json || a.jsonl {
		_ = json.NewEncoder(os.Stdout).Encode(protocol.Response{OK: false, Error: pe})
	} else {
		fmt.Fprintf(os.Stderr, "%s: %s\n", pe.Code, pe.Message)
	}
}
func fatal(e error) { fmt.Fprintln(os.Stderr, e); os.Exit(1) }
func (a *app) root() *cobra.Command {
	r := &cobra.Command{Use: "amux", SilenceUsage: true, SilenceErrors: true}
	r.PersistentFlags().BoolVar(&a.json, "json", false, "emit JSON")
	r.PersistentFlags().BoolVar(&a.jsonl, "jsonl", false, "emit JSON Lines")
	r.AddCommand(a.workspace(), a.run(), a.ps(), a.inspect(), a.logs(), a.attach(), a.signal(), a.stop(false), a.stop(true), a.wait(), a.remove(), a.restart(), a.shell())
	return r
}
func (a *app) call(method, path string, body any) error {
	b, e := a.c.Do(context.Background(), method, path, body)
	if e != nil {
		return e
	}
	if a.json || a.jsonl {
		_, e = os.Stdout.Write(b)
		return e
	}
	var v any
	if json.Unmarshal(b, &v) == nil {
		pretty, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(pretty))
		return nil
	}
	_, e = os.Stdout.Write(b)
	return e
}
func (a *app) workspace() *cobra.Command {
	c := &cobra.Command{Use: "workspace"}
	create := &cobra.Command{Use: "create NAME", Args: cobra.ExactArgs(1)}
	var runtime, root string
	var envs []string
	create.Flags().StringVar(&runtime, "runtime", "host", "runtime")
	create.Flags().StringVar(&root, "root-dir", "", "default working directory")
	create.Flags().StringSliceVarP(&envs, "env", "e", nil, "environment KEY=VALUE")
	create.RunE = func(c *cobra.Command, args []string) error {
		return a.call(http.MethodPost, "/v1/workspaces", model.CreateWorkspaceRequest{Name: args[0], Runtime: runtime, RootDir: root, Env: envMap(envs)})
	}
	ls := &cobra.Command{Use: "ls", RunE: func(*cobra.Command, []string) error { return a.call(http.MethodGet, "/v1/workspaces", nil) }}
	inspect := &cobra.Command{Use: "inspect TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodGet, "/v1/workspaces/"+url.PathEscape(x[0]), nil)
	}}
	rm := &cobra.Command{Use: "rm TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodDelete, "/v1/workspaces/"+url.PathEscape(x[0]), nil)
	}}
	c.AddCommand(create, ls, inspect, rm)
	return c
}
func (a *app) run() *cobra.Command {
	var ws, name, cwd, runtimeName string
	var tty bool
	var envs []string
	c := &cobra.Command{Use: "run --workspace NAME --name NAME -- COMMAND [ARG...]", Args: cobra.MinimumNArgs(1), RunE: func(c *cobra.Command, args []string) error {
		mode := model.IOPipe
		if tty {
			mode = model.IOTTY
		}
		return a.call(http.MethodPost, "/v1/workspaces/"+url.PathEscape(ws)+"/processes", model.RunRequest{Name: name, Command: args, Cwd: cwd, Env: envMap(envs), IOMode: mode, Runtime: runtimeName})
	}}
	c.Flags().StringVarP(&ws, "workspace", "w", "", "workspace")
	c.Flags().StringVar(&name, "name", "", "process name")
	c.Flags().StringVar(&cwd, "cwd", "", "working directory")
	c.Flags().StringVar(&runtimeName, "runtime", "", "runtime (must match workspace)")
	c.Flags().BoolVarP(&tty, "tty", "t", false, "allocate TTY")
	c.Flags().StringSliceVarP(&envs, "env", "e", nil, "environment KEY=VALUE")
	_ = c.MarkFlagRequired("workspace")
	_ = c.MarkFlagRequired("name")
	return c
}
func (a *app) ps() *cobra.Command {
	var ws string
	c := &cobra.Command{Use: "ps", RunE: func(*cobra.Command, []string) error {
		q := url.Values{}
		if ws != "" {
			q.Set("workspace", ws)
		}
		return a.call(http.MethodGet, amuxclient.Query("/v1/processes", q), nil)
	}}
	c.Flags().StringVarP(&ws, "workspace", "w", "", "workspace")
	return c
}
func (a *app) inspect() *cobra.Command {
	return &cobra.Command{Use: "inspect TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodGet, "/v1/processes/"+url.PathEscape(x[0]), nil)
	}}
}
func (a *app) logs() *cobra.Command {
	var follow bool
	var after uint64
	c := &cobra.Command{Use: "logs TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		q := url.Values{"after": {strconv.FormatUint(after, 10)}}
		if follow {
			q.Set("follow", "true")
		}
		ctx := context.Background()
		body, e := a.c.Stream(ctx, amuxclient.Query("/v1/processes/"+url.PathEscape(x[0])+"/logs", q))
		if e != nil {
			return e
		}
		defer body.Close()
		sc := bufio.NewScanner(body)
		buf := make([]byte, 64*1024)
		sc.Buffer(buf, 2*1024*1024)
		for sc.Scan() {
			if a.jsonl {
				fmt.Println(sc.Text())
				continue
			}
			var ev model.OutputEvent
			if json.Unmarshal(sc.Bytes(), &ev) != nil {
				continue
			}
			if ev.Data != nil {
				if ev.Data.Encoding == "utf8" {
					fmt.Print(ev.Data.Text)
				} else {
					b, _ := base64.StdEncoding.DecodeString(ev.Data.Base64)
					_, _ = os.Stdout.Write(b)
				}
			}
		}
		return sc.Err()
	}}
	c.Flags().BoolVarP(&follow, "follow", "f", false, "follow output")
	c.Flags().Uint64Var(&after, "after", 0, "events strictly after sequence")
	return c
}
func (a *app) signal() *cobra.Command {
	return &cobra.Command{Use: "signal TARGET SIGNAL", Args: cobra.ExactArgs(2), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodPost, "/v1/processes/"+url.PathEscape(x[0])+"/signal", model.SignalRequest{Signal: x[1]})
	}}
}
func (a *app) stop(kill bool) *cobra.Command {
	name := "stop"
	sig := ""
	if kill {
		name = "kill"
		sig = "KILL"
	}
	var grace time.Duration
	c := &cobra.Command{Use: name + " TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		base := "/v1/processes/" + url.PathEscape(x[0])
		if kill {
			return a.call(http.MethodPost, base+"/signal", model.SignalRequest{Signal: sig})
		}
		return a.call(http.MethodPost, base+"/stop", model.StopRequest{GraceMillis: grace.Milliseconds()})
	}}
	if !kill {
		c.Flags().DurationVarP(&grace, "time", "t", 10*time.Second, "grace period")
	}
	return c
}
func (a *app) wait() *cobra.Command {
	return &cobra.Command{Use: "wait TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodGet, "/v1/processes/"+url.PathEscape(x[0])+"/wait", nil)
	}}
}
func (a *app) remove() *cobra.Command {
	return &cobra.Command{Use: "rm TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodDelete, "/v1/processes/"+url.PathEscape(x[0]), nil)
	}}
}
func (a *app) restart() *cobra.Command {
	return &cobra.Command{Use: "restart TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		return a.call(http.MethodPost, "/v1/processes/"+url.PathEscape(x[0])+"/restart", nil)
	}}
}
func (a *app) shell() *cobra.Command {
	var ws, name, shell string
	c := &cobra.Command{Use: "shell", RunE: func(c *cobra.Command, args []string) error {
		if shell == "" {
			shell = os.Getenv("SHELL")
			if shell == "" {
				shell = "/bin/sh"
			}
		}
		return a.call(http.MethodPost, "/v1/workspaces/"+url.PathEscape(ws)+"/processes", model.RunRequest{Name: name, Command: []string{shell}, IOMode: model.IOTTY})
	}}
	c.Flags().StringVarP(&ws, "workspace", "w", "", "workspace")
	c.Flags().StringVar(&name, "name", "shell", "process name")
	c.Flags().StringVar(&shell, "shell", "", "shell executable")
	_ = c.MarkFlagRequired("workspace")
	return c
}
func envMap(xs []string) map[string]string {
	m := map[string]string{}
	for _, x := range xs {
		if i := strings.IndexByte(x, '='); i > 0 {
			m[x[:i]] = x[i+1:]
		}
	}
	return m
}
func (a *app) attach() *cobra.Command {
	var readonly, takeover bool
	var replay int
	var detachKeys string
	c := &cobra.Command{Use: "attach TARGET", Args: cobra.ExactArgs(1), RunE: func(c *cobra.Command, x []string) error {
		q := url.Values{}
		if readonly {
			q.Set("readonly", "true")
		}
		if takeover {
			q.Set("takeover", "true")
		}
		if replay > 0 {
			q.Set("replay_bytes", strconv.Itoa(replay))
		}
		conn, resp, e := a.c.Attach(amuxclient.Query("/v1/processes/"+url.PathEscape(x[0])+"/attach", q))
		if e != nil {
			if resp != nil {
				resp.Body.Close()
			}
			return e
		}
		defer conn.Close()
		keys, e := parseDetachKeys(detachKeys)
		if e != nil {
			return e
		}
		return attachTerminal(conn, readonly, keys)
	}}
	c.Flags().BoolVar(&readonly, "readonly", false, "observe without input")
	c.Flags().BoolVar(&takeover, "takeover", false, "take controller ownership")
	c.Flags().IntVar(&replay, "replay-bytes", 0, "replay recent raw bytes")
	c.Flags().StringVar(&detachKeys, "detach-keys", "ctrl-p,ctrl-q", "two-key detach sequence")
	c.MarkFlagsMutuallyExclusive("readonly", "takeover")
	return c
}
func attachTerminal(c *websocket.Conn, readonly bool, detach [2]byte) error {
	fd := int(os.Stdin.Fd())
	var old *term.State
	var e error
	if term.IsTerminal(fd) {
		old, e = term.MakeRaw(fd)
		if e != nil {
			return e
		}
		defer term.Restore(fd, old)
	}
	var wm sync.Mutex
	send := func(t int, b []byte) error { wm.Lock(); defer wm.Unlock(); return c.WriteMessage(t, b) }
	if !readonly {
		if w, h, e := term.GetSize(int(os.Stdout.Fd())); e == nil {
			b, _ := json.Marshal(map[string]any{"type": "resize", "rows": h, "cols": w})
			_ = send(websocket.TextMessage, b)
		}
		win := make(chan os.Signal, 1)
		signal.Notify(win, syscall.SIGWINCH)
		defer signal.Stop(win)
		go func() {
			for range win {
				if w, h, e := term.GetSize(int(os.Stdout.Fd())); e == nil {
					b, _ := json.Marshal(map[string]any{"type": "resize", "rows": h, "cols": w})
					_ = send(websocket.TextMessage, b)
				}
			}
		}()
		go func() {
			buf := make([]byte, 4096)
			pending := false
			for {
				n, e := os.Stdin.Read(buf)
				if e != nil {
					return
				}
				out := make([]byte, 0, n+1)
				for _, b := range buf[:n] {
					if pending {
						if b == detach[1] {
							_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "detach"), time.Now().Add(time.Second))
							return
						}
						out = append(out, detach[0])
						pending = false
					}
					if b == detach[0] {
						pending = true
					} else {
						out = append(out, b)
					}
				}
				if len(out) > 0 {
					_ = send(websocket.BinaryMessage, out)
				}
			}
		}()
	}
	for {
		t, b, e := c.ReadMessage()
		if e != nil {
			return nil
		}
		if t == websocket.BinaryMessage {
			_, _ = os.Stdout.Write(b)
		} else {
			var v map[string]any
			if json.Unmarshal(b, &v) == nil {
				if v["type"] == "error" {
					return fmt.Errorf("attach: %v", v["code"])
				}
				if v["type"] == "exit" {
					return nil
				}
			}
		}
	}
}
func parseDetachKeys(s string) ([2]byte, error) {
	var out [2]byte
	parts := strings.Split(strings.ToLower(s), ",")
	if len(parts) != 2 {
		return out, fmt.Errorf("detach keys must contain two comma-separated keys")
	}
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, "ctrl-") && len(p) == 6 {
			c := p[5]
			if c >= 'a' && c <= 'z' {
				out[i] = c - 'a' + 1
				continue
			}
		}
		if len(p) == 1 {
			out[i] = p[0]
			continue
		}
		return out, fmt.Errorf("unsupported detach key %q", p)
	}
	return out, nil
}
func init() { _ = io.EOF }
