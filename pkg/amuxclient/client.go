//go:build unix

package amuxclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/win5do/amux/internal/config"
	"github.com/win5do/amux/internal/protocol"
)

type Client struct {
	Config config.Config
	HTTP   *http.Client
}

func New() (Client, error) {
	c, e := config.Load()
	if e != nil {
		return Client{}, e
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", c.Socket)
	}}
	return Client{c, &http.Client{Transport: tr}}, nil
}
func (c Client) Do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return nil, e
		}
		rd = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, "http://unix"+path, rd)
	if e != nil {
		return nil, e
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := c.HTTP.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode >= 300 {
		var x protocol.Response
		if json.Unmarshal(b, &x) == nil && x.Error != nil {
			return nil, x.Error
		}
		return nil, fmt.Errorf("amux API: %s", resp.Status)
	}
	return b, nil
}
func (c Client) Stream(ctx context.Context, path string) (io.ReadCloser, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix"+path, nil)
	if e != nil {
		return nil, e
	}
	resp, e := c.HTTP.Do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("amux API: %s: %s", resp.Status, b)
	}
	return resp.Body, nil
}
func (c Client) Attach(path string) (*websocket.Conn, *http.Response, error) {
	d := websocket.Dialer{NetDial: func(network, address string) (net.Conn, error) { return net.Dial("unix", c.Config.Socket) }}
	return d.Dial("ws://unix"+path, nil)
}
func (c Client) Ready(ctx context.Context) bool {
	b, e := c.Do(ctx, http.MethodGet, "/v1/health", nil)
	return e == nil && len(b) > 0
}
func (c Client) EnsureDaemon(ctx context.Context) error {
	if c.Ready(ctx) {
		return nil
	}
	bin, e := daemonExecutable()
	if e != nil {
		return fmt.Errorf("find amuxd: %w", e)
	}
	rd, wr, e := os.Pipe()
	if e != nil {
		return e
	}
	defer rd.Close()
	if e = os.MkdirAll(c.Config.StateDir, 0700); e != nil {
		wr.Close()
		return e
	}
	logf, e := os.OpenFile(filepath.Join(c.Config.StateDir, "amuxd.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		wr.Close()
		return e
	}
	defer logf.Close()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "AMUX_DAEMON_READY_FD=3")
	cmd.ExtraFiles = []*os.File{wr}
	cmd.Stdin = nil
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if e = cmd.Start(); e != nil {
		wr.Close()
		if c.Ready(ctx) {
			return nil
		}
		return e
	}
	wr.Close()
	done := make(chan error, 1)
	go func() { b := make([]byte, 1); _, x := io.ReadFull(rd, b); done <- x }()
	select {
	case e = <-done:
	case <-ctx.Done():
		e = ctx.Err()
	case <-time.After(5 * time.Second):
		e = fmt.Errorf("daemon readiness timeout")
	}
	_ = cmd.Process.Release()
	if e != nil {
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			if c.Ready(ctx) {
				return nil
			}
			select {
			case <-ctx.Done():
				return e
			case <-tick.C:
			}
		}
	}
	return e
}
func daemonExecutable() (string, error) {
	self, e := os.Executable()
	if e == nil {
		p := filepath.Join(filepath.Dir(self), "amuxd")
		if st, x := os.Stat(p); x == nil && !st.IsDir() {
			return p, nil
		}
	}
	return exec.LookPath("amuxd")
}
func Query(path string, q url.Values) string {
	if len(q) == 0 {
		return path
	}
	return path + "?" + q.Encode()
}
