//go:build unix

package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/win5do/amux/internal/config"
	"github.com/win5do/amux/internal/model"
	"github.com/win5do/amux/internal/protocol"
)

func TestPipeLifecycle(t *testing.T) {
	tmp := t.TempDir()
	shim := filepath.Join(tmp, "amux-shim")
	cmd := exec.Command("go", "build", "-o", shim, "../../cmd/amux-shim")
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("build shim: %v: %s", e, b)
	}
	cfg := config.Config{StateDir: filepath.Join(tmp, "state"), Socket: filepath.Join(tmp, "amux.sock")}
	d, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	d.shimPath = shim
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rd, wr, _ := os.Pipe()
	errc := make(chan error, 1)
	go func() { errc <- d.Serve(ctx, wr) }()
	one := make([]byte, 1)
	if _, e = io.ReadFull(rd, one); e != nil {
		t.Fatal(e)
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Socket)
	}}}
	call := func(method, path string, body any) []byte {
		var r io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, "http://unix"+path, r)
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			t.Fatalf("%s %s: %s", method, path, b)
		}
		return b
	}
	var created protocol.Response
	if e = json.Unmarshal(call("POST", "/v1/workspaces", model.CreateWorkspaceRequest{Name: "dev"}), &created); e != nil {
		t.Fatal(e)
	}
	var ran protocol.Response
	if e = json.Unmarshal(call("POST", "/v1/workspaces/dev/processes", model.RunRequest{Name: "io", Command: []string{"/bin/sh", "-c", "printf out; printf err >&2; exit 7"}}), &ran); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	var waited protocol.Response
	for time.Now().Before(deadline) {
		b := call("GET", "/v1/processes/dev/io", nil)
		_ = json.Unmarshal(b, &waited)
		raw, _ := json.Marshal(waited.Process)
		var p model.Process
		_ = json.Unmarshal(raw, &p)
		if p.Terminal() {
			if p.ExitCode == nil || *p.ExitCode != 7 {
				t.Fatalf("exit: %+v", p)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	logs := string(call("GET", "/v1/processes/dev/io/logs?after=0", nil))
	if !strings.Contains(logs, `"stream":"stdout"`) || !strings.Contains(logs, `"stream":"stderr"`) {
		t.Fatalf("separate logs missing: %s", logs)
	}
	call("POST", "/v1/processes/dev/io/restart", nil)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b := call("GET", "/v1/processes/dev/io", nil)
		_ = json.Unmarshal(b, &waited)
		raw, _ := json.Marshal(waited.Process)
		var p model.Process
		_ = json.Unmarshal(raw, &p)
		if p.Terminal() && p.Execution == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	logs = string(call("GET", "/v1/processes/dev/io/logs?after=0", nil))
	if !strings.Contains(logs, `"execution":2`) {
		t.Fatalf("restart did not append execution 2: %s", logs)
	}
	call("DELETE", "/v1/workspaces/dev", nil)
	cancel()
	select {
	case e := <-errc:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop")
	}
}
