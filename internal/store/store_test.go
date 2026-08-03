package store

import (
	"testing"

	"github.com/win5do/amux/internal/model"
)

func TestWorkspaceDefaultsAndProcessNames(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	w, e := s.CreateWorkspace(model.CreateWorkspaceRequest{Name: "dev"})
	if e != nil {
		t.Fatal(e)
	}
	if w.State != model.WorkspaceIdle || w.Runtime != "host" {
		t.Fatalf("unexpected workspace: %+v", w)
	}
	g, env, e := s.Defaults(w.ID)
	if e != nil {
		t.Fatal(e)
	}
	if g.Name != "default" || env.Name != "host" || env.GroupID != g.ID {
		t.Fatalf("bad defaults: %+v %+v", g, env)
	}
	p, e := s.CreateProcess(w, model.RunRequest{Name: "api", Command: []string{"true"}}, "boot")
	if e != nil {
		t.Fatal(e)
	}
	if p.GroupID != g.ID || p.EnvironmentID != env.ID {
		t.Fatal("process did not use defaults")
	}
	if _, e = s.CreateProcess(w, model.RunRequest{Name: "api", Command: []string{"true"}}, "boot"); e == nil {
		t.Fatal("duplicate name accepted")
	}
	if got, e := s.FindProcess("dev/api"); e != nil || got.ID != p.ID {
		t.Fatalf("target lookup: %v %+v", e, got)
	}
}
func TestNameValidation(t *testing.T) {
	for _, x := range []string{"", "../x", "a/b", " has-space"} {
		if ValidName(x) {
			t.Fatalf("accepted %q", x)
		}
	}
	for _, x := range []string{"dev", "a.b-c_1"} {
		if !ValidName(x) {
			t.Fatalf("rejected %q", x)
		}
	}
}
