package config

import (
	"fmt"
	"os"
	"path/filepath"
)

type Config struct{ StateDir, Socket string }

func Load() (Config, error) {
	state := os.Getenv("AMUX_STATE_DIR")
	if state == "" {
		if x := os.Getenv("XDG_STATE_HOME"); x != "" {
			state = filepath.Join(x, "amux")
		} else {
			h, e := os.UserHomeDir()
			if e != nil {
				return Config{}, e
			}
			state = filepath.Join(h, ".local", "state", "amux")
		}
	}
	sock := os.Getenv("AMUX_SOCKET")
	if sock == "" {
		if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
			sock = filepath.Join(x, "amux", "amuxd.sock")
		} else {
			sock = filepath.Join(os.TempDir(), fmt.Sprintf("amux-%d", currentUID()), "amuxd.sock")
		}
	}
	return Config{state, sock}, nil
}
