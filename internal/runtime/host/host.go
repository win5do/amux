//go:build unix

package host

import (
	"fmt"
	"syscall"
)

func SignalGroup(pgid int, s syscall.Signal) error {
	if pgid <= 0 {
		return fmt.Errorf("invalid process group")
	}
	return syscall.Kill(-pgid, s)
}
func ParseSignal(s string) (syscall.Signal, error) {
	m := map[string]syscall.Signal{"HUP": syscall.SIGHUP, "INT": syscall.SIGINT, "QUIT": syscall.SIGQUIT, "KILL": syscall.SIGKILL, "TERM": syscall.SIGTERM, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2}
	if v, ok := m[s]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("unsupported signal %q", s)
}
