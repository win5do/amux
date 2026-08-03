//go:build darwin

package host

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func BootID() string {
	tv, e := unix.SysctlTimeval("kern.boottime")
	if e != nil {
		return "darwin-unknown-boot"
	}
	return fmt.Sprintf("darwin-%d-%d", tv.Sec, tv.Usec)
}
