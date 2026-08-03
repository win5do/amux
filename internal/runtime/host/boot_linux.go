//go:build linux

package host

import (
	"os"
	"strings"
)

func BootID() string {
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return "linux-unknown-boot"
	}
	return strings.TrimSpace(string(b))
}
