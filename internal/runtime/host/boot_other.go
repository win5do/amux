//go:build unix && !linux && !darwin

package host

func BootID() string { return "unix-unknown-boot" }
