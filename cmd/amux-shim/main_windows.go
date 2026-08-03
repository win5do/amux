//go:build windows

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "runtime_not_supported: amux-shim host runtime is not supported on Windows")
	os.Exit(1)
}
