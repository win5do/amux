//go:build windows

package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	b, _ := json.Marshal(map[string]any{"ok": false, "error": map[string]any{"code": "runtime_not_supported", "message": "amux host runtime is not supported on Windows", "details": map[string]any{}}})
	fmt.Fprintln(os.Stderr, string(b))
	os.Exit(1)
}
