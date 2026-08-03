//go:build unix

package config

import "os"

func currentUID() int { return os.Getuid() }
