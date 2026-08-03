//go:build unix

package main

import (
	"context"
	"github.com/win5do/amux/internal/config"
	"github.com/win5do/amux/internal/daemon"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, e := config.Load()
	if e != nil {
		log.Fatal(e)
	}
	d, e := daemon.New(cfg)
	if e != nil {
		log.Fatal(e)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var ready *os.File
	if os.Getenv("AMUX_DAEMON_READY_FD") == "3" {
		ready = os.NewFile(3, "ready")
	}
	if e = d.Serve(ctx, ready); e != nil {
		log.Fatal(e)
	}
}
