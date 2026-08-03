//go:build unix

package main

import (
	"context"
	"flag"
	"github.com/win5do/amux/internal/shim"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dir := flag.String("process-dir", "", "process state directory")
	socket := flag.String("socket", "", "shim Unix socket")
	flag.Parse()
	if *dir == "" {
		log.Fatal("--process-dir is required")
	}
	s, e := shim.Load(*dir, *socket)
	if e != nil {
		log.Fatal(e)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var ready *os.File
	if os.Getenv("AMUX_SHIM_READY_FD") == "3" {
		ready = os.NewFile(3, "ready")
	}
	if e = s.Run(ctx, ready); e != nil {
		log.Fatal(e)
	}
}
