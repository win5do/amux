# Architecture

`amux` speaks HTTP/1.1 over a private Unix socket to `amuxd`. The daemon owns
metadata and routing, but never owns the only process I/O descriptor. Every
process has a detached `amux-shim` that owns its process group, pipes or PTY,
log sequence, wait status, and attach controller.

```text
amux -> amuxd -> per-process amux-shim -> OS process group
```

The state tree uses the schema-v1 JSON layout from the original Go
implementation. Durable files live below `AMUX_STATE_DIR`; short shim socket
paths live below `/tmp/amux-UID/shims` to fit macOS `sockaddr_un` limits.

Each output stream has one authoritative reader. The shim appends bytes to the
raw stream log, syncs it, appends a monotonically sequenced event to
`events.jsonl`, then broadcasts it. A TTY has one writable controller and any
number of readonly observers. `--takeover` replaces the controller.

Restarted daemons discover processes from disk and reconnect to their shim
sockets. A live-looking record whose shim cannot be reached becomes `unknown`.
An unknown process from the current boot is not restarted automatically,
because its unmanaged process tree may still exist.

