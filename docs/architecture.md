# Architecture

`amux` talks HTTP/1.1 over a Unix socket to `amuxd`. The daemon owns metadata and routing, but never owns the only copy of a process I/O descriptor. For every process it starts a detached `amux-shim`; the shim owns the process group, pipes or PTY, log sequencer, wait status and attach ownership.

```text
amux -> amuxd -> per-process amux-shim -> OS process group
```

CLI and daemon termination therefore do not terminate managed processes. A restarted daemon discovers processes from the state tree and reconnects to their persisted shim socket handles. If a live-looking record has no shim, it becomes `unknown`; an unknown record from the current boot cannot be restarted automatically because an unmanaged process tree may still exist.

Process output has one authoritative reader per stream. A serialized critical section appends raw bytes, fsyncs them, appends an indexed event with a monotonically increasing sequence, then broadcasts. Subscribers have bounded queues and are disconnected when slow. Input has one controller and is written through the shim; readonly observers never write or resize.

State is organized by workspace and process. macOS has a small Unix socket path limit, so shim sockets use the short private `/tmp/amux-UID/shims` runtime directory while durable data remains under `AMUX_STATE_DIR`.
