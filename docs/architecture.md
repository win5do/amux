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

## Runtime model and direction

The current implementation supports one execution combination only:

- execution location: `local`;
- execution method: a host OS process group.

The CLI and protocol currently identify this execution method with the runtime
value `host`. Container and remote SSH execution are not native runtimes today.
They can still be launched through foreground host commands, but in that case
amux owns the local CLI process rather than the detached container, container
application, or remote process.

Future runtime support should separate two independent concerns instead of
adding each combination to one flat runtime enum:

- **placement** describes where execution happens, such as `local` or a future
  remote SSH endpoint;
- **executor** describes how the command is run at that placement, such as a
  host process or a future container.

This produces composable combinations without introducing special runtime
names for every pairing:

| Placement | Executor | Support |
| --- | --- | --- |
| local | host process | supported |
| local | container | future direction |
| remote SSH | host process | future direction |
| remote SSH | container | future direction |

The entries marked as future direction illustrate the intended architecture;
they are not current compatibility promises.

Runtime configuration belongs below the workspace boundary. A workspace is a
durable logical context and may provide defaults, but it should eventually be
able to contain processes using different execution combinations. The desired
placement and executor configuration should live on a reusable environment,
while each process references that environment and records the resolved native
handle, such as a local process-group ID, container ID, or remote process token.

In that model, a runtime backend provides the common lifecycle contract—start,
inspect, logs, signal, stop, kill, wait, restart, attach, remove, and recovery—
while declaring any unsupported capabilities explicitly. The existing
workspace-level `runtime` constraint represents the current local/host-only
implementation, not the intended boundary for multi-runtime support.
