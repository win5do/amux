# Architecture

`amux` speaks HTTP/1.1 over a private Unix socket to `amuxd`. The daemon owns
metadata and routing, but never owns the only process I/O descriptor. Every
workspace has a detached group shim. The group shim hosts active per-process
supervisors, each of which owns its process group, pipes or PTY, log sequence,
wait status, and attach controller.

```text
amux -> amuxd -> workspace group shim -> per-process supervisor -> OS process group
```

The state tree uses the schema-v1 JSON layout from the original Go
implementation. Durable files live below `AMUX_STATE_DIR`; short shim socket
paths live below `/tmp/amux-UID/shims` to fit macOS `sockaddr_un` limits.
Daemon socket names hash the normalized absolute state directory (including the
daemon name digest), and live in `XDG_RUNTIME_DIR` or the system temporary
directory. Different state roots therefore cannot share a named daemon socket.
Existing path prefixes are canonicalized before missing directories are appended,
so first startup, relative paths, and existing symlink aliases keep one identity.

Daemon registration includes the OS process identity (PID, UID, boot ID, start
identity, and process group). The same instance/identity pair is written into
the state lock inode while holding `flock`; recovery checks both records and
the live OS identity before signaling. Registration alone is not authority to
kill a process. macOS uses `proc_pidinfo`; Linux uses `/proc/<pid>/stat`.

`lifecycle` owns bounded inspection and explicit recovery. A separate recovery
lock serializes recovery and excludes daemon startup/removal while cleanup is
in progress. A durable `recovery-pending.json` prevents ordinary startup after
interrupted cleanup. The shim lifeline handles daemon death; recovery verifies
managed group termination and checks persisted shim/process identities before
any additional signals. Legacy records remain readable but cannot authorize
forced termination without the new ownership evidence.

Each output stream has one authoritative reader. The shim appends bytes to the
raw stream log, appends a monotonically sequenced event to `events.jsonl`, then
broadcasts it. Dirty raw streams and their event index are synced in that order
on a 25 ms cadence, and synchronously at process exit and rotation. A system
crash can therefore lose at most the active sync batch; startup repairs an
incomplete current-generation tail before replay. Raw streams and their event
index are rotated together as a generation under `output/`: `current/` is active
and UTC timestamped `*.bak/` directories are retained history. Sequence numbers
remain monotonic across generations and process restarts. Legacy process-root log
files remain readable and are migrated when a shim next starts without a schema
reset. A TTY has one writable controller and any number of readonly observers.
`--takeover` replaces the controller.

Historical log reads walk generations from oldest to newest directly from
disk. A shim retains only the latest 4096 output event descriptors for live
coordination; a lagging follower catches up from disk and receives a `gap`
record only if rotation has already discarded the requested sequence. TTY
replay remains capped at 4 MiB. Terminal supervisors are unloaded from the
group shim after metadata and lifecycle state have been persisted. Later inspect
and log requests use the durable store.

The shim persists each lifecycle transition before publishing its notification,
and drains queued notifications before closing the last process's group socket.
The daemon keeps an ordered in-memory process snapshot for lifecycle events and
wait requests. Notifications never write their snapshots back to metadata.
Removal shares the event lock, waits for the terminal state to be observed, and
removes the process from that snapshot; late notifications cannot register it
again. Restart advances the execution number so older notifications are ignored.
Disk reconciliation at startup, on bridge reconnect, and periodically repairs
missed notifications. Process names remain unique within each workspace across
all records, including stopped processes; deleting a record releases its name.

The daemon and group shim redirect their own stdout and stderr through an
in-process rotating writer. Their active `amuxd.log` and `shim.log` files use
the same configurable size/count policy as process output and retain
timestamped file backups.

Internal daemon and shim observability is emitted as compact `tracing` events
to stderr, which is captured by that rotating writer. `AMUX_LOG` (then
`RUST_LOG`) controls filtering. Diagnostic spans carry stable control-plane
identifiers, while managed stdout/stderr/TTY bytes remain exclusively in the
`OutputEvent` data plane. CLI JSON and human-facing output are a third, public
interface and are never produced through tracing. The panic hook also emits
through tracing once diagnostics have initialized; bootstrap failures still
fall back to the inherited stderr file descriptor.

Restarted daemons discover processes from disk and reconnect to their shim
sockets. A live-looking record whose shim cannot be reached becomes `unknown`.
An unknown process from the current boot is not restarted automatically,
because its unmanaged process tree may still exist.

Named-daemon state removal uses the same persistent flock as daemon startup.
The lock file lives outside the daemon state directory and is intentionally
retained, allowing `daemon start` and destructive `daemon rm` to coordinate
even when no state directory currently exists.

## Runtime model and direction

A runtime should be modeled as an **execution target** plus an **execution
object**, rather than as one flat enum whose values encode every possible
combination.

The execution target is a typed object that identifies the control domain in
which work runs. It is more general than a `local` or `remote` flag. Examples
include the local machine, a cloud development machine, and a cluster with a
specific context and namespace. Connection details such as SSH or a cluster API
are properties of the target; they are not themselves the workload being
managed.

The executor resolves a command or workload specification into a typed,
persisted execution object. That object records the native identity and
resources that amux owns. It may represent one resource or an aggregate:

| Execution target | Execution object | Native identity | Support |
| --- | --- | --- | --- |
| local machine | process | PID and process-group ID | supported |
| machine | container | container ID | future direction |
| machine | container application | application ID and member resources | future direction |
| cluster | resource set | resource UIDs, such as controllers, pods, and services | future direction |

The entries marked as future direction illustrate the intended architecture;
they are not current compatibility promises. A foreground container or remote
client launched by the current host runtime is still recorded as a local
process, because amux owns the client process rather than the underlying
container, application, or remote resources.

Conceptually, the resolved state has this shape:

```text
runtime
├── target: typed target configuration
└── object
    ├── kind: process | container | container_application | resource_set
    ├── identity: provider-native identifiers
    └── resources: zero or more owned resource references
```

Runtime configuration belongs below the workspace boundary. A workspace is a
durable logical context and may provide defaults, but it should eventually be
able to contain executions targeting different machines or clusters. A reusable
environment stores the desired target and executor specification. The durable
execution record references that environment and stores the resolved execution
object. The current `Process` model is that durable execution record for the
local-process implementation; future object kinds do not have to imply a single
OS process.

A runtime backend interprets its execution object when implementing the common
lifecycle contract—start, inspect, logs, signal, stop, kill, wait, restart,
attach, remove, and recovery. State and logs may be aggregated when the object
owns multiple resources, while provider-native identities remain available for
inspection and targeted operations. Backends must declare unsupported
capabilities explicitly.

Current support is limited to a local-machine target and a process execution
object backed by an OS process group. The CLI and protocol identify this
implementation with the runtime value `host`. The existing workspace-level
`runtime` constraint represents that current limitation, not the intended
boundary for future runtime support.
