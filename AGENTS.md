# amux Engineering Constraints

## Diagnostic logging

amux has three distinct output planes. Keep them separate:

- Daemon and shim diagnostics use `tracing` events and spans.
- CLI stdout, JSON, tables, and user-facing stderr are public interfaces and
  must remain explicit writes. Never replace protocol output with tracing.
- Managed process stdout, stderr, and TTY bytes use `OutputEvent`. Never route
  them through tracing, because they may be binary and their ordering and raw
  offsets are part of the persistence contract.

Use these module-specific conventions:

- `src/daemon.rs`: trace daemon, workspace, group, bridge, and process lifecycle.
- `src/shim.rs`: trace supervision, spawn/wait, signal, PTY, attach ownership,
  and group lifecycle. Never trace an output chunk.
- `src/client.rs`: use `debug` for connection, spawn, and readiness diagnostics;
  do not record request or response bodies.
- `src/unix_http.rs`: use `debug` for transport failures only; do not record log
  streams, WebSocket frames, or payloads.
- `src/store.rs`: return contextual errors and remain quiet on successful I/O.
  Log persistence failures once at the daemon or shim boundary that handles them.
- `src/output.rs`: rotation, migration, pruning, and gaps may be diagnosed at
  operation boundaries. Never log event data or raw bytes.
- `src/logging.rs`: do not emit tracing events from the writer that backs the
  tracing sink; doing so can recurse. Propagate errors to a safe caller or use a
  non-tracing last-resort path.
- `src/config.rs`, `src/model.rs`, and `src/protocol.rs`: remain pure and return
  typed or contextual errors rather than logging.
- `src/bin/amux.rs`: preserve exact CLI and JSON output. Only hidden daemon/shim
  runtimes initialize the default diagnostics pipeline.

Level guidance:

- `info`: service readiness/shutdown and durable lifecycle transitions.
- `warn`: recovery, forced termination, replay gaps, and degraded operation.
- `error`: failures that prevent an operation or make durable state uncertain.
- `debug`: retries, routing, reconciliation, and rotation details.
- `trace`: very high-volume control details only; never process output bytes.

Attach stable identifiers as structured fields (`daemon_name`,
`daemon_instance_id`, `workspace_id`, `group_id`, `process_id`, `execution`).
Do not log environment variables, complete command arguments, HTTP bodies,
terminal data, raw output, or secrets. Prefer `#[instrument(skip_all)]` or an
explicit `.instrument(span)` for async work; do not keep an entered span guard
across `.await`.

Log an error once at the boundary that has enough context to act on it. Lower
layers should add `anyhow::Context` and return it. Expected API outcomes such as
not-found, conflicts, and invalid arguments are not `error` events.

Tests for diagnostics must also verify that CLI JSON stays parseable, managed
output is not copied into daemon/shim logs, filters work, and high-volume output
does not create one diagnostic event per chunk.
