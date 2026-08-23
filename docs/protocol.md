# Protocol

The control API is HTTP/1.1 over `AMUX_SOCKET`, versioned under `/v1`.

Endpoints cover health, workspace CRUD, group/environment listing, process
create/list/inspect/delete, signal, stop, wait, restart, logs, and attach.
Workspace targets accept IDs or names. Process targets accept IDs or
`workspace/process`.

Normal JSON responses contain `{ "ok": true, ... }`. Errors contain
`{ "ok": false, "error": { "code", "message", "details" } }`. Logs are
NDJSON events with sequence, execution, stream, offset, length, timestamp, and
UTF-8 or base64 data. If the requested `after` sequence has been discarded by
rotation, the stream first emits
`{"type":"gap","requested_after":10,"oldest_available":123}` and then
continues with retained output. Existing output-event JSON fields are unchanged.

Process output is persisted in `output/current/` and zero or more sortable UTC
timestamped `output/<timestamp>.bak/` generations. An event's raw stream offset
always refers to the stream file in the same generation as its `events.jsonl`.
The default active-generation limit is 64 MiB with two backups; it is configured
by `AMUX_LOG_ROTATE_SIZE` and `AMUX_LOG_BACKUP_COUNT`.

`amux daemon rm <name>` is a local CLI operation rather than an HTTP endpoint.
It takes the daemon startup lock, returns `daemon_running` if the lock is owned,
and removes the stopped daemon's state directory and stale socket. Its JSON
success response is `{"ok":true,"name":"dev","removed":true}`; repeated
removal succeeds with `removed:false`.

`GET /v1/processes/{target}/attach` upgrades to WebSocket. Binary frames are
raw terminal bytes. JSON frames carry `resize`, `signal`, and `exit` messages.
Query parameters are `readonly`, `takeover`, and `replay_bytes`.

Daemon/shim tracing diagnostics are not part of the control protocol. They are
written only to `amuxd.log` and workspace `shim.log`; they never appear in HTTP
responses, CLI JSON, or process log NDJSON.
