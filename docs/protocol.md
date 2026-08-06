# Protocol

The control API is HTTP/1.1 over `AMUX_SOCKET`, versioned under `/v1`.

Endpoints cover health, workspace CRUD, group/environment listing, process
create/list/inspect/delete, signal, stop, wait, restart, logs, and attach.
Workspace targets accept IDs or names. Process targets accept IDs or
`workspace/process`.

Normal JSON responses contain `{ "ok": true, ... }`. Errors contain
`{ "ok": false, "error": { "code", "message", "details" } }`. Logs are
NDJSON events with sequence, execution, stream, offset, length, timestamp, and
UTF-8 or base64 data.

`GET /v1/processes/{target}/attach` upgrades to WebSocket. Binary frames are
raw terminal bytes. JSON frames carry `resize`, `signal`, and `exit` messages.
Query parameters are `readonly`, `takeover`, and `replay_bytes`.

