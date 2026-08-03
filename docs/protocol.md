# Protocol

The control API is HTTP/1.1 over the `AMUX_SOCKET` Unix socket and is versioned under `/v1`.

Endpoints include workspace CRUD, group/environment listing, process create/list/inspect/delete, signal, stop, wait, restart, logs, attach, and health. Workspace identifiers accept ID or name. Process identifiers accept ID or `workspace/process`.

Normal JSON responses use `{ "ok": true, ... }`. Errors use `{ "ok": false, "error": { "code", "message", "details" } }`. Streaming logs are NDJSON. Each event contains sequence, execution, stream, offset, length, timestamp and data encoded as UTF-8 text or base64.

`GET /v1/processes/{target}/attach` upgrades to WebSocket. Binary frames are raw terminal bytes: client-to-server is input and server-to-client is output. JSON frames carry `resize`, `signal`, `ownership_changed`, `exit`, and `error`. Query parameters select `readonly`, `takeover`, and `replay_bytes`. The attach protocol is v1 by virtue of the endpoint version; incompatible future framing will use a new version.
