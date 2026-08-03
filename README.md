# amux

amux is a persistent process and TTY runtime for AI agents, with seamless human attach. A workspace is a durable logical context; its processes are peers and remain owned by per-process shims when the CLI or daemon exits.

## Build and quick start

```sh
make build
export PATH="$PWD/bin:$PATH"

amux workspace create dev
amux run --workspace dev --name api -- sh -c 'while true; do date; sleep 1; done'
amux run --workspace dev --name shell --tty -- /bin/sh
amux ps --workspace dev --json
amux attach dev/shell
```

The CLI starts `amuxd` automatically when its socket is absent. During attach, `Ctrl-P Ctrl-Q` detaches without terminating the remote process. Pipe processes are observed with `amux logs --follow dev/api`; they are not exposed as fake TTY sessions.

Useful commands:

```sh
amux logs --after 10 --jsonl dev/api
amux attach --readonly dev/shell
amux attach --takeover dev/shell
amux signal dev/shell INT
amux stop dev/api
amux wait dev/api --json
amux restart dev/api
amux rm dev/api
amux workspace rm dev
```

Configuration:

- `AMUX_STATE_DIR`: persistent metadata and output logs.
- `AMUX_SOCKET`: daemon Unix socket.
- State directories are mode `0700`; sockets and state files are mode `0600`.

## Development

```sh
make test
make test-race
make vet
```

Linux and macOS are supported. Windows binaries return `runtime_not_supported`. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), [runtime drivers](docs/runtime-drivers.md), and [topology](docs/topology-model.md).

## Current limitations

- Host runtime only; no container/image/network/cgroup management.
- Persistence covers client and daemon restarts, not machine reboot. A stale process record can be manually restarted after reboot.
- TTY replay is raw trailing bytes, not a reconstructed terminal screen.
- Local single-user Unix-socket security model only.
- No automatic restart policies, TUI, VT parser, shell markers, or waiting-for-input detection.
