# amux

English | [简体中文](README.zh-CN.md)

`amux` is a persistent process and TTY runtime for AI agents, with durable logs
and human attach. It is implemented entirely in Rust.

A workspace is a durable logical context backed by a named daemon. Processes in
the same physical workspace share a group shim while retaining independent
supervisors, PTYs, logs, and process groups:

```text
amux CLI -> named daemon -> one group shim per workspace -> OS process groups
```

## Build and quick start

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"

amux workspace create dev
amux run --workspace dev --name api -- sh -c 'while true; do date; sleep 1; done'
amux run --workspace dev --name shell --tty -- /bin/sh
amux ps --workspace dev --json
amux attach dev/shell
```

The CLI lazily starts the selected daemon when its Unix socket is absent. During
attach, `Ctrl-P Ctrl-Q` detaches without terminating the process. Pipe output
is read with `amux logs`; it is never exposed as a fake terminal.

Inside an amux-managed process, `AMUX_WORKSPACE` supplies the default for
`amux run --workspace` and `amux shell --workspace`. This lets an enclosing
workspace manager select an isolated runtime without wrapping the amux CLI.
`AMUX_PROCESS_ID` is also inherited: nested `amux run` and `amux shell`
requests automatically record their parent process.

Managed processes receive trusted `AMUX`, `AMUX_WORKSPACE`,
`AMUX_WORKSPACE_ID`, `AMUX_PROCESS`, `AMUX_PROCESS_ID`, and `AMUX_IO_MODE`
variables. Nested attach from a managed process is rejected.

Useful commands:

```sh
amux daemon list
amux daemon start work
amux daemon status work
amux --daemon work ps
amux daemon stop work
amux daemon rm work
amux workspace ls
amux ps --workspace dev
amux inspect dev/api
amux logs --after 10 --jsonl dev/api
amux logs --follow dev/api
amux attach --readonly dev/shell
amux attach --takeover dev/shell
amux signal dev/shell INT
amux stop dev/api
amux kill dev/api
amux wait dev/api --json
amux restart dev/api
amux rm dev/api
amux workspace rm dev
```

Configuration:

- `AMUX_DAEMON`: selected daemon name (default: `default`).
- `AMUX_STATE_DIR`: persistent metadata and output logs.
- `AMUX_SOCKET`: test/debug-only direct Unix socket override.
- `AMUX_LOG_ROTATE_SIZE`: active logical log size before rotation (default
  `64MiB`; accepts bytes, `KiB`, `MiB`, or `GiB`, and must be greater than
  zero).
- `AMUX_LOG_BACKUP_COUNT`: retained rotated generations (default `2`, range
  `0..16`).
- `AMUX_LOG`: tracing filter for daemon/shim diagnostics. It takes precedence
  over `RUST_LOG` and defaults to `amux=info`.
- State directories are mode `0700`; sockets and state files are mode `0600`.

Named daemon sockets are scoped to the normalized absolute state root and daemon
name, so separate `AMUX_STATE_DIR` roots can each use `default`. Relative paths
and existing symlink aliases of the same root select the same socket. Sockets
remain in `XDG_RUNTIME_DIR` or the system temporary directory to keep paths short.
An explicit `AMUX_SOCKET` still selects that exact endpoint.

When upgrading from name-only socket addressing, stop running daemons with the
old binary before starting the new binary. The new socket address does not
change stored metadata or logs; the existing state lock prevents a second daemon
from opening state still owned by an old daemon. Stopping a daemon also stops
its managed processes.

`amux daemon rm <name>` permanently deletes all persistent state for a named
daemon. It is accepted only after that daemon has stopped, is idempotent when
the state is already absent, and does not lazily start or connect to a daemon.

Process output is stored as one active `output/current/` generation plus UTC
timestamped `output/<timestamp>.bak/` generations. Every generation contains
its raw stream files and matching `events.jsonl`, so event offsets always refer
to files in the same directory. `amux logs` reads retained generations oldest
first. When `--after` points into discarded history, JSONL output includes a
`gap` record and plain output reports the gap on stderr before continuing with
the oldest retained output. `amuxd.log` and workspace `shim.log` use the same
size and backup-count policy with `<name>.<timestamp>.bak` backups.

Daemon and shim diagnostics use compact `tracing` output. They include stable
daemon, workspace, group, process, and execution identifiers, but never managed
process output, environment variables, request bodies, or terminal bytes.
Ordinary CLI tables/JSON and managed process `OutputEvent` streams remain
separate public output planes. Panic information is routed through the same
diagnostic pipeline after startup.

The distributable Codex Skill lives at [`skills/amux`](skills/amux). Install or
link that directory as `amux` in the active Codex skills home. Treefold-managed
Sessions can use the same Skill directly because Treefold injects
`AMUX_DAEMON`, `AMUX_WORKSPACE`, and `AMUX_PROCESS_ID`.

## Development

```sh
just test
just lint
just build
```

Linux and macOS are supported. The first runtime driver is `host`. Persistence
covers Treefold/CLI reconnects. Stopping or losing a daemon deliberately stops
all of its group shims and business process groups.

## License

Copyright (c) 2026 win5do and amux contributors.

Licensed under either of:

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
