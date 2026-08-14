# amux

`amux` is a persistent process and TTY runtime for AI agents, with seamless
human attach. It is implemented entirely in Rust.

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
- State directories are mode `0700`; sockets and state files are mode `0600`.

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

Licensed under either of:

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
