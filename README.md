# amux

`amux` is a persistent process and TTY runtime for AI agents, with seamless
human attach. It is implemented entirely in Rust.

A workspace is a durable logical context. Its processes are peers and remain
owned by per-process shims when the CLI or daemon exits:

```text
amux CLI -> amuxd -> one amux-shim per process -> OS process group
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

The CLI starts `amuxd` automatically when its Unix socket is absent. During
attach, `Ctrl-P Ctrl-Q` detaches without terminating the process. Pipe output
is read with `amux logs`; it is never exposed as a fake terminal.

Inside an amux-managed process, `AMUX_WORKSPACE` supplies the default for
`amux run --workspace` and `amux shell --workspace`. This lets an enclosing
workspace manager select an isolated runtime without wrapping the amux CLI.

Managed processes receive trusted `AMUX`, `AMUX_WORKSPACE`,
`AMUX_WORKSPACE_ID`, `AMUX_PROCESS`, `AMUX_PROCESS_ID`, and `AMUX_IO_MODE`
variables. Nested attach from a managed process is rejected.

Useful commands:

```sh
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

- `AMUX_STATE_DIR`: persistent metadata and output logs.
- `AMUX_SOCKET`: daemon Unix socket.
- State directories are mode `0700`; sockets and state files are mode `0600`.

The distributable Codex Skill lives at [`skills/amux`](skills/amux). Install or
link that directory as `amux` in the active Codex skills home. Treefold-managed
Sessions can use the same Skill directly because Treefold injects its private
`AMUX_STATE_DIR`, `AMUX_SOCKET`, and `AMUX_WORKSPACE`.

## Development

```sh
just test
just lint
just build
```

Linux and macOS are supported. The first runtime driver is `host`. Persistence
covers CLI and daemon restarts; process shims are not automatically restored
after a machine reboot.

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
