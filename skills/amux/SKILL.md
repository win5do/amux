---
name: amux
description: Run persistent services and watchers, or processes whose logs the user needs to follow or retain independently, with the `amux` CLI. Inspect and control managed processes, logs, and TTYs, including in Treefold Sessions. Ordinary one-shot tests, builds, checks, scripts, and temporary commands should use the Agent's normal execution tool.
---

# amux

Use amux for persistent services or watchers that should keep running across Agent turns, or when the user needs to follow or retain a process's logs independently of the Agent's command output.

Run one-shot tests, builds, lint/type checks, scripts, and temporary diagnostics directly with the Agent's normal execution tool. A command taking a long time, needing a TTY, or inheriting Treefold/amux environment variables does not by itself require management. An ongoing test watch session belongs in amux; a normal test run does not unless the user needs that independent log access.

Apply the following workflow only when starting or managing a process that needs amux.

## Select the runtime

Inspect the inherited context before creating a workspace:

```bash
printf '%s\n' "$AMUX_WORKSPACE"
amux workspace inspect "$AMUX_WORKSPACE" --json
```

Inside an amux- or Treefold-managed process, keep the inherited `AMUX_SOCKET`, `AMUX_STATE_DIR`, and `AMUX_WORKSPACE`. `amux run` and `amux shell` use `AMUX_WORKSPACE` as their default, so do not create a second workspace or daemon.

Outside a managed context, create a workspace explicitly:

```bash
amux workspace create dev --root-dir "$PWD"
```

## Run processes

`amux run` defaults to pipe mode outside managed contexts. Treefold Sessions
set `AMUX_IO_MODE=tty` so services and watchers open as interactive terminals
in the sidebar, including Ctrl+C support. Keep this inherited default:

```bash
amux run --name api -- cargo run
amux run --name web -- npm run dev
```

For a managed process, use `--tty` when terminal semantics are required regardless of the inherited default:

```bash
amux run --name repl --tty -- python
```

For a managed process, use `--io-mode pipe` explicitly when separate stdout/stderr pipes are required:

```bash
amux run --name api --io-mode pipe -- cargo run
```

Choose stable, descriptive names. Do not use amux as a second child-Agent orchestrator when the active Agent already provides delegation.

## Inspect and read output

```bash
amux ps --workspace "$AMUX_WORKSPACE"
amux inspect "$AMUX_WORKSPACE/api" --json
amux logs "$AMUX_WORKSPACE/api"
amux logs --follow "$AMUX_WORKSPACE/api"
amux logs --after 100 --jsonl "$AMUX_WORKSPACE/api"
```

Use `--json` for finite commands and `--jsonl` for log streams. Do not combine `amux logs` with `--json`.

## Control lifecycle

```bash
amux signal "$AMUX_WORKSPACE/api" INT
amux restart "$AMUX_WORKSPACE/api"
amux stop "$AMUX_WORKSPACE/api"
amux wait "$AMUX_WORKSPACE/api" --json
amux rm "$AMUX_WORKSPACE/api"
```

- Inspect before destructive control when ownership is unclear.
- Prefer graceful `stop`; use `kill` only when graceful shutdown fails or the user explicitly requests it.
- Remove only exited processes whose retained metadata and logs are no longer needed.
- Never stop, restart, kill, or remove the current process identified by `AMUX_PROCESS_ID`.

To permanently delete every workspace, process record, and retained log owned
by a named daemon, stop it first and then remove its state:

```bash
amux daemon stop dev
amux daemon rm dev
```

`daemon rm` is destructive, operates directly on disk, and refuses a running
daemon. Inspect the daemon and confirm ownership before using it. Repeating the
command for already-absent state is safe.

Process logs rotate by logical generation and `amux logs` transparently reads
retained backups. A plain log stream reports discarded history on stderr; a
JSONL stream emits a `type: "gap"` record. Long-running daemon and shim logs use
the same defaults. Override them only when needed with
`AMUX_LOG_ROTATE_SIZE` (default `64MiB`) and `AMUX_LOG_BACKUP_COUNT` (default
`2`, allowed `0..16`).

Daemon and shim diagnostics are compact tracing logs in `amuxd.log` and the
workspace `shim.log`. Set `AMUX_LOG=amux=debug` before starting the daemon for
deeper control-plane diagnostics. These files do not contain managed process
stdout/stderr/TTY data; continue to use `amux logs` for that output.

## Attach to TTYs

```bash
amux attach --readonly "$AMUX_WORKSPACE/shell"
amux attach --takeover "$AMUX_WORKSPACE/shell"
```

Use readonly attach for observation. Use takeover only when interactive control is required. Nested attach from within an amux-managed process is unsupported; detach first and attach from an external terminal.
