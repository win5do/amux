---
name: amux
description: Run and manage persistent processes and TTYs with the `amux` CLI, including starting workspace services, listing or inspecting processes, reading or following logs, attaching to terminals, signaling, stopping, restarting, waiting for, and removing processes. Use for debug servers, build watchers, test runners, shells, or other long-lived commands, including inside Treefold-managed Sessions that provide `AMUX_SOCKET` and `AMUX_WORKSPACE`.
---

# amux

Use amux as the process runtime. Prefer it for long-lived commands whose logs or lifecycle must survive the invoking shell or Agent turn.

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

Use pipe mode for services and watchers:

```bash
amux run --name api -- cargo run
amux run --name web -- npm run dev
```

Use `--tty` only when the command needs terminal semantics:

```bash
amux run --name repl --tty -- python
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
