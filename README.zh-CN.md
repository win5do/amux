# amux

[English](README.md) | 简体中文

`amux` 是面向 AI Agent 的持久化进程与 TTY 运行时，支持日志持久化和人工终端接入，完全使用 Rust 实现。

工作区是由具名守护进程管理的持久化逻辑上下文。同一物理工作区中的进程共享一个 group shim，同时各自拥有独立的监督器、PTY、日志和进程组：

```text
amux CLI -> named daemon -> one group shim per workspace -> OS process groups
```

## 构建与快速开始

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"

amux workspace create dev
amux run --workspace dev --name api -- sh -c 'while true; do date; sleep 1; done'
amux run --workspace dev --name shell --tty -- /bin/sh
amux ps --workspace dev --json
amux attach dev/shell
```

当所选守护进程的 Unix socket 不存在时，CLI 会按需启动它。接入终端后，按 `Ctrl-P Ctrl-Q` 可以脱离终端而不终止进程。管道输出通过 `amux logs` 读取，不会被模拟成终端。

在 amux 管理的进程中，`AMUX_WORKSPACE` 为 `amux run --workspace` 和 `amux shell --workspace` 提供默认值。外层工作区管理器因此可以选择隔离的运行时，无需额外封装 amux CLI。`AMUX_PROCESS_ID` 也会被继承：嵌套的 `amux run` 和 `amux shell` 请求会自动记录父进程。

受管进程会收到由运行时设置的可信环境变量：`AMUX`、`AMUX_WORKSPACE`、`AMUX_WORKSPACE_ID`、`AMUX_PROCESS`、`AMUX_PROCESS_ID` 和 `AMUX_IO_MODE`。不允许从受管进程中嵌套执行 attach。

常用命令：

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

配置：

- `AMUX_DAEMON`：选择守护进程名称，默认为 `default`。
- `AMUX_STATE_DIR`：持久化元数据和输出日志的存储目录。
- `AMUX_SOCKET`：直接指定 Unix socket 路径，仅用于测试或调试。
- `AMUX_LOG_ROTATE_SIZE`：当前逻辑日志触发轮转的大小，默认为 `64MiB`；支持字节数、`KiB`、`MiB` 或 `GiB`，且必须大于零。
- `AMUX_LOG_BACKUP_COUNT`：保留的轮转代数，默认为 `2`，取值范围为 `0..16`。
- `AMUX_LOG`：守护进程和 shim 诊断日志的 tracing 过滤器，优先于 `RUST_LOG`，默认为 `amux=info`。
- 状态目录权限为 `0700`；socket 和状态文件权限为 `0600`。

具名守护进程的 socket 由规范化后的绝对数据根目录和守护进程名称共同确定，因此不同的 `AMUX_STATE_DIR` 可以各自使用 `default`。指向同一根目录的相对路径和已有符号链接会选择同一个 socket。socket 仍放在 `XDG_RUNTIME_DIR` 或系统临时目录下，以保持路径简短。显式设置 `AMUX_SOCKET` 时仍直接使用指定地址。

从仅按名称生成 socket 的旧版升级时，需要先用旧版程序停止运行中的守护进程，再启动新版。socket 地址变化不会迁移或删除已有元数据和日志；原有状态锁会阻止新版同时打开旧版仍占用的数据。停止守护进程也会停止它管理的进程。

`amux daemon rm <name>` 会永久删除指定守护进程的全部持久化状态。该命令只能在守护进程停止后执行；如果状态已不存在，重复执行也不会报错。它不会按需启动或连接守护进程。

进程输出存储在当前使用的 `output/current/` 目录，以及以 UTC 时间戳命名的 `output/<timestamp>.bak/` 历史目录中。每一代目录都包含原始流文件和对应的 `events.jsonl`，因此事件偏移量始终指向同一目录内的文件。`amux logs` 按从旧到新的顺序读取保留的日志。当 `--after` 指向已丢弃的历史记录时，JSONL 输出会包含一条 `gap` 记录；纯文本输出会先在 stderr 报告缺口，再从最早保留的输出继续读取。`amuxd.log` 和工作区的 `shim.log` 使用相同的大小与备份数量策略，备份文件名为 `<name>.<timestamp>.bak`。

守护进程和 shim 的诊断日志使用紧凑的 `tracing` 输出，包含稳定的守护进程、工作区、组、进程和执行标识，但不会包含受管进程输出、环境变量、请求体或终端字节。普通 CLI 表格及 JSON、受管进程的 `OutputEvent` 流各自保持独立的公共输出通道。启动后的 panic 信息也会进入同一诊断管线。

可分发的 Codex Skill 位于 [`skills/amux`](skills/amux)。将该目录以 `amux` 为名安装或链接到当前使用的 Codex 技能目录即可。Treefold 管理的 Session 可以直接使用同一个 Skill，因为 Treefold 会注入 `AMUX_DAEMON`、`AMUX_WORKSPACE` 和 `AMUX_PROCESS_ID`。

## 开发

```sh
just test
just lint
just build
```

支持 Linux 和 macOS。首个运行时驱动为 `host`。持久化能力支持 Treefold 和 CLI 重新连接；守护进程停止或意外退出时，系统会按设计停止其全部 group shim 和业务进程组。

## 许可证

版权所有 (c) 2026 win5do 和 amux 贡献者。

你可以任选以下一种许可证使用本项目：

- Apache License, Version 2.0
  （[LICENSE-APACHE](LICENSE-APACHE) 或 <https://www.apache.org/licenses/LICENSE-2.0>）
- MIT 许可证（[LICENSE-MIT](LICENSE-MIT) 或 <https://opensource.org/licenses/MIT>）

## 贡献

除非你明确声明，否则你有意提交并希望纳入本项目的任何贡献（定义见 Apache-2.0 许可证），均按上述双许可证授权，不附加任何其他条款或条件。
