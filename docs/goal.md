# Implementation Prompt：构建 amux

请在当前仓库中设计并实现 **amux** 的第一个可运行版本。

amux 是一个参考 Docker、containerd-shim、Kubernetes Pod 和 tmux client/server 架构的持久化进程与 TTY runtime，主要面向 AI Agent，同时允许人类随时 attach 到正在运行的交互式进程。

请使用 **Go** 实现。

开始编码前，先检查当前仓库结构与已有代码。如果仓库为空，则创建完整项目结构。

不要只输出设计文档。请实际实现代码、测试、CLI、daemon 和必要文档，并运行能够在当前环境中运行的测试。

---

# 1. 产品定位

amux 的核心定位：

> A persistent process and TTY runtime for AI agents, with seamless human attach.

它负责：

* 创建和管理持久 Workspace
* 在 Workspace 中运行零个或多个同层 Process
* 管理 Process 生命周期
* 支持 pipe 和 TTY 两种 I/O 模式
* 允许 Agent 通过结构化 CLI/API 控制 Process
* 允许人类 attach 到已有 TTY
* 保存可增量读取的输出日志
* 提供 host runtime
* 为 containerd 和 Kubernetes runtime 保留明确扩展接口

amux 不是：

* 终端模拟器
* tmux 或 Zellij 的 TUI 替代品
* window、tab、pane 或 split 管理器
* 容器镜像引擎
* Kubernetes 调度器
* shell 字符串包装脚本

---

# 2. 核心资源模型

amux 使用以下四层逻辑模型：

```text
Workspace
└── Group
    └── Environment
        └── Process
```

## 2.1 Workspace

Workspace 是长期存在的逻辑执行上下文，包含：

* 名称与稳定 ID
* runtime 类型
* 默认工作目录
* 默认环境变量
* 零个或多个 Group
* Process 状态和事件
* 日志
* 生命周期策略

Workspace 生命周期不能绑定到某个所谓的“主进程”。

一个 Workspace 可以在没有任何运行中 Process 时继续存在。

## 2.2 Group

Group 表示：

* 共置边界
* 调度边界
* 共享网络或 volume 的边界
* runtime-specific placement

它在不同 runtime 中的映射可以不同。

典型映射：

```text
host        → 逻辑分组，可选映射到 cgroup
containerd  → sandbox 或一组关联 container
kubernetes  → Pod
```

第一版 host runtime 中，Group 可以是隐式的 `default`。

## 2.3 Environment

Environment 表示执行环境和隔离边界，包括：

* image 或 root filesystem
* user
* mounts
* environment variables
* security context
* resource limits

典型映射：

```text
host        → host 环境
containerd  → OCI container
kubernetes  → Pod 中的 container
```

多个 Process 可以共享一个 Environment，也可以分别属于不同 Environment。

第一版 host runtime 中，Environment 可以是隐式的 `host`。

## 2.4 Process

Process 是 amux 真正管理、观察和 attach 的执行单元。

Process 包含：

* argv
* cwd
* environment
* runtime
* Group 和 Environment 引用
* pipe 或 TTY I/O
* PID 或 backend process identifier
* 状态
* exit code
* terminating signal
* restart policy
* 输出日志
* attach ownership

Workspace 内的所有 Process 都是同层资源。

不得强制定义 primary process 或 main process。

例如：

```text
Workspace: dev
└── Group: default
    └── Environment: host
        ├── Process: api
        ├── Process: tests
        ├── Process: agent
        └── Process: shell
```

其中：

* `api` 不比 `tests` 更主要
* `shell` 不是特殊生命周期实体
* 任意 Process 都可以独立启动、停止、失败或重启
* 某个 Process 退出不能自动销毁其他 Process

## 2.5 TTY 是 Process 的 I/O 模式

不要把 Terminal、Shell 或 Pane 设计成独立的底层生命周期资源。

```text
Process
├── io_mode: pipe
└── io_mode: tty
```

Shell 只是一个 TTY Process：

```text
command: ["/bin/zsh", "-l"]
io_mode: tty
```

以下内容也都是普通 TTY Process：

* Python REPL
* debugger
* SSH client
* Claude Code
* Codex CLI
* Neovim
* 数据库 shell

---

# 3. Runtime 映射原则

用户控制逻辑拓扑，但不直接控制 backend-specific wiring。

用户可以决定：

* 哪些 Process 位于同一个 Group
* 哪些 Process 共享同一个 Environment
* 哪些 Process 使用独立 Environment
* 哪些 Environment 共置
* image、mount、resource 和 security 设置

用户不应该直接操作：

* namespace keeper
* pause process
* containerd task ID
* shim FIFO
* Kubernetes remotecommand protocol
* PTY 文件描述符
* runtime 内部 socket

统一规则：

```text
Group 决定共置边界
Environment 决定容器或隔离边界
Process 决定执行与 TTY 边界
```

---

# 4. 不同 Runtime 的预期映射

第一版只实现 host，但设计和文档必须包含以下映射。

## 4.1 Host

默认映射：

```text
Workspace
└── Group: default
    └── Environment: host
        ├── Process A
        ├── Process B
        └── Process C
```

每个 Process 对应：

* 一个 OS process tree
* 一个独立 process group
* 独立 I/O
* 独立退出状态
* 可选 PTY

## 4.2 containerd

containerd runtime 后续应支持两种拓扑。

### 共享 Environment

```text
Group
└── OCI Container: tools
    ├── Process: agent
    ├── Process: shell
    └── Process: tests
```

Environment container 内部可以运行一个隐藏的 amux shim 或 keeper，用于：

* 保持 Environment 存活
* 启动用户 Process
* 持有 PTY
* 保存日志
* 回收子进程

隐藏基础设施进程不能成为用户可见的 primary process。

### 独立 Environment

```text
Group
├── Container: agent-env
│   └── Process: agent
├── Container: api-env
│   └── Process: api
└── Container: test-env
    └── Process: tests
```

Environment 引用关系本身应定义共享或隔离拓扑，不要只依赖类似 `container_per_process` 的布尔开关。

## 4.3 Kubernetes

推荐映射：

```text
Group       → Pod
Environment → Container
Process     → Container 内由 amux shim 管理的进程
```

示例：

```text
Workspace: dev
└── Group: dev-pod
    ├── Environment: tools
    │   ├── Process: agent
    │   ├── Process: shell
    │   └── Process: api
    └── Environment: database
        └── Process: postgres
```

需要在设计文档中明确：

* 同一个 Group 通常映射为同一个 Pod
* 不同 Group 可以映射为不同 Pod
* 同一个 Environment 中可以动态创建多个 Process
* 新增 Environment 通常意味着新增 container
* 普通 Kubernetes Pod 运行后不能随意动态增加普通 container
* Kubernetes runtime 不应仅依赖 `kubectl exec` 来实现长期可重连 Process
* 更合理的设计是在 Environment container 内运行 amux shim

Kubernetes 负责：

* Pod 调度
* container 创建
* image
* volume
* network
* resource limit

amux shim 负责：

* Process 生命周期
* pipe/TTY
* attach/detach
* input ownership
* output event log
* exit status

---

# 5. 默认拓扑与高级配置

大多数用户不应被迫理解 Group 和 Environment。

## 5.1 Host 默认值

下面的命令：

```bash
amux workspace create dev --runtime host
amux run --workspace dev --name agent --tty -- claude
```

自动使用：

```text
Group       = default
Environment = host
```

## 5.2 未来 containerd 默认值

默认使用 shared 模式：

```text
一个 Workspace
└── 一个默认 Group
    └── 一个共享 Environment
        └── 多个 Process
```

适用于 AI coding workspace，因为：

* shell、agent 和测试共享文件系统
* 工具链一致
* Process 启动快
* 便于人工 attach

后续可提供三种预设：

```text
shared
isolated
custom
```

含义：

```text
shared:
  多个 Process 共享一个 Environment

isolated:
  每个 Process 自动使用独立 Environment

custom:
  用户显式定义 Group 和 Environment
```

不要在第一版实现这些容器模式，但接口和配置模型不能阻止它们。

---

# 6. MVP 进程架构

实现三个二进制：

```text
amux
amuxd
amux-shim
```

## 6.1 amux

CLI 客户端，服务于：

* AI Agent
* 人类操作
* shell script
* 自动化工具

CLI 通过 Unix domain socket 与 amuxd 通信。

## 6.2 amuxd

控制平面，负责：

* Workspace 管理
* Group 和 Environment 元数据
* Process 元数据
* runtime driver 注册
* shim 启动和发现
* API 服务
* target 解析
* 状态查询
* 请求路由

amuxd 不应直接成为所有 PTY 的唯一生命周期 owner。

## 6.3 amux-shim

每个 Process 启动一个独立 shim。

shim 负责：

* 启动真实 Process
* 持有 PTY master 或 pipes
* 等待和回收 Process
* 记录输出事件
* 接收输入
* resize TTY
* 转发 signal
* 管理 attach client
* 保存最终退出状态

参考 containerd-shim：

```text
amux CLI
    │
    ▼
amuxd
    │
    ▼
amux-shim
    │
    ├── PTY master / pipes
    └── managed process tree
```

要求：

* CLI 退出不能影响 Process
* attach client 退出不能关闭 Process
* amuxd 重启时 shim 和 Process 应继续运行
* amuxd 应能重新发现已有 shim
* Process 状态必须可以从 shim 或持久状态恢复

---

# 7. 第一版 Runtime：host

只实现：

```text
host
```

支持：

* Linux
* macOS

Windows 返回明确的 `runtime_not_supported` 或 `unsupported_platform`。

## 7.1 Host runtime 能力

必须支持：

* argv 形式启动命令
* 不经过 shell 字符串拼接
* cwd
* environment
* process group
* pipe I/O
* PTY I/O
* signal
* graceful stop
* force kill
* wait
* exit code
* terminating signal
* TTY resize
* Process tree 清理

停止 Process 时：

1. 向整个 process group 发送 `SIGTERM`
2. 等待可配置宽限期
3. 仍未退出则发送 `SIGKILL`

## 7.2 PTY 要求

TTY 模式必须正确处理：

* PTY master/slave
* controlling terminal
* session/process group
* stdin/stdout/stderr 连接
* `SIGWINCH`
* detach 后 Process 保持运行
* Linux PTY close 时可能返回的 `EIO`
* 非 UTF-8 字节
* client resize

建议使用：

```text
os/exec
github.com/creack/pty
golang.org/x/sys/unix
```

不要引入 cgo。

---

# 8. Runtime Driver 接口

runtime driver 接口应保持小而清晰。

不要让上层 daemon 直接依赖 `os/exec.Cmd`。

建议定义类似：

```go
type Runtime interface {
    Name() string

    PrepareWorkspace(
        ctx context.Context,
        workspace WorkspaceSpec,
    ) error

    StartProcess(
        ctx context.Context,
        workspace Workspace,
        group Group,
        environment Environment,
        process ProcessSpec,
    ) (ProcessHandle, error)

    InspectProcess(
        ctx context.Context,
        handle ProcessHandle,
    ) (ProcessStatus, error)

    SignalProcess(
        ctx context.Context,
        handle ProcessHandle,
        signal Signal,
    ) error

    StopProcess(
        ctx context.Context,
        handle ProcessHandle,
        gracePeriod time.Duration,
    ) error

    RemoveWorkspace(
        ctx context.Context,
        workspace Workspace,
    ) error
}
```

具体接口可根据实现调整，但必须满足：

* host syscall 代码隔离在 `internal/runtime/host`
* runtime driver 可以注册
* Workspace 明确记录 runtime
* Group、Environment 和 Process 不依赖 host PID 模型
* containerd/Kubernetes 可在未来实现同一逻辑资源模型

不要为了预留扩展创建大量无行为的空接口。

---

# 9. 数据模型

## 9.1 Workspace

```go
type Workspace struct {
    ID        string
    Name      string
    Runtime   string
    RootDir   string
    Env       map[string]string
    State     WorkspaceState
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

状态：

```text
created
active
idle
removing
error
```

## 9.2 Group

```go
type Group struct {
    ID          string
    WorkspaceID string
    Name        string
    Placement   map[string]string
    CreatedAt   time.Time
}
```

第一版 host runtime 自动创建 `default` Group。

## 9.3 Environment

```go
type Environment struct {
    ID          string
    WorkspaceID string
    GroupID     string
    Name        string
    Image       string
    Env         map[string]string
    Mounts      []MountSpec
    RuntimeData map[string]string
    CreatedAt   time.Time
}
```

第一版 host runtime 自动创建 `host` Environment。

`Image`、`Mounts` 和部分 runtime 字段在 host runtime 中可以为空。

## 9.4 Process

```go
type Process struct {
    ID            string
    WorkspaceID   string
    GroupID       string
    EnvironmentID string

    Name        string
    Command     []string
    Cwd         string
    Env         map[string]string
    IOMode      IOMode
    Runtime     string

    PID            int
    ProcessGroupID int
    RuntimeHandle  string

    State      ProcessState
    ExitCode   *int
    ExitSignal string

    CreatedAt  time.Time
    StartedAt  *time.Time
    FinishedAt *time.Time
}
```

Process 状态：

```text
created
starting
running
stopping
exited
failed
unknown
```

同一个 Workspace 内 Process 名称必须唯一。

所有资源同时具有：

* 稳定 ID
* 用户可读 Name

target 同时支持：

```text
workspace-name/process-name
process-id
```

---

# 10. CLI 设计

CLI 风格参考 Docker，但优化为 Agent-first。

## 10.1 Workspace

```bash
amux workspace create dev
amux workspace create dev --runtime host

amux workspace ls
amux workspace inspect dev
amux workspace rm dev
```

## 10.2 Process

```bash
amux run \
  --workspace dev \
  --name api \
  -- npm run dev

amux run \
  --workspace dev \
  --name agent \
  --tty \
  -- claude

amux ps
amux ps --workspace dev

amux inspect dev/agent
amux logs dev/api
amux logs --follow dev/api
amux logs --after 1024 dev/api

amux attach dev/agent
amux attach --readonly dev/agent
amux attach --takeover dev/agent

amux signal dev/agent INT
amux stop dev/agent
amux kill dev/agent
amux wait dev/agent
amux rm dev/agent
```

可以提供：

```bash
amux shell --workspace dev --name debug
```

它只是创建默认 shell TTY Process 的便利命令。

不得形成独立的 Terminal 或 Shell 底层资源模型。

## 10.3 Group 和 Environment CLI

第一版 host runtime 不要求用户显式创建 Group 或 Environment。

但公共模型和 API 应允许未来增加：

```bash
amux group create ...
amux environment create ...
amux run --environment tools ...
```

第一版不要为了未来命令实现无用的复杂 CLI。

---

# 11. `run` 语义

`amux run` 默认异步启动 Process。

命令成功表示：

* daemon 已接受请求
* shim 已成功启动
* Process 已进入 `starting` 或 `running`

支持：

```text
--tty
--name
--workspace
--cwd
--env KEY=VALUE
--runtime host
```

未来可以增加：

```text
--group
--environment
```

但 host 默认自动解析为：

```text
group       = default
environment = host
```

---

# 12. Attach 语义

## 12.1 TTY Process

`amux attach`：

* 将本地终端切换到 raw mode
* 转发键盘字节
* 转发 PTY 输出
* 监听本地终端 resize
* 向 shim 发送 rows/cols
* detach 后远端 Process 继续运行

默认 detach sequence：

```text
Ctrl-P Ctrl-Q
```

支持：

```bash
amux attach --detach-keys ctrl-p,ctrl-q ...
```

再次 attach 必须进入同一个 TTY Process，而不是启动新 shell。

## 12.2 Pipe Process

Pipe Process 使用：

```bash
amux logs --follow ...
```

不要把 pipe I/O 假装成完整 TTY attach。

---

# 13. Attach Ownership

每个 TTY Process 可以有：

* 一个 controller
* 多个 readonly observer

只有 controller 可以：

* 写入 PTY
* resize PTY
* 发送交互 signal

规则：

```text
amux attach
  无 controller 时获取控制权

amux attach --readonly
  只观察

amux attach
  已有 controller 时返回 controller_exists

amux attach --takeover
  强制接管
```

controller 断开后必须自动释放。

observer 不能改变 TTY size。

所有写入必须进入 shim 内的单一串行输入循环。

不能允许多个 client 直接并发写 PTY。

当前 controller 的终端尺寸决定 PTY size。

readonly observer 的尺寸不能影响远端 PTY。

---

# 14. 输出与日志

每个 Process 的输出具有单调递增 sequence number。

```go
type OutputEvent struct {
    Sequence  uint64
    Timestamp time.Time
    Stream    string
    Offset    int64
    Length    int
}
```

Stream：

```text
stdout
stderr
tty
```

要求：

* pipe 模式分别保存 stdout 和 stderr
* TTY 模式保存合并后的原始 PTY 字节流
* 日志必须持久化到磁盘
* Agent 可以通过 sequence 增量读取
* 不能要求 Agent 重复消费完整日志

建议目录：

```text
process-dir/
├── metadata.json
├── status.json
├── events.jsonl
├── stdout.log
├── stderr.log
├── tty.log
└── shim.sock
```

支持：

```bash
amux logs --after <sequence>
```

第一版不需要：

* VT framebuffer
* ANSI normalization
* 当前屏幕 snapshot
* shell command marker
* prompt 检测
* 自动判断 waiting for input

可提供：

```bash
amux attach --replay-bytes N ...
```

进入 live stream 前重放一段原始 TTY 输出。

必须在文档中说明：重放字节不等价于恢复完整终端屏幕。

---

# 15. API 与通信协议

## 15.1 Control API

使用：

```text
HTTP/1.1 over Unix domain socket
```

API 版本：

```text
/v1
```

至少实现：

```text
POST   /v1/workspaces
GET    /v1/workspaces
GET    /v1/workspaces/{id}
DELETE /v1/workspaces/{id}

GET    /v1/groups
GET    /v1/environments

POST   /v1/workspaces/{id}/processes
GET    /v1/processes
GET    /v1/processes/{id}
DELETE /v1/processes/{id}

POST   /v1/processes/{id}/signal
POST   /v1/processes/{id}/stop
GET    /v1/processes/{id}/logs
GET    /v1/processes/{id}/wait
```

CLI 必须调用统一 client package。

CLI 不能直接修改 daemon 状态文件。

## 15.2 Attach API

使用以下任一方案：

* Unix socket 上的 WebSocket upgrade
* 明确文档化的双向 framed protocol

客户端到服务端：

```text
input
resize
signal
take_control
release_control
```

服务端到客户端：

```text
output
state
ownership_changed
exit
error
```

要求：

* 协议有版本号
* 原始终端 I/O 使用 binary frame
* 控制消息使用 JSON 或其他明确编码
* 慢 client 不能阻塞 PTY authoritative reader

---

# 16. Agent 友好的结构化输出

所有非交互命令支持：

```bash
--json
```

流式命令支持：

```bash
--jsonl
```

例如：

```bash
amux run --json ...
amux ps --json
amux inspect --json ...
amux logs --jsonl --follow ...
amux wait --json ...
```

成功响应：

```json
{
  "ok": true,
  "process": {
    "id": "proc_123",
    "workspace_id": "ws_123",
    "group_id": "group_default",
    "environment_id": "env_host",
    "name": "agent",
    "runtime": "host",
    "state": "running",
    "io_mode": "tty"
  }
}
```

错误响应：

```json
{
  "ok": false,
  "error": {
    "code": "process_not_found",
    "message": "process dev/agent was not found",
    "details": {}
  }
}
```

稳定错误 code 至少包括：

```text
invalid_argument
workspace_not_found
group_not_found
environment_not_found
process_not_found
name_conflict
runtime_not_supported
process_not_running
tty_required
controller_exists
permission_denied
internal_error
```

JSON 模式不能混入人类提示文本。

结构化日志中的原始字节：

合法 UTF-8：

```json
{
  "sequence": 42,
  "stream": "tty",
  "data": {
    "encoding": "utf8",
    "text": "hello\n"
  }
}
```

非法 UTF-8：

```json
{
  "sequence": 43,
  "stream": "tty",
  "data": {
    "encoding": "base64",
    "base64": "AAEC"
  }
}
```

---

# 17. 状态目录与安全

支持：

```text
AMUX_SOCKET
AMUX_STATE_DIR
```

要求：

* state directory 权限为 `0700`
* Unix socket 权限为 `0600`
* 默认不监听 TCP
* 命令必须使用 argv 执行
* 禁止未经转义的 shell 拼接
* 校验所有资源名称
* 防止路径穿越
* 原子更新 metadata/status 文件
* daemon 和 shim 日志不能污染用户 Process 输出
* 本地单用户模型
* 第一版不需要 RBAC

---

# 18. 建议项目结构

```text
amux/
├── cmd/
│   ├── amux/
│   ├── amuxd/
│   └── amux-shim/
├── internal/
│   ├── api/
│   ├── attach/
│   ├── client/
│   ├── daemon/
│   ├── model/
│   ├── protocol/
│   ├── runtime/
│   │   ├── runtime.go
│   │   └── host/
│   ├── shim/
│   ├── store/
│   └── target/
├── pkg/
│   └── amuxclient/
├── docs/
│   ├── architecture.md
│   ├── protocol.md
│   ├── runtime-drivers.md
│   └── topology-model.md
├── go.mod
├── Makefile
└── README.md
```

公共 package 保持最小。

不要过早暴露内部实现类型。

---

# 19. 明确不做的内容

第一版不要实现：

* GUI
* TUI workspace
* window/tab/pane/layout
* terminal emulator
* VT parser/framebuffer
* containerd runtime
* Kubernetes runtime
* Docker runtime
* OCI image 管理
* network namespace
* cgroup 管理
* 多节点调度
* SSH runtime
* MCP server
* plugin system
* shell command marker
* waiting-input 自动判断
* Windows 支持
* 用户可见 primary process

但架构不能阻止以后增加：

```text
containerd runtime driver
Kubernetes runtime driver
GUI client
MCP adapter
remote daemon transport
resource and sandbox policy
```

---

# 20. 并发与可靠性要求

* PTY 只能有一个 authoritative reader
* PTY 只能有一个串行 writer
* pipe stdout/stderr 各有唯一 reader
* 输出由 reader 广播给日志和 client
* 慢 observer 不能阻塞 Process 输出
* 每个 observer 使用有界队列
* 明确慢 client 的断开或丢弃策略
* 不允许无界 goroutine
* 不允许无界 channel
* 使用 `context.Context` 控制请求生命周期
* Process 生命周期不能绑定 API request context
* shim 生命周期不能绑定 daemon connection
* 不能依赖 `time.Sleep` 解决同步问题
* 不能吞掉 wait、close、flush 或 signal 错误

---

# 21. 测试要求

必须包含单元测试和集成测试。

## 21.1 Workspace 模型

验证：

1. 创建 Workspace。
2. 自动创建默认 Group。
3. 自动创建 host Environment。
4. Workspace 可以不包含 Process。
5. 所有 Process 退出后 Workspace 仍存在。
6. 删除 Workspace 会停止并清理其 Process。

## 21.2 Pipe Process

验证：

1. 启动 pipe Process。
2. stdout 和 stderr 分离。
3. 正确获得 exit code。
4. `logs --after` 可以增量读取。
5. `stop` 终止整个 process group。
6. 一个 Process 退出不影响同 Workspace 中其他 Process。

## 21.3 TTY Process

验证：

1. 启动 shell 或 REPL。
2. attach 后可发送输入。
3. 可读取输出。
4. resize 传递到远端 PTY。
5. detach 后 Process 继续运行。
6. 再次 attach 进入同一个 Process。
7. readonly observer 不能写入。
8. 多 client 不会交错写入。
9. takeover 产生 ownership event。
10. controller 断开后 ownership 被释放。

## 21.4 Daemon 与 shim

验证：

1. CLI 退出不影响 Process。
2. amuxd 重启时 shim 和 Process 继续运行。
3. amuxd 能重新发现 shim。
4. shim 保存最终退出状态。
5. shim 异常退出时 Process 变为 `unknown` 或 `failed`。
6. daemon 不直接拥有用户 Process 的唯一 PTY 生命周期。

## 21.5 JSON

验证：

1. 所有 `--json` 输出可解析。
2. JSON 模式不包含额外文本。
3. JSONL 每行是合法 JSON。
4. 错误 code 稳定。
5. 非 UTF-8 输出使用 base64。

运行：

```bash
go test ./...
go test -race ./...
go vet ./...
```

依赖 PTY 的测试可以使用 Unix build tag。

测试不能依赖用户 shell 配置。

---

# 22. 验收流程

下面流程必须能够工作：

启动 daemon：

```bash
amuxd
```

创建 Workspace：

```bash
amux workspace create dev
```

启动两个同层 Process：

```bash
amux run \
  --workspace dev \
  --name api \
  -- sh -c 'while true; do date; sleep 1; done'

amux run \
  --workspace dev \
  --name shell \
  --tty \
  -- /bin/sh
```

查询：

```bash
amux ps --workspace dev --json
amux inspect dev/shell --json
```

观察 pipe Process：

```bash
amux logs --follow dev/api
```

attach：

```bash
amux attach dev/shell
```

在 attach 中执行：

```bash
pwd
echo hello
sleep 100
```

按 detach keys 后：

* shell 继续运行
* `sleep` 继续运行
* api 继续运行
* Workspace 继续存在

再次执行：

```bash
amux attach dev/shell
```

必须进入同一个 TTY Process。

随后：

```bash
amux signal dev/shell INT
amux stop dev/api
amux ps --workspace dev --json
```

两个 Process 停止后：

```bash
amux workspace inspect dev
```

Workspace 必须仍存在，并处于 `idle` 或等价状态。

最后：

```bash
amux workspace rm dev
```

---

# 23. 实现顺序

## Milestone 1

* 项目骨架
* Workspace/Group/Environment/Process 模型
* 状态目录
* Unix socket API
* Workspace CRUD
* 默认 Group 和 host Environment
* CLI 框架
* `--json` 规范

## Milestone 2

* host pipe Process
* process group
* stdout/stderr 日志
* wait/signal/stop
* output sequence
* incremental logs

## Milestone 3

* host PTY Process
* raw attach
* resize
* detach keys
* controller/observer
* ownership arbitration

## Milestone 4

* 独立 amux-shim
* daemon/shim 协议
* daemon 重启发现
* Process 跨 daemon 重启存活

## Milestone 5

* 集成测试
* race test
* README
* API 和协议文档
* topology model 文档
* containerd runtime 设计
* Kubernetes runtime 设计

---

# 24. 代码质量要求

* idiomatic Go
* 错误附带上下文
* 明确资源 owner
* 明确 shutdown 顺序
* 避免全局可变状态
* 避免过度抽象
* 依赖尽量少
* 公共 JSON schema 版本化
* attach protocol 版本化
* 原子状态写入
* 不为理论性能过早优化
* 不为未来 runtime 编写无意义空壳

设计选择优先级：

1. 正确性
2. 清晰生命周期
3. Process 和 Workspace 独立性
4. 可测试性
5. Agent 可编程性
6. 简单部署
7. 性能

---

# 25. 最终交付

完成后输出：

1. 实现摘要
2. 最终架构说明
3. Workspace/Group/Environment/Process 映射说明
4. 新增和修改文件列表
5. CLI 使用示例
6. 已运行测试及真实结果
7. 已知限制
8. containerd runtime 的建议实现方式
9. Kubernetes runtime 的建议实现方式
10. 当前实现与本 Prompt 偏离的地方及原因

不要声称未运行的测试已经通过。

遇到环境限制时，明确说明：

* 哪些功能已经实现
* 哪些测试未运行
* 为什么不能运行
* 如何在正常环境中验证
