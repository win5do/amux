# Topology model

The stable logical hierarchy is Workspace → Group → Environment → Process.

- Workspace is durable and may contain zero running processes.
- Group is the co-location and placement boundary.
- Environment is the filesystem, environment and isolation boundary.
- Process is the independently started, stopped, observed and attached execution unit.

For host v1 every Workspace automatically receives Group `default` and Environment `host`; each Process maps to an independent OS process group. There is no primary process. A shell, REPL, debugger or agent is simply a Process with `io_mode: tty`.

Future shared container layouts put multiple Processes in one Environment; isolated layouts give Processes separate Environments. This reference relationship, rather than a `container_per_process` boolean, defines sharing. Kubernetes normally maps all Environments in one Group into one Pod.
