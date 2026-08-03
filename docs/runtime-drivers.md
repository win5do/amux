# Runtime drivers

Logical runtime types and handles live in `internal/runtime`; Unix host syscalls live in `internal/runtime/host`. Runtime handles are opaque strings in the public model. The first implementation launches a detached host shim and routes inspect, signal and stop operations through its socket rather than exposing `exec.Cmd` to the daemon API.

A containerd driver should map Group to a sandbox/placement boundary and Environment to an OCI container. A hidden keeper/shim inside a shared Environment keeps it alive, launches multiple peer processes, owns PTYs and records output. It must not be presented as a primary user process.

A Kubernetes driver should map Group to Pod, Environment to Container, and Process to a shim-managed process inside that container. Kubernetes remains responsible for scheduling, image, volume, network and resource limits. Because ordinary containers cannot be added freely to a running Pod, dynamic process creation must use the in-container shim rather than treating `kubectl exec` as durable process ownership.
