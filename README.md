<p align="center">
  <img src="logo.png" alt="extractedprism logo" width="256">
</p>

# extractedprism

[![Go Report Card](https://goreportcard.com/badge/github.com/lexfrei/extractedprism)](https://goreportcard.com/report/github.com/lexfrei/extractedprism)
[![License](https://img.shields.io/github/license/lexfrei/extractedprism)](LICENSE)
[![Latest Release](https://img.shields.io/github/v/release/lexfrei/extractedprism)](https://github.com/lexfrei/extractedprism/releases/latest)

Per-node TCP load balancer for Kubernetes API server high availability, inspired by [Talos KubePrism](https://www.talos.dev/latest/kubernetes-guides/configuration/kubeprism/).

## Overview

extractedprism runs as a DaemonSet on every node in a Kubernetes cluster, providing a local TCP proxy on `127.0.0.1:7445` that routes traffic to healthy API server endpoints. Each node connects to the API server through its own local proxy, eliminating the need for a shared Virtual IP (VIP) and the associated VRRP/keepalived infrastructure.

Key properties:

- **No VIP dependency** -- each node connects to `localhost`, removing the single point of failure inherent in shared VIP solutions
- **Two-level endpoint discovery** -- static bootstrap endpoints for immediate availability at boot, plus dynamic Kubernetes EndpointSlice watching for runtime updates
- **CNI-independent bootstrap** -- runs with `hostNetwork: true` and uses static endpoints, so it works before the CNI plugin starts
- **Health-checked upstreams** -- only routes traffic to API servers that pass TCP health checks
- **Minimal footprint** -- scratch container image, non-root, no capabilities required

## Motivation

Traditional Kubernetes control plane HA solutions rely on a shared Virtual IP managed by keepalived (VRRP), kube-vip, or similar tools. These approaches have inherent limitations:

- **Single point of failure at the network level** -- all nodes depend on a single VIP; if VRRP failover is slow or fails, the entire cluster loses API access
- **Fragile failover with NIC driver bugs** -- hardware-level issues (e.g., NIC driver crashes) can silently break the VIP holder without triggering VRRP failover, leaving the VIP unreachable
- **Requires privileged capabilities** -- keepalived and kube-vip need `NET_ADMIN` and `NET_RAW` to manipulate network interfaces and ARP tables
- **VRRP protocol complexity** -- split-brain scenarios, gratuitous ARP propagation delays, and L2 domain restrictions add operational burden

extractedprism takes a different approach: each node runs its own load balancer bound to localhost. There is no shared state, no leader election, and no network-level failover. If one node's proxy fails, only that node is affected. The proxy starts with static endpoints (no cluster access needed), then discovers API server changes at runtime via the Kubernetes API.

## Architecture

```text
                       +-------------------------------------------+
                       |              Kubernetes Cluster           |
                       |                                           |
+--------------------+ |   +--------------+   +--------------+     |
|    Node (any)      | |   | CP Node 1    |   | CP Node 2    |     |
|                    | |   | API :6443    |   | API :6443    |     |
| kubelet / pods     | |   +--------------+   +--------------+     |
|   |                | |          ^                   ^            |
|   v                | |          |                   |            |
| 127.0.0.1:7445 ----+-+----------+-------------------+            |
| [extractedprism]   | |   health-checked TCP proxy                |
|   |                | |          ^                   ^            |
|   v                | |          |                   |            |
| 127.0.0.1:7446     | |   +--------------+   +--------------+     |
| [health server]    | |   | CP Node 3    |   | CP Node N    |     |
|  /healthz /readyz  | |   | API :6443    |   | API :6443    |     |
+--------------------+ |   +--------------+   +--------------+     |
                       +-------------------------------------------+
```

### Core library

extractedprism uses [`siderolabs/go-loadbalancer`](https://github.com/siderolabs/go-loadbalancer) as its TCP L4 proxy engine. This library provides connection-level load balancing with configurable health checks, keepalive, and dial timeouts.

### Two-level endpoint discovery

1. **Static (bootstrap)**: Control plane IPs provided via the `--endpoints` flag. Available immediately at startup. No cluster access or CNI required.
2. **Dynamic (runtime)**: Watches Kubernetes EndpointSlice resources for the `kubernetes` service in the `default` namespace (label selector: `kubernetes.io/service-name=kubernetes`). Automatically detects control plane membership changes. Reconnects with exponential backoff (1s--30s with jitter) on Watch errors. On 410 Gone (etcd compaction), performs a full re-list to rebuild the endpoint cache.

The merged provider deduplicates endpoints across all sub-providers and never sends an empty list to the load balancer, ensuring at least the static endpoints are always present.

### Boot sequence

1. k3s/kubelet starts on the node, API server listens on port 6443
2. extractedprism DaemonSet starts with `hostNetwork: true` and static endpoints
3. Load balancer on `127.0.0.1:7445` begins proxying to healthy API servers
4. CNI plugin (Cilium, Calico, etc.) starts and can use `localhost:7445` for API access
5. Kubernetes EndpointSlice discovery activates for dynamic endpoint updates

## Installation

### Helm chart (recommended)

The recommended deployment method is via the Helm chart published as an OCI artifact:

```bash
helm install extractedprism oci://ghcr.io/lexfrei/charts/extractedprism \
  --namespace kube-system \
  --set endpoints="10.0.0.1:6443,10.0.0.2:6443,10.0.0.3:6443"
```

The chart creates a DaemonSet with:

- `hostNetwork: true` for CNI-independent operation
- `system-node-critical` PriorityClass
- Proper RBAC for EndpointSlice access
- Health probes on port 7446 (`/healthz` for liveness, `/readyz` for readiness)
- Non-root execution (UID 65534) with read-only root filesystem
- No `NET_ADMIN` or `NET_RAW` capabilities

Chart source: [github.com/lexfrei/charts](https://github.com/lexfrei/charts) (directory `charts/extractedprism/`)

### Container image

Pre-built multi-arch container images are available:

```bash
docker pull ghcr.io/lexfrei/extractedprism:latest
```

The image is built `FROM scratch` with only the static binary and CA certificates. No shell or other tools are included.

### Building from source

```bash
git clone https://github.com/lexfrei/extractedprism.git
cd extractedprism

go build \
  -ldflags="-s -w -X main.Version=$(git describe --tags) -X main.Revision=$(git rev-parse --short HEAD)" \
  -trimpath \
  -o extractedprism \
  ./cmd/extractedprism
```

Requires Go 1.25 or later.

## Configuration

All flags are bound to environment variables with the `EP_` prefix. For example, `--bind-address` maps to `EP_BIND_ADDRESS`.

| Flag | Environment Variable | Default | Description |
| --- | --- | --- | --- |
| `--bind-address` | `EP_BIND_ADDRESS` | `127.0.0.1` | Address for the TCP load balancer listener |
| `--bind-port` | `EP_BIND_PORT` | `7445` | Port for the TCP load balancer listener |
| `--health-port` | `EP_HEALTH_PORT` | `7446` | Port for the health check HTTP server |
| `--endpoints` | `EP_ENDPOINTS` | (required) | Comma-separated list of API server endpoints (`host:port`) |
| `--health-interval` | `EP_HEALTH_INTERVAL` | `20s` | Interval between upstream health checks |
| `--health-timeout` | `EP_HEALTH_TIMEOUT` | `15s` | Timeout for each upstream health check |
| `--enable-discovery` | `EP_ENABLE_DISCOVERY` | `true` | Enable Kubernetes EndpointSlice discovery |
| `--log-level` | `EP_LOG_LEVEL` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

### Validation rules

- `--endpoints` must not be empty; each entry must be a valid `host:port` pair
- `--bind-port` and `--health-port` must be in the range 1-65535 and must differ
- `--health-timeout` must be less than `--health-interval`

## How it works

### Load balancer

The TCP load balancer is created via `controlplane.NewLoadBalancer` from `siderolabs/go-loadbalancer`. It accepts a channel of upstream endpoint lists and routes incoming TCP connections to healthy backends. The load balancer applies:

- Configurable dial timeout (matches `--health-timeout`)
- TCP keepalive with a 30-second period
- TCP user timeout of 30 seconds
- Periodic health checks at the configured interval and timeout

### Endpoint discovery

The merged discovery provider runs all configured sub-providers concurrently and combines their results:

- **Static provider**: Sends the configured endpoints once at startup, then blocks until shutdown. Provides the baseline set of API server addresses that is always available.
- **Kubernetes provider**: Lists EndpointSlice resources on startup, then establishes a Watch. It maintains a local cache of all known EndpointSlice objects; on each Watch event (add, modify, delete), the cache is updated and the full endpoint list is recomputed from all cached slices. This ensures that updating one slice does not lose endpoints from other slices. On Watch errors, it reconnects with exponential backoff and jitter. On 410 Gone (etcd compaction), it performs a full re-list to rebuild the cache. The Kubernetes client connects directly to one of the static endpoint IPs (bypassing the `kubernetes.default.svc` ClusterIP) to avoid a circular dependency with the CNI plugin. When running outside a cluster, the provider gracefully degrades and the server operates with static endpoints only.

The merged provider deduplicates endpoints across all sub-providers and sends the combined list to the load balancer. If the merged list would be empty, the update is skipped to prevent routing to zero backends.

### Health server

The health HTTP server exposes two endpoints on the configured health port:

- **`/healthz`** (liveness): Always returns HTTP 200. The proxy process is alive.
- **`/readyz`** (readiness): Queries the load balancer's `Healthy()` method. Returns HTTP 200 if at least one upstream is reachable, HTTP 503 otherwise.

### Graceful shutdown

On `SIGINT` or `SIGTERM`, the server cancels its context, which stops endpoint discovery, shuts down the TCP load balancer (closing the listener and waiting for active connections to drain), and initiates a 5-second graceful shutdown of the health HTTP server.

## Security

- **Non-root execution**: Runs as UID 65534 (nobody) with no privilege escalation
- **Read-only filesystem**: Container runs with a read-only root filesystem
- **No capabilities**: Does not require `NET_ADMIN`, `NET_RAW`, or any other Linux capabilities
- **Scratch image**: Built `FROM scratch` -- no shell, no package manager, no tools. Contains only the static binary and CA certificates
- **Localhost binding**: Binds to `127.0.0.1` by default, not accessible from outside the node
- **Minimal dependencies**: Uses only well-established Go libraries; binary is statically compiled with CGO disabled

## Comparison with alternatives

| Feature | extractedprism | keepalived/VIP | kube-vip | Talos KubePrism |
| --- | --- | --- | --- | --- |
| Architecture | Per-node local proxy | Shared VIP (VRRP) | Shared VIP (BGP/ARP) | Per-node local proxy |
| VIP-free | Yes | No | No | Yes |
| Per-node fault isolation | Yes | No | No | Yes |
| CNI-independent bootstrap | Yes (hostNetwork + static) | Yes | Partial | Yes (built into Talos) |
| Capabilities required | None | NET_ADMIN, NET_RAW | NET_ADMIN, NET_RAW | N/A (kernel-level) |
| Runtime endpoint discovery | EndpointSlice Watch | N/A | N/A | EndpointSlice Watch |
| Standalone binary | Yes | No | Yes | No (Talos only) |
| Works with any K8s distro | Yes | Yes | Yes | Talos only |
| Protocol | TCP L4 | L2/L3 (VRRP) | L2/L3 (ARP/BGP) | TCP L4 |
| Failure blast radius | Single node | All nodes | All nodes | Single node |

## Project structure

```text
cmd/extractedprism/main.go              # CLI entry point (cobra/viper), signal handling
internal/
├── config/config.go                    # Config struct, validation, defaults
├── discovery/
│   ├── provider.go                     # EndpointProvider interface
│   ├── static/static.go               # Static endpoints from --endpoints flag
│   ├── kubernetes/kubernetes.go        # EndpointSlice Watch-based discovery
│   └── merged/merged.go               # Merge and deduplicate multiple providers
├── health/health.go                    # HTTP /healthz and /readyz server
└── server/server.go                    # Orchestrates LB, discovery, and health
Containerfile                           # Multi-stage build (golang:alpine -> scratch)
```

## Contributing

Contributions are welcome. To get started:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure tests pass: `go test ./...`
5. Ensure linting passes: `golangci-lint run`
6. Submit a pull request

Requirements:

- Go 1.25 or later
- golangci-lint v2 (the project uses strict linting with nearly all linters enabled)

## License

BSD 3-Clause License. See [LICENSE](LICENSE) for the full text.
