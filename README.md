<p align="center">
  <img src="logo.png" alt="extractedprism logo" width="256">
</p>

# extractedprism

[![Go Report Card](https://goreportcard.com/badge/github.com/lexfrei/extractedprism)](https://goreportcard.com/report/github.com/lexfrei/extractedprism)
[![License](https://img.shields.io/github/license/lexfrei/extractedprism)](LICENSE)
[![Latest Release](https://img.shields.io/github/v/release/lexfrei/extractedprism)](https://github.com/lexfrei/extractedprism/releases/latest)

Per-node TCP load balancer for Kubernetes API server high availability, inspired by [Talos KubePrism](https://docs.siderolabs.com/talos/v1.12/configure-your-talos-cluster/system-configuration/kubeprism).

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

### Data path

extractedprism ships its own TCP L4 proxy (`internal/proxy`): an accept loop forwarding to a health-checked set of upstream endpoints, with per-upstream connection tracking, Prometheus metrics, and graceful connection draining. No external load-balancing library is involved.

### Two-level endpoint discovery

1. **Static (bootstrap)**: Control plane IPs provided via the `--endpoints` flag. Available immediately at startup. No cluster access or CNI required.
2. **Dynamic (runtime)**: Watches Kubernetes EndpointSlice resources for the `kubernetes` service in the `default` namespace (label selector: `kubernetes.io/service-name=kubernetes`). Automatically detects control plane membership changes. Reconnects with exponential backoff on Watch errors: the delay starts at 1s and doubles up to 30s, and each delay gets up to 25% random jitter on top. A watch that stayed open for at least 30s and then closed without an error is re-opened immediately. Any other error after such a watch, except 410 Gone, starts the backoff from 1s again. On 410 Gone (etcd compaction), performs a full re-list to rebuild the endpoint cache. After a successful re-list, the next watch starts at once if the 410 ended a watch that was open for at least 30s, or hit the re-watch right after such a watch closed without an error. Otherwise it waits for the backoff.

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

The image is built `FROM scratch` with only the static binary. No shell, CA certificates, or other tools are included.

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

Requires Go 1.26 or later.

## Configuration

All flags are bound to environment variables with the `EP_` prefix. For example, `--bind-address` maps to `EP_BIND_ADDRESS`.

| Flag | Environment Variable | Default | Description |
| --- | --- | --- | --- |
| `--bind-address` | `EP_BIND_ADDRESS` | `127.0.0.1` | Address for the TCP load balancer listener |
| `--bind-port` | `EP_BIND_PORT` | `7445` | Port for the TCP load balancer listener |
| `--health-port` | `EP_HEALTH_PORT` | `7446` | Port for the health check HTTP server |
| `--health-bind-address` | `EP_HEALTH_BIND_ADDRESS` | (same as `--bind-address`) | Address for the health check HTTP server |
| `--endpoints` | `EP_ENDPOINTS` | (required) | Comma-separated list of API server endpoints (`host:port`) |
| `--health-interval` | `EP_HEALTH_INTERVAL` | `20s` | Interval between upstream health checks |
| `--health-timeout` | `EP_HEALTH_TIMEOUT` | `15s` | Timeout for each upstream health check |
| `--enable-discovery` | `EP_ENABLE_DISCOVERY` | `true` | Enable Kubernetes EndpointSlice discovery |
| `--log-level` | `EP_LOG_LEVEL` | `info` | Log level (`debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`) |
| `--liveness-interval` | `EP_LIVENESS_INTERVAL` | `5s` | Heartbeat probe interval for liveness detection |
| `--liveness-threshold` | `EP_LIVENESS_THRESHOLD` | `15s` | Maximum time since last heartbeat before liveness fails |
| `--drain-timeout` | `EP_DRAIN_TIMEOUT` | `30s` | Grace period for connections to removed endpoints, and with `readyz` to unready ones while another upstream is ready, before force close (`0` closes immediately) |
| `--upstream-selection` | `EP_UPSTREAM_SELECTION` | `random` | How new connections pick an upstream: `random` (uniform among pickable upstreams) or `latency` (prefer the lowest-latency tier, see below) |
| `--health-check` | `EP_HEALTH_CHECK` | `tcp` | Upstream health check: `tcp` (connect only) or `readyz` (connect, then prefer upstreams answering `200` on `GET /readyz`, see below) |
| `--health-check-ca-file` | `EP_HEALTH_CHECK_CA_FILE` | | CA bundle that verifies the upstream serving certificates; required with `readyz` |
| `--health-check-cert-file` | `EP_HEALTH_CHECK_CERT_FILE` | | Client certificate for the `readyz` probe |
| `--health-check-key-file` | `EP_HEALTH_CHECK_KEY_FILE` | | Client key for the `readyz` probe; may be the same file as the certificate |
| `--health-check-token-file` | `EP_HEALTH_CHECK_TOKEN_FILE` | | Bearer token for the `readyz` probe |
| `--health-check-server-name` | `EP_HEALTH_CHECK_SERVER_NAME` | `kubernetes.default.svc` | Name the upstream serving certificates are verified against |

### Validation rules

- `--bind-address` and `--health-bind-address` must each be a valid IP address or RFC 1123-compliant hostname
- `--endpoints` must not be empty; each entry must be a valid `host:port` pair where the host is a valid IP address or RFC 1123 hostname and port is a number in the range 1-65535
- `--bind-port` and `--health-port` must be in the range 1-65535 and must differ
- `--health-interval` and `--health-timeout` must be at least 1 second
- `--health-timeout` must be less than `--health-interval`
- `--log-level` must be one of `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`
- `--liveness-interval` and `--liveness-threshold` must be at least 1 second
- `--liveness-threshold` must be greater than `--liveness-interval`
- `--drain-timeout` must not be negative
- `--upstream-selection` must be `random` or `latency`
- `--health-check` must be `tcp` or `readyz`; `readyz` requires `--health-check-ca-file`
- `--health-check-cert-file` and `--health-check-key-file` must be set together
- `--health-check-server-name` must not be empty

## Examples

- [Shared, SNI-routed control plane endpoint](examples/shared-sni-endpoint/) -- route kubelet through extractedprism to a shared endpoint that selects the backend by TLS SNI (for example nginx-ingress `ssl-passthrough`), using kubelet's `tls-server-name` while extractedprism stays a pure passthrough.
- [Talos Linux static pod](examples/talos/) -- replace the built-in KubePrism with extractedprism on Talos Linux.

## How it works

### Load balancer

The TCP load balancer is implemented in `internal/proxy`. It accepts a channel of upstream endpoint lists and routes incoming TCP connections to healthy backends. The proxy applies:

- Configurable dial timeout (matches `--health-timeout`)
- TCP keepalive on both client and upstream connections: probes start after 30 seconds idle, with Go's default probe interval and count
- TCP user timeout of 30 seconds on both client and upstream connections (Linux only)
- Periodic health checks at the configured interval and timeout; two consecutive failures exclude an upstream, one success re-includes it. Failed and successful client dials count toward the same streak

### Graceful draining

When an endpoint disappears from the upstream list (node cordon, API server rolling restart), the proxy stops routing new connections to it but lets existing connections finish within `--drain-timeout`. Remaining connections are force-closed when the timeout expires. Re-adding the endpoint before the timeout aborts the drain. A timeout of `0` closes connections to removed endpoints immediately. A new connection whose endpoint starts draining while it is being dialed gets one retry on another pickable endpoint, if there is one.

### Latency-aware selection

With `--upstream-selection=latency`, every successful health check feeds its connect time into a smoothed average (EWMA) per upstream; failed checks do not. For an endpoint given as a hostname, the connect time includes name resolution. Each sample is capped at four times the current average (but never below 2ms), so one slow check, such as a retransmitted SYN, moves the average by a bounded step instead of jumping to the retransmission time, while a lasting increase still shows up within a few checks. A sample below a quarter of the average replaces it, so a first check that took more than four times the usual connect time is undone by the next normal one. Upstreams with a sample are grouped into tiers by latency alone, healthy or not: tier 0 holds every upstream whose average is at most `max(best + 2ms, 2 × best)`, where `best` is the lowest average of all; tier 1 applies the same rule to the rest, and so on. An upstream leaves its tier only when its average exceeds the limit by a further 25%, so an upstream near the boundary does not flap. Membership follows the group rather than the tier number, so a tier appearing or disappearing above does not split a group. New connections are picked at random among the pickable upstreams of the lowest tier that has one, together with pickable upstreams that have no sample yet. With `--health-check=readyz` only ready upstreams count here while any is ready, so the tier is the lowest one with a ready upstream (see [Readiness-aware selection](#readiness-aware-selection)). Unhealthy and draining upstreams keep their tier, so losing some upstreams of a tier does not let a lower tier in, and losing all of them hands over to the next tier. An upstream removed from the endpoint list leaves the tiers once its drain completes, and the tiers are rebuilt without it. A new upstream takes picks before its first successful check; if it is unreachable, it keeps an equal share of that tier's picks, not of all upstreams as in `random` mode, until the consecutive-failure threshold excludes it. Every upstream within 2ms of the best one is in tier 0, so a single site with sub-millisecond differences behaves like `random`. Only health checks feed the average, so tiers follow latency changes at the pace of `--health-interval`. A change of tier does not move existing connections.

### Readiness-aware selection

A TCP check only proves that the API server process accepts connections. An API server whose etcd member has lost quorum still accepts them, but fails every request that needs storage, so with `--health-check=tcp` the node keeps sending part of its traffic to it. This happens when a network partition leaves a control-plane node in the minority: the node's own API server stays reachable, the healthy ones may be too, and `random` selection splits requests between them.

With `--health-check=readyz`, every successful TCP check is followed by an HTTPS `GET /readyz`; one `--health-timeout` deadline covers both the connect and the request. Any status other than `200` (redirects are not followed), a TLS error, or a timeout counts as a failed probe. As with TCP checks, two consecutive failures mark the upstream not ready and one success restores it. A new upstream, and one that becomes reachable again after a TCP failure, counts as not ready until its next probe passes; that probe runs right after the successful connect, so a restarted API server still cut off from etcd does not get new connections in between. New connections go to ready upstreams only. If no healthy upstream is ready, selection falls back to every TCP-healthy upstream, so losing quorum everywhere degrades to the `tcp` behavior instead of leaving the node without an API path. Readiness filters before latency tiers: with `--upstream-selection=latency` the tier is chosen among the ready upstreams only, so a ready upstream in a farther tier wins over an unready one in a closer tier, and with none ready the tiers apply to all TCP-healthy upstreams.

Readiness also moves existing connections. A client such as the kubelet keeps one long-lived HTTP/2 connection with watches on it, which never goes idle, and an API server without etcd quorum still answers HTTP/2 pings, so without help the client would stay on it for the whole outage. When an upstream has failed its probes while another healthy upstream is ready, its connections are closed after `--drain-timeout`, the same grace period a removed endpoint gets (`0` closes them at once), and the clients reconnect to a ready upstream. The closing is called off if, before the timeout, the upstream turns ready again or no other healthy upstream is ready any more. With no upstream ready, existing connections stay where they are, and so do the connections of an upstream whose first probe after a reset is still pending. The `/readyz` endpoint of extractedprism itself is unaffected.

The probe connects to the endpoint address and verifies the serving certificate against `--health-check-ca-file` and `--health-check-server-name`. `GET /readyz` is allowed to any authenticated client through the default `system:public-info-viewer` ClusterRole. If anonymous authentication is disabled, configure a credential: a client certificate, such as the node's kubelet client certificate (see the [Talos example](examples/talos/#readiness-check-not-validated)), or a bearer token. The client certificate and the token file are re-read on every probe, so rotation is picked up without a restart, and a credential that does not exist yet fails the probe instead of startup. On a joining node the kubelet obtains its certificate through this proxy, which works because of the fallback above.

### Endpoint discovery

The merged discovery provider runs all configured sub-providers concurrently and combines their results:

- **Static provider**: Sends the configured endpoints once at startup, then blocks until shutdown. Provides the baseline set of API server addresses that is always available.
- **Kubernetes provider**: Lists EndpointSlice resources on startup, then establishes a Watch. It maintains a local cache of all known EndpointSlice objects; on each Watch event (add, modify, delete), the cache is updated and the full endpoint list is recomputed from all cached slices. Endpoints where `Conditions.Ready` is explicitly `false` are excluded from the list. This ensures that updating one slice does not lose endpoints from other slices. On Watch errors, it reconnects with exponential backoff and jitter. A stream that stayed open for at least 30 seconds and then closed without an error is re-opened immediately. On 410 Gone (etcd compaction), it performs a full re-list to rebuild the cache. After a successful re-list it starts the next watch at once if the 410 ended a watch that was open for at least 30 seconds, or hit the re-watch right after such a watch closed without an error; otherwise it waits for the backoff. The Kubernetes client connects through the local load balancer (bypassing the `kubernetes.default.svc` ClusterIP) to avoid a circular dependency with the CNI plugin and to benefit from failover across all configured endpoints. When running outside a cluster, the provider gracefully degrades and the server operates with static endpoints only.

The merged provider deduplicates endpoints across all sub-providers and sends the combined list to the load balancer. If the merged list would be empty, the update is skipped to prevent routing to zero backends. Provider failures are isolated: if the Kubernetes provider fails, the static provider continues operating independently. The merged provider only returns an error if all sub-providers fail.

### Health server

The health HTTP server exposes two probe endpoints on the configured health port, plus `/metrics` (see below):

- **`/healthz`** (liveness): Returns HTTP 200 while the Run loop and load balancer goroutine are responsive. Returns HTTP 503 if the heartbeat probe detects the system is no longer alive (e.g., deadlocked LB, crashed Run loop, exited discovery pipeline).
- **`/readyz`** (readiness): Queries the proxy's `Healthy()` method. Returns HTTP 200 if at least one upstream is healthy and not draining, HTTP 503 otherwise.

Both endpoints accept only GET and HEAD requests. Other methods return 405 Method Not Allowed. OPTIONS returns 204 with the allowed methods.

### Metrics

The health server also exposes Prometheus metrics at `/metrics`. It binds to `--bind-address` by default, which is `127.0.0.1`; set `--health-bind-address` to an address Prometheus can reach to scrape it.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `extractedprism_upstreams_active` | Gauge | | Upstreams eligible for new connections (healthy, not draining) |
| `extractedprism_upstreams_total` | Gauge | | Total known upstreams, including unhealthy and draining |
| `extractedprism_connections_active` | Gauge | | TCP connections currently passing through the proxy |
| `extractedprism_connections_total` | Counter | | Connections established to upstreams since start |
| `extractedprism_connection_errors_total` | Counter | `upstream` | Failed connection attempts (`none` when no upstream was available); failures caused by proxy shutdown are not counted. The series of an upstream is deleted once it is drained and removed, and failures reported after that are not counted until the address is added again |
| `extractedprism_discovery_updates_total` | Counter | `provider` | Endpoint list updates received (`static`, `kubernetes`) |
| `extractedprism_discovery_errors_total` | Counter | `provider` | Discovery errors: provider failures, failed Watch calls, watch error events and failed re-lists. A watch stream ending is not counted, including on a dropped connection, and neither is 410 Gone expiry; a dead upstream shows in `health_check_status` instead |
| `extractedprism_upstream_rtt_seconds` | Gauge | `upstream` | Smoothed health check connect time, including name resolution for hostname endpoints; only with `--upstream-selection=latency`, after the first successful check |
| `extractedprism_upstream_latency_tier` | Gauge | `upstream` | Latency tier of the upstream, 0 being the closest group; new connections go to the lowest tier with a healthy, non-draining upstream (with `--health-check=readyz`, a ready one while any is ready), together with upstreams that have no sample yet. Only with `--upstream-selection=latency`, after the first successful check |
| `extractedprism_upstream_ready` | Gauge | `upstream` | `/readyz` probe state after the consecutive-failure threshold (1 ready, 0 not ready); new connections prefer ready upstreams, and an unready upstream loses its connections while another is ready. Only with `--health-check=readyz`, after the first probe |
| `extractedprism_health_check_status` | Gauge | `upstream` | Upstream health state after the consecutive-failure threshold, fed by health checks and client dials (1 healthy, 0 unhealthy). Health checks stop while an upstream drains, so the value holds until it is removed or re-added |

### Graceful shutdown

On `SIGINT` or `SIGTERM`, the server cancels its context, which stops endpoint discovery, shuts down the TCP proxy (closing the listener, force-closing in-flight connections, and waiting up to 5 seconds for proxy goroutines to exit), and initiates a 5-second graceful shutdown of the health HTTP server.

## Security

- **Non-root execution**: Runs as UID 65534 (nobody) with no privilege escalation
- **Read-only filesystem**: Container runs with a read-only root filesystem
- **No capabilities**: Does not require `NET_ADMIN`, `NET_RAW`, or any other Linux capabilities
- **Scratch image**: Built `FROM scratch` -- no shell, no package manager, no tools. Contains only the static binary
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
| Runtime endpoint discovery | EndpointSlice Watch | N/A | N/A | Talos cluster discovery |
| Standalone binary | Yes | No | Yes | No (Talos only) |
| Works with any K8s distro | Yes | Yes | Yes | Talos only |
| Protocol | TCP L4 | L2/L3 (VRRP) | L2/L3 (ARP/BGP) | TCP L4 |
| Failure blast radius | Single node | All nodes | All nodes | Single node |

## Project structure

```text
cmd/extractedprism/main.go              # CLI entry point (cobra/viper), signal handling
internal/
├── config/config.go                    # Config struct, validation, base defaults
├── discovery/
│   ├── provider.go                     # EndpointProvider interface
│   ├── static/static.go               # Static endpoints from --endpoints flag
│   ├── kubernetes/kubernetes.go        # EndpointSlice Watch-based discovery
│   └── merged/merged.go               # Merge and deduplicate multiple providers
├── health/health.go                    # HTTP /healthz, /readyz, and /metrics server
├── metrics/metrics.go                  # Prometheus metric set
├── proxy/proxy.go                      # TCP data path: accept, dial, health checks, draining
└── server/server.go                    # Orchestrates proxy, discovery, and health
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

- Go 1.26 or later
- golangci-lint v2 (the project uses strict linting with nearly all linters enabled)

## License

BSD 3-Clause License. See [LICENSE](LICENSE) for the full text.
