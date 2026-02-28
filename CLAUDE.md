# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

extractedprism is a per-node TCP load balancer for Kubernetes API server high availability. It runs as a DaemonSet, listens on `127.0.0.1:7445`, and proxies traffic to healthy control plane endpoints. Inspired by Talos KubePrism but designed for standard Kubernetes distributions (k3s, kubeadm).

Two-level endpoint discovery: static endpoints for CNI-independent bootstrap, plus optional Kubernetes EndpointSlice watch for runtime updates.

## Key design constraint: CNI-independence

extractedprism starts BEFORE cluster networking (CNI) is available. This single fact drives most architectural decisions:

- **Static endpoints are mandatory** — Kubernetes API discovery requires a working API client, which requires networking, which requires CNI, which requires the API server. Static endpoints break this circular dependency.
- **Kube client routes through local LB** — `buildInClusterClient()` overrides `restCfg.Host` to point at the local load balancer (`127.0.0.1:bindPort`), not at the `kubernetes.default` ClusterIP. The LB is seeded with static endpoints before discovery starts, so it has reachable backends from the first API call. TLS ServerName is set to `kubernetes.default.svc` for certificate verification through the L4 proxy.
- **Bind to localhost** — the load balancer listens on `127.0.0.1:7445` so kubelet and CNI plugins can reach the API server without any network dependency beyond loopback.
- **Static-first, dynamic-second** — the merged provider always includes static endpoints. Kubernetes discovery is additive and optional. If it fails, the system still works.

Any change that assumes working cluster networking at startup is wrong.

## Build, test, lint

```bash
# Build
go build -trimpath -o extractedprism ./cmd/extractedprism

# Build with version info
go build \
  -ldflags="-s -w -X main.Version=$(git describe --tags) -X main.Revision=$(git rev-parse --short HEAD)" \
  -trimpath -o extractedprism ./cmd/extractedprism

# Test (all packages, race detector)
go test --verbose --race --coverprofile=coverage.out --covermode=atomic ./...

# Test single package
go test --verbose --race ./internal/config/...

# Test single test function
go test --verbose --race --run TestValidate_InvalidPort ./internal/config/...

# Lint
golangci-lint run --timeout=5m
```

## Architecture

Entry point: `cmd/extractedprism/main.go` — Cobra CLI with Viper config binding (`EP_*` env vars).

Core pipeline: `Server.Run()` starts the load balancer, then runs discovery and health in parallel via errgroup.

```text
main.go → server.Server
              ├─ controlplane.LoadBalancer (siderolabs/go-loadbalancer)
              ├─ merged.Provider
              │     ├─ static.Provider (immediate, always present)
              │     └─ kubernetes.Provider (optional, EndpointSlice watch)
              └─ health.Server (/healthz, /readyz)
```

**Key interface**: `discovery.EndpointProvider` — `Run(ctx, updateCh chan<- []string) error`. Providers send endpoint lists on `updateCh`; the merged provider deduplicates and forwards to the load balancer.

**Dependency injection**: `server.New()` accepts `Option` funcs (`WithKubeClient`, `WithHealthServer`) for testing.

## Development workflow

Every change follows: **ticket first, then code**.

1. **Ticket**: Every fix and improvement must have a GitHub issue before work starts. No exceptions, no "I'll file it later".
2. **Branch**: Create a feature branch from master (`fix/short-name` or `feat/short-name`).
3. **RED**: Write failing tests first. Tests define the contract before implementation exists.
4. **GREEN**: Write the minimum code to make tests pass.
5. **Refactor**: Clean up while tests stay green.
6. **Review**: Get LGTM from `/review-branch --ticket <url>` before merging.
7. **PR**: Draft PR, enable auto-merge after review passes.

**Proactive issue creation**: When you discover a bug, smell, optimization opportunity, or anything that should be better — create a GitHub issue immediately. Do not ask for permission, do not batch them, do not "mention it later". See a problem → `gh issue create`. This applies during code review, during implementation, during exploration — always.

## Testing philosophy

Tests in this project are **the second form of documentation**. They are not a safety net — they are a specification that a reader can trust.

- **Boundary combinations are mandatory**. If a field accepts 1–65535, tests must cover 0, 1, 65535, 65536, -1, and non-numeric input. If two fields interact (e.g., timeout < interval), test the exact boundary where they are equal, and both sides of it.
- **Every sentinel error must be reachable from tests**. If `ErrInvalidPort` exists, there must be a test that triggers it and asserts `errors.Is`.
- **Table-driven tests must cover the full decision tree**, not just the happy path and one sad path. Think: what are ALL the ways this can fail? Test each one.
- **Test names describe behavior**, not implementation. `TestValidate_PortZero_ReturnsErrInvalidPort` — not `TestValidateLine47`.
- **Stubs must have tests**: If you create a placeholder that returns "not implemented", write a test that documents expected behavior and fails on the stub.

A new contributor should be able to understand the system's contract by reading the tests alone, without reading the implementation.

## Conventions

**Error handling**: Use `github.com/cockroachdb/errors` exclusively (not `fmt.Errorf`). Sentinel errors at package level. Wrap with context: `errors.Wrap(err, "description")`. Check with `errors.Is()`.

**Testing**: testify (`assert` + `require`). Table-driven tests with `tt` variable. Compile-time interface checks: `var _ Interface = (*Type)(nil)`.

**Logging**: `go.uber.org/zap` structured logger, passed via constructor injection.

**Configuration**: All flags have `EP_` prefixed env vars via Viper. `config.NewBaseConfig()` provides defaults for optional fields; `Endpoints` is required.

## Linting strictness

All linters enabled (`.golangci.yml`). Key limits:

- `funlen`: max 60 lines
- `cyclop`/`gocyclo`: max complexity 15
- `varnamelen`: min length 3, max distance 5
- `dupl`: threshold 100
- `nolintlint`: requires explanation and specific linter name

Relaxed in tests: funlen, dupl, gocyclo, testifylint. Relaxed in `cmd/`: funlen, gocyclo, mnd, godox.

## Container image

`Containerfile`: multi-stage build, `FROM scratch` final image, non-root (UID 65534), static binary (`CGO_ENABLED=0`). Exposes 7445/tcp (LB) and 7446/tcp (health).
