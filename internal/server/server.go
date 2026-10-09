// Package server orchestrates the load balancer, discovery, and health subsystems.
package server

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/discovery"
	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
	"github.com/lexfrei/extractedprism/internal/discovery/merged"
	"github.com/lexfrei/extractedprism/internal/discovery/static"
	"github.com/lexfrei/extractedprism/internal/health"
	"github.com/lexfrei/extractedprism/internal/metrics"
	"github.com/lexfrei/extractedprism/internal/proxy"
)

const (
	keepAlivePeriod   = 30 * time.Second
	tcpUserTimeout    = 30 * time.Second
	shutdownTimeout   = 5 * time.Second
	defaultAPIPort    = "6443"
	kubeAPIServerName = "kubernetes.default.svc"

	// upstreamChBuffer is the buffer size for the channel between the merged
	// discovery provider and the load balancer.
	//
	// Value 16 is a heuristic sufficient for typical clusters (3-5 control
	// plane nodes). Under extreme churn the pipeline may still briefly block
	// but will not lose data -- blocking only delays delivery, it does not
	// drop updates.
	upstreamChBuffer = 16
)

// Compile-time interface checks.
var (
	_ health.Checker         = (*Server)(nil)
	_ health.LivenessChecker = (*Server)(nil)
)

// healthServer abstracts the health HTTP server for testing.
type healthServer interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// Option configures a Server.
type Option func(*Server)

// WithKubeClient injects a pre-built Kubernetes client for endpoint discovery.
func WithKubeClient(client kubernetes.Interface) Option {
	return func(srv *Server) {
		srv.kubeClient = client
	}
}

// WithHealthServer injects a custom health server implementation (for testing).
func WithHealthServer(hs healthServer) Option {
	return func(srv *Server) {
		srv.healthSrv = hs
	}
}

// WithLivenessConfig overrides the heartbeat interval and liveness threshold
// from Config.LivenessInterval and Config.LivenessThreshold. Intended for
// testing with sub-second intervals that would be rejected by config validation.
// Both values must be positive, and threshold must be greater than interval.
// Panics on invalid values.
func WithLivenessConfig(interval, threshold time.Duration) Option {
	if interval <= 0 || threshold <= 0 {
		panic("WithLivenessConfig: interval and threshold must be positive")
	}

	if threshold <= interval {
		panic("WithLivenessConfig: threshold must be greater than interval")
	}

	return func(srv *Server) {
		srv.heartbeatInterval = interval
		srv.livenessThreshold = threshold
	}
}

// WithDiscoveryProviders overrides the discovery providers used by the server.
// Intended for testing the discovery pipeline exit path.
// Panics if no providers are given.
func WithDiscoveryProviders(providers ...discovery.EndpointProvider) Option {
	if len(providers) == 0 {
		panic("WithDiscoveryProviders: at least one provider is required")
	}

	return func(srv *Server) {
		srv.testProviders = providers
	}
}

// WithLivenessProbe overrides the function the heartbeat goroutine uses to
// probe system health. The default probes the load balancer. Intended for
// testing deadlock detection with a blocking probe.
// Panics if probe is nil.
func WithLivenessProbe(probe func()) Option {
	if probe == nil {
		panic("WithLivenessProbe: probe must not be nil")
	}

	return func(srv *Server) {
		srv.probeFn = probe
	}
}

// Server ties together the load balancer, endpoint discovery, and health checking.
type Server struct {
	cfg         *config.Config
	logger      *zap.Logger
	proxyHandle *proxy.Proxy
	metrics     *metrics.Metrics
	healthSrv   healthServer
	upstreamCh  chan []string
	kubeClient  kubernetes.Interface

	// Liveness heartbeat: the heartbeat goroutine periodically probes the
	// load balancer and stores a timestamp. Alive() checks whether the
	// timestamp is recent. If the LB goroutine deadlocks, the probe hangs,
	// the heartbeat stops, and Alive() returns false after the threshold.
	lastHeartbeat     atomic.Int64 // unix nanos; 0 means not started
	heartbeatInterval time.Duration
	livenessThreshold time.Duration
	probeFn           func()

	// testProviders overrides the discovery providers when set (testing only).
	testProviders []discovery.EndpointProvider

	// discoveryDone is set when runDiscovery exits. During normal
	// operation this goroutine should run for the entire server lifetime.
	// An early exit (error or unexpected nil return) indicates a critical
	// goroutine failure.
	discoveryDone atomic.Bool
}

// New creates a Server from the given config.
// Config is expected to be pre-validated by the caller; ApplyDefaults and
// Validate are retained as defense-in-depth for direct callers and tests.
func New(cfg *config.Config, logger *zap.Logger, opts ...Option) (*Server, error) {
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	metricsHandle := metrics.New()

	proxyHandle := proxy.New(proxyConfig(cfg), logger, metricsHandle)

	srv := &Server{
		cfg:               cfg,
		logger:            logger,
		proxyHandle:       proxyHandle,
		metrics:           metricsHandle,
		upstreamCh:        make(chan []string, upstreamChBuffer),
		heartbeatInterval: cfg.LivenessInterval,
		livenessThreshold: cfg.LivenessThreshold,
	}

	for _, opt := range opts {
		opt(srv)
	}

	if srv.probeFn == nil {
		// The probe discards the Healthy() result. Liveness only cares whether
		// the proxy can answer at all (its state lock is not deadlocked); a
		// proxy with no healthy upstreams is still "alive" — the readiness
		// probe (/readyz) is responsible for signaling unhealthy upstreams.
		srv.probeFn = func() {
			_ = srv.proxyHandle.Healthy()
		}
	}

	if srv.healthSrv == nil {
		srv.healthSrv = health.NewServer(cfg.HealthBindAddress, cfg.HealthPort, srv, srv, logger,
			health.WithMetrics(srv.metrics.Handler()))
	}

	return srv, nil
}

// Alive reports whether the server's Run loop is active and responsive.
// Returns false when:
//   - Run has not been called yet (lastHeartbeat == 0)
//   - Run has returned (lastHeartbeat reset to 0)
//   - The heartbeat probe has not responded within the liveness threshold
//     (indicating a deadlocked load balancer goroutine)
//   - The discovery pipeline has exited unexpectedly
func (srv *Server) Alive() bool {
	last := srv.lastHeartbeat.Load()
	if last == 0 {
		return false
	}

	if srv.discoveryDone.Load() {
		return false
	}

	return time.Since(time.Unix(0, last)) < srv.livenessThreshold
}

// Healthy delegates to the proxy health status.
func (srv *Server) Healthy() (bool, error) {
	return srv.proxyHandle.Healthy(), nil
}

// Run starts all subsystems and blocks until ctx is cancelled.
func (srv *Server) Run(ctx context.Context) error {
	defer srv.lastHeartbeat.Store(0)

	// Reset discoveryDone for correctness. Note: proxy.Shutdown runs at most
	// once per instance, so a restarted proxy could never be shut down again;
	// Run therefore cannot be called twice on the same Server instance in
	// production. This reset ensures Alive() does not return a stale value
	// from a prior call during tests.
	srv.discoveryDone.Store(false)

	// Mark alive before launching goroutines so the health server
	// never sees lastHeartbeat == 0 during normal startup.
	srv.lastHeartbeat.Store(time.Now().UnixNano())

	err := srv.proxyHandle.Start(ctx, srv.upstreamCh)
	if err != nil {
		return errors.Wrap(err, "start proxy")
	}

	//nolint:contextcheck // parent ctx is done; fresh timeout needed for graceful shutdown
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutCancel()

		shutErr := srv.proxyHandle.Shutdown(shutCtx)
		if shutErr != nil {
			srv.logger.Warn("proxy shutdown error", zap.Error(shutErr))
		}
	}()

	// Seed the LB with static endpoints before starting discovery.
	// This ensures the kube discovery client (which routes through the LB)
	// has at least one reachable backend on its first API call.
	seed := make([]string, len(srv.cfg.Endpoints))
	copy(seed, srv.cfg.Endpoints)

	srv.upstreamCh <- seed

	// Save parent context before errgroup wraps it. Used to distinguish
	// normal shutdown (parent cancelled) from unexpected discovery exit.
	parentCtx := ctx
	grp, ctx := errgroup.WithContext(ctx)

	grp.Go(func() error { return srv.runHeartbeat(ctx) })
	grp.Go(func() error {
		defer func() {
			// Only signal discovery failure for unexpected exits, not normal
			// context-driven shutdowns. During graceful shutdown, the parent
			// context is cancelled and the discovery pipeline exits as expected.
			//
			// There is a narrow TOCTOU window: the parent context could be
			// cancelled between the Err() check and the Store(). In that case
			// discoveryDone may not be set, but Alive() will still return false
			// after the heartbeat timestamp expires (bounded by livenessThreshold).
			if parentCtx.Err() == nil {
				srv.discoveryDone.Store(true)
			}
		}()

		return srv.runDiscovery(ctx)
	})
	grp.Go(func() error { return srv.runHealth(ctx) })

	waitErr := grp.Wait()
	if waitErr != nil {
		return errors.Wrap(waitErr, "server run")
	}

	return nil
}

// runHeartbeat periodically probes the load balancer and updates the
// heartbeat timestamp. If the probe function blocks (e.g., because the LB
// goroutine is deadlocked), the timestamp goes stale and Alive() returns
// false, causing the liveness probe to fail and Kubernetes to restart the pod.
func (srv *Server) runHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(srv.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !srv.probeWithContext(ctx) {
				return nil
			}

			srv.lastHeartbeat.Store(time.Now().UnixNano())
		}
	}
}

// probeWithContext runs the probe function in a separate goroutine so that
// context cancellation is respected even when the probe is blocked (e.g.,
// deadlocked LB). Returns true if the probe completed, false if the
// context was cancelled first.
//
// When the context is cancelled while the probe goroutine is still running,
// the goroutine is orphaned. This is an intentional tradeoff: a deadlocked
// probe cannot be interrupted, so we accept a bounded leak (at most one
// goroutine per heartbeat tick between cancellation and Run return).
// The default probe (proxyHandle.Healthy) is safe to call on a stopped
// proxy: it only takes a read lock and has no side effects.
func (srv *Server) probeWithContext(ctx context.Context) bool {
	probeDone := make(chan struct{})

	go func() {
		srv.probeFn()
		close(probeDone)
	}()

	select {
	case <-probeDone:
		return true
	case <-ctx.Done():
		return false
	}
}

func (srv *Server) runDiscovery(ctx context.Context) error {
	var providers []discovery.EndpointProvider

	if srv.testProviders != nil {
		providers = srv.testProviders
	} else {
		built, err := srv.buildProviders()
		if err != nil {
			return err
		}

		providers = built
	}

	mp := merged.NewMergedProvider(srv.logger, srv.metrics, providers...)

	runErr := mp.Run(ctx, srv.upstreamCh)
	if runErr != nil {
		return errors.Wrap(runErr, "merged provider")
	}

	return nil
}

func (srv *Server) buildProviders() ([]discovery.EndpointProvider, error) {
	staticProv, err := static.NewStaticProvider(srv.cfg.Endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "create static provider")
	}

	providers := []discovery.EndpointProvider{staticProv}

	if srv.cfg.EnableDiscovery {
		kubeProv, kubeErr := srv.buildKubeProvider()
		if kubeErr != nil {
			srv.logger.Warn("kubernetes discovery unavailable, using static endpoints only",
				zap.Error(kubeErr))
		} else {
			providers = append(providers, kubeProv)
		}
	}

	return providers, nil
}

func (srv *Server) buildKubeProvider() (discovery.EndpointProvider, error) {
	client, err := srv.getKubeClient()
	if err != nil {
		return nil, err
	}

	apiPort := ExtractAPIPort(srv.cfg.Endpoints)

	countError := func() { srv.metrics.DiscoveryError(kubediscovery.ProviderName) }

	return kubediscovery.NewProvider(client, srv.logger, apiPort, kubediscovery.WithErrorHook(countError)), nil
}

func (srv *Server) getKubeClient() (kubernetes.Interface, error) {
	if srv.kubeClient != nil {
		return srv.kubeClient, nil
	}

	lbHost := net.JoinHostPort(srv.cfg.BindAddress, strconv.Itoa(srv.cfg.BindPort))

	return buildInClusterClient(lbHost)
}

func buildInClusterClient(lbHost string) (kubernetes.Interface, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "in-cluster config")
	}

	applyLBOverride(restCfg, lbHost)

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, errors.Wrap(err, "create kubernetes client")
	}

	return client, nil
}

// applyLBOverride configures a rest.Config to route API traffic through the
// local load balancer instead of a single control plane endpoint. ServerName
// is set to the standard Kubernetes service name which is always present in
// the API server TLS certificate SANs.
func applyLBOverride(restCfg *rest.Config, lbHost string) {
	restCfg.Host = "https://" + lbHost
	restCfg.ServerName = kubeAPIServerName
}

// ExtractAPIPort returns the port from the first endpoint, or "6443" as default.
func ExtractAPIPort(endpoints []string) string {
	if len(endpoints) == 0 {
		return defaultAPIPort
	}

	_, port, err := net.SplitHostPort(endpoints[0])
	if err != nil {
		return defaultAPIPort
	}

	return port
}

func (srv *Server) runHealth(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.healthSrv.Start(ctx)
	}()

	select {
	case <-ctx.Done():
		shutErr := srv.shutdownHealth(ctx)

		// Wait for the Start goroutine to return after shutdown to prevent
		// it from logging via the test logger after the test has finished.
		<-errCh

		return shutErr
	case err := <-errCh:
		if err != nil {
			return errors.Wrap(err, "health server start")
		}

		return errors.New("health server exited unexpectedly")
	}
}

func (srv *Server) shutdownHealth(_ context.Context) error {
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	//nolint:contextcheck // parent ctx is done; fresh timeout needed for graceful shutdown
	shutErr := srv.healthSrv.Shutdown(shutCtx)
	if shutErr != nil {
		return errors.Wrap(shutErr, "health server shutdown")
	}

	return nil
}

func proxyConfig(cfg *config.Config) *proxy.Config {
	return &proxy.Config{
		BindAddress:      cfg.BindAddress,
		BindPort:         cfg.BindPort,
		DialTimeout:      cfg.HealthTimeout,
		KeepAlivePeriod:  keepAlivePeriod,
		TCPUserTimeout:   tcpUserTimeout,
		HealthInterval:   cfg.HealthInterval,
		HealthTimeout:    cfg.HealthTimeout,
		DrainTimeout:     cfg.DrainTimeout,
		LatencySelection: cfg.UpstreamSelection == config.UpstreamSelectionLatency,
	}
}
