package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/discovery"
	"github.com/lexfrei/extractedprism/internal/server"
)

// immediateProvider returns nil from Run immediately, causing the merged
// provider to exit. Used to test the discoveryDone path in Alive().
type immediateProvider struct{}

func (immediateProvider) Run(_ context.Context, _ chan<- []string) error {
	return nil
}

func (immediateProvider) Name() string { return "immediate" }

// errorProvider returns an error from Run immediately, simulating a
// discovery failure. Used to test the discoveryDone path in Alive().
type errorProvider struct {
	err error
}

func (e errorProvider) Run(_ context.Context, _ chan<- []string) error {
	return e.err
}

func (errorProvider) Name() string { return "error" }

// Compile-time checks.
var (
	_ discovery.EndpointProvider = immediateProvider{}
	_ discovery.EndpointProvider = errorProvider{}
)

const (
	waitTimeout = 5 * time.Second
	pollTick    = 10 * time.Millisecond

	// runReturnTimeout bounds how long tests wait for Run to return. It must
	// exceed the server's own 5s graceful health-server shutdown, so a slow
	// but successful shutdown is not reported as a hang.
	runReturnTimeout = 10 * time.Second
)

var portCounter atomic.Int32

func init() {
	portCounter.Store(17400)
}

func nextPortPair() (int, int) {
	base := int(portCounter.Add(2))

	return base - 1, base
}

func validConfig() *config.Config {
	bindPort, healthPort := nextPortPair()
	cfg := config.NewBaseConfig()
	cfg.BindPort = bindPort
	cfg.HealthPort = healthPort
	cfg.Endpoints = []string{"127.0.0.1:6443"}
	cfg.HealthInterval = 5 * time.Second
	cfg.HealthTimeout = 2 * time.Second

	return cfg
}

// newTestHTTPClient returns a client without keep-alive. A pooled transport
// can dial a spare connection and park it unused; the health server counts
// such a connection as active for 5 seconds during Shutdown, the same as
// its own shutdown budget, so a parked connection fails Run with a deadline
// error.
func newTestHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
}

// waitForHealthz polls the health endpoint until it responds with 200 or the
// timeout fires. Since /healthz now checks Alive(), this implicitly verifies
// that Run() has stored the initial heartbeat. This is safe because Run()
// stores lastHeartbeat before launching the errgroup (including the health server).
func waitForHealthz(t *testing.T, healthPort int) {
	t.Helper()

	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", healthPort)
	client := newTestHTTPClient()

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if err != nil {
			return false
		}

		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, waitTimeout, pollTick, "health server did not start in time")
}

func TestNew_ValidConfig(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)
	assert.NotNil(t, srv)
}

func TestAlive_FalseBeforeRun(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)
	assert.False(t, srv.Alive(), "Alive must return false before Run is called")
}

func TestAlive_TrueDuringRun(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	assert.True(t, srv.Alive(), "Alive must return true while Run is active")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestAlive_FalseAfterRunReturns(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}

	assert.False(t, srv.Alive(), "Alive must return false after Run returns")
}

func TestAlive_HeartbeatStopsOnBlockedProbe(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	blockCh := make(chan struct{})

	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 200*time.Millisecond),
		server.WithLivenessProbe(func() {
			<-blockCh // blocks until channel is closed, simulating deadlocked LB
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	// Initially alive from the first heartbeat stored before probe runs.
	assert.True(t, srv.Alive(), "should be alive from initial heartbeat")

	// The probe blocks on every tick, so no further heartbeats are stored.
	// After the liveness threshold expires, Alive must return false.
	require.Eventually(t, func() bool {
		return !srv.Alive()
	}, 1*time.Second, 25*time.Millisecond,
		"Alive must become false when heartbeat probe is blocked")

	// Clean up: cancel context and unblock the stuck probe goroutine.
	cancel()
	close(blockCh)

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestAlive_GracefulShutdownWithBlockedProbe(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	// Use a channel-based probe so the goroutine can be cleaned up.
	blockCh := make(chan struct{})
	t.Cleanup(func() { close(blockCh) })

	// Verifies that context cancellation alone is sufficient for Run
	// to return, even when the probe goroutine is stuck (graceful shutdown).
	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 200*time.Millisecond),
		server.WithLivenessProbe(func() {
			<-blockCh
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	// Cancel without unblocking the probe — Run must still return.
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("Run did not return after cancel — goroutine leak in heartbeat probe")
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := config.NewBaseConfig()
	cfg.Endpoints = nil

	srv, err := server.New(cfg, log)
	require.Error(t, err)
	assert.Nil(t, srv)
}

func TestRun_StartsAndShutdowns(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRun_StaticOnlyMode(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()
	cfg.EnableDiscovery = false

	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestNew_WithKubeClientOption(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()
	cfg.EnableDiscovery = true

	fakeClient := fake.NewClientset()

	srv, err := server.New(cfg, log, server.WithKubeClient(fakeClient))
	require.NoError(t, err)
	assert.NotNil(t, srv)
}

func TestRun_WithDiscoveryEnabled(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()
	cfg.EnableDiscovery = true

	eps := &discoveryv1.EndpointSlice{
		Name:      "kubernetes",
		Namespace: "default",
		Labels: map[string]string{
			"kubernetes.io/service-name": "kubernetes",
		},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"127.0.0.1"}},
		},
	}

	fakeClient := fake.NewClientset(eps)

	srv, err := server.New(cfg, log, server.WithKubeClient(fakeClient))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

// immediateHealthServer returns nil from Start immediately, simulating
// an externally closed listener or unexpected clean exit.
type immediateHealthServer struct {
	startErr error
}

func (m *immediateHealthServer) Start(_ context.Context) error {
	return m.startErr
}

func (m *immediateHealthServer) Shutdown(_ context.Context) error {
	return nil
}

func TestRunHealth_NilReturnDoesNotBlock(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log,
		server.WithHealthServer(&immediateHealthServer{startErr: nil}),
	)
	require.NoError(t, err)

	ctx := t.Context()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// Run should complete promptly when health Start returns nil,
	// not block forever waiting on errCh.
	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health server exited unexpectedly")
	case <-time.After(runReturnTimeout):
		t.Fatal("Run blocked — goroutine leak: Start returned nil but runHealth did not exit")
	}
}

func TestRunHealth_StartErrorPropagates(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log,
		server.WithHealthServer(&immediateHealthServer{
			startErr: errors.New("bind: address already in use"),
		}),
	)
	require.NoError(t, err)

	ctx := t.Context()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health server start")
		assert.Contains(t, err.Error(), "address already in use")
	case <-time.After(runReturnTimeout):
		t.Fatal("Run blocked on health start error")
	}
}

// blockingHealthServer follows the real health server contract: Start
// returns stopErr only after Shutdown has been called.
type blockingHealthServer struct {
	stopErr error
	stop    chan struct{}
	once    sync.Once
}

func (m *blockingHealthServer) Start(_ context.Context) error {
	<-m.stop

	return m.stopErr
}

func (m *blockingHealthServer) Shutdown(_ context.Context) error {
	m.once.Do(func() { close(m.stop) })

	return nil
}

func TestRunHealth_StartErrorDuringShutdownIsLogged(t *testing.T) {
	tests := []struct {
		name     string
		stopErr  error
		wantWarn int
	}{
		{name: "real error is logged", stopErr: errors.New("accept: boom"), wantWarn: 1},
		{name: "wrapped cancellation is not logged", stopErr: errors.Wrap(context.Canceled, "health server listen"), wantWarn: 0},
		{name: "clean exit is not logged", stopErr: nil, wantWarn: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)

			srv, err := server.New(validConfig(), zap.New(core),
				server.WithHealthServer(&blockingHealthServer{stopErr: tt.stopErr, stop: make(chan struct{})}),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)

			go func() { errCh <- srv.Run(ctx) }()

			cancel()

			select {
			case err := <-errCh:
				assert.NoError(t, err)
			case <-time.After(runReturnTimeout):
				t.Fatal("Run did not return after cancellation")
			}

			entries := logs.FilterMessage("health server stopped with error during shutdown").All()
			assert.Len(t, entries, tt.wantWarn)
		})
	}
}

func TestExtractAPIPort(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		expected  string
	}{
		{
			name:      "standard port",
			endpoints: []string{"172.16.101.1:6443"},
			expected:  "6443",
		},
		{
			name:      "custom port",
			endpoints: []string{"10.0.0.1:8443"},
			expected:  "8443",
		},
		{
			name:      "empty endpoints returns default",
			endpoints: []string{},
			expected:  "6443",
		},
		{
			name:      "nil endpoints returns default",
			endpoints: nil,
			expected:  "6443",
		},
		{
			name:      "malformed endpoint returns default",
			endpoints: []string{"not-a-host-port"},
			expected:  "6443",
		},
		{
			name:      "uses first endpoint only",
			endpoints: []string{"10.0.0.1:9443", "10.0.0.2:8443"},
			expected:  "9443",
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			result := server.ExtractAPIPort(tcase.endpoints)
			assert.Equal(t, tcase.expected, result)
		})
	}
}

func TestWithLivenessConfig_ZeroInterval_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(0, time.Second)
	}, "zero interval must panic")
}

func TestWithLivenessConfig_NegativeThreshold_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(time.Second, -1*time.Second)
	}, "negative threshold must panic")
}

func TestWithLivenessConfig_NegativeInterval_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(-1*time.Second, time.Second)
	}, "negative interval must panic")
}

func TestWithLivenessConfig_ZeroThreshold_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(time.Second, 0)
	}, "zero threshold must panic")
}

func TestWithLivenessConfig_ValidValues_Applied(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	// Use a short interval and a threshold longer than the interval
	// to verify that the server stays alive through multiple heartbeat ticks.
	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 500*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	// Alive must remain true across multiple heartbeat intervals because
	// the heartbeat keeps refreshing within the threshold window.
	require.Never(t, func() bool {
		return !srv.Alive()
	}, 200*time.Millisecond, 25*time.Millisecond,
		"server must stay alive when heartbeat interval < threshold")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestWithLivenessConfig_ThresholdEqualsInterval_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(time.Second, time.Second)
	}, "threshold equal to interval must panic")
}

func TestWithLivenessConfig_ThresholdLessThanInterval_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessConfig(5*time.Second, 3*time.Second)
	}, "threshold less than interval must panic")
}

func TestWithLivenessProbe_Nil_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithLivenessProbe(nil)
	}, "nil probe function must panic")
}

func TestWithDiscoveryProviders_Empty_Panics(t *testing.T) {
	assert.Panics(t, func() {
		server.WithDiscoveryProviders()
	}, "empty providers must panic")
}

func TestAlive_FalseWhenDiscoveryExits(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 5*time.Second),
		server.WithDiscoveryProviders(immediateProvider{}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// The immediate provider returns nil right away, causing runDiscovery
	// to exit and discoveryDone to be set. Alive() must return false.
	require.Eventually(t, func() bool {
		return !srv.Alive()
	}, 2*time.Second, 25*time.Millisecond,
		"Alive must become false when discovery pipeline exits")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRun_DiscoveryErrorPropagates(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 5*time.Second),
		server.WithDiscoveryProviders(errorProvider{
			err: errors.New("discovery failed"),
		}),
	)
	require.NoError(t, err)

	ctx := t.Context()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// The error provider returns an error right away. The merged provider
	// propagates it via errgroup, causing Run to return with the error.
	// After Run returns, lastHeartbeat is reset to 0, so Alive() is false.
	select {
	case runErr := <-errCh:
		require.Error(t, runErr)
		assert.Contains(t, runErr.Error(), "discovery failed")
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for Run to return after discovery error")
	}

	assert.False(t, srv.Alive(),
		"Alive must be false after Run returns due to discovery error")
}

func TestRun_DiscoveryFallbackWithoutCluster(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()
	cfg.EnableDiscovery = true

	// No kube client injected, not in cluster -> should gracefully degrade to static only.
	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRun_SeedsLBWithStaticEndpoints(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// The health server starts after Run seeds the LB and launches
	// the errgroup. If healthz is reachable, the seed already happened.
	waitForHealthz(t, cfg.HealthPort)

	// Verify the original config slice was not mutated by seeding.
	assert.Equal(t, []string{"127.0.0.1:6443"}, cfg.Endpoints,
		"config endpoints must not be mutated by seeding")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestNew_UsesConfigLivenessValues(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()
	cfg.LivenessInterval = 2 * time.Second
	cfg.LivenessThreshold = 5 * time.Second

	srv, err := server.New(cfg, log,
		server.WithLivenessProbe(func() {}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	// With config-based liveness values (no WithLivenessConfig override),
	// the server should be alive as the no-op probe completes quickly.
	assert.True(t, srv.Alive(),
		"server must be alive with config-based liveness values")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

// replacingProvider sends one endpoint list and then blocks, replacing the
// static seed so the seeded endpoint is removed and drained.
type replacingProvider struct {
	endpoints []string
}

func (p replacingProvider) Run(ctx context.Context, ch chan<- []string) error {
	select {
	case ch <- p.endpoints:
	case <-ctx.Done():
		return nil
	}

	<-ctx.Done()

	return nil
}

func (replacingProvider) Name() string { return "replacing" }

var _ discovery.EndpointProvider = replacingProvider{}

func TestRun_ServesMetricsOnHealthPort(t *testing.T) {
	cfg := validConfig()

	srv, err := server.New(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	waitForHealthz(t, cfg.HealthPort)

	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.HealthPort)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := newTestHTTPClient().Do(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "/metrics must be served on the health port")
	assert.Contains(t, string(body), "extractedprism_upstreams_total")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRun_DrainTimeoutReachesProxy(t *testing.T) {
	cfg := validConfig()
	cfg.DrainTimeout = 7 * time.Second

	core, logs := observer.New(zap.InfoLevel)

	srv, err := server.New(cfg, zap.New(core),
		server.WithDiscoveryProviders(replacingProvider{endpoints: []string{"127.0.0.1:6444"}}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// The provider replaces the seeded static endpoint, so the proxy drains
	// it with the configured timeout.
	require.Eventually(t, func() bool {
		entries := logs.FilterMessage("draining upstream").All()
		if len(entries) == 0 {
			return false
		}

		fields := entries[0].ContextMap()

		return fields["upstream"] == cfg.Endpoints[0] && fields["timeout"] == 7*time.Second
	}, waitTimeout, pollTick, "configured drain timeout must reach the proxy")

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(runReturnTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRun_RecoveredDiscoveryErrors_ReachMetrics(t *testing.T) {
	cfg := validConfig()
	cfg.EnableDiscovery = true

	eps := &discoveryv1.EndpointSlice{
		Name:      "kubernetes",
		Namespace: "default",
		Labels:    map[string]string{"kubernetes.io/service-name": "kubernetes"},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"127.0.0.1"}}},
	}

	fakeClient := fake.NewClientset(eps)
	fakeWatcher := watch.NewFake()

	// Later watches get a fresh, quiet stream, as a real API server would
	// open.
	var watchCalls atomic.Int32

	fakeClient.PrependWatchReactor("endpointslices", func(_ k8stesting.Action) (bool, watch.Interface, error) {
		if watchCalls.Add(1) == 1 {
			return true, fakeWatcher, nil
		}

		return true, watch.NewFake(), nil
	})

	srv, err := server.New(cfg, zaptest.NewLogger(t), server.WithKubeClient(fakeClient))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// Stop the server on every exit path: a failed assertion must not leave
	// it running and logging into a finished test.
	t.Cleanup(func() {
		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(runReturnTimeout):
			t.Error("timed out waiting for shutdown")
		}
	})

	waitForHealthz(t, cfg.HealthPort)

	// The provider recovers from this error itself, so Run never returns
	// it; the server must still count it on the discovery metric.
	fakeWatcher.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 500, Reason: metav1.StatusReasonInternalError})

	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.HealthPort)

	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if reqErr != nil {
			return false
		}

		resp, getErr := newTestHTTPClient().Do(req)
		if getErr != nil {
			return false
		}
		defer resp.Body.Close()

		body, readErr := io.ReadAll(resp.Body)

		return readErr == nil &&
			strings.Contains(string(body), `extractedprism_discovery_errors_total{provider="kubernetes"} 1`)
	}, waitTimeout, pollTick, "recovered watch error must reach discovery_errors_total")
}
