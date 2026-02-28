package server_test

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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

// errorProvider returns an error from Run immediately, simulating a
// discovery failure. Used to test the discoveryDone path in Alive().
type errorProvider struct {
	err error
}

func (e errorProvider) Run(_ context.Context, _ chan<- []string) error {
	return e.err
}

// Compile-time checks.
var (
	_ discovery.EndpointProvider = immediateProvider{}
	_ discovery.EndpointProvider = errorProvider{}
)

const (
	waitTimeout = 5 * time.Second
	pollTick    = 10 * time.Millisecond
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

// waitForHealthz polls the health endpoint until it responds with 200 or the
// timeout fires. Since /healthz now checks Alive(), this implicitly verifies
// that Run() has stored the initial heartbeat. This is safe because Run()
// stores lastHeartbeat before launching the errgroup (including the health server).
func waitForHealthz(t *testing.T, healthPort int) {
	t.Helper()

	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", healthPort)

	require.Eventually(t, func() bool {
		resp, err := http.Get(url) //nolint:noctx // test helper, no context needed
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
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
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
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
	case <-time.After(waitTimeout):
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// Run should complete promptly when health Start returns nil,
	// not block forever waiting on errCh.
	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health server exited unexpectedly")
	case <-time.After(waitTimeout):
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health server start")
		assert.Contains(t, err.Error(), "address already in use")
	case <-time.After(waitTimeout):
		t.Fatal("Run blocked on health start error")
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestAlive_FalseWhenDiscoveryErrors(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log,
		server.WithLivenessConfig(50*time.Millisecond, 5*time.Second),
		server.WithDiscoveryProviders(errorProvider{
			err: errors.New("discovery failed"),
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// The error provider returns an error right away. The merged provider
	// propagates it, errgroup cancels context, and discoveryDone is set.
	// Run should return with an error containing "discovery failed".
	select {
	case runErr := <-errCh:
		require.Error(t, runErr)
		assert.Contains(t, runErr.Error(), "discovery failed")
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for Run to return after discovery error")
	}

	assert.False(t, srv.Alive(),
		"Alive must be false after discovery exits with error")
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
	case <-time.After(waitTimeout):
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
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for shutdown")
	}
}
