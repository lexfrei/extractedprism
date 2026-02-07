package server_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/server"
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
	cfg := config.NewDefault()
	cfg.BindPort = bindPort
	cfg.HealthPort = healthPort
	cfg.Endpoints = []string{"127.0.0.1:6443"}
	cfg.HealthInterval = 5 * time.Second
	cfg.HealthTimeout = 2 * time.Second

	return cfg
}

func TestNew_ValidConfig(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := validConfig()

	srv, err := server.New(cfg, log)
	require.NoError(t, err)
	assert.NotNil(t, srv)
}

func TestNew_InvalidConfig(t *testing.T) {
	log := zaptest.NewLogger(t)
	cfg := config.NewDefault()
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

	time.Sleep(300 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
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

	time.Sleep(300 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
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

	time.Sleep(500 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
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

	time.Sleep(300 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
}
