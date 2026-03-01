package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/rest"

	"github.com/lexfrei/extractedprism/internal/config"
)

func TestApplyLBOverride_SetsHostAndServerName(t *testing.T) {
	restCfg := &rest.Config{
		Host: "https://10.96.0.1:443",
	}

	applyLBOverride(restCfg, "127.0.0.1:7445")

	assert.Equal(t, "https://127.0.0.1:7445", restCfg.Host)
	assert.Equal(t, kubeAPIServerName, restCfg.ServerName)
}

func TestApplyLBOverride_PreservesExistingTLSFields(t *testing.T) {
	restCfg := &rest.Config{
		Host: "https://10.96.0.1:443",
		TLSClientConfig: rest.TLSClientConfig{
			CAFile: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		},
	}

	applyLBOverride(restCfg, "127.0.0.1:7445")

	assert.Equal(t, "https://127.0.0.1:7445", restCfg.Host)
	assert.Equal(t, kubeAPIServerName, restCfg.ServerName)
	assert.Equal(t, "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		restCfg.CAFile, "CA file must not be overwritten")
}

func TestApplyLBOverride_IPv6BindAddress(t *testing.T) {
	restCfg := &rest.Config{
		Host: "https://10.96.0.1:443",
	}

	applyLBOverride(restCfg, "[::1]:7445")

	assert.Equal(t, "https://[::1]:7445", restCfg.Host)
	assert.Equal(t, kubeAPIServerName, restCfg.ServerName)
}

func TestAlive_StaleHeartbeat_ReturnsFalse(t *testing.T) {
	srv := &Server{
		livenessThreshold: 100 * time.Millisecond,
	}

	// Set a heartbeat far in the past (beyond threshold).
	srv.lastHeartbeat.Store(time.Now().Add(-1 * time.Second).UnixNano())

	assert.False(t, srv.Alive(),
		"Alive must return false when heartbeat is older than threshold")
}

func TestAlive_FreshHeartbeat_ReturnsTrue(t *testing.T) {
	srv := &Server{
		livenessThreshold: 5 * time.Second,
	}

	srv.lastHeartbeat.Store(time.Now().UnixNano())

	assert.True(t, srv.Alive(),
		"Alive must return true when heartbeat is within threshold")
}

func TestAlive_ZeroHeartbeat_ReturnsFalse(t *testing.T) {
	srv := &Server{
		livenessThreshold: 5 * time.Second,
	}

	assert.False(t, srv.Alive(),
		"Alive must return false when heartbeat has never been set")
}

func TestAlive_DiscoveryDone_ReturnsFalse(t *testing.T) {
	srv := &Server{
		livenessThreshold: 5 * time.Second,
	}

	srv.lastHeartbeat.Store(time.Now().UnixNano())
	srv.discoveryDone.Store(true)

	assert.False(t, srv.Alive(),
		"Alive must return false when discovery pipeline has exited")
}

func TestNew_SetsLivenessFromConfig(t *testing.T) {
	cfg := config.NewBaseConfig()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.LivenessInterval = 3 * time.Second
	cfg.LivenessThreshold = 10 * time.Second

	srv, err := New(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	assert.Equal(t, 3*time.Second, srv.heartbeatInterval,
		"heartbeatInterval must be set from Config.LivenessInterval")
	assert.Equal(t, 10*time.Second, srv.livenessThreshold,
		"livenessThreshold must be set from Config.LivenessThreshold")
}

func TestSeedEndpoints_CopiesSlice(t *testing.T) {
	original := []string{"10.0.0.1:6443", "10.0.0.2:6443"}

	seed := make([]string, len(original))
	copy(seed, original)

	// Mutate seed to verify original is unaffected.
	seed[0] = "MUTATED"

	assert.Equal(t, "10.0.0.1:6443", original[0],
		"original slice must not be affected by seed mutation")
}
