package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
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

func TestSeedEndpoints_CopiesSlice(t *testing.T) {
	original := []string{"10.0.0.1:6443", "10.0.0.2:6443"}

	seed := make([]string, len(original))
	copy(seed, original)

	// Mutate seed to verify original is unaffected.
	seed[0] = "MUTATED"

	assert.Equal(t, "10.0.0.1:6443", original[0],
		"original slice must not be affected by seed mutation")
}
