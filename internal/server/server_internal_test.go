package server

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestGetKubeClient_ConstructsLBHost(t *testing.T) {
	tests := []struct {
		name        string
		bindAddress string
		bindPort    int
		expected    string
	}{
		{
			name:        "IPv4 default",
			bindAddress: "127.0.0.1",
			bindPort:    7445,
			expected:    "127.0.0.1:7445",
		},
		{
			name:        "IPv6 loopback",
			bindAddress: "::1",
			bindPort:    7445,
			expected:    "[::1]:7445",
		},
		{
			name:        "custom port",
			bindAddress: "127.0.0.1",
			bindPort:    9443,
			expected:    "127.0.0.1:9443",
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			cfg := &config.Config{
				BindAddress: tcase.bindAddress,
				BindPort:    tcase.bindPort,
			}

			result := net.JoinHostPort(cfg.BindAddress, strconv.Itoa(cfg.BindPort))
			assert.Equal(t, tcase.expected, result)
		})
	}
}
