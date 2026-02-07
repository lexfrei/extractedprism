package config_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/config"
)

func TestDefaults(t *testing.T) {
	cfg := config.NewDefault()

	assert.Equal(t, "127.0.0.1", cfg.BindAddress)
	assert.Equal(t, 7445, cfg.BindPort)
	assert.Equal(t, 7446, cfg.HealthPort)
	assert.Empty(t, cfg.Endpoints)
	assert.Equal(t, 20*time.Second, cfg.HealthInterval)
	assert.Equal(t, 15*time.Second, cfg.HealthTimeout)
	assert.True(t, cfg.EnableDiscovery)
	assert.Equal(t, "info", cfg.LogLevel)
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Endpoints = []string{"10.0.0.1:6443", "10.0.0.2:6443"}

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestValidate_NoEndpoints(t *testing.T) {
	cfg := config.NewDefault()

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrNoEndpoints))
}

func TestValidate_InvalidEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "missing port", endpoint: "10.0.0.1"},
		{name: "empty string", endpoint: ""},
		{name: "port only", endpoint: ":6443"},
		{name: "no colon", endpoint: "localhost"},
		{name: "triple colon", endpoint: "host::6443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Endpoints = []string{tt.endpoint}

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
		})
	}
}

func TestValidate_InvalidPort(t *testing.T) {
	tests := []struct {
		name     string
		port     int
		setField string
	}{
		{name: "bind port zero", port: 0, setField: "bind"},
		{name: "bind port negative", port: -1, setField: "bind"},
		{name: "bind port too high", port: 70000, setField: "bind"},
		{name: "health port zero", port: 0, setField: "health"},
		{name: "health port negative", port: -1, setField: "health"},
		{name: "health port too high", port: 70000, setField: "health"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Endpoints = []string{"10.0.0.1:6443"}

			switch tt.setField {
			case "bind":
				cfg.BindPort = tt.port
			case "health":
				cfg.HealthPort = tt.port
			}

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidPort))
		})
	}
}

func TestValidate_PortConflict(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.BindPort = 8080
	cfg.HealthPort = 8080

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrPortConflict))
}

func TestValidate_HealthTimingInvalid(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.HealthTimeout = 30 * time.Second
	cfg.HealthInterval = 20 * time.Second

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrInvalidHealthTiming))
}

func TestParseEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single endpoint",
			input:    "10.0.0.1:6443",
			expected: []string{"10.0.0.1:6443"},
		},
		{
			name:     "multiple endpoints",
			input:    "10.0.0.1:6443,10.0.0.2:6443,10.0.0.3:6443",
			expected: []string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
		},
		{
			name:     "with spaces",
			input:    " 10.0.0.1:6443 , 10.0.0.2:6443 ",
			expected: []string{"10.0.0.1:6443", "10.0.0.2:6443"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := config.ParseEndpoints(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
