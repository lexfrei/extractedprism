package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/config"
)

func TestBaseConfig_Defaults(t *testing.T) {
	cfg := config.NewBaseConfig()

	assert.Equal(t, "127.0.0.1", cfg.BindAddress)
	assert.Equal(t, 7445, cfg.BindPort)
	assert.Equal(t, 7446, cfg.HealthPort)
	assert.Empty(t, cfg.Endpoints)
	assert.Equal(t, 20*time.Second, cfg.HealthInterval)
	assert.Equal(t, 15*time.Second, cfg.HealthTimeout)
	assert.True(t, cfg.EnableDiscovery)
	assert.Equal(t, "info", cfg.LogLevel)
}

func TestBaseConfig_RequiresEndpoints(t *testing.T) {
	cfg := config.NewBaseConfig()

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrNoEndpoints),
		"NewBaseConfig without endpoints must fail validation")
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := config.NewBaseConfig()
	cfg.Endpoints = []string{"10.0.0.1:6443", "10.0.0.2:6443"}

	err := cfg.Validate()
	require.NoError(t, err)
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
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{tt.endpoint}

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
		})
	}
}

func TestValidate_InvalidEndpoint_ErrorFormat(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Endpoints = []string{"10.0.0.1"}

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
	assert.Contains(t, err.Error(), `endpoint "10.0.0.1"`,
		"error message must use quoted endpoint format")
}

func TestValidate_InvalidEndpointPort_ErrorFormat(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Endpoints = []string{"10.0.0.1:abc"}

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
	assert.Contains(t, err.Error(), `endpoint "10.0.0.1:abc"`,
		"error message must use quoted endpoint format")
}

func TestValidate_InvalidEndpointPort(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "non-numeric port", endpoint: "10.0.0.1:abc"},
		{name: "port zero", endpoint: "10.0.0.1:0"},
		{name: "port too high", endpoint: "10.0.0.1:99999"},
		{name: "negative port", endpoint: "10.0.0.1:-1"},
		{name: "float port", endpoint: "10.0.0.1:6443.5"},
		{name: "port 65536", endpoint: "10.0.0.1:65536"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{tt.endpoint}

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
			assert.Contains(t, err.Error(), "port must be a number between 1 and 65535")
		})
	}
}

func TestValidate_EndpointPortBoundaryValid(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "minimum valid port", endpoint: "10.0.0.1:1"},
		{name: "maximum valid port", endpoint: "10.0.0.1:65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{tt.endpoint}

			err := cfg.Validate()
			require.NoError(t, err)
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
			cfg := config.NewBaseConfig()
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

func TestValidate_InvalidBindAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{name: "empty string", address: ""},
		{name: "contains spaces", address: "127.0.0 .1"},
		{name: "contains slash", address: "host/path"},
		{name: "contains at sign", address: "user@host"},
		{name: "contains colon", address: "host:port"},
		{name: "leading hyphen label", address: "-invalid.example.com"},
		{name: "trailing hyphen label", address: "invalid-.example.com"},
		{name: "contains underscore", address: "_srv.example.com"},
		{name: "label too long 64 chars", address: strings.Repeat("a", 64) + ".example.com"},
		{name: "hostname too long", address: strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{"10.0.0.1:6443"}
			cfg.BindAddress = tt.address

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidBindAddress))
		})
	}
}

func TestValidate_ValidBindAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{name: "IPv4 loopback", address: "127.0.0.1"},
		{name: "IPv4 all interfaces", address: "0.0.0.0"},
		{name: "IPv6 loopback", address: "::1"},
		{name: "IPv6 all interfaces", address: "::"},
		{name: "localhost", address: "localhost"},
		{name: "hostname with dots", address: "my-host.example.com"},
		{name: "single label hostname", address: "myhost"},
		{name: "FQDN with trailing dot", address: "my-host.example.com."},
		{name: "label at max 63 chars", address: strings.Repeat("a", 63) + ".example.com"},
		{name: "hostname at max 253 chars", address: strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{"10.0.0.1:6443"}
			cfg.BindAddress = tt.address

			err := cfg.Validate()
			require.NoError(t, err)
		})
	}
}

func TestValidate_BindAddressSyntacticOnly(t *testing.T) {
	// Validation checks syntactic correctness only, not DNS resolution.
	// Non-resolvable but syntactically valid hostnames pass validation.
	// This is intentional: DNS may be unavailable during early boot.
	tests := []struct {
		name    string
		address string
	}{
		{name: "non-resolvable hostname", address: "this-host-does-not-exist.invalid"},
		{name: "ticket example input", address: "not-an-ip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{"10.0.0.1:6443"}
			cfg.BindAddress = tt.address

			err := cfg.Validate()
			require.NoError(t, err)
		})
	}
}

func TestValidate_InvalidBindAddressErrorMessages(t *testing.T) {
	tests := []struct {
		name            string
		address         string
		expectedMessage string
	}{
		{name: "empty address", address: "", expectedMessage: "must not be empty"},
		{name: "invalid chars", address: "host/path", expectedMessage: "must be a valid IP address or hostname"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{"10.0.0.1:6443"}
			cfg.BindAddress = tt.address

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidBindAddress))
			assert.Contains(t, err.Error(), tt.expectedMessage)
		})
	}
}

func TestValidate_PortConflict(t *testing.T) {
	cfg := config.NewBaseConfig()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.BindPort = 8080
	cfg.HealthPort = 8080

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrPortConflict))
}

func TestValidate_HealthTimingInvalid(t *testing.T) {
	cfg := config.NewBaseConfig()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.HealthTimeout = 30 * time.Second
	cfg.HealthInterval = 20 * time.Second

	err := cfg.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrInvalidHealthTiming))
}

func TestValidate_InvalidHealthDuration(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		timeout  time.Duration
	}{
		{name: "zero interval", interval: 0, timeout: 15 * time.Second},
		{name: "negative interval", interval: -1 * time.Second, timeout: 15 * time.Second},
		{name: "sub-second interval", interval: 500 * time.Millisecond, timeout: 15 * time.Second},
		{name: "zero timeout", interval: 20 * time.Second, timeout: 0},
		{name: "negative timeout", interval: 20 * time.Second, timeout: -1 * time.Second},
		{name: "sub-second timeout", interval: 20 * time.Second, timeout: 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewBaseConfig()
			cfg.Endpoints = []string{"10.0.0.1:6443"}
			cfg.HealthInterval = tt.interval
			cfg.HealthTimeout = tt.timeout

			err := cfg.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidHealthDuration))
		})
	}
}

func TestValidate_ValidHealthDurationBoundary(t *testing.T) {
	cfg := config.NewBaseConfig()
	cfg.Endpoints = []string{"10.0.0.1:6443"}
	cfg.HealthInterval = 2 * time.Second
	cfg.HealthTimeout = 1 * time.Second

	err := cfg.Validate()
	require.NoError(t, err)
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
