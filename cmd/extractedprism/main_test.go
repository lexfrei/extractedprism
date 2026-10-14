package main

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLogger_ValidLevels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error", "dpanic", "panic", "fatal"}

	for _, level := range levels {
		t.Run(level, func(t *testing.T) {
			logger, err := buildLogger(level)
			require.NoError(t, err)
			assert.NotNil(t, logger)
		})
	}
}

func TestBuildLogger_InvalidLevel(t *testing.T) {
	logger, err := buildLogger("invalid")
	require.Error(t, err)
	assert.Nil(t, logger)
}

func TestRun_InvalidConfigRejectedBeforeLogger(t *testing.T) {
	// Validate() is called before buildLogger(), so invalid config
	// is rejected with "invalid configuration" rather than a logger error.
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("endpoints", "")

	err := run(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestRun_InvalidLogLevelRejectedByValidation(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("log_level", "banana")

	err := run(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
	assert.Contains(t, err.Error(), "invalid log level")
}

func TestBuildConfig_HealthBindAddressFromViper(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("health_bind_address", "0.0.0.0")

	cfg := buildConfig()
	assert.Equal(t, "0.0.0.0", cfg.HealthBindAddress,
		"HealthBindAddress must be read from viper")
}

func TestBuildConfig_HealthBindAddressEmptyDefault(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	cfg := buildConfig()
	assert.Empty(t, cfg.HealthBindAddress,
		"HealthBindAddress must default to empty when not set in viper")
}

func setValidViperDefaults() {
	viper.Reset()
	viper.Set("bind_address", "127.0.0.1")
	viper.Set("bind_port", 17999)
	viper.Set("health_port", 18000)
	// health_bind_address intentionally not set: defaults to empty via
	// viper zero value, matching the production behavior where the flag
	// is not provided and Validate() defaults it to BindAddress.
	viper.Set("endpoints", "10.0.0.1:6443")
	viper.Set("health_interval", "20s")
	viper.Set("health_timeout", "15s")
	viper.Set("enable_discovery", false)
	viper.Set("log_level", "info")
	viper.Set("liveness_interval", "5s")
	viper.Set("liveness_threshold", "15s")
}

func TestBuildConfig_DrainTimeoutFromViper(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("drain_timeout", "7s")

	cfg := buildConfig()
	assert.Equal(t, 7*time.Second, cfg.DrainTimeout,
		"DrainTimeout must be read from viper")
}

func TestBuildConfig_UpstreamSelectionFromViper(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("upstream_selection", "latency")

	cfg := buildConfig()
	assert.Equal(t, "latency", cfg.UpstreamSelection,
		"UpstreamSelection must be read from viper")
}

func TestRegisterFlags_UpstreamSelectionDefaultsToRandom(t *testing.T) {
	flag := rootCmd.PersistentFlags().Lookup("upstream-selection")
	require.NotNil(t, flag, "--upstream-selection must be registered")
	assert.Equal(t, "random", flag.DefValue)
}

func TestBuildConfig_HealthCheckFromViper(t *testing.T) {
	setValidViperDefaults()
	t.Cleanup(viper.Reset)

	viper.Set("health_check", "readyz")
	viper.Set("health_check_ca_file", "/ca.crt")
	viper.Set("health_check_cert_file", "/client.crt")
	viper.Set("health_check_key_file", "/client.key")
	viper.Set("health_check_token_file", "/token")
	viper.Set("health_check_server_name", "api.example")

	cfg := buildConfig()
	assert.Equal(t, "readyz", cfg.HealthCheck)
	assert.Equal(t, "/ca.crt", cfg.HealthCheckCAFile)
	assert.Equal(t, "/client.crt", cfg.HealthCheckCertFile)
	assert.Equal(t, "/client.key", cfg.HealthCheckKeyFile)
	assert.Equal(t, "/token", cfg.HealthCheckTokenFile)
	assert.Equal(t, "api.example", cfg.HealthCheckServerName)
}

func TestRegisterFlags_HealthCheckDefaults(t *testing.T) {
	flags := rootCmd.PersistentFlags()

	mode := flags.Lookup("health-check")
	require.NotNil(t, mode, "--health-check must be registered")
	assert.Equal(t, "tcp", mode.DefValue)

	name := flags.Lookup("health-check-server-name")
	require.NotNil(t, name, "--health-check-server-name must be registered")
	assert.Equal(t, "kubernetes.default.svc", name.DefValue)

	for _, flagName := range []string{"health-check-ca-file", "health-check-cert-file", "health-check-key-file", "health-check-token-file"} {
		flag := flags.Lookup(flagName)
		require.NotNil(t, flag, "--%s must be registered", flagName)
		assert.Empty(t, flag.DefValue)
	}
}
