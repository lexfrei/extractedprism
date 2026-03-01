package main

import (
	"testing"

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
	viper.Set("health_bind_address", "")
	viper.Set("endpoints", "10.0.0.1:6443")
	viper.Set("health_interval", "20s")
	viper.Set("health_timeout", "15s")
	viper.Set("enable_discovery", false)
	viper.Set("log_level", "info")
	viper.Set("liveness_interval", "5s")
	viper.Set("liveness_threshold", "15s")
}
