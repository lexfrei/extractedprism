package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLogger_ValidLevels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error"}

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
