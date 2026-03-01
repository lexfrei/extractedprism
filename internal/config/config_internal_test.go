package config

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateHealthBindAddress_EmptyString(t *testing.T) {
	// This path is unreachable through Validate() because Validate() defaults
	// HealthBindAddress to BindAddress before calling validateHealthBindAddress.
	// The empty-string check exists as defense-in-depth for direct callers.
	err := validateHealthBindAddress("")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidHealthBindAddress))
	assert.Contains(t, err.Error(), "must not be empty")
}

func TestValidateHealthBindAddress_InvalidHost(t *testing.T) {
	err := validateHealthBindAddress("host/path")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidHealthBindAddress))
	assert.Contains(t, err.Error(), "must be a valid IP address or hostname")
}

func TestValidateHealthBindAddress_ValidHost(t *testing.T) {
	err := validateHealthBindAddress("127.0.0.1")
	require.NoError(t, err)
}
