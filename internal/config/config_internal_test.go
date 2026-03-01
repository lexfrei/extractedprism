package config

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAddress_EmptyString(t *testing.T) {
	tests := []struct {
		name     string
		sentinel error
	}{
		{name: "bind address sentinel", sentinel: ErrInvalidBindAddress},
		{name: "health bind address sentinel", sentinel: ErrInvalidHealthBindAddress},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAddress("", tt.sentinel)
			require.Error(t, err)
			assert.True(t, errors.Is(err, tt.sentinel))
			assert.Contains(t, err.Error(), "must not be empty")
		})
	}
}

func TestValidateAddress_InvalidHost(t *testing.T) {
	err := validateAddress("host/path", ErrInvalidBindAddress)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidBindAddress))
	assert.Contains(t, err.Error(), "must be a valid IP address or hostname")
}

func TestValidateAddress_ValidHost(t *testing.T) {
	err := validateAddress("127.0.0.1", ErrInvalidBindAddress)
	require.NoError(t, err)
}
