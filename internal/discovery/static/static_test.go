package static_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/discovery/static"
)

const waitTimeout = 5 * time.Second

func receiveEndpoints(t *testing.T, ch <-chan []string) []string {
	t.Helper()

	select {
	case eps := <-ch:
		return eps
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for endpoints on channel")

		return nil
	}
}

func waitForResult(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for Run to return")

		return nil
	}
}

func TestStaticProvider_New_Valid(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{"10.0.0.1:6443", "10.0.0.2:6443"})
	require.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestStaticProvider_New_Empty(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{})
	require.Error(t, err)
	assert.Nil(t, provider)
	assert.True(t, errors.Is(err, config.ErrNoEndpoints))
}

func TestStaticProvider_New_InvalidEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "missing port", endpoint: "10.0.0.1"},
		{name: "empty string", endpoint: ""},
		{name: "no host", endpoint: ":6443"},
		{name: "port zero", endpoint: "10.0.0.1:0"},
		{name: "port above max", endpoint: "10.0.0.1:65536"},
		{name: "invalid hostname", endpoint: "-invalid:6443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := static.NewStaticProvider([]string{tt.endpoint})
			require.Error(t, err)
			assert.Nil(t, provider)
			assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
		})
	}
}

func TestStaticProvider_New_InvalidEndpoint_ErrorFormat(t *testing.T) {
	_, err := static.NewStaticProvider([]string{"10.0.0.1"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
	assert.Contains(t, err.Error(), `endpoint "10.0.0.1"`,
		"error message must use quoted endpoint format")
	assert.Contains(t, err.Error(), "static provider",
		"error must include wrap context from static provider")
}

func TestStaticProvider_New_InvalidEndpoint_ErrorWrapChain(t *testing.T) {
	tests := []struct {
		name            string
		endpoint        string
		expectedMessage string
	}{
		{
			name:            "invalid port includes port range message",
			endpoint:        "10.0.0.1:0",
			expectedMessage: "port must be a number between 1 and 65535",
		},
		{
			name:            "invalid host includes host message",
			endpoint:        "-invalid:6443",
			expectedMessage: "invalid host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := static.NewStaticProvider([]string{tt.endpoint})
			require.Error(t, err)
			assert.True(t, errors.Is(err, config.ErrInvalidEndpoint))
			assert.Contains(t, err.Error(), "static provider",
				"error must include wrap context from static provider")
			assert.Contains(t, err.Error(), tt.expectedMessage,
				"error must include validation detail from config.ValidateEndpoint")
		})
	}
}

func TestStaticProvider_Run_SendsEndpoints(t *testing.T) {
	endpoints := []string{"10.0.0.1:6443", "10.0.0.2:6443"}
	provider, err := static.NewStaticProvider(endpoints)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 1)

	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	received := receiveEndpoints(t, updateCh)
	assert.Equal(t, endpoints, received)

	cancel()

	runErr := waitForResult(t, errCh)
	assert.NoError(t, runErr)
}

func TestStaticProvider_Run_SliceIsolation(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{"10.0.0.1:6443", "10.0.0.2:6443"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 1)

	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	received := receiveEndpoints(t, updateCh)
	// Mutate the received slice.
	received[0] = "CORRUPTED"

	cancel()

	// Run the provider again to verify internal state was not corrupted.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	updateCh2 := make(chan []string, 1)

	go func() {
		_ = provider.Run(ctx2, updateCh2)
	}()

	received2 := receiveEndpoints(t, updateCh2)
	assert.Equal(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, received2,
		"internal endpoints should not be affected by consumer mutation")

	cancel2()
}

func TestStaticProvider_Run_CancelledBeforeSend(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{"10.0.0.1:6443"})
	require.NoError(t, err)

	// Unbuffered channel with no reader — send would block forever without ctx guard.
	updateCh := make(chan []string)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before Run starts.

	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	runErr := waitForResult(t, errCh)
	assert.NoError(t, runErr)
}

func TestStaticProvider_Run_CancelsCleanly(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{"10.0.0.1:6443"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 1)

	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain the initial send.
	receiveEndpoints(t, updateCh)

	cancel()

	runErr := waitForResult(t, errCh)
	assert.NoError(t, runErr)
}
