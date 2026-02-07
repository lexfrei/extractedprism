package static_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/discovery/static"
)

func TestStaticProvider_New_Valid(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{"10.0.0.1:6443", "10.0.0.2:6443"})
	require.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestStaticProvider_New_Empty(t *testing.T) {
	provider, err := static.NewStaticProvider([]string{})
	require.Error(t, err)
	assert.Nil(t, provider)
	assert.True(t, errors.Is(err, static.ErrNoEndpoints))
}

func TestStaticProvider_New_InvalidEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "missing port", endpoint: "10.0.0.1"},
		{name: "empty string", endpoint: ""},
		{name: "no host", endpoint: ":6443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := static.NewStaticProvider([]string{tt.endpoint})
			require.Error(t, err)
			assert.Nil(t, provider)
			assert.True(t, errors.Is(err, static.ErrInvalidEndpoint))
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

	select {
	case received := <-updateCh:
		assert.Equal(t, endpoints, received)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
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

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run blocked on cancelled context — goroutine leak")
	}
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
	<-updateCh

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for clean cancellation")
	}
}
