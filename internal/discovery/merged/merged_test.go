package merged_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/lexfrei/extractedprism/internal/discovery"
	"github.com/lexfrei/extractedprism/internal/discovery/merged"
)

// mockProvider implements EndpointProvider for testing.
type mockProvider struct {
	sendFunc func(ctx context.Context, ch chan<- []string) error
}

var _ discovery.EndpointProvider = (*mockProvider)(nil)

func (m *mockProvider) Run(ctx context.Context, ch chan<- []string) error {
	return m.sendFunc(ctx, ch)
}

// newImmediateProvider returns a mock that sends endpoints once and blocks.
func newImmediateProvider(endpoints []string) *mockProvider {
	return &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- endpoints

			<-ctx.Done()

			return nil
		},
	}
}

// newDelayedUpdateProvider sends initial, waits for trigger, then sends updated.
func newDelayedUpdateProvider(
	initial []string,
	updated []string,
	trigger <-chan struct{},
) *mockProvider {
	return &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- initial

			select {
			case <-trigger:
				ch <- updated
			case <-ctx.Done():
				return nil
			}

			<-ctx.Done()

			return nil
		},
	}
}

// newErrorProvider returns a mock that fails immediately.
func newErrorProvider(err error) *mockProvider {
	return &mockProvider{
		sendFunc: func(_ context.Context, _ chan<- []string) error {
			return err
		},
	}
}

// newEmptyThenNothingProvider sends an empty list and blocks.
func newEmptyThenNothingProvider() *mockProvider {
	return &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{}

			<-ctx.Done()

			return nil
		},
	}
}

// receiveWithTimeout reads from ch with a timeout.
func receiveWithTimeout(
	t *testing.T,
	ch <-chan []string,
	timeout time.Duration,
) []string {
	t.Helper()

	select {
	case val := <-ch:
		return val
	case <-time.After(timeout):
		t.Fatal("timed out waiting for endpoints")

		return nil
	}
}

func TestRun_SingleProvider(t *testing.T) {
	log := zaptest.NewLogger(t)
	provider := newImmediateProvider([]string{"10.0.0.1:6443"})

	mp := merged.NewMergedProvider(log, provider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	got := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, got)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_MergesProviders(t *testing.T) {
	log := zaptest.NewLogger(t)
	prov1 := newImmediateProvider([]string{"10.0.0.1:6443"})
	prov2 := newImmediateProvider([]string{"10.0.0.2:6443"})

	mp := merged.NewMergedProvider(log, prov1, prov2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// We may get partial results first, drain until we see the full merged set.
	deadline := time.After(2 * time.Second)

	var last []string

	for {
		select {
		case got := <-updateCh:
			last = got
			if len(last) == 2 {
				assert.Equal(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, last)
				cancel()

				return
			}
		case <-deadline:
			t.Fatalf("timed out; last received: %v", last)
		}
	}
}

func TestRun_Deduplicates(t *testing.T) {
	log := zaptest.NewLogger(t)
	prov1 := newImmediateProvider([]string{"10.0.0.1:6443", "10.0.0.2:6443"})
	prov2 := newImmediateProvider([]string{"10.0.0.2:6443", "10.0.0.3:6443"})

	mp := merged.NewMergedProvider(log, prov1, prov2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	deadline := time.After(2 * time.Second)

	var last []string

	for {
		select {
		case got := <-updateCh:
			last = got
			// Once we see all 3 unique endpoints, verify no dups.
			if len(last) == 3 {
				assert.Equal(t, []string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"}, last)
				cancel()

				return
			}
		case <-deadline:
			t.Fatalf("timed out; last received: %v", last)
		}
	}
}

func TestRun_NeverSendsEmpty(t *testing.T) {
	log := zaptest.NewLogger(t)
	prov := newEmptyThenNothingProvider()

	mp := merged.NewMergedProvider(log, prov)

	ctx, cancel := context.WithCancel(context.Background())

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Give time for potential empty send to arrive.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}

	// Drain channel — should be empty (no endpoints were sent).
	select {
	case got := <-updateCh:
		t.Fatalf("expected no sends, but received: %v", got)
	default:
		// Good — nothing was sent.
	}
}

func TestRun_DynamicUpdatePropagates(t *testing.T) {
	log := zaptest.NewLogger(t)
	trigger := make(chan struct{})
	prov := newDelayedUpdateProvider(
		[]string{"10.0.0.1:6443"},
		[]string{"10.0.0.1:6443", "10.0.0.5:6443"},
		trigger,
	)

	mp := merged.NewMergedProvider(log, prov)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	first := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, first)

	// Trigger the update.
	close(trigger)

	second := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443", "10.0.0.5:6443"}, second)

	cancel()
}

func TestRun_ContextCancellation(t *testing.T) {
	log := zaptest.NewLogger(t)
	prov := newImmediateProvider([]string{"10.0.0.1:6443"})

	mp := merged.NewMergedProvider(log, prov)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Drain initial.
	receiveWithTimeout(t, updateCh, time.Second)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for clean shutdown")
	}
}

func TestRun_ProviderError(t *testing.T) {
	log := zaptest.NewLogger(t)
	provErr := errors.New("provider failed")
	prov := newErrorProvider(provErr)

	mp := merged.NewMergedProvider(log, prov)

	ctx := t.Context()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, provErr))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error")
	}
}
