package merged_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/lexfrei/extractedprism/internal/discovery"
	"github.com/lexfrei/extractedprism/internal/discovery/merged"
)

const stableEndpoint = "10.0.0.1:6443"

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

// newGracefulExitProvider sends endpoints once and returns nil (graceful exit).
func newGracefulExitProvider(endpoints []string) *mockProvider {
	return &mockProvider{
		sendFunc: func(_ context.Context, ch chan<- []string) error {
			ch <- endpoints

			return nil
		},
	}
}

// newSendThenErrorProvider sends endpoints, waits for trigger, then returns an error.
// The trigger allows tests to control when the error happens, eliminating race conditions.
func newSendThenErrorProvider(
	endpoints []string,
	err error,
	trigger <-chan struct{},
) *mockProvider {
	return &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- endpoints

			select {
			case <-trigger:
				return err
			case <-ctx.Done():
				return nil
			}
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

func TestRun_SingleProviderFailureDoesNotKillOthers(t *testing.T) {
	log := zaptest.NewLogger(t)
	healthy := newImmediateProvider([]string{"10.0.0.1:6443"})
	failing := newErrorProvider(errors.New("kubernetes API unavailable"))

	mp := merged.NewMergedProvider(log, healthy, failing)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Healthy provider should still deliver endpoints.
	got := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, got)

	// Give time for the error provider to fail and be logged.
	time.Sleep(100 * time.Millisecond)

	// Merged provider must still be running (not killed by the failing provider).
	select {
	case err := <-errCh:
		t.Fatalf("Run returned unexpectedly: %v", err)
	default:
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_FailedProviderEndpointsAreCleared(t *testing.T) {
	log := zaptest.NewLogger(t)

	errorTrigger := make(chan struct{})

	// Failing provider sends an endpoint, waits for trigger, then errors out.
	failing := newSendThenErrorProvider(
		[]string{"10.0.0.99:6443"},
		errors.New("kubernetes API unavailable"),
		errorTrigger,
	)
	// Healthy provider stays alive.
	healthy := newImmediateProvider([]string{"10.0.0.1:6443"})

	mp := merged.NewMergedProvider(log, healthy, failing)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Phase 1: Wait for the combined state (both providers' data visible).
	waitForCombined(t, updateCh)

	// Phase 2: Trigger the error, then wait for the stale data to be cleared.
	close(errorTrigger)

	waitForCleanup(t, updateCh)

	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return after cancel")
	}
}

func TestRun_AllProvidersFailReturnsError(t *testing.T) {
	log := zaptest.NewLogger(t)
	err1 := errors.New("provider 1 failed")
	err2 := errors.New("provider 2 failed")

	mp := merged.NewMergedProvider(log,
		newErrorProvider(err1),
		newErrorProvider(err2),
	)

	ctx := t.Context()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, err1), "combined error must contain err1")
		assert.True(t, errors.Is(err, err2), "combined error must contain err2")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error")
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

func TestRun_GracefulExitPlusErrorDoesNotHang(t *testing.T) {
	log := zaptest.NewLogger(t)

	graceful := newGracefulExitProvider([]string{"10.0.0.1:6443"})
	failing := newErrorProvider(errors.New("provider failed"))

	mp := merged.NewMergedProvider(log, graceful, failing)

	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(context.Background(), make(chan []string, 10)) }()

	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() hung: deadlock when graceful + error providers both exit")
	}
}

func TestRun_AllGracefulExitReturnsNil(t *testing.T) {
	log := zaptest.NewLogger(t)

	prov1 := newGracefulExitProvider([]string{"10.0.0.1:6443"})
	prov2 := newGracefulExitProvider([]string{"10.0.0.2:6443"})

	mp := merged.NewMergedProvider(log, prov1, prov2)

	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(context.Background(), make(chan []string, 10)) }()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run() hung: deadlock when all providers exit gracefully")
	}
}

func TestRun_ZeroProvidersReturnsError(t *testing.T) {
	log := zaptest.NewLogger(t)
	mp := merged.NewMergedProvider(log)

	err := mp.Run(t.Context(), make(chan []string, 1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no providers configured")
}

func TestRun_BurstUpdatesWithSlowConsumer(t *testing.T) {
	log := zaptest.NewLogger(t)

	// burstSize intentionally exceeds providerChBuffer (16) to verify that
	// the drain pattern in mergeLoop handles bursts of any size, not just
	// those that fit in the buffer.
	const burstSize = 30

	// Provider sends burstSize rapid sequential updates.
	burstProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			for i := range burstSize {
				ep := []string{fmt.Sprintf("10.0.0.%d:6443", i+1)}
				select {
				case ch <- ep:
				case <-ctx.Done():
					return nil
				}
			}

			<-ctx.Done()

			return nil
		},
	}

	mp := merged.NewMergedProvider(log, burstProv)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 1)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Drain all updates until we see the final burst value.
	// The drain pattern in mergeLoop ensures that even with burst > buffer,
	// all updates eventually flow through without deadlock.
	deadline := time.After(5 * time.Second)
	var lastReceived []string

	for {
		select {
		case got := <-updateCh:
			lastReceived = got
			if len(got) == 1 && got[0] == fmt.Sprintf("10.0.0.%d:6443", burstSize) {
				goto done
			}
		case <-deadline:
			t.Fatalf("timed out waiting for final burst update; last received: %v", lastReceived)
		}
	}

done:
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_DrainCoalescesUpdates(t *testing.T) {
	log := zaptest.NewLogger(t)

	trigger := make(chan struct{})

	// Provider sends initial, waits for trigger, then sends a rapid burst.
	burstProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{"10.0.0.1:6443"}

			select {
			case <-trigger:
			case <-ctx.Done():
				return nil
			}

			// Rapid burst of 5 updates while consumer is slow.
			for i := range 5 {
				ch <- []string{fmt.Sprintf("10.0.%d.1:6443", i)}
			}

			<-ctx.Done()

			return nil
		},
	}

	mp := merged.NewMergedProvider(log, burstProv)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 1)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Consume the initial update.
	initial := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, initial)

	// Trigger the burst, then give time for updates to queue.
	close(trigger)
	time.Sleep(100 * time.Millisecond)

	// When we read the next update, mergeLoop should have drained
	// multiple pending updates and sent the latest merged state.
	// We don't know exactly how many reads it coalesced, but the
	// final value (10.0.4.1:6443) must be present.
	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 1 && got[0] == "10.0.4.1:6443" {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for coalesced final update")
		}
	}

done:
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

// --- Tests: stale map entry cleanup ---

func TestRun_NilUpdateDeletesMapEntry(t *testing.T) {
	// When a provider sends nil endpoints (e.g., after failure),
	// the map entry must be deleted, not retained as nil.
	// Verify by checking that after cleanup the merged result
	// only contains data from the remaining provider.
	log := zaptest.NewLogger(t)

	trigger := make(chan struct{})

	// Provider 0: sends initial, waits for trigger, then sends nil to clear.
	clearingProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{"10.0.0.99:6443"}

			select {
			case <-trigger:
				ch <- nil
			case <-ctx.Done():
				return nil
			}

			<-ctx.Done()

			return nil
		},
	}

	// Provider 1: stays alive with stable endpoints.
	stableProv := newImmediateProvider([]string{"10.0.0.1:6443"})

	mp := merged.NewMergedProvider(log, clearingProv, stableProv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Wait for combined state.
	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 2 {
				goto combined
			}
		case <-deadline:
			t.Fatal("timed out waiting for combined endpoints")
		}
	}

combined:
	// Trigger nil update from provider 0.
	close(trigger)

	// Wait for cleanup: only stable provider's endpoint should remain.
	deadline = time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 1 && got[0] == stableEndpoint {
				goto cleaned
			}
		case <-deadline:
			t.Fatal("timed out waiting for stale entry cleanup")
		}
	}

cleaned:
	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_EmptySliceUpdateDeletesMapEntry(t *testing.T) {
	// Same as TestRun_NilUpdateDeletesMapEntry but the clearing provider
	// sends []string{} (empty non-nil) instead of nil. Both must trigger
	// map entry deletion per applyUpdate semantics.
	log := zaptest.NewLogger(t)

	trigger := make(chan struct{})

	clearingProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{"10.0.0.99:6443"}

			select {
			case <-trigger:
				ch <- []string{}
			case <-ctx.Done():
				return nil
			}

			<-ctx.Done()

			return nil
		},
	}

	stableProv := newImmediateProvider([]string{"10.0.0.1:6443"})

	mp := merged.NewMergedProvider(log, clearingProv, stableProv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Wait for combined state.
	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 2 {
				goto combined
			}
		case <-deadline:
			t.Fatal("timed out waiting for combined endpoints")
		}
	}

combined:
	close(trigger)

	deadline = time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 1 && got[0] == stableEndpoint {
				goto cleaned
			}
		case <-deadline:
			t.Fatal("timed out waiting for empty-slice entry cleanup")
		}
	}

cleaned:
	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

// --- Tests: backpressure cascade prevention ---

func TestRun_BackpressureDoesNotBlockProviders(t *testing.T) {
	// When updateCh is slow (unbuffered), providers sending to internalCh
	// must not block indefinitely. The mergeLoop should continue reading
	// internalCh while waiting to send on updateCh.
	log := zaptest.NewLogger(t)

	allSent := make(chan struct{})

	// Provider sends a burst of updates rapidly.
	burstProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			for i := range 5 {
				select {
				case ch <- []string{fmt.Sprintf("10.0.0.%d:6443", i+1)}:
				case <-ctx.Done():
					return nil
				}
			}
			close(allSent)

			<-ctx.Done()

			return nil
		},
	}

	mp := merged.NewMergedProvider(log, burstProv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unbuffered updateCh to maximize blocking.
	updateCh := make(chan []string)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Slowly consume updates to create backpressure.
	deadline := time.After(5 * time.Second)
	var lastReceived []string

	for {
		select {
		case got := <-updateCh:
			lastReceived = got
		case <-allSent:
			// All updates sent by provider. Read remaining.
			goto drain
		case <-deadline:
			t.Fatalf("timed out; provider blocked due to backpressure; last: %v", lastReceived)
		}
	}

drain:
	// Drain any remaining updates.
	for {
		select {
		case got := <-updateCh:
			lastReceived = got
		case <-time.After(500 * time.Millisecond):
			goto verify
		}
	}

verify:
	// The last received must contain the final update.
	assert.Contains(t, lastReceived, "10.0.0.5:6443",
		"final provider update must be reflected despite backpressure")

	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_BackpressureSendsLatestValue(t *testing.T) {
	// While blocked on updateCh, new updates from internalCh must
	// be incorporated. The value eventually sent should be the latest,
	// not a stale intermediate.
	log := zaptest.NewLogger(t)

	trigger := make(chan struct{})

	// Provider sends initial, waits for trigger, then sends rapid burst.
	burstProv := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{"10.0.0.1:6443"}

			select {
			case <-trigger:
			case <-ctx.Done():
				return nil
			}

			// Rapid burst: each overwrites the previous.
			for i := range 10 {
				select {
				case ch <- []string{fmt.Sprintf("10.0.%d.1:6443", i)}:
				case <-ctx.Done():
					return nil
				}
			}

			<-ctx.Done()

			return nil
		},
	}

	mp := merged.NewMergedProvider(log, burstProv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Buffer 1 to allow initial send, then create pressure.
	updateCh := make(chan []string, 1)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Read initial.
	initial := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, initial)

	// Trigger burst.
	close(trigger)

	// Drain until we see the final value (10.0.9.1:6443).
	deadline := time.After(5 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 1 && got[0] == "10.0.9.1:6443" {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for final burst value")
		}
	}

done:
	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_SendWithDrainEmptyMergeDoesNotSendEmpty(t *testing.T) {
	// Exercise the empty-merge branch inside sendWithDrain: while the merge
	// loop is blocked trying to deliver a valid result on an unbuffered
	// updateCh, all providers clear their endpoints. The merged result
	// becomes empty and must NOT be sent. The provider must stay alive.
	log := zaptest.NewLogger(t)

	clearTrigger := make(chan struct{})
	resumeTrigger := make(chan struct{})

	// Provider sends endpoints, waits for clearTrigger → sends nil,
	// waits for resumeTrigger → sends new endpoints, then blocks.
	prov := &mockProvider{
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{"10.0.0.1:6443"}

			select {
			case <-clearTrigger:
			case <-ctx.Done():
				return nil
			}

			ch <- nil

			select {
			case <-resumeTrigger:
			case <-ctx.Done():
				return nil
			}

			ch <- []string{"10.0.0.5:6443"}

			<-ctx.Done()

			return nil
		},
	}

	mp := merged.NewMergedProvider(log, prov)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unbuffered updateCh creates backpressure, forcing sendWithDrain path.
	updateCh := make(chan []string)
	errCh := make(chan error, 1)

	go func() { errCh <- mp.Run(ctx, updateCh) }()

	// Consume initial valid result.
	initial := receiveWithTimeout(t, updateCh, time.Second)
	assert.Equal(t, []string{"10.0.0.1:6443"}, initial)

	// Trigger nil send while mergeLoop/sendWithDrain might be blocked.
	close(clearTrigger)

	// Give time for the nil update to propagate through internalCh.
	time.Sleep(100 * time.Millisecond)

	// Resume with new valid endpoints.
	close(resumeTrigger)

	// The next value on updateCh must be the new valid endpoints,
	// never an empty list.
	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			assert.NotEmpty(t, got, "empty endpoint list must never be sent to updateCh")

			if len(got) == 1 && got[0] == "10.0.0.5:6443" {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for resumed endpoints after empty merge")
		}
	}

done:
	// Verify provider is still alive (not terminated by empty merge).
	select {
	case err := <-errCh:
		t.Fatalf("Run returned unexpectedly: %v", err)
	default:
	}

	cancel()

	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func waitForCombined(t *testing.T, updateCh <-chan []string) {
	t.Helper()

	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 2 {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for combined endpoints")
		}
	}
}

func waitForCleanup(t *testing.T, updateCh <-chan []string) {
	t.Helper()

	deadline := time.After(3 * time.Second)

	for {
		select {
		case got := <-updateCh:
			if len(got) == 1 && got[0] == stableEndpoint {
				// Verify stale endpoints do not reappear.
				time.Sleep(100 * time.Millisecond)

				select {
				case recheck := <-updateCh:
					for _, ep := range recheck {
						if ep == "10.0.0.99:6443" {
							t.Fatal("stale endpoint reappeared after clearing")
						}
					}
				default:
				}

				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for stale endpoints to be cleared")
		}
	}
}
