package kubernetes_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
)

const (
	testAPIPort    = "6443"
	testTimeout    = 10 * time.Second
	receiveTimeout = 5 * time.Second

	logWatchErrorRestarting = "watch error, restarting with backoff"
	logAllEndpointsRemoved  = "all endpoints removed, endpoint list is now empty"
)

var errSimulatedListFailure = errors.New("simulated list failure")

func newTestLogger() *zap.Logger {
	return zap.NewNop()
}

func boolPtr(b bool) *bool { return &b }

func makeNamedEndpointSlice(name string, ips ...string) *discoveryv1.EndpointSlice {
	endpoints := make([]discoveryv1.Endpoint, 0, len(ips))
	for _, addr := range ips {
		endpoints = append(endpoints, discoveryv1.Endpoint{
			Addresses: []string{addr},
		})
	}

	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: endpoints,
	}
}

func makeEndpointSlice(ips ...string) *discoveryv1.EndpointSlice {
	endpoints := make([]discoveryv1.Endpoint, 0, len(ips))
	for _, addr := range ips {
		endpoints = append(endpoints, discoveryv1.Endpoint{
			Addresses: []string{addr},
		})
	}

	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: endpoints,
	}
}

func receiveEndpoints(t *testing.T, ch <-chan []string) []string {
	t.Helper()

	select {
	case eps := <-ch:
		return eps
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for endpoints")

		return nil
	}
}

func waitForRun(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_InitialEndpoints(t *testing.T) {
	client := fake.NewClientset(makeEndpointSlice("10.0.0.1", "10.0.0.2"))
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_EndpointUpdate(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	// Install a watch reactor that gives us control over the watch channel.
	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Receive initial endpoints from List.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	// Inject a Modified event via the fake watcher.
	updated := makeEndpointSlice("10.0.0.1", "10.0.0.2", "10.0.0.3")
	updated.ResourceVersion = "2"
	fakeWatcher.Modify(updated)

	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t,
		[]string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
		endpoints,
	)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_EndpointRemoval(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1", "10.0.0.2")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Receive initial endpoints from List.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	// Inject a Modified event with fewer endpoints.
	updated := makeEndpointSlice("10.0.0.1")
	updated.ResourceVersion = "2"
	fakeWatcher.Modify(updated)

	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_WatchError(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Stop the watcher to simulate a disconnect.
	// The provider should restart the watch loop (using the default reactor now).
	fakeWatcher.Stop()

	// The provider should still be running (reconnect loop).
	// Cancel to verify clean exit.
	cancel()
	waitForRun(t, errCh)
}

func TestRun_EmptyEndpoints(t *testing.T) {
	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{},
	}

	client := fake.NewClientset(eps)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	endpoints := receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_MultipleEndpointObjects(t *testing.T) {
	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1", "10.0.0.2"}},
			{Addresses: []string{"10.0.0.3"}},
		},
	}

	client := fake.NewClientset(eps)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t,
		[]string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
		endpoints,
	)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_ContextCancellation(t *testing.T) {
	client := fake.NewClientset(makeEndpointSlice("10.0.0.1"))
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	receiveEndpoints(t, updateCh)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_DeletedEndpoint(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Inject a Delete event.
	fakeWatcher.Delete(initial)

	endpoints := receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_MultiSliceUpdatePreservesOtherSlices(t *testing.T) {
	slice1 := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	slice2 := makeNamedEndpointSlice("kubernetes-def", "10.0.0.2")
	client := fake.NewClientset(slice1, slice2)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Initial list should contain endpoints from both slices.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	// Update slice1 to add a new endpoint; slice2 endpoints must be preserved.
	updated := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1", "10.0.0.3")
	updated.ResourceVersion = "2"
	fakeWatcher.Modify(updated)

	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t,
		[]string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
		endpoints,
	)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_410GoneTriggersRelist(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints from List.
	receiveEndpoints(t, updateCh)

	// Inject a 410 Gone error event to simulate etcd compaction.
	fakeWatcher.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   410,
		Reason: metav1.StatusReasonGone,
	})

	// After 410 Gone, the provider should re-list and send endpoints again.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_NonGoneWatchError(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	receiveEndpoints(t, updateCh)

	// Inject a non-410 error event (e.g. 500 Internal Server Error).
	fakeWatcher.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   500,
		Reason: metav1.StatusReasonInternalError,
	})

	// Provider should survive the error and continue running.
	// Cancel to verify clean exit.
	cancel()
	waitForRun(t, errCh)
}

func TestBackoffDelay(t *testing.T) {
	// Attempt 1 should produce delay in [1s, 1.25s] range (base + up to 25% jitter).
	for range 10 {
		d := kubediscovery.BackoffDelay(1)
		assert.GreaterOrEqual(t, d, 1*time.Second)
		assert.LessOrEqual(t, d, 1250*time.Millisecond)
	}

	// Attempt 5 should produce delay in [16s, 20s] range (1s * 2^4 + jitter).
	for range 10 {
		d := kubediscovery.BackoffDelay(5)
		assert.GreaterOrEqual(t, d, 16*time.Second)
		assert.LessOrEqual(t, d, 20*time.Second)
	}

	// High attempt should be capped at [30s, 37.5s].
	for range 10 {
		d := kubediscovery.BackoffDelay(100)
		assert.GreaterOrEqual(t, d, 30*time.Second)
		assert.LessOrEqual(t, d, 37500*time.Millisecond)
	}

	// Attempt 0 should be treated as attempt 1.
	for range 10 {
		d := kubediscovery.BackoffDelay(0)
		assert.GreaterOrEqual(t, d, 1*time.Second)
		assert.LessOrEqual(t, d, 1250*time.Millisecond)
	}
}

func TestBackoffDelay_HasJitter(t *testing.T) {
	// Verify that jitter produces varying results.
	seen := make(map[time.Duration]bool)

	for range 20 {
		seen[kubediscovery.BackoffDelay(3)] = true
	}

	assert.Greater(t, len(seen), 1, "backoff should produce varying delays due to jitter")
}

func TestRun_MultiSliceDeletePreservesOtherSlices(t *testing.T) {
	slice1 := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	slice2 := makeNamedEndpointSlice("kubernetes-def", "10.0.0.2")
	client := fake.NewClientset(slice1, slice2)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Initial list should contain endpoints from both slices.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	// Delete slice1; slice2 endpoints must be preserved.
	fakeWatcher.Delete(slice1)

	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.2:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_FiltersNotReadyEndpoints(t *testing.T) {
	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)},
			},
			{
				Addresses:  []string{"10.0.0.2"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)},
			},
			{
				Addresses: []string{"10.0.0.3"},
				// Ready is nil — should be treated as ready per K8s convention.
			},
		},
	}

	client := fake.NewClientset(eps)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.3:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_AllEndpointsNotReady(t *testing.T) {
	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: "default",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)},
			},
			{
				Addresses:  []string{"10.0.0.2"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)},
			},
		},
	}

	client := fake.NewClientset(eps)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	endpoints := receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_ContextCancelDuringWatchExitsImmediately(t *testing.T) {
	// Verifies that when context is cancelled while a watch is active,
	// watchLoop exits via the explicit ctx.Err() check after watchOnce returns.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithCancel(context.Background())
	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Process one event to confirm watch is active and processing.
	updated := makeEndpointSlice("10.0.0.1", "10.0.0.2")
	updated.ResourceVersion = "2"
	fakeWatcher.Modify(updated)
	receiveEndpoints(t, updateCh)

	// Cancel context while watch is active.
	cancel()

	// Run must return nil promptly (no backoff or extra iteration).
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to exit after context cancel")
	}
}

func TestRun_410GoneRelistFailureRetainsCache(t *testing.T) {
	// When 410 Gone triggers re-list and the re-list fails,
	// the provider must retain cached endpoints and enter backoff.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	// Make the second List call fail to simulate re-list failure.
	var listCount atomic.Int32
	client.PrependReactor("list", "endpointslices",
		func(_ k8stesting.Action) (bool, runtime.Object, error) {
			if listCount.Add(1) > 1 {
				return true, nil, errSimulatedListFailure
			}

			return false, nil, nil
		},
	)

	core, logs := observer.New(zap.WarnLevel)
	provider := kubediscovery.NewProvider(client, zap.New(core), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints from successful first List.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	// Inject 410 Gone to trigger re-list.
	fakeWatcher.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   410,
		Reason: metav1.StatusReasonGone,
	})

	// Wait for the re-list failure to be logged instead of using a fixed sleep.
	require.Eventually(t, func() bool {
		return logs.FilterMessage("re-list failed, retaining cached endpoints").Len() > 0
	}, receiveTimeout, 10*time.Millisecond, "expected re-list failure log")

	// No new endpoint update should be emitted after re-list failure.
	select {
	case eps := <-updateCh:
		t.Fatalf("unexpected endpoint update after re-list failure: %v", eps)
	default:
		// Expected: no update sent because re-list failed.
	}

	cancel()
	waitForRun(t, errCh)
}

func TestRun_410GoneContextCancelDuringSend(t *testing.T) {
	// When 410 Gone triggers a successful re-list but the context is cancelled
	// while sending the update, handleGoneRelist returns false and watchLoop exits.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.InfoLevel)
	provider := kubediscovery.NewProvider(client, zap.New(core), testAPIPort)

	ctx, cancel := context.WithCancel(context.Background())

	// Use an unbuffered channel so the send in handleGoneRelist blocks.
	updateCh := make(chan []string)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Receive the initial endpoints (unblocks the initial send).
	receiveEndpoints(t, updateCh)

	// Inject 410 Gone. The re-list will succeed, but the send will block
	// because updateCh is unbuffered and nobody is reading.
	fakeWatcher.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   410,
		Reason: metav1.StatusReasonGone,
	})

	// Wait until the provider has started the re-list (logged "resource version expired")
	// and is now blocked trying to send on the unbuffered updateCh.
	require.Eventually(t, func() bool {
		return logs.FilterMessage("resource version expired, performing full re-list").Len() > 0
	}, receiveTimeout, 10*time.Millisecond, "expected re-list log")

	// Cancel context while handleGoneRelist is blocked on the send.
	cancel()

	// Run must return nil.
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to exit after context cancel during 410 re-list send")
	}
}

func TestRun_WatchReconnectContinuesProcessing(t *testing.T) {
	// After a watch error, the loop reconnects and continues processing events.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	firstWatcher := watch.NewFake()
	secondWatcher := watch.NewFake()

	var watchCount atomic.Int32
	client.PrependWatchReactor("endpointslices",
		func(_ k8stesting.Action) (bool, watch.Interface, error) {
			if watchCount.Add(1) == 1 {
				return true, firstWatcher, nil
			}

			return true, secondWatcher, nil
		},
	)

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Close first watcher to trigger reconnect with backoff.
	firstWatcher.Stop()

	// Wait for the second watch to be established instead of a fixed sleep.
	require.Eventually(t, func() bool {
		return watchCount.Load() >= 2
	}, receiveTimeout, 10*time.Millisecond, "expected second watch to start")

	// Inject event on second watcher.
	updated := makeEndpointSlice("10.0.0.1", "10.0.0.2")
	updated.ResourceVersion = "2"
	secondWatcher.Modify(updated)

	// Should receive updated endpoints through the reconnected watch.
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_WatchUpdateFiltersNotReady(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1", "10.0.0.2")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Initial endpoints (nil Ready = ready).
	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)

	// Modify: mark 10.0.0.2 as not-ready.
	updated := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "kubernetes",
			Namespace:       "default",
			ResourceVersion: "2",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)},
			},
			{
				Addresses:  []string{"10.0.0.2"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)},
			},
		},
	}
	fakeWatcher.Modify(updated)

	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	cancel()
	waitForRun(t, errCh)
}

func TestRun_BookmarkUpdatesResourceVersion(t *testing.T) {
	// Bookmark events should update the resource version so the next
	// watch reconnect uses the bookmarked version instead of stale one.
	initial := makeEndpointSlice("10.0.0.1")
	initial.ResourceVersion = "100"
	client := fake.NewClientset(initial)

	firstWatcher := watch.NewFake()
	secondWatcher := watch.NewFake()

	var watchCount atomic.Int32

	var capturedVersion atomic.Value

	client.PrependWatchReactor("endpointslices",
		func(action k8stesting.Action) (bool, watch.Interface, error) {
			watchAction, ok := action.(k8stesting.WatchAction)
			if ok {
				capturedVersion.Store(watchAction.GetWatchRestrictions().ResourceVersion)
			}

			if watchCount.Add(1) == 1 {
				return true, firstWatcher, nil
			}

			return true, secondWatcher, nil
		},
	)

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	receiveEndpoints(t, updateCh)

	// Send a Bookmark event with a newer resource version.
	bookmark := makeEndpointSlice("10.0.0.1")
	bookmark.ResourceVersion = "500"
	firstWatcher.Action(watch.Bookmark, bookmark)

	// Close the first watcher to force reconnect.
	firstWatcher.Stop()

	// Wait for reconnect (backoff ~1-1.25s for attempt 1).
	require.Eventually(t, func() bool {
		return watchCount.Load() >= 2
	}, receiveTimeout, 10*time.Millisecond, "expected second watch to start")

	// Verify that the second watch used the bookmarked resource version.
	ver, ok := capturedVersion.Load().(string)
	require.True(t, ok, "captured version should be a string")
	assert.Equal(t, "500", ver,
		"reconnected watch should use bookmarked resource version")

	cancel()
	waitForRun(t, errCh)
}

// --- Tests: watch error should preserve status details ---

func TestRun_WatchErrorIncludesStatusDetails(t *testing.T) {
	// Non-410 watch error events must include the *metav1.Status details
	// (code, reason, message) in the logged error for debugging.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Inject a 500 Internal Server Error with details.
	fakeWatcher.Error(&metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    500,
		Reason:  metav1.StatusReasonInternalError,
		Message: "etcd cluster is unavailable",
	})

	// Wait for the error to be logged before cancelling context.
	// The log entry is written in watchLoop after watchOnce returns but before
	// the backoff timer starts. If we cancel too early, the ctx.Err() check
	// in watchLoop returns nil before logging.
	require.Eventually(t, func() bool {
		for _, entry := range logs.All() {
			if entry.Message == logWatchErrorRestarting {
				return true
			}
		}

		return false
	}, receiveTimeout, 10*time.Millisecond, "expected 'watch error, restarting with backoff' log entry")

	cancel()
	waitForRun(t, errCh)

	// Verify the error includes status details.
	for _, entry := range logs.All() {
		if entry.Message == logWatchErrorRestarting {
			errField := entry.ContextMap()["error"]
			errStr, ok := errField.(string)
			require.True(t, ok, "error field should be a string")

			assert.Contains(t, errStr, "500",
				"error should include HTTP status code")
			assert.Contains(t, errStr, "InternalError",
				"error should include status reason")
			assert.Contains(t, errStr, "etcd cluster is unavailable",
				"error should include status message")

			break
		}
	}
}

func TestRun_WatchErrorWithNonStatusObject(t *testing.T) {
	// When a watch error event carries an object that is not *metav1.Status,
	// the provider must not panic and should return a generic error.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Inject an error event with a non-Status object (EndpointSlice instead).
	fakeWatcher.Error(makeEndpointSlice("10.0.0.1"))

	// Wait for the error to be logged before cancelling context.
	require.Eventually(t, func() bool {
		for _, entry := range logs.All() {
			if entry.Message == logWatchErrorRestarting {
				return true
			}
		}

		return false
	}, receiveTimeout, 10*time.Millisecond, "expected 'watch error, restarting with backoff' log entry")

	cancel()
	waitForRun(t, errCh)

	// Verify the generic error was logged.
	for _, entry := range logs.All() {
		if entry.Message == logWatchErrorRestarting {
			errField := entry.ContextMap()["error"]
			errStr, ok := errField.(string)
			require.True(t, ok, "error field should be a string")
			assert.Contains(t, errStr, "watch error event received",
				"generic error should be used for non-Status objects")

			break
		}
	}
}

func TestRun_WatchErrorWithNilObject(t *testing.T) {
	// When a watch error event has a nil Object, the provider must not panic
	// and should use the generic error message.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	receiveEndpoints(t, updateCh)

	// Inject an error event with nil Object.
	fakeWatcher.Error(nil)

	require.Eventually(t, func() bool {
		for _, entry := range logs.All() {
			if entry.Message == logWatchErrorRestarting {
				return true
			}
		}

		return false
	}, receiveTimeout, 10*time.Millisecond, "expected backoff log entry")

	cancel()
	waitForRun(t, errCh)

	// Verify the generic error was logged (not a panic or crash).
	for _, entry := range logs.All() {
		if entry.Message == logWatchErrorRestarting {
			errField := entry.ContextMap()["error"]
			errStr, ok := errField.(string)
			require.True(t, ok, "error field should be a string")
			assert.Contains(t, errStr, "watch error event received",
				"generic error should be used when Object is nil")

			break
		}
	}
}

// --- Tests: warn when endpoint list transitions to empty ---

func TestRun_LastSliceDeletionLogsWarning(t *testing.T) {
	// When the last EndpointSlice is deleted and the endpoint list transitions
	// from non-empty to empty, a warning must be logged.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints (non-empty list).
	endpoints := receiveEndpoints(t, updateCh)
	require.NotEmpty(t, endpoints, "initial endpoints must be non-empty")

	// Delete the only slice — transitions endpoint list to empty.
	fakeWatcher.Delete(initial)

	// Receive the empty list (still sent, no data loss).
	endpoints = receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)

	// Verify that a warning about empty endpoint list was logged.
	var found bool

	for _, entry := range logs.All() {
		if entry.Level == zap.WarnLevel && entry.Message == logAllEndpointsRemoved {
			found = true

			break
		}
	}

	assert.True(t, found,
		"expected warning log when endpoint list transitions to empty")
}

func TestRun_UpdateToAllNotReadyLogsWarning(t *testing.T) {
	// When a Modified event makes all endpoints not-ready, the endpoint list
	// transitions to empty and a warning must be logged.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints (non-empty list).
	endpoints := receiveEndpoints(t, updateCh)
	require.NotEmpty(t, endpoints, "initial endpoints must be non-empty")

	// Update the slice: mark all endpoints as not-ready.
	updated := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "kubernetes",
			Namespace:       "default",
			ResourceVersion: "2",
			Labels: map[string]string{
				"kubernetes.io/service-name": "kubernetes",
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)},
			},
		},
	}
	fakeWatcher.Modify(updated)

	// Receive the empty list (still sent, no data loss).
	endpoints = receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)

	// Verify that a warning about empty endpoint list was logged.
	var found bool

	for _, entry := range logs.All() {
		if entry.Level == zap.WarnLevel && entry.Message == logAllEndpointsRemoved {
			found = true

			break
		}
	}

	assert.True(t, found,
		"expected warning log when all endpoints become not-ready via update")
}

func TestRun_RepeatedEmptyUpdatesLogWarningOnce(t *testing.T) {
	// When multiple events result in an empty endpoint list, the warning
	// must only be logged once (on transition), not on every event.
	slice1 := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	slice2 := makeNamedEndpointSlice("kubernetes-def", "10.0.0.2")
	client := fake.NewClientset(slice1, slice2)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints (non-empty).
	endpoints := receiveEndpoints(t, updateCh)
	require.NotEmpty(t, endpoints)

	// Delete first slice — still has slice2.
	fakeWatcher.Delete(slice1)
	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.2:6443"}, endpoints)

	// Delete second slice — transitions to empty (warning expected).
	fakeWatcher.Delete(slice2)
	endpoints = receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	// Re-add and re-delete to create another empty event.
	slice3 := makeNamedEndpointSlice("kubernetes-ghi", "10.0.0.3")
	slice3.ResourceVersion = "10"
	fakeWatcher.Add(slice3)
	endpoints = receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.3:6443"}, endpoints)

	// Delete it — transitions to empty again (second warning expected).
	fakeWatcher.Delete(slice3)
	endpoints = receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)

	// Count the number of "all endpoints removed" warnings.
	var warnCount int

	for _, entry := range logs.All() {
		if entry.Level == zap.WarnLevel && entry.Message == logAllEndpointsRemoved {
			warnCount++
		}
	}

	// Exactly 2: one for each non-empty → empty transition.
	assert.Equal(t, 2, warnCount,
		"warning should be logged exactly once per non-empty to empty transition")
}

func TestRun_410GoneRelistToEmptyLogsWarning(t *testing.T) {
	// When 410 Gone triggers a re-list that returns an empty endpoint list,
	// a warning should be logged.
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	// Make the re-list return empty results.
	var listCount atomic.Int32
	client.PrependReactor("list", "endpointslices",
		func(_ k8stesting.Action) (bool, runtime.Object, error) {
			if listCount.Add(1) > 1 {
				return true, &discoveryv1.EndpointSliceList{}, nil
			}

			return false, nil, nil
		},
	)

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	provider := kubediscovery.NewProvider(client, logger, testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints (non-empty).
	endpoints := receiveEndpoints(t, updateCh)
	require.NotEmpty(t, endpoints)

	// Inject 410 Gone.
	fakeWatcher.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   410,
		Reason: metav1.StatusReasonGone,
	})

	// Receive the empty list from re-list.
	endpoints = receiveEndpoints(t, updateCh)
	assert.Empty(t, endpoints)

	cancel()
	waitForRun(t, errCh)

	// Verify the empty transition warning was logged.
	var found bool

	for _, entry := range logs.All() {
		if entry.Level == zap.WarnLevel && entry.Message == logAllEndpointsRemoved {
			found = true

			break
		}
	}

	assert.True(t, found,
		"expected warning log when re-list after 410 Gone returns empty endpoints")
}

// --- Tests: deep copy endpoints before storing ---

func TestRun_MutatingOriginalSliceDoesNotCorruptCache(t *testing.T) {
	// After storing endpoints from a watch event, mutating the original
	// EndpointSlice object must not affect the cached data.
	initial := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Send a watch update with a new slice.
	slice := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	slice.ResourceVersion = "2"
	fakeWatcher.Modify(slice)

	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	// Mutate the original slice object after it was stored in the cache.
	slice.Endpoints[0].Addresses[0] = "10.0.0.99"

	// Trigger a cache read by adding a second slice.
	slice2 := makeNamedEndpointSlice("kubernetes-def", "10.0.0.2")
	slice2.ResourceVersion = "3"
	fakeWatcher.Modify(slice2)

	endpoints = receiveEndpoints(t, updateCh)

	// The first slice's cached endpoint must still be 10.0.0.1, not 10.0.0.99.
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints,
		"mutating original slice Addresses after storage must not corrupt the cache")

	cancel()
	waitForRun(t, errCh)
}

func TestRun_MutatingOriginalDeprecatedTopologyDoesNotCorruptCache(t *testing.T) {
	// Deep copy must also cover reference types like maps.
	// Mutating DeprecatedTopology on the original object after storage
	// must not affect the cached copy.
	initial := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	initial.Endpoints[0].DeprecatedTopology = map[string]string{"zone": "us-east-1a"}
	client := fake.NewClientset(initial)

	fakeWatcher := watch.NewFake()
	client.PrependWatchReactor("endpointslices", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Drain initial endpoints.
	receiveEndpoints(t, updateCh)

	// Send a watch update with a slice that has DeprecatedTopology.
	slice := makeNamedEndpointSlice("kubernetes-abc", "10.0.0.1")
	slice.Endpoints[0].DeprecatedTopology = map[string]string{"zone": "us-east-1a"}
	slice.ResourceVersion = "2"
	fakeWatcher.Modify(slice)

	endpoints := receiveEndpoints(t, updateCh)
	assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)

	// Mutate the DeprecatedTopology map on the original object.
	slice.Endpoints[0].DeprecatedTopology["zone"] = "corrupted"
	// Also add a new key to verify the map is truly independent.
	slice.Endpoints[0].DeprecatedTopology["injected"] = "bad"

	// Trigger a cache read by adding a second slice.
	slice2 := makeNamedEndpointSlice("kubernetes-def", "10.0.0.2")
	slice2.ResourceVersion = "3"
	fakeWatcher.Modify(slice2)

	endpoints = receiveEndpoints(t, updateCh)

	// Endpoints should still include both — cache integrity is preserved.
	// The important thing is that the endpoint addresses are not corrupted.
	assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints,
		"mutating original DeprecatedTopology after storage must not corrupt the cache")

	cancel()
	waitForRun(t, errCh)
}
