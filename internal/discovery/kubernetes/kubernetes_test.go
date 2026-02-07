package kubernetes_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
)

const (
	testAPIPort    = "6443"
	testTimeout    = 10 * time.Second
	receiveTimeout = 5 * time.Second
)

func newTestLogger() *zap.Logger {
	return zap.NewNop()
}

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

func TestExtractEndpoints(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []discoveryv1.Endpoint
		port      string
		expected  []string
	}{
		{
			name: "single endpoint single address",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1"}},
			},
			port:     "6443",
			expected: []string{"10.0.0.1:6443"},
		},
		{
			name: "single endpoint multiple addresses",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}},
			},
			port:     "6443",
			expected: []string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
		},
		{
			name: "multiple endpoints",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1"}},
				{Addresses: []string{"10.0.0.2"}},
			},
			port:     "6443",
			expected: []string{"10.0.0.1:6443", "10.0.0.2:6443"},
		},
		{
			name:      "empty endpoints",
			endpoints: []discoveryv1.Endpoint{},
			port:      "6443",
			expected:  []string{},
		},
		{
			name: "endpoint with no addresses",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{}},
			},
			port:     "6443",
			expected: []string{},
		},
		{
			name: "deduplicate addresses",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1"}},
				{Addresses: []string{"10.0.0.1"}},
			},
			port:     "6443",
			expected: []string{"10.0.0.1:6443"},
		},
		{
			name: "custom port",
			endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1"}},
			},
			port:     "8443",
			expected: []string{"10.0.0.1:8443"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kubediscovery.ExtractEndpoints(tt.endpoints, tt.port)
			assert.Equal(t, tt.expected, result)
		})
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
