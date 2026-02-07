package kubernetes_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
)

const (
	testAPIPort    = "6443"
	testTimeout    = 5 * time.Second
	receiveTimeout = 2 * time.Second
)

func newTestLogger() *zap.Logger {
	return zap.NewNop()
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

	updateCh := make(chan []string, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_EndpointUpdate(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1")
	client := fake.NewClientset(initial)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Receive initial endpoints.
	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	// Update the endpoint slice resource.
	updated := makeEndpointSlice("10.0.0.1", "10.0.0.2", "10.0.0.3")

	_, err := client.DiscoveryV1().EndpointSlices("default").Update(ctx, updated, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Receive updated endpoints.
	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t,
			[]string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
			endpoints,
		)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for updated endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_EndpointRemoval(t *testing.T) {
	initial := makeEndpointSlice("10.0.0.1", "10.0.0.2")
	client := fake.NewClientset(initial)
	provider := kubediscovery.NewProvider(client, newTestLogger(), testAPIPort)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	updateCh := make(chan []string, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- provider.Run(ctx, updateCh)
	}()

	// Receive initial endpoints.
	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t, []string{"10.0.0.1:6443", "10.0.0.2:6443"}, endpoints)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	// Update to single endpoint (removal of 10.0.0.2).
	updated := makeEndpointSlice("10.0.0.1")

	_, err := client.DiscoveryV1().EndpointSlices("default").Update(ctx, updated, metav1.UpdateOptions{})
	require.NoError(t, err)

	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t, []string{"10.0.0.1:6443"}, endpoints)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for updated endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
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

	// Should receive empty list.
	select {
	case endpoints := <-updateCh:
		assert.Empty(t, endpoints)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
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

	select {
	case endpoints := <-updateCh:
		assert.ElementsMatch(t,
			[]string{"10.0.0.1:6443", "10.0.0.2:6443", "10.0.0.3:6443"},
			endpoints,
		)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for Run to return")
	}
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

	// Drain initial endpoints.
	select {
	case <-updateCh:
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for initial endpoints")
	}

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for clean cancellation")
	}
}
