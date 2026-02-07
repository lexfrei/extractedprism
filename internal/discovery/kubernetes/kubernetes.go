// Package kubernetes provides a Kubernetes EndpointSlice-based discovery provider.
package kubernetes

import (
	"context"
	"net"
	"sort"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/lexfrei/extractedprism/internal/discovery"
)

const (
	endpointSliceNamespace = "default"
	kubernetesServiceLabel = "kubernetes.io/service-name=kubernetes"
)

// Compile-time interface check.
var _ discovery.EndpointProvider = (*Provider)(nil)

// Provider watches EndpointSlice resources for the kubernetes service.
type Provider struct {
	client      kubernetes.Interface
	logger      *zap.Logger
	apiPort     string
	knownSlices map[string][]discoveryv1.Endpoint
}

// NewProvider creates a Kubernetes discovery provider.
func NewProvider(client kubernetes.Interface, logger *zap.Logger, apiPort string) *Provider {
	return &Provider{
		client:      client,
		logger:      logger,
		apiPort:     apiPort,
		knownSlices: make(map[string][]discoveryv1.Endpoint),
	}
}

// Run watches kubernetes EndpointSlices and sends updates on updateCh until ctx is done.
func (p *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	endpoints, resVer, err := p.listEndpoints(ctx)
	if err != nil {
		return errors.Wrap(err, "initial endpoint slice list")
	}

	select {
	case updateCh <- endpoints:
	case <-ctx.Done():
		return nil
	}

	return p.watchLoop(ctx, updateCh, resVer)
}

func (p *Provider) listEndpoints(ctx context.Context) ([]string, string, error) {
	sliceList, err := p.client.DiscoveryV1().EndpointSlices(endpointSliceNamespace).List(
		ctx, metav1.ListOptions{LabelSelector: kubernetesServiceLabel},
	)
	if err != nil {
		return nil, "", errors.Wrap(err, "list endpoint slices")
	}

	for idx := range sliceList.Items {
		slice := &sliceList.Items[idx]
		p.knownSlices[slice.Name] = slice.Endpoints
	}

	return p.endpointsFromCache(), sliceList.ResourceVersion, nil
}

func (p *Provider) watchLoop(
	ctx context.Context,
	updateCh chan<- []string,
	resourceVersion string,
) error {
	resVer := resourceVersion

	for ctx.Err() == nil {
		watchErr := p.watchOnce(ctx, updateCh, &resVer)
		if watchErr != nil && ctx.Err() == nil {
			p.logger.Warn("watch error, restarting", zap.Error(watchErr))
		}
	}

	return nil
}

func (p *Provider) watchOnce(
	ctx context.Context,
	updateCh chan<- []string,
	resVer *string,
) error {
	watcher, err := p.client.DiscoveryV1().EndpointSlices(endpointSliceNamespace).Watch(
		ctx, metav1.ListOptions{
			LabelSelector:   kubernetesServiceLabel,
			ResourceVersion: *resVer,
		},
	)
	if err != nil {
		return errors.Wrap(err, "watch endpoint slices")
	}

	defer watcher.Stop()

	return p.handleEvents(ctx, watcher, updateCh, resVer)
}

func (p *Provider) handleEvents(
	ctx context.Context,
	watcher watch.Interface,
	updateCh chan<- []string,
	resVer *string,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return errors.New("watch channel closed")
			}

			processErr := p.processEvent(ctx, event, updateCh, resVer)
			if processErr != nil {
				return processErr
			}
		}
	}
}

func (p *Provider) processEvent(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
	resVer *string,
) error {
	switch event.Type {
	case watch.Added, watch.Modified:
		return p.handleSliceUpdate(ctx, event, updateCh, resVer)
	case watch.Deleted:
		return p.handleSliceDelete(ctx, event, updateCh)
	case watch.Error:
		return errors.New("watch error event received")
	case watch.Bookmark:
		p.updateResourceVersion(event, resVer)
	}

	return nil
}

func (p *Provider) handleSliceUpdate(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
	resVer *string,
) error {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok {
		return errors.New("unexpected object type in watch event")
	}

	*resVer = slice.ResourceVersion
	p.knownSlices[slice.Name] = slice.Endpoints

	extracted := p.endpointsFromCache()

	select {
	case updateCh <- extracted:
	case <-ctx.Done():
		return nil
	}

	p.logger.Info("endpoints updated", zap.Strings("endpoints", extracted))

	return nil
}

func (p *Provider) handleSliceDelete(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
) error {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if ok {
		delete(p.knownSlices, slice.Name)
	}

	p.logger.Warn("kubernetes endpoint slice deleted")

	extracted := p.endpointsFromCache()

	select {
	case updateCh <- extracted:
	case <-ctx.Done():
		return nil
	}

	return nil
}

func (p *Provider) updateResourceVersion(event watch.Event, resVer *string) {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok {
		return
	}

	*resVer = slice.ResourceVersion
}

func (p *Provider) endpointsFromCache() []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)

	for _, endpoints := range p.knownSlices {
		for _, endpoint := range endpoints {
			for _, addr := range endpoint.Addresses {
				hostPort := net.JoinHostPort(addr, p.apiPort)
				if _, exists := seen[hostPort]; exists {
					continue
				}

				seen[hostPort] = struct{}{}

				result = append(result, hostPort)
			}
		}
	}

	sort.Strings(result)

	return result
}

// ExtractEndpoints extracts deduplicated host:port strings from endpoint slice endpoints.
func ExtractEndpoints(endpoints []discoveryv1.Endpoint, apiPort string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(endpoints))

	for _, ep := range endpoints {
		for _, addr := range ep.Addresses {
			endpoint := net.JoinHostPort(addr, apiPort)
			if _, exists := seen[endpoint]; exists {
				continue
			}

			seen[endpoint] = struct{}{}

			result = append(result, endpoint)
		}
	}

	sort.Strings(result)

	return result
}
