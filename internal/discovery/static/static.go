// Package static provides a static endpoint discovery provider.
package static

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/discovery"
)

// Compile-time interface check.
var _ discovery.EndpointProvider = (*Provider)(nil)

// ErrNoEndpoints is returned when no endpoints are provided to the static provider.
var ErrNoEndpoints = errors.New("no endpoints provided")

// Provider serves a fixed list of endpoints.
type Provider struct {
	endpoints []string
}

// NewStaticProvider creates a provider from a fixed endpoint list.
func NewStaticProvider(endpoints []string) (*Provider, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}

	for _, endpoint := range endpoints {
		err := config.ValidateEndpoint(endpoint)
		if err != nil {
			return nil, errors.Wrap(err, "static provider")
		}
	}

	dst := make([]string, len(endpoints))
	copy(dst, endpoints)

	return &Provider{endpoints: dst}, nil
}

// Run sends the static endpoints on updateCh and blocks until ctx is cancelled.
func (stp *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	eps := make([]string, len(stp.endpoints))
	copy(eps, stp.endpoints)

	select {
	case updateCh <- eps:
	case <-ctx.Done():
		return nil
	}

	<-ctx.Done()

	return nil
}
