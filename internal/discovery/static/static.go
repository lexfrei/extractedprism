// Package static provides a static endpoint discovery provider.
package static

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"

	"github.com/lexfrei/extractedprism/internal/discovery"
)

// Compile-time interface check.
var _ discovery.EndpointProvider = (*Provider)(nil)

// Sentinel errors for static provider validation.
var (
	ErrNoEndpoints     = errors.New("no endpoints provided")
	ErrInvalidEndpoint = errors.New("invalid endpoint")
)

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
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil || host == "" || port == "" {
			return nil, errors.Wrap(ErrInvalidEndpoint, endpoint)
		}
	}

	dst := make([]string, len(endpoints))
	copy(dst, endpoints)

	return &Provider{endpoints: dst}, nil
}

// Run sends the static endpoints on updateCh and blocks until ctx is cancelled.
func (stp *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	select {
	case updateCh <- stp.endpoints:
	case <-ctx.Done():
		return nil
	}

	<-ctx.Done()

	return nil
}
