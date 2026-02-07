// Package merged combines multiple EndpointProviders into one.
package merged

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/lexfrei/extractedprism/internal/discovery"
)

// Provider merges endpoints from multiple discovery providers.
type Provider struct {
	logger    *zap.Logger
	providers []discovery.EndpointProvider
}

// NewMergedProvider creates a provider that merges results from all given providers.
func NewMergedProvider(
	logger *zap.Logger,
	providers ...discovery.EndpointProvider,
) *Provider {
	return &Provider{
		logger:    logger,
		providers: providers,
	}
}

// Run launches all sub-providers and merges their endpoints.
func (mp *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	internalCh := make(chan providerUpdate, len(mp.providers))

	var wg sync.WaitGroup

	errCh := make(chan error, len(mp.providers))

	for idx, prov := range mp.providers {
		wg.Add(1)

		go func(idx int, prov discovery.EndpointProvider) {
			defer wg.Done()

			provCh := make(chan []string, 1)

			go mp.forwardUpdates(ctx, idx, provCh, internalCh)

			err := prov.Run(ctx, provCh)
			if err != nil {
				errCh <- errors.Wrapf(err, "provider %d", idx)

				cancel()
			}
		}(idx, prov)
	}

	mp.mergeLoop(ctx, internalCh, updateCh)

	cancel()
	wg.Wait()

	select {
	case provErr := <-errCh:
		return provErr
	default:
		return nil
	}
}

type providerUpdate struct {
	index     int
	endpoints []string
}

func (mp *Provider) forwardUpdates(
	ctx context.Context,
	idx int,
	provCh <-chan []string,
	internalCh chan<- providerUpdate,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case eps, ok := <-provCh:
			if !ok {
				return
			}

			select {
			case internalCh <- providerUpdate{index: idx, endpoints: eps}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (mp *Provider) mergeLoop(
	ctx context.Context,
	internalCh <-chan providerUpdate,
	updateCh chan<- []string,
) {
	latest := make(map[int][]string, len(mp.providers))

	for {
		select {
		case <-ctx.Done():
			return
		case upd := <-internalCh:
			latest[upd.index] = upd.endpoints
			merged := mergeAndDedup(latest)

			if len(merged) == 0 {
				mp.logger.Error("merged endpoint list is empty, skipping send")

				continue
			}

			select {
			case updateCh <- merged:
			case <-ctx.Done():
				return
			}
		}
	}
}

func mergeAndDedup(latest map[int][]string) []string {
	seen := make(map[string]struct{})

	for _, eps := range latest {
		for _, ep := range eps {
			seen[ep] = struct{}{}
		}
	}

	result := make([]string, 0, len(seen))
	for ep := range seen {
		result = append(result, ep)
	}

	sort.Strings(result)

	return result
}
