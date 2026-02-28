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

// providerChBuffer is the buffer size for each sub-provider's internal channel.
//
// Value 16 matches the upstream channel buffer in the server package: each
// stage in the pipeline (provider -> merge loop -> load balancer) should
// absorb the same burst magnitude so no intermediate stage becomes the new
// bottleneck. See server.go upstreamChBuffer for the rationale on the value.
const providerChBuffer = 16

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
// Individual provider failures are logged but do not stop the remaining providers.
// Run returns an error only if ALL providers fail.
func (mp *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	if len(mp.providers) == 0 {
		return errors.New("no providers configured")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	internalCh := make(chan providerUpdate, len(mp.providers))

	var (
		wg       sync.WaitGroup
		provWg   sync.WaitGroup
		errMu    sync.Mutex
		provErrs []error
	)

	for idx, prov := range mp.providers {
		wg.Add(1)
		provWg.Add(1)

		go mp.runProvider(ctx, idx, prov, internalCh, &wg, &provWg, &errMu, &provErrs)
	}

	// Cancel context when all providers have completed (error or graceful),
	// so mergeLoop exits and Run can return.
	go func() {
		provWg.Wait()
		cancel()
	}()

	mp.mergeLoop(ctx, internalCh, updateCh)

	wg.Wait()

	errMu.Lock()
	defer errMu.Unlock()

	if len(provErrs) > 0 && len(provErrs) == len(mp.providers) {
		return errors.Join(provErrs...)
	}

	return nil
}

func (mp *Provider) runProvider(
	ctx context.Context,
	idx int,
	prov discovery.EndpointProvider,
	internalCh chan<- providerUpdate,
	wg *sync.WaitGroup,
	provWg *sync.WaitGroup,
	errMu *sync.Mutex,
	provErrs *[]error,
) {
	defer wg.Done()
	defer provWg.Done()

	provCh := make(chan []string, providerChBuffer)

	wg.Go(func() {
		mp.forwardUpdates(ctx, idx, provCh, internalCh)
	})

	runErr := prov.Run(ctx, provCh)

	// Close provCh so forwardUpdates drains any remaining buffered data
	// and then sends a nil-update to clear the provider's cache entry.
	// The nil-update is sent by forwardUpdates (not here) to guarantee
	// correct ordering: all real data is forwarded before the cleanup.
	close(provCh)

	if runErr == nil || ctx.Err() != nil {
		return
	}

	mp.logger.Warn("provider failed, continuing with remaining providers",
		zap.Int("provider", idx), zap.Error(runErr))

	errMu.Lock()

	*provErrs = append(*provErrs, errors.Wrapf(runErr, "provider %d", idx))
	errMu.Unlock()
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
				// Provider channel closed: send nil-update to clear stale
				// endpoints. This runs after all buffered data is drained,
				// guaranteeing no stale data can arrive after the cleanup.
				select {
				case internalCh <- providerUpdate{index: idx, endpoints: nil}:
				case <-ctx.Done():
				}

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
