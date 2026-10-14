package proxy

import (
	"time"

	"go.uber.org/zap"
)

// Readiness only steers new connections. A client that keeps one long-lived
// connection, such as the kubelet with its HTTP/2 watches, never reconnects
// on its own: the connection is never idle, and an API server without etcd
// quorum still answers HTTP/2 pings. Such clients would stay pinned to the
// unready upstream for the whole outage, so its connections are closed
// after DrainTimeout, the grace period removed upstreams get, as long as
// another healthy upstream is ready to take the reconnects. With nothing
// ready the connections stay: the unready upstreams are then the best path
// left.

// updateEvictionsLocked schedules the closing of the connections of every
// unready upstream while another upstream is pickable and ready, and
// cancels the scheduled closings when that no longer holds. Callers must
// hold prx.mu.
func (prx *Proxy) updateEvictionsLocked() {
	if prx.cfg.Readiness == nil {
		return
	}

	anyReady := prx.anyReadyLocked()

	for _, bck := range prx.backends {
		evict := anyReady && evictable(bck)

		switch {
		case evict && bck.evictCh == nil && prx.ctx.Err() == nil:
			bck.evictCh = make(chan struct{})

			prx.logger.Info("closing connections of unready upstream after drain timeout",
				zap.String("upstream", bck.addr),
				zap.Int("connections", bck.connCount()),
				zap.Duration("timeout", prx.cfg.DrainTimeout))

			prx.wg.Add(1)

			go prx.evictAfterDrainTimeout(bck, bck.evictCh)
		case !evict && bck.evictCh != nil:
			close(bck.evictCh)
			bck.evictCh = nil
		}
	}
}

// anyReadyLocked reports whether a pickable upstream passes its probe.
// Callers must hold prx.mu.
func (prx *Proxy) anyReadyLocked() bool {
	for _, bck := range prx.backends {
		if bck.pickable() && !bck.unready.Load() {
			return true
		}
	}

	return false
}

// evictable reports whether the backend's connections should be closed when
// another upstream is ready: it failed a probe since it became reachable,
// and it is not draining, which closes its connections on its own.
func evictable(bck *backend) bool {
	return bck.unready.Load() && bck.probed.Load() && !bck.draining.Load()
}

// evictAfterDrainTimeout closes the backend's connections once DrainTimeout
// has passed, unless the eviction was cancelled through cancelCh. The
// condition is checked again at that point, under the lock that guards the
// scheduling, so a closing never outlives its reason.
func (prx *Proxy) evictAfterDrainTimeout(bck *backend, cancelCh <-chan struct{}) {
	defer prx.wg.Done()

	timer := time.NewTimer(prx.cfg.DrainTimeout)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-cancelCh:
		return
	case <-prx.ctx.Done():
		return
	}

	prx.mu.Lock()
	defer prx.mu.Unlock()

	// A cancelled epoch, possibly followed by a new one, must not act.
	if bck.evictCh != cancelCh || prx.backends[bck.addr] != bck {
		return
	}

	bck.evictCh = nil

	if !evictable(bck) || !prx.anyReadyLocked() {
		return
	}

	closed := bck.connCount()
	bck.closeAllConns()

	prx.logger.Warn("closed connections of unready upstream",
		zap.String("upstream", bck.addr), zap.Int("connections", closed))
}
