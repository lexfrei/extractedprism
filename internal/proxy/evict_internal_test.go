package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

var errProbeFailed = errors.New("readyz status 500")

func newEvictProxy(t *testing.T, drainTimeout time.Duration) *Proxy {
	t.Helper()

	prx := New(&Config{
		HealthTimeout:  time.Second,
		HealthInterval: time.Hour,
		DrainTimeout:   drainTimeout,
		Readiness:      &ReadinessCheck{Client: http.DefaultClient},
	}, zap.NewNop(), metrics.New())

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_ = prx.Shutdown(shutCtx)
	})

	return prx
}

func addBackend(prx *Proxy, addr string) *backend {
	bck := newTestBackend(addr)

	prx.mu.Lock()
	prx.backends[addr] = bck
	prx.mu.Unlock()

	return bck
}

// trackConn registers a proxied connection with the backend and returns a
// channel closed once the proxy closes the client side.
func trackConn(t *testing.T, prx *Proxy, bck *backend) <-chan struct{} {
	t.Helper()

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	require.True(t, prx.tryRegister(bck, &trackedConn{client: client, upstream: upstream}))

	closed := make(chan struct{})

	go func() {
		_, _ = io.Copy(io.Discard, clientPeer)
		close(closed)
	}()

	return closed
}

func markUnready(prx *Proxy, bck *backend) {
	for range healthFailuresToUnhealthy {
		prx.recordNotReady(bck, errProbeFailed)
	}
}

func requireClosedWithin(t *testing.T, closed <-chan struct{}, within time.Duration, msg string) {
	t.Helper()

	select {
	case <-closed:
	case <-time.After(within):
		require.Fail(t, msg)
	}
}

func requireOpenFor(t *testing.T, closed <-chan struct{}, period time.Duration, msg string) {
	t.Helper()

	select {
	case <-closed:
		require.Fail(t, msg)
	case <-time.After(period):
	}
}

func TestEviction_UnreadyWhileAnotherIsReady_ClosesConnsAfterDrainTimeout(t *testing.T) {
	const drainTimeout = 300 * time.Millisecond

	prx := newEvictProxy(t, drainTimeout)
	addBackend(prx, "192.0.2.1:6443")
	minority := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, minority)

	markUnready(prx, minority)

	requireOpenFor(t, closed, drainTimeout/3, "connections get the drain timeout to finish, as on removal")
	requireClosedWithin(t, closed, 3*time.Second,
		"a long-lived client pinned to an unready upstream must be moved to a ready one")
	assert.True(t, minority.healthy.Load(), "closing connections does not remove the upstream as a fallback")
}

func TestEviction_ProxiedConnection_EndsAndClientReconnectsToReady(t *testing.T) {
	prx := newEvictProxy(t, 0)
	prx.cfg.DialTimeout = time.Second

	minorityAddr, _ := startEchoBackend(t)
	minority := addBackend(prx, minorityAddr)
	readyAddr, readyAccepts := startEchoBackend(t)

	client, clientPeer := net.Pipe()
	t.Cleanup(func() { clientPeer.Close() })

	prx.wg.Add(1)

	go prx.handleConn(client)

	// The only upstream so far, so the long-lived connection lands on it.
	_, err := clientPeer.Write([]byte("ping"))
	require.NoError(t, err)

	buf := make([]byte, 4)
	_, err = io.ReadFull(clientPeer, buf)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return minority.connCount() == 1 }, time.Second, 5*time.Millisecond)

	ready := addBackend(prx, readyAddr)
	markUnready(prx, minority)

	_, err = io.ReadFull(clientPeer, buf)
	require.Error(t, err, "the pinned connection must end so the client reconnects")
	require.Eventually(t, func() bool { return minority.connCount() == 0 }, time.Second, 5*time.Millisecond)

	assert.Same(t, ready, prx.pickBackend(), "the reconnect goes to the ready upstream")
	assert.Zero(t, readyAccepts.Load())
}

func TestEviction_ZeroDrainTimeout_ClosesConnsAtOnce(t *testing.T) {
	prx := newEvictProxy(t, 0)
	addBackend(prx, "192.0.2.1:6443")
	minority := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, minority)

	markUnready(prx, minority)

	requireClosedWithin(t, closed, time.Second, "a zero drain timeout closes immediately")
}

func TestEviction_NoUpstreamReady_KeepsConns(t *testing.T) {
	const drainTimeout = 20 * time.Millisecond

	prx := newEvictProxy(t, drainTimeout)
	first := addBackend(prx, "192.0.2.1:6443")
	second := addBackend(prx, "192.0.2.2:6443")
	closedFirst := trackConn(t, prx, first)
	closedSecond := trackConn(t, prx, second)

	markUnready(prx, first)
	markUnready(prx, second)

	requireOpenFor(t, closedFirst, 10*drainTimeout, "with nothing ready the unready upstreams are the best path left")
	requireOpenFor(t, closedSecond, drainTimeout, "with nothing ready the unready upstreams are the best path left")
}

func TestEviction_ReadyButTCPDeadPeer_KeepsConns(t *testing.T) {
	const drainTimeout = 20 * time.Millisecond

	prx := newEvictProxy(t, drainTimeout)
	dead := addBackend(prx, "192.0.2.1:6443")
	dead.healthy.Store(false)

	minority := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, minority)

	markUnready(prx, minority)

	requireOpenFor(t, closed, 10*drainTimeout, "a ready upstream that is TCP-dead cannot take the reconnects")
}

func TestEviction_UpstreamReadyAgainBeforeTimeout_CancelsClosing(t *testing.T) {
	const drainTimeout = 200 * time.Millisecond

	prx := newEvictProxy(t, drainTimeout)
	addBackend(prx, "192.0.2.1:6443")
	flapping := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, flapping)

	markUnready(prx, flapping)
	prx.recordReady(flapping)

	requireOpenFor(t, closed, 3*drainTimeout, "an upstream ready again keeps its connections")
}

func TestEviction_LastReadyPeerLostBeforeTimeout_CancelsClosing(t *testing.T) {
	const drainTimeout = 200 * time.Millisecond

	prx := newEvictProxy(t, drainTimeout)
	peer := addBackend(prx, "192.0.2.1:6443")
	minority := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, minority)

	markUnready(prx, minority)

	for range healthFailuresToUnhealthy {
		prx.recordFailure(peer, errProbeFailed)
	}

	require.False(t, peer.healthy.Load())
	requireOpenFor(t, closed, 3*drainTimeout, "without a ready upstream left the connections stay")
}

func TestEviction_ReadyPeerAppearsLater_ClosesConnsOfUnready(t *testing.T) {
	prx := newEvictProxy(t, 0)
	peer := addBackend(prx, "192.0.2.1:6443")
	minority := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, minority)

	markUnready(prx, peer)
	markUnready(prx, minority)
	requireOpenFor(t, closed, 50*time.Millisecond, "with nothing ready the connections stay")

	prx.recordReady(peer)

	requireClosedWithin(t, closed, time.Second, "once quorum returns elsewhere the unready upstream's clients move")
}

func TestEviction_FirstProbePending_KeepsConns(t *testing.T) {
	prx := newEvictProxy(t, 0)
	addBackend(prx, "192.0.2.1:6443")
	pending := addBackend(prx, "192.0.2.2:6443")
	closed := trackConn(t, prx, pending)

	// Back from a TCP failure: unready until its probe answers.
	pending.healthy.Store(false)
	prx.recordSuccess(pending)
	require.True(t, pending.unready.Load())

	requireOpenFor(t, closed, 100*time.Millisecond, "an upstream that has not failed a probe keeps its connections")
}

func TestEviction_DrainingUpstream_LeftToItsDrain(t *testing.T) {
	prx := newEvictProxy(t, time.Hour)
	addBackend(prx, "192.0.2.1:6443")
	minority := addBackend(prx, "192.0.2.2:6443")
	trackConn(t, prx, minority)

	markUnready(prx, minority)

	prx.mu.RLock()
	scheduled := minority.evictCh != nil
	prx.mu.RUnlock()
	require.True(t, scheduled)

	prx.reconcile([]string{"192.0.2.1:6443"})

	prx.mu.RLock()
	defer prx.mu.RUnlock()

	assert.Nil(t, minority.evictCh, "a removed upstream's drain owns its connections")
}
