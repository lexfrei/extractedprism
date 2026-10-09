package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

func newTestProxy(t *testing.T) *Proxy {
	t.Helper()

	// Short health timeout keeps dials to TEST-NET addresses bounded; the
	// long interval keeps ticker-driven checks out of the tests.
	return New(&Config{HealthTimeout: 50 * time.Millisecond, HealthInterval: time.Hour}, zap.NewNop(), metrics.New())
}

func newTestBackend(addr string) *backend {
	bck := &backend{
		addr:       addr,
		stopHealth: make(chan struct{}),
		conns:      make(map[*trackedConn]struct{}),
	}
	bck.healthy.Store(true)

	return bck
}

func scrapeInternal(t *testing.T, m *metrics.Metrics) string {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestTryRegister_RefusesDrainingBackend(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")

	require.True(t, prx.tryRegister(bck, &trackedConn{}), "active backend must accept connection registration")

	bck.startDrain()

	assert.False(t, prx.tryRegister(bck, &trackedConn{}),
		"draining backend must refuse new connection registration")
}

func TestTryRegister_RefusesAfterShutdown(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")

	prx.cancel()

	assert.False(t, prx.tryRegister(bck, &trackedConn{}),
		"a connection dialed before shutdown must not register after it")
}

func TestProxy_RemoveBackend_IgnoresStaleEpoch(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	_, _, gen1 := bck.startDrain()
	bck.cancelDrain()
	bck.restartHealthLoop()

	_, _, gen2 := bck.startDrain()

	prx.removeBackend(bck, gen1)

	_, ok := prx.backends[bck.addr]
	assert.True(t, ok, "a stale drain epoch must not remove the backend")

	prx.removeBackend(bck, gen2)

	_, ok = prx.backends[bck.addr]
	assert.False(t, ok, "the current drain epoch removes the backend")
}

func TestProxy_HealthChange_SkipsRemovedBackend(t *testing.T) {
	prx := newTestProxy(t)
	m := prx.metrics

	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	prx.metrics.SetBackendHealth(bck.addr, true)

	_, _, gen := bck.startDrain()
	prx.removeBackend(bck, gen)

	// A health check that was in flight during the removal reports now:
	// it must NOT resurrect the deleted series.
	prx.recordFailure(bck, errors.New("dial refused"))
	prx.recordFailure(bck, errors.New("dial refused"))
	prx.recordSuccess(bck)

	body := scrapeInternal(t, m)
	assert.NotContains(t, body, "extractedprism_health_check_status{",
		"removed backend's series must stay deleted")
	assert.Contains(t, body, "extractedprism_upstreams_total 0")
}

func TestProxy_RecordFailure_Threshold(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	prx.metrics.SetBackendHealth(bck.addr, true)

	prx.recordFailure(bck, errors.New("one"))
	assert.True(t, bck.healthy.Load(), "single failure must be tolerated")

	prx.recordFailure(bck, errors.New("two"))
	assert.False(t, bck.healthy.Load(), "two consecutive failures exclude the backend")

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_health_check_status{upstream="192.0.2.1:6443"} 0`)

	prx.recordSuccess(bck)
	assert.True(t, bck.healthy.Load(), "one success re-includes the backend")

	body = scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_health_check_status{upstream="192.0.2.1:6443"} 1`)
}

func TestNew_NilLogger_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "proxy.New: logger must not be nil", func() {
		New(&Config{}, nil, metrics.New())
	})
}

func TestNew_NilMetrics_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "proxy.New: metrics must not be nil", func() {
		New(&Config{}, zap.NewNop(), nil)
	})
}

func TestRecordFailure_IgnoredDuringShutdown(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	prx.cancel()

	// Failures caused by shutdown say nothing about the upstream: they must
	// not count toward exclusion, whichever path reports them.
	prx.recordFailure(bck, errors.New("context canceled"))
	prx.recordFailure(bck, errors.New("context canceled"))

	assert.True(t, bck.healthy.Load(), "shutdown-caused failures must not exclude the backend")
	assert.Equal(t, int32(0), bck.failures.Load())
}

func TestCheckOnce_AbortedByShutdown_DoesNotExclude(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	// One genuine failure is already recorded: a single further failure
	// would cross the threshold.
	bck.failures.Store(1)

	prx.cancel()
	prx.checkOnce(prx.ctx, bck)

	assert.True(t, bck.healthy.Load(),
		"a health dial aborted by shutdown must not flip the backend to unhealthy")
}

func TestHealthLoop_UsesStopChannelPassedAtSpawn(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")

	// The backend field holds a fresh, open channel (as after a re-add),
	// while the loop was spawned for an epoch whose channel is already
	// closed. The loop must stop on its own epoch's channel; reading the
	// field at run time would bind it to the newer epoch and leave two
	// loops running for one backend.
	spawnedFor := make(chan struct{})
	close(spawnedFor)

	done := make(chan struct{})

	prx.wg.Add(1)

	go func() {
		prx.healthLoop(bck, spawnedFor)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("health loop must exit on the stop channel it was spawned with")
	}
}

func TestHandleConn_SuccessfulDialResetsFailureStreak(t *testing.T) {
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			go func() {
				defer conn.Close()

				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	addr := listener.Addr().String()

	prx := New(&Config{
		BindAddress:    "127.0.0.1",
		DialTimeout:    time.Second,
		HealthInterval: time.Hour,
		HealthTimeout:  time.Second,
	}, zap.NewNop(), metrics.New())

	require.NoError(t, prx.Start(t.Context(), make(chan []string)))

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = prx.Shutdown(shutCtx)
	})

	// Insert the backend directly instead of sending an update: reconcile
	// would start a health loop whose immediate check also resets the
	// streak, and the test must observe the client dial alone.
	bck := newTestBackend(addr)

	prx.mu.Lock()
	prx.backends[addr] = bck
	prx.mu.Unlock()

	// One failure recorded, then a successful proxied dial: the streak is
	// broken, so a single further failure must NOT exclude the backend.
	prx.recordFailure(bck, errors.New("earlier failure"))

	client, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	require.Eventually(t, func() bool {
		return bck.failures.Load() == 0
	}, 3*time.Second, 10*time.Millisecond, "a successful client dial must reset the failure streak")

	prx.recordFailure(bck, errors.New("isolated failure"))

	assert.True(t, bck.healthy.Load(), "failures separated by a success are not consecutive")
}

func TestRecordSuccess_IgnoredDuringShutdown(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	bck.healthy.Store(false)
	bck.failures.Store(2)

	prx.cancel()

	// A dial that completes as shutdown begins must not log a false
	// "became healthy" transition.
	prx.recordSuccess(bck)

	assert.False(t, bck.healthy.Load(), "shutdown-time success must not re-include the backend")
	assert.Equal(t, int32(2), bck.failures.Load())
}

func TestOnHealthChange_WritesCurrentStateNotArgument(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	// A racing transition can arrive late: the series still shows the
	// earlier "unhealthy" flip while the backend is healthy again. Publishing
	// must export the backend's current state.
	prx.metrics.SetBackendHealth(bck.addr, false)
	bck.healthy.Store(true)
	prx.onHealthChange(bck)

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_health_check_status{upstream="192.0.2.1:6443"} 1`)
}

func TestHandleConn_DialAbortedByShutdown_NotCountedAsConnError(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	prx := New(&Config{DialTimeout: time.Second, HealthInterval: time.Hour}, zap.New(core), metrics.New())

	bck := newTestBackend("127.0.0.1:6443")
	prx.backends[bck.addr] = bck

	prx.cancel()

	client, peer := net.Pipe()
	t.Cleanup(func() { peer.Close() })

	prx.wg.Add(1)
	prx.handleConn(client)

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, "extractedprism_connection_errors_total{",
		"a dial aborted by shutdown says nothing about the upstream")
	assert.Zero(t, logs.FilterMessage("upstream dial failed").Len(),
		"a dial aborted by shutdown must not be logged as an upstream failure")
}

func TestServeConn_RefusedByShutdown_NotCountedAsConnError(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	// The dial completed, then shutdown began before registration.
	prx.cancel()

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(bck, &trackedConn{client: client, upstream: upstream}, true)

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, "extractedprism_connection_errors_total{",
		"a connection refused at registration by shutdown is not an upstream error")
}

func TestServeConn_RefusedByDrain_RetryExhausted_CountedAsConnError(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	bck.startDrain()

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(bck, &trackedConn{client: client, upstream: upstream}, false)

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.1:6443"} 1`,
		"outside shutdown a refused registration still fails the client connection")
}

func TestRemoveBackend_DeletesConnErrorSeries(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	prx.metrics.ConnError(bck.addr)

	_, _, gen := bck.startDrain()
	prx.removeBackend(bck, gen)

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, "extractedprism_connection_errors_total{",
		"a removed upstream must not keep its error series")
}

func TestServeConn_RefusedAfterRemoval_DoesNotRecreateConnErrorSeries(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck

	// The dial finished while the backend drained and was removed.
	_, _, gen := bck.startDrain()
	prx.removeBackend(bck, gen)

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(bck, &trackedConn{client: client, upstream: upstream}, false)

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, "extractedprism_connection_errors_total{",
		"a late error for a removed upstream must not recreate its series")
}

func TestServeConn_RefusedAfterReAdd_CountsOnLiveSeries(t *testing.T) {
	prx := newTestProxy(t)
	stale := newTestBackend("192.0.2.1:6443")
	prx.backends[stale.addr] = stale

	// The dial finished on a backend that was removed and then re-added
	// under the same address as a new backend.
	_, _, gen := stale.startDrain()
	prx.removeBackend(stale, gen)
	prx.backends[stale.addr] = newTestBackend(stale.addr)

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(stale, &trackedConn{client: client, upstream: upstream}, false)

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.1:6443"} 1`,
		"the address is back in the set, so its series is live and the failure counts")
}

// startEchoBackend counts accepts so a test can prove which backends the
// proxy dialed.
func startEchoBackend(t *testing.T) (string, *atomic.Int32) {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	accepts := new(atomic.Int32)

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			accepts.Add(1)

			go func() {
				defer conn.Close()

				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	return listener.Addr().String(), accepts
}

// requireNoEarlierAccept dials the backend once more and waits for that probe
// to be accepted: accepts are served in order, so a count of exactly one
// proves no connection reached the backend before the probe.
func requireNoEarlierAccept(t *testing.T, addr string, accepts *atomic.Int32) {
	t.Helper()

	probe, err := new(net.Dialer).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { probe.Close() })

	require.Eventually(t, func() bool { return accepts.Load() >= 1 }, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(1), accepts.Load(), "the proxy must not have dialed this backend")
}

func deadAddr(t *testing.T) string {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()
	listener.Close()

	return addr
}

func newObservedProxy() (*Proxy, *observer.ObservedLogs) {
	core, logs := observer.New(zap.InfoLevel)

	return New(&Config{DialTimeout: time.Second, HealthInterval: time.Hour}, zap.New(core), metrics.New()), logs
}

func TestServeConn_RefusedByDrain_RetriesAnotherBackend(t *testing.T) {
	prx, logs := newObservedProxy()

	addrB, _ := startEchoBackend(t)
	bckB := newTestBackend(addrB)
	prx.backends[addrB] = bckB

	// A was picked and dialed, then started draining before registration.
	bckA := newTestBackend("192.0.2.1:6443")
	prx.backends[bckA.addr] = bckA
	bckA.startDrain()

	client, clientPeer := net.Pipe()
	upstreamA, upstreamAPeer := net.Pipe()

	require.NoError(t, clientPeer.SetDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, upstreamAPeer.SetDeadline(time.Now().Add(3*time.Second)))

	done := make(chan struct{})

	go func() {
		defer close(done)

		prx.serveConn(bckA, &trackedConn{client: client, upstream: upstreamA}, true)
	}()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamAPeer.Close()
		<-done
	})

	_, err := clientPeer.Write([]byte("ping"))
	require.NoError(t, err, "the client must stay open across the retry")

	reply := make([]byte, 4)
	_, err = io.ReadFull(clientPeer, reply)
	require.NoError(t, err, "the client must be served by the retried backend")
	assert.Equal(t, "ping", string(reply))

	_, err = upstreamAPeer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the connection dialed to the draining backend must be closed")

	assert.Equal(t, 1, logs.FilterMessage("upstream started draining during dial, retrying").Len())
	assert.NotContains(t, scrapeInternal(t, prx.metrics), "extractedprism_connection_errors_total{",
		"a refusal that was retried did not fail the client")
}

func TestServeConn_RefusedByDrain_NoPickableForRetry_ClosesClient(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	bck.startDrain()

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(bck, &trackedConn{client: client, upstream: upstream}, true)

	_, err := clientPeer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "with nothing left to pick, the client is closed")

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="none"} 1`)
	assert.NotContains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.1:6443"}`)
}

func TestServeConn_RefusedByShutdown_DoesNotRetry(t *testing.T) {
	prx, logs := newObservedProxy()

	addrB, acceptsB := startEchoBackend(t)
	prx.backends[addrB] = newTestBackend(addrB)

	bckA := newTestBackend("192.0.2.1:6443")
	prx.backends[bckA.addr] = bckA

	prx.cancel()

	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()

	t.Cleanup(func() {
		clientPeer.Close()
		upstreamPeer.Close()
	})

	prx.serveConn(bckA, &trackedConn{client: client, upstream: upstream}, true)

	assert.Zero(t, logs.FilterMessage("upstream started draining during dial, retrying").Len(),
		"a shutdown refusal must not retry")
	requireNoEarlierAccept(t, addrB, acceptsB)
}

func TestDialAndServe_DialFailure_DoesNotRetry(t *testing.T) {
	prx, logs := newObservedProxy()

	addrB, acceptsB := startEchoBackend(t)
	prx.backends[addrB] = newTestBackend(addrB)

	// One earlier failure: this dial failure excludes A, so a retry would
	// have only B left to pick.
	bckA := newTestBackend(deadAddr(t))
	bckA.failures.Store(1)
	prx.backends[bckA.addr] = bckA

	client, clientPeer := net.Pipe()
	t.Cleanup(func() { clientPeer.Close() })

	done := make(chan struct{})

	go func() {
		defer close(done)

		prx.dialAndServe(client, bckA, true)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a failed dial must close the client instead of serving it elsewhere")
	}

	requireNoEarlierAccept(t, addrB, acceptsB)

	assert.False(t, bckA.healthy.Load(), "the dial failure must count toward exclusion")
	assert.Contains(t, scrapeInternal(t, prx.metrics),
		`extractedprism_connection_errors_total{upstream="`+bckA.addr+`"} 1`)
	assert.Equal(t, 1, logs.FilterMessage("upstream dial failed").Len())
	assert.Zero(t, logs.FilterMessage("upstream started draining during dial, retrying").Len())
}

func TestHandleConn_BothPicksDrainMidDial_RetriesOnceThenFails(t *testing.T) {
	prx, logs := newObservedProxy()

	addrA, acceptsA := startEchoBackend(t)
	addrB, acceptsB := startEchoBackend(t)
	bckA := newTestBackend(addrA)
	bckB := newTestBackend(addrB)
	bckB.healthy.Store(false) // the first pick must be A
	prx.backends[addrA] = bckA
	prx.backends[addrB] = bckB

	// Holding a backend's mutex parks registration right after its dial,
	// which opens the mid-dial drain window without timing.
	bckA.mu.Lock()
	bckB.mu.Lock()

	client, clientPeer := net.Pipe()
	t.Cleanup(func() { clientPeer.Close() })

	prx.wg.Add(1)

	go prx.handleConn(client)

	require.Eventually(t, func() bool { return acceptsA.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	bckA.draining.Store(true)
	bckB.healthy.Store(true)
	bckA.mu.Unlock()

	require.Eventually(t, func() bool { return acceptsB.Load() == 1 }, 3*time.Second, 10*time.Millisecond,
		"the refused connection must be retried on the other backend")
	bckB.draining.Store(true)
	bckB.mu.Unlock()

	require.NoError(t, clientPeer.SetDeadline(time.Now().Add(3*time.Second)))

	_, err := clientPeer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "a refused retry closes the client")

	// The client sees EOF before the handler records the error.
	prx.wg.Wait()

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="`+addrB+`"} 1`,
		"the refused retry counts against its backend")
	assert.NotContains(t, body, `extractedprism_connection_errors_total{upstream="`+addrA+`"}`,
		"the retried refusal did not fail the client")
	assert.NotContains(t, body, `upstream="none"`, "there is only one retry")
	assert.Equal(t, 1, logs.FilterMessage("upstream started draining during dial, retrying").Len())
}
