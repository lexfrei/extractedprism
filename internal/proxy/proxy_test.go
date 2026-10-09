package proxy_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/lexfrei/extractedprism/internal/metrics"
	"github.com/lexfrei/extractedprism/internal/proxy"
)

const (
	waitTimeout = 3 * time.Second
	pollTick    = 10 * time.Millisecond
)

var errEchoMismatch = errors.New("echo mismatch")

func testConfig() proxy.Config {
	return proxy.Config{
		BindAddress:     "127.0.0.1",
		BindPort:        0,
		DialTimeout:     time.Second,
		KeepAlivePeriod: time.Second,
		TCPUserTimeout:  time.Second,
		HealthInterval:  20 * time.Millisecond,
		HealthTimeout:   50 * time.Millisecond,
		DrainTimeout:    5 * time.Second,
	}
}

// startTaggedEcho starts a TCP server that greets every connection with its
// tag followed by a colon, then echoes back everything it receives. The tag
// lets tests identify which backend a connection landed on.
func startTaggedEcho(t *testing.T, tag string) string {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go serveTagged(listener, tag)

	t.Cleanup(func() { listener.Close() })

	return listener.Addr().String()
}

func startProxy(t *testing.T, cfg proxy.Config, updates chan []string) (*proxy.Proxy, *metrics.Metrics) {
	t.Helper()

	m := metrics.New()
	prx := proxy.New(&cfg, zaptest.NewLogger(t), m)
	require.NoError(t, prx.Start(t.Context(), updates))

	// The shutdown context must not derive from t.Context(): it is canceled
	// before cleanups run, so Shutdown would return without waiting for the
	// proxy goroutines and they would outlive the test.
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = prx.Shutdown(shutCtx)
	})

	return prx, m
}

// addBackends sends an endpoint update and waits until the proxy has applied
// it. Reconcile is asynchronous: a connection accepted before the update
// lands finds no backend and is closed, which tests must not race against.
func addBackends(t *testing.T, prx *proxy.Proxy, updates chan<- []string, addrs ...string) {
	t.Helper()

	updates <- addrs

	require.Eventually(t, prx.Healthy, waitTimeout, pollTick, "endpoint update was not applied")
}

// dialTag connects to the proxy and returns the tag of the backend it landed
// on, verifying the greeting (upstream-to-client direction).
func dialTag(t *testing.T, addr string) (net.Conn, string) {
	t.Helper()

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	tag, err := bufio.NewReader(conn).ReadString(':')
	require.NoError(t, err)

	return conn, strings.TrimSuffix(tag, ":")
}

// verifyEcho writes payload and asserts it comes back (client-to-upstream
// direction).
func verifyEcho(t *testing.T, conn net.Conn, payload string) {
	t.Helper()

	require.NoError(t, echoErr(conn, payload))
}

// echoErr is verifyEcho's error-returning form for use in polling conditions
// where require is not available.
func echoErr(conn net.Conn, payload string) error {
	_, err := io.WriteString(conn, payload)
	if err != nil {
		return err //nolint:wrapcheck // test helper, assertion prints it
	}

	buf := make([]byte, len(payload))

	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return err //nolint:wrapcheck // test helper, assertion prints it
	}

	if string(buf) != payload {
		return errors.Wrapf(errEchoMismatch, "got %q, want %q", buf, payload)
	}

	return nil
}

func TestProxy_ForwardsTrafficBidirectional(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 1)
	prx, _ := startProxy(t, testConfig(), updates)
	addBackends(t, prx, updates, backend)

	conn, tag := dialTag(t, prx.Addr())
	defer conn.Close()

	assert.Equal(t, "A", tag, "greeting must come from the backend (upstream-to-client)")
	verifyEcho(t, conn, "ping")
}

func TestProxy_NoBackends_ClosesConnectionImmediately(t *testing.T) {
	updates := make(chan []string, 1)
	prx, m := startProxy(t, testConfig(), updates)

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	defer conn.Close()

	// Negative contract: with no pickable backend the proxy must not leave
	// the client hanging — the connection is closed promptly.
	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "connection without backends must be closed, not held")

	require.Eventually(t, func() bool {
		return strings.Contains(scrapeMetrics(t, m), `extractedprism_connection_errors_total{upstream="none"} 1`)
	}, waitTimeout, pollTick)
}

func TestProxy_UnreachableBackend_ClosesConnectionAndCountsError(t *testing.T) {
	cfg := testConfig()
	// Only the immediate check runs: one failure keeps the backend pickable,
	// so the client dial below is the one that hits the dead address.
	cfg.HealthInterval = time.Hour

	updates := make(chan []string, 1)
	prx, m := startProxy(t, cfg, updates)
	addBackends(t, prx, updates, "127.0.0.1:1")

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "connection to unreachable backend must be closed")

	require.Eventually(t, func() bool {
		body := scrapeMetrics(t, m)

		return strings.Contains(body, `extractedprism_connection_errors_total{upstream="127.0.0.1:1"} 1`) &&
			strings.Contains(body, "extractedprism_connections_active 0")
	}, waitTimeout, pollTick, "dial error must be counted per upstream and active must return to zero")
}

func TestProxy_DeadBackend_ExcludedAfterHealthFailures(t *testing.T) {
	// Occupy and release a port to get an address that refuses connections.
	deadListener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := deadListener.Addr().String()
	require.NoError(t, deadListener.Close())

	live := startTaggedEcho(t, "LIVE")

	updates := make(chan []string, 1)
	prx, m := startProxy(t, testConfig(), updates)
	updates <- []string{deadAddr, live}

	require.Eventually(t, func() bool {
		body := scrapeMetrics(t, m)

		return strings.Contains(body, `extractedprism_health_check_status{upstream="`+deadAddr+`"} 0`) &&
			strings.Contains(body, "extractedprism_upstreams_active 1") &&
			strings.Contains(body, "extractedprism_upstreams_total 2")
	}, waitTimeout, pollTick, "dead backend must be marked unhealthy after consecutive failures")

	// Once the dead backend is excluded, every new connection must land on
	// the live one. Health checks themselves must NOT be counted as proxied
	// connections.
	for range 5 {
		conn, tag := dialTag(t, prx.Addr())
		assert.Equal(t, "LIVE", tag)
		verifyEcho(t, conn, "x")
		conn.Close()
	}

	body := scrapeMetrics(t, m)
	assert.Contains(t, body, "extractedprism_connections_total 5")
	assert.NotContains(t, body, `connection_errors_total{upstream="`+deadAddr+`"}`,
		"health check failures must not be counted as connection errors")
}

func TestProxy_RecoveredBackend_RejoinsRotation(t *testing.T) {
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	go serveTagged(listener, "R")

	updates := make(chan []string, 1)
	prx, _ := startProxy(t, testConfig(), updates)
	updates <- []string{addr}

	// Break the backend: stop accepting and free the port.
	require.NoError(t, listener.Close())

	require.Eventually(t, func() bool {
		return !prx.Healthy()
	}, waitTimeout, pollTick, "proxy must become unhealthy when its only backend dies")

	// Revive the backend on the same address.
	revived, err := new(net.ListenConfig).Listen(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { revived.Close() })

	go serveTagged(revived, "R")

	require.Eventually(t, func() bool {
		return prx.Healthy()
	}, waitTimeout, pollTick, "proxy must recover when the backend comes back")

	conn, tag := dialTag(t, prx.Addr())
	defer conn.Close()
	assert.Equal(t, "R", tag)
}

func TestProxy_RemovedBackend_ReceivesNoNewConnections(t *testing.T) {
	backendA := startTaggedEcho(t, "A")
	backendB := startTaggedEcho(t, "B")

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, testConfig(), updates)
	updates <- []string{backendA, backendB}

	// Negative contract: a removed backend must not receive NEW
	// connections, even while its existing ones are still draining.
	updates <- []string{backendB}

	// Give the reconcile a moment to apply, then verify convergence: a burst
	// of fresh connections must all land on B.
	require.Eventually(t, func() bool {
		conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
		if err != nil {
			return false
		}
		defer conn.Close()

		tag, err := bufio.NewReader(conn).ReadString(':')
		if err != nil {
			return false
		}

		return strings.TrimSuffix(tag, ":") == "B"
	}, waitTimeout, pollTick)

	for range 10 {
		conn, tag := dialTag(t, prx.Addr())
		assert.Equal(t, "B", tag, "removed backend must receive no new connections")
		conn.Close()
	}
}

func TestProxy_DrainingBackend_ExistingConnectionsSurvive(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 10)
	prx, m := startProxy(t, testConfig(), updates)
	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()
	verifyEcho(t, conn, "before-drain")

	// Remove the only backend: the proxy stops picking it, but the existing
	// connection belongs to the pre-removal cohort and must keep working
	// for the whole drain timeout.
	updates <- []string{}

	require.Eventually(t, func() bool {
		return !prx.Healthy()
	}, waitTimeout, pollTick, "a proxy with only a draining backend must report unhealthy")

	verifyEcho(t, conn, "during-drain")

	// After the client closes, the drain completes and the backend is gone.
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		body := scrapeMetrics(t, m)

		return strings.Contains(body, "extractedprism_upstreams_total 0") &&
			!strings.Contains(body, "extractedprism_health_check_status{")
	}, waitTimeout, pollTick, "drained backend must be removed, including its health series")
}

func TestProxy_DrainTimeout_ForceClosesIdleConnection(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	cfg := testConfig()
	cfg.DrainTimeout = 200 * time.Millisecond

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, cfg, updates)
	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()

	updates <- []string{}

	// The connection is idle and outlives the drain timeout: the proxy must
	// force-close it shortly after the timeout, not let it hang forever.
	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err, "drain timeout must force-close idle connections")
}

func TestProxy_ZeroDrainTimeout_ForceClosesImmediately(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	cfg := testConfig()
	cfg.DrainTimeout = 0

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, cfg, updates)
	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()

	updates <- []string{}

	// Zero timeout means no grace period at all: the connection must die
	// immediately after the removal is reconciled.
	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err, "zero drain timeout must close connections immediately")
}

func TestProxy_ReaddDuringDrain_KeepsBackendAndConnections(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	cfg := testConfig()
	cfg.DrainTimeout = 300 * time.Millisecond

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, cfg, updates)
	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()

	// Remove and re-add within the drain timeout. The re-add must cancel the
	// drain: neither the existing connection nor the backend may be killed.
	updates <- []string{}
	updates <- []string{backend}

	// Poll echo across twice the original drain timeout: the moment the
	// aborted drain would have fired must pass without the connection dying.
	assert.Never(t, func() bool {
		return echoErr(conn, "x") != nil
	}, 2*cfg.DrainTimeout, 20*time.Millisecond,
		"aborted drain must not touch existing connections")

	assert.True(t, prx.Healthy(), "re-added backend must be pickable again")
}

func TestProxy_Healthy_ReflectsPickableBackends(t *testing.T) {
	updates := make(chan []string, 10)
	prx, _ := startProxy(t, testConfig(), updates)

	assert.False(t, prx.Healthy(), "no backends at all: unhealthy")

	backend := startTaggedEcho(t, "A")
	updates <- []string{backend}

	// Reconcile is asynchronous: the update travels through the channel.
	require.Eventually(t, func() bool {
		return prx.Healthy()
	}, waitTimeout, pollTick, "one live backend: healthy")
}

func TestProxy_Shutdown_ClosesConnectionsAndListener(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 1)
	m := metrics.New()
	prx := proxy.New(new(testConfig()), zaptest.NewLogger(t), m)
	require.NoError(t, prx.Start(t.Context(), updates))
	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()
	verifyEcho(t, conn, "pre-shutdown")

	shutCtx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	require.NoError(t, prx.Shutdown(shutCtx))

	// Existing connection must be closed by shutdown.
	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err, "shutdown must close in-flight connections")

	// The listener must be gone: new dials to the proxy address fail.
	_, err = new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.Error(t, err, "shutdown must close the listener")
}

func TestProxy_ConnectionMetrics_BalancedExactlyOnce(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 1)
	prx, m := startProxy(t, testConfig(), updates)
	addBackends(t, prx, updates, backend)

	const connCount = 7

	conns := make([]net.Conn, 0, connCount)

	for range connCount {
		conn, _ := dialTag(t, prx.Addr())
		verifyEcho(t, conn, "x")
		conns = append(conns, conn)
	}

	require.Eventually(t, func() bool {
		body := scrapeMetrics(t, m)

		return strings.Contains(body, "extractedprism_connections_active 7") &&
			strings.Contains(body, "extractedprism_connections_total 7")
	}, waitTimeout, pollTick)

	for _, conn := range conns {
		conn.Close()
	}

	// Exactly-once accounting: after all closes (both directions racing),
	// active must return to exactly zero — never negative, never stuck.
	require.Eventually(t, func() bool {
		return strings.Contains(scrapeMetrics(t, m), "extractedprism_connections_active 0")
	}, waitTimeout, pollTick)
}

func TestProxy_Start_ListenError(t *testing.T) {
	blocker, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer blocker.Close()

	port := blocker.Addr().(*net.TCPAddr).Port

	cfg := testConfig()
	cfg.BindPort = port

	m := metrics.New()
	prx := proxy.New(&cfg, zaptest.NewLogger(t), m)

	err = prx.Start(t.Context(), make(chan []string))
	require.Error(t, err, "binding an occupied port must fail")
}

func TestProxy_EmptyUpdate_DrainsAllBackends(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, testConfig(), updates)
	updates <- []string{backend}

	require.Eventually(t, func() bool {
		return prx.Healthy()
	}, waitTimeout, pollTick)

	// An empty update means every known backend is gone. The merged provider
	// never sends empty lists, but the proxy must not treat one as "no
	// change" — it reconciles to empty.
	updates <- []string{}

	require.Eventually(t, func() bool {
		return !prx.Healthy()
	}, waitTimeout, pollTick)
}

func scrapeMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestProxy_ConcurrentReconcileAndShutdown_NoDeadlock(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 100)
	m := metrics.New()
	prx := proxy.New(new(testConfig()), zaptest.NewLogger(t), m)
	require.NoError(t, prx.Start(t.Context(), updates))
	updates <- []string{backend}

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range 50 {
			if i%2 == 0 {
				updates <- []string{backend}
			} else {
				updates <- []string{}
			}
		}
	})

	shutCtx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	require.NoError(t, prx.Shutdown(shutCtx))

	wg.Wait()
}

func TestProxy_DrainWithoutConnections_CompletesPromptly(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	cfg := testConfig()
	cfg.DrainTimeout = 30 * time.Second // deliberately long

	updates := make(chan []string, 10)
	prx, m := startProxy(t, cfg, updates)
	updates <- []string{backend}

	require.Eventually(t, func() bool {
		return prx.Healthy()
	}, waitTimeout, pollTick)

	updates <- []string{}

	// A backend holding no connections must be removed as soon as the
	// removal is reconciled — it must NOT linger for the drain timeout.
	require.Eventually(t, func() bool {
		return strings.Contains(scrapeMetrics(t, m), "extractedprism_upstreams_total 0")
	}, waitTimeout, pollTick, "connectionless backend must be removed long before the drain timeout")
}

func TestProxy_ClientDialFailures_ExcludeBackend(t *testing.T) {
	deadListener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := deadListener.Addr().String()
	require.NoError(t, deadListener.Close())

	cfg := testConfig()
	// No ticks during the test: the only health signal is the immediate
	// check at add time, which records one failure for the dead address.
	cfg.HealthInterval = time.Hour

	updates := make(chan []string, 1)
	prx, m := startProxy(t, cfg, updates)
	updates <- []string{deadAddr}

	require.Eventually(t, func() bool {
		return prx.Healthy()
	}, waitTimeout, pollTick, "backend starts optimistic-healthy before any failure")

	// One client-facing dial failure. Together with the immediate health
	// check this crosses the two-failure threshold — no health tick needed.
	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	conn.Close()

	require.Eventually(t, func() bool {
		body := scrapeMetrics(t, m)

		return !prx.Healthy() &&
			strings.Contains(body, `extractedprism_health_check_status{upstream="`+deadAddr+`"} 0`)
	}, waitTimeout, pollTick, "immediate check plus one client dial failure must exclude the backend")
}

func TestProxy_DrainStart_LogsEndpointAndConnectionCount(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	core, logs := observer.New(zap.InfoLevel)

	m := metrics.New()
	prx := proxy.New(new(testConfig()), zap.New(core), m)

	updates := make(chan []string, 10)
	require.NoError(t, prx.Start(t.Context(), updates))

	// The shutdown context must not derive from t.Context(): it is canceled
	// before cleanups run, so Shutdown would return without waiting for the
	// proxy goroutines and they would outlive the test.
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = prx.Shutdown(shutCtx)
	})

	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()
	verifyEcho(t, conn, "x")

	updates <- []string{}

	require.Eventually(t, func() bool {
		entries := logs.FilterMessage("draining upstream").All()
		if len(entries) != 1 {
			return false
		}

		fields := entries[0].ContextMap()

		return fields["upstream"] == backend && fields["connections"] == int64(1)
	}, waitTimeout, pollTick, "drain start must log the endpoint and its live connection count")
}

func TestProxy_ClientHalfClose_StillReceivesResponse(t *testing.T) {
	// Upstream reads the full request until EOF, then answers. A client that
	// half-closes after sending (CloseWrite) must still get the answer: EOF in
	// one direction must not tear down the other.
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

				req, _ := io.ReadAll(conn)
				_, _ = io.WriteString(conn, "resp:"+string(req))
			}()
		}
	}()

	updates := make(chan []string, 1)
	prx, _ := startProxy(t, testConfig(), updates)
	addBackends(t, prx, updates, listener.Addr().String())

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = io.WriteString(conn, "hello")
	require.NoError(t, err)

	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcpConn.CloseWrite())

	_ = conn.SetReadDeadline(time.Now().Add(waitTimeout))

	resp, err := io.ReadAll(conn)
	require.NoError(t, err)
	assert.Equal(t, "resp:hello", string(resp), "half-closed client must still receive the response")
}

// serveTagged runs a tagged echo server on an existing listener until it is
// closed.
func serveTagged(listener net.Listener, tag string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		go func() {
			defer conn.Close()

			_, _ = io.WriteString(conn, tag+":")
			_, _ = io.Copy(conn, conn)
		}()
	}
}

func TestProxy_ReaddDuringDrain_ResumesHealthChecks(t *testing.T) {
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	go serveTagged(listener, "A")

	cfg := testConfig()
	cfg.DrainTimeout = 30 * time.Second

	updates := make(chan []string, 10)
	prx, _ := startProxy(t, cfg, updates)
	addBackends(t, prx, updates, addr)

	// An open connection keeps the backend draining instead of being
	// removed at once, so the re-add aborts this drain rather than creating
	// a fresh backend.
	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()

	updates <- []string{}

	require.Eventually(t, func() bool { return !prx.Healthy() }, waitTimeout, pollTick)

	updates <- []string{addr}

	require.Eventually(t, prx.Healthy, waitTimeout, pollTick, "re-added backend must be pickable")

	// Health checking must be running again: a dead re-added backend has to
	// be excluded without any client traffic...
	require.NoError(t, listener.Close())

	require.Eventually(t, func() bool { return !prx.Healthy() }, waitTimeout, pollTick,
		"re-added backend must be health checked again")

	// ...and brought back once it recovers.
	revived, err := new(net.ListenConfig).Listen(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { revived.Close() })

	go serveTagged(revived, "A")

	require.Eventually(t, prx.Healthy, waitTimeout, pollTick, "recovered backend must rejoin")
}

func TestProxy_RepeatedRemovalDuringDrain_DrainsOnce(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	core, logs := observer.New(zap.InfoLevel)

	cfg := testConfig()
	cfg.DrainTimeout = 300 * time.Millisecond

	m := metrics.New()
	prx := proxy.New(&cfg, zap.New(core), m)

	updates := make(chan []string, 10)
	require.NoError(t, prx.Start(t.Context(), updates))

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = prx.Shutdown(shutCtx)
	})

	addBackends(t, prx, updates, backend)

	conn, _ := dialTag(t, prx.Addr())
	defer conn.Close()

	// Discovery forwards every update, so further lists without the backend
	// keep arriving while it drains. They must not restart the drain.
	updates <- []string{}
	updates <- []string{}
	updates <- []string{}

	require.Eventually(t, func() bool {
		return logs.FilterMessage("upstream drained and removed").Len() == 1
	}, waitTimeout, pollTick, "drain must complete")

	assert.Equal(t, 1, logs.FilterMessage("draining upstream").Len(), "a draining backend must not be drained again")
}

func TestProxy_ClosedUpdates_KeepsCurrentBackends(t *testing.T) {
	backend := startTaggedEcho(t, "A")

	updates := make(chan []string, 1)
	prx, _ := startProxy(t, testConfig(), updates)
	addBackends(t, prx, updates, backend)

	// A closed update channel means no more discovery input, not "no
	// endpoints": the current backends must stay in service.
	close(updates)

	assert.Never(t, func() bool { return !prx.Healthy() }, 300*time.Millisecond, pollTick,
		"closing the update channel must not drain the backends")
}
