package proxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

// readyzServer is a TLS upstream whose /readyz answers with the status held
// in code; it records the Authorization header of the last request.
func readyzServer(t *testing.T, code *atomic.Int32) (*httptest.Server, *atomic.Value) {
	t.Helper()

	var auth atomic.Value

	auth.Store("")

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))

		if r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.WriteHeader(int(code.Load()))
	}))
	t.Cleanup(srv.Close)

	return srv, &auth
}

func newReadinessProxy(t *testing.T, client *http.Client, token func() (string, error)) *Proxy {
	t.Helper()

	return New(&Config{
		HealthTimeout:  time.Second,
		HealthInterval: time.Hour,
		Readiness:      &ReadinessCheck{Client: client, Token: token},
	}, zap.NewNop(), metrics.New())
}

func TestCheckOnce_Readyz_Non200_MarksNotReadyAfterThreshold(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusInternalServerError)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)
	assert.False(t, bck.unready.Load(), "one failed readyz probe must not exclude the upstream")

	prx.checkOnce(t.Context(), bck)
	assert.True(t, bck.unready.Load(), "two consecutive failed readyz probes must mark the upstream unready")
	assert.True(t, bck.healthy.Load(), "a reachable but unready upstream stays TCP-healthy as a fallback")
}

func TestCheckOnce_Readyz_200_RestoresReady(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusInternalServerError)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)
	prx.checkOnce(t.Context(), bck)
	require.True(t, bck.unready.Load())

	code.Store(http.StatusOK)
	prx.checkOnce(t.Context(), bck)

	assert.False(t, bck.unready.Load(), "a single 200 must make the upstream ready again")
}

func TestCheckOnce_Readyz_SendsBearerTokenFromSource(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusOK)
	srv, auth := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), func() (string, error) { return "tok-1", nil })
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)

	assert.Equal(t, "Bearer tok-1", auth.Load())
}

func TestCheckOnce_Readyz_TokenError_CountsAsNotReady(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusOK)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), func() (string, error) { return "", errors.New("token unreadable") })
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)
	prx.checkOnce(t.Context(), bck)

	assert.True(t, bck.unready.Load(), "a probe that cannot authenticate must not vouch for the upstream")
}

func TestCheckOnce_TCPOnly_SendsNoReadyzRequest(t *testing.T) {
	var (
		code atomic.Int32
		hits atomic.Int32
	)

	code.Store(http.StatusInternalServerError)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(int(code.Load()))
	}))
	t.Cleanup(srv.Close)

	prx := newTestProxy(t)
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)
	prx.checkOnce(t.Context(), bck)

	assert.Zero(t, hits.Load(), "without a readiness check only TCP is dialed")
	assert.False(t, bck.unready.Load())
}

func TestCheckOnce_Readyz_UsesHTTPSAndPath(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusOK)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)
	prx.checkOnce(t.Context(), bck)

	assert.False(t, bck.unready.Load(), "a TLS upstream answering 200 on /readyz is ready")
	assert.True(t, strings.HasPrefix(srv.URL, "https://"))
}

func TestPickBackend_PrefersReadyBackends(t *testing.T) {
	prx := newReadinessProxy(t, http.DefaultClient, nil)
	ready := newTestBackend("192.0.2.1:6443")
	unready := newTestBackend("192.0.2.2:6443")
	unready.unready.Store(true)
	prx.backends[ready.addr] = ready
	prx.backends[unready.addr] = unready

	for range 50 {
		assert.Same(t, ready, prx.pickBackend(), "an unready upstream must not be picked while a ready one exists")
	}
}

func TestPickBackend_AllUnready_FallsBackToHealthy(t *testing.T) {
	prx := newReadinessProxy(t, http.DefaultClient, nil)
	first := newTestBackend("192.0.2.1:6443")
	second := newTestBackend("192.0.2.2:6443")
	first.unready.Store(true)
	second.unready.Store(true)
	prx.backends[first.addr] = first
	prx.backends[second.addr] = second

	assert.NotNil(t, prx.pickBackend(), "with every upstream unready the TCP-healthy ones remain pickable")
}

func TestPickBackend_TCPMode_IgnoresReadinessState(t *testing.T) {
	prx := newTestProxy(t)
	first := newTestBackend("192.0.2.1:6443")
	second := newTestBackend("192.0.2.2:6443")
	// Never set without a readiness check; the pick must not even look.
	second.unready.Store(true)
	prx.backends[first.addr] = first
	prx.backends[second.addr] = second

	seen := make(map[string]bool)

	for range 200 {
		seen[prx.pickBackend().addr] = true
	}

	assert.Len(t, seen, 2, "in tcp mode readiness takes no part in the pick")
}

func TestPickBackend_UnreadyAndUnhealthyIsNeverPicked(t *testing.T) {
	prx := newReadinessProxy(t, http.DefaultClient, nil)
	dead := newTestBackend("192.0.2.1:6443")
	dead.unready.Store(true)
	dead.healthy.Store(false)
	prx.backends[dead.addr] = dead

	assert.Nil(t, prx.pickBackend())
}

func TestCheckOnce_Readyz_ExportsReadyMetric(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusInternalServerError)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())
	prx.backends[bck.addr] = bck
	series := `extractedprism_upstream_ready{upstream="` + bck.addr + `"} `

	prx.checkOnce(t.Context(), bck)
	assert.Contains(t, scrapeInternal(t, prx.metrics), series+"1", "below the threshold the upstream is still ready")

	prx.checkOnce(t.Context(), bck)
	assert.Contains(t, scrapeInternal(t, prx.metrics), series+"0")

	code.Store(http.StatusOK)
	prx.checkOnce(t.Context(), bck)
	assert.Contains(t, scrapeInternal(t, prx.metrics), series+"1")
}

func TestCheckOnce_Readyz_SkipsMetricOfRemovedBackend(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusOK)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	prx.checkOnce(t.Context(), bck)

	assert.NotContains(t, scrapeInternal(t, prx.metrics), "extractedprism_upstream_ready{")
}

func TestRecordSuccess_TCPRecovery_ResetsReadinessUntilNextPassingProbe(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusInternalServerError)
	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	other := newTestBackend("192.0.2.1:6443")
	restarted := newTestBackend(srv.Listener.Addr().String())
	prx.backends[other.addr] = other
	prx.backends[restarted.addr] = restarted

	// Ready before the outage, then unreachable.
	restarted.healthy.Store(false)
	restarted.failures.Store(healthFailuresToUnhealthy)

	prx.recordSuccess(restarted)

	require.True(t, restarted.healthy.Load())
	assert.True(t, restarted.unready.Load(), "a recovered upstream must not be trusted on its pre-outage verdict")

	for range 50 {
		require.Same(t, other, prx.pickBackend(), "a recovered upstream gets no picks until a probe passes")
	}

	prx.checkReady(t.Context(), restarted)
	assert.True(t, restarted.unready.Load(), "one failed probe after recovery keeps the upstream unready")

	code.Store(http.StatusOK)
	prx.checkReady(t.Context(), restarted)
	assert.False(t, restarted.unready.Load(), "one passing probe makes it ready again")
}

func TestRecordSuccess_TCPMode_LeavesReadinessAlone(t *testing.T) {
	prx := newTestProxy(t)
	bck := newTestBackend("192.0.2.1:6443")
	prx.backends[bck.addr] = bck
	bck.healthy.Store(false)

	prx.recordSuccess(bck)

	assert.False(t, bck.unready.Load())
	assert.NotContains(t, scrapeInternal(t, prx.metrics), "extractedprism_upstream_ready{",
		"the readiness series exists only in readyz mode")
}

func TestReconcile_Readyz_NewUpstreamStartsUnready(t *testing.T) {
	prx := newReadinessProxy(t, http.DefaultClient, nil)
	t.Cleanup(func() { _ = prx.Shutdown(context.Background()) })

	prx.reconcile([]string{"192.0.2.1:6443"})

	prx.mu.RLock()
	bck := prx.backends["192.0.2.1:6443"]
	prx.mu.RUnlock()

	require.NotNil(t, bck)
	assert.True(t, bck.healthy.Load(), "TCP health stays optimistic")
	assert.True(t, bck.unready.Load(), "readiness waits for the first passing probe")
}

// slowHealthDial makes the health check dial take delay before connecting,
// or fail when ctx ends first.
func slowHealthDial(delay time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		dialer := net.Dialer{}

		return dialer.DialContext(ctx, network, addr)
	}
}

func TestCheckOnce_Readyz_DialAndProbeShareOneDeadline(t *testing.T) {
	const timeout = 300 * time.Millisecond

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Fits a fresh timeout, but not what the slow dial leaves of it.
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	prx := New(&Config{
		HealthTimeout:  timeout,
		HealthInterval: time.Hour,
		Readiness:      &ReadinessCheck{Client: srv.Client()},
	}, zap.NewNop(), metrics.New())
	prx.dialHealth = slowHealthDial(200 * time.Millisecond)
	bck := newTestBackend(srv.Listener.Addr().String())

	for range healthFailuresToUnhealthy {
		start := time.Now()

		prx.checkOnce(t.Context(), bck)

		assert.Less(t, time.Since(start), timeout+100*time.Millisecond,
			"one check must not outlast --health-timeout")
	}

	assert.True(t, bck.healthy.Load(), "the TCP dial itself fits the deadline")
	assert.True(t, bck.unready.Load(),
		"the probe gets what the dial left of the deadline, not a fresh --health-timeout")
}
