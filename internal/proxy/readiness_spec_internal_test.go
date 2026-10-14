package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

var errDialRefused = errors.New("connection refused")

func checkTwice(t *testing.T, prx *Proxy, bck *backend) {
	t.Helper()

	for range healthFailuresToUnhealthy {
		prx.checkOnce(t.Context(), bck)
	}
}

func TestCheckOnce_Readyz_OnlyStatus200IsReady(t *testing.T) {
	tests := []struct {
		code    int
		unready bool
	}{
		{code: http.StatusOK, unready: false},
		{code: http.StatusNoContent, unready: true},
		{code: http.StatusMovedPermanently, unready: true},
		{code: http.StatusFound, unready: true},
		{code: http.StatusTemporaryRedirect, unready: true},
		{code: http.StatusUnauthorized, unready: true},
		{code: http.StatusForbidden, unready: true},
		{code: http.StatusInternalServerError, unready: true},
		{code: http.StatusServiceUnavailable, unready: true},
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.code), func(t *testing.T) {
			var code atomic.Int32

			code.Store(int32(tt.code))
			srv, _ := readyzServer(t, &code)
			prx := newReadinessProxy(t, srv.Client(), nil)
			bck := newTestBackend(srv.Listener.Addr().String())

			checkTwice(t, prx, bck)

			assert.Equal(t, tt.unready, bck.unready.Load())
			assert.True(t, bck.healthy.Load(), "the probe verdict never touches TCP health")
		})
	}
}

func TestCheckOnce_Readyz_Timeout_CountsAsNotReady(t *testing.T) {
	const timeout = 100 * time.Millisecond

	release := make(chan struct{})

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	prx := New(&Config{
		HealthTimeout:  timeout,
		HealthInterval: time.Hour,
		Readiness:      &ReadinessCheck{Client: srv.Client()},
	}, zap.NewNop(), metrics.New())
	bck := newTestBackend(srv.Listener.Addr().String())

	start := time.Now()

	checkTwice(t, prx, bck)

	assert.True(t, bck.unready.Load(), "an upstream that does not answer within --health-timeout is not ready")
	assert.True(t, bck.healthy.Load())
	assert.Less(t, time.Since(start), 2*timeout+time.Second, "a hanging probe is cut at the deadline")
}

func TestCheckOnce_Readyz_TLSError_CountsAsNotReady(t *testing.T) {
	var code atomic.Int32

	code.Store(http.StatusOK)
	srv, _ := readyzServer(t, &code)

	// A pool without the test server's CA: verification must fail.
	untrusting := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12},
	}}
	prx := newReadinessProxy(t, untrusting, nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	checkTwice(t, prx, bck)

	assert.True(t, bck.unready.Load(), "an upstream whose certificate does not verify must not vouch for itself")
	assert.True(t, bck.healthy.Load())
}

func TestCheckOnce_Readyz_FailSuccessFail_StaysReady(t *testing.T) {
	var code atomic.Int32

	srv, _ := readyzServer(t, &code)
	prx := newReadinessProxy(t, srv.Client(), nil)
	bck := newTestBackend(srv.Listener.Addr().String())

	for _, status := range []int32{http.StatusInternalServerError, http.StatusOK, http.StatusInternalServerError} {
		code.Store(status)
		prx.checkOnce(t.Context(), bck)
	}

	assert.False(t, bck.unready.Load(), "a 200 in between resets the streak; failures must be consecutive")
}

func TestCheckOnce_Readyz_FailedTCPCheck_SendsNoProbe(t *testing.T) {
	var hits atomic.Int32

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	prx := newReadinessProxy(t, srv.Client(), nil)
	prx.dialHealth = func(context.Context, string, string) (net.Conn, error) { return nil, errDialRefused }
	bck := newTestBackend(srv.Listener.Addr().String())

	checkTwice(t, prx, bck)

	assert.False(t, bck.healthy.Load())
	assert.Zero(t, hits.Load(), "an upstream that fails TCP is not probed")
	assert.False(t, bck.probed.Load(), "no probe outcome is recorded without a probe")
}

// newLatencyReadinessProxy combines latency selection with a readiness check
// whose client is never used: readiness state is set directly.
func newLatencyReadinessProxy(t *testing.T) *Proxy {
	t.Helper()

	prx := newLatencyProxy(t)
	prx.cfg.Readiness = &ReadinessCheck{Client: http.DefaultClient}

	return prx
}

func TestPickBackend_Latency_ReadinessBeforeTiers_PicksReadyFartherTier(t *testing.T) {
	prx := newLatencyReadinessProxy(t)
	near := addSampledBackend(prx, "192.0.2.1:6443", time.Millisecond)
	far := addSampledBackend(prx, "192.0.2.2:6443", 50*time.Millisecond)
	require.Equal(t, 0, tierOf(prx, near))
	require.Equal(t, 1, tierOf(prx, far))

	near.unready.Store(true)

	counts := pickCounts(t, prx)

	assert.Equal(t, pickRounds, counts[far.addr],
		"an unready closest tier must hand over to the closest ready one")
}

func TestPickBackend_Latency_AllUnready_FallsBackToClosestTier(t *testing.T) {
	prx := newLatencyReadinessProxy(t)
	near := addSampledBackend(prx, "192.0.2.1:6443", time.Millisecond)
	far := addSampledBackend(prx, "192.0.2.2:6443", 50*time.Millisecond)
	near.unready.Store(true)
	far.unready.Store(true)

	counts := pickCounts(t, prx)

	assert.Equal(t, pickRounds, counts[near.addr],
		"with nothing ready the fallback keeps the latency preference among TCP-healthy upstreams")
}

func TestPickBackend_Latency_ReadyNearAndUnreadyNear_PicksReadyOnly(t *testing.T) {
	prx := newLatencyReadinessProxy(t)
	ready := addSampledBackend(prx, "192.0.2.1:6443", time.Millisecond)
	unready := addSampledBackend(prx, "192.0.2.2:6443", time.Millisecond)
	addSampledBackend(prx, "192.0.2.3:6443", 50*time.Millisecond)
	unready.unready.Store(true)

	counts := pickCounts(t, prx)

	assert.Equal(t, pickRounds, counts[ready.addr], "within a tier readiness still applies")
}
