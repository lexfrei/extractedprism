package proxy

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

const (
	nearAddr = "192.0.2.1:6443"
	near2    = "192.0.2.2:6443"
	midAddr  = "198.51.100.1:6443"
	farAddr  = "203.0.113.1:6443"

	pickRounds = 1000
)

func newLatencyProxy(t *testing.T) *Proxy {
	t.Helper()

	prx := New(&Config{
		HealthTimeout:    50 * time.Millisecond,
		HealthInterval:   time.Hour,
		DrainTimeout:     time.Hour,
		LatencySelection: true,
	}, zap.NewNop(), metrics.New())

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_ = prx.Shutdown(shutCtx)
	})

	return prx
}

// addSampledBackend inserts a backend the way reconcile does and, when rtt is
// positive, feeds it one health check sample.
func addSampledBackend(prx *Proxy, addr string, rtt time.Duration) *backend {
	bck := newTestBackend(addr)

	prx.mu.Lock()
	prx.backends[addr] = bck
	prx.updateUpstreamMetricsLocked()
	prx.mu.Unlock()

	if rtt > 0 {
		prx.recordRTT(bck, rtt)
	}

	return bck
}

func pickCounts(t *testing.T, prx *Proxy) map[string]int {
	t.Helper()

	counts := make(map[string]int)

	for range pickRounds {
		bck := prx.pickBackend()
		require.NotNil(t, bck, "a pickable backend exists, pick must not fail")

		counts[bck.addr]++
	}

	return counts
}

func tierOf(prx *Proxy, bck *backend) int {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	return bck.tier
}

func smoothedRTT(prx *Proxy, bck *backend) (time.Duration, bool) {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	return bck.rtt, bck.sampled
}

func TestInTier_Boundaries(t *testing.T) {
	tests := []struct {
		name  string
		rtt   time.Duration
		best  time.Duration
		wasIn bool
		want  bool
	}{
		{name: "the best backend itself is in", rtt: 10 * time.Millisecond, best: 10 * time.Millisecond, want: true},
		{name: "relative bound: exactly twice the best joins", rtt: 20 * time.Millisecond, best: 10 * time.Millisecond, want: true},
		{name: "relative bound: one nanosecond above does not join", rtt: 20*time.Millisecond + 1, best: 10 * time.Millisecond, want: false},
		{name: "relative bound: just below joins", rtt: 20*time.Millisecond - 1, best: 10 * time.Millisecond, want: true},
		{name: "relative bound: a member above the join limit stays", rtt: 20*time.Millisecond + 1, best: 10 * time.Millisecond, wasIn: true, want: true},
		{name: "relative bound: a member at the leave limit stays", rtt: 25 * time.Millisecond, best: 10 * time.Millisecond, wasIn: true, want: true},
		{name: "relative bound: a member above the leave limit leaves", rtt: 25*time.Millisecond + 1, best: 10 * time.Millisecond, wasIn: true, want: false},
		{name: "absolute floor: best plus floor joins", rtt: 2500 * time.Microsecond, best: 500 * time.Microsecond, want: true},
		{name: "absolute floor: one nanosecond above does not join", rtt: 2500*time.Microsecond + 1, best: 500 * time.Microsecond, want: false},
		{name: "absolute floor: a member at the leave limit stays", rtt: 3125 * time.Microsecond, best: 500 * time.Microsecond, wasIn: true, want: true},
		{name: "absolute floor: a member above the leave limit leaves", rtt: 3125*time.Microsecond + 1, best: 500 * time.Microsecond, wasIn: true, want: false},
		{name: "sub-millisecond differences share one tier", rtt: 900 * time.Microsecond, best: 100 * time.Microsecond, want: true},
		{name: "another country is a lower tier", rtt: 50 * time.Millisecond, best: 500 * time.Microsecond, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, inTier(tt.rtt, tt.best, tt.wasIn))
		})
	}
}

func TestPickBackend_Latency_SingleSiteSubMillisecond_AllInFirstTier(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, 100*time.Microsecond)
	addSampledBackend(prx, near2, 400*time.Microsecond)
	addSampledBackend(prx, midAddr, 900*time.Microsecond)

	counts := pickCounts(t, prx)

	assert.Positive(t, counts[nearAddr])
	assert.Positive(t, counts[near2])
	assert.Positive(t, counts[midAddr], "a sub-millisecond difference must not demote a backend")
}

func TestPickBackend_Latency_NeverPicksLowerTierWhileFirstTierPickable(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, near2, 1500*time.Microsecond)
	addSampledBackend(prx, farAddr, 60*time.Millisecond)

	counts := pickCounts(t, prx)

	assert.Zero(t, counts[farAddr], "a lower tier must not be picked while the first tier is pickable")
	assert.Positive(t, counts[nearAddr], "picks are spread over the first tier")
	assert.Positive(t, counts[near2], "picks are spread over the first tier, not pinned to the best")
}

func TestPickBackend_Latency_FirstTierUnhealthy_FallsBackToNextTierOnly(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, midAddr, 20*time.Millisecond)
	addSampledBackend(prx, farAddr, 100*time.Millisecond)

	prx.recordFailure(near, errors.New("refused"))
	prx.recordFailure(near, errors.New("refused"))
	require.False(t, near.healthy.Load())

	counts := pickCounts(t, prx)
	assert.Equal(t, pickRounds, counts[midAddr], "the next tier takes every pick, the one below it none")

	prx.recordSuccess(near)

	counts = pickCounts(t, prx)
	assert.Equal(t, pickRounds, counts[nearAddr], "a recovered first tier takes the picks back")
}

func TestPickBackend_Latency_FirstTierDraining_FallsBackToNextTierOnly(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, time.Millisecond)
	mid := addSampledBackend(prx, midAddr, 20*time.Millisecond)
	addSampledBackend(prx, farAddr, 100*time.Millisecond)

	// An open connection keeps the near backend draining for DrainTimeout.
	client, upstream := net.Pipe()
	require.True(t, prx.tryRegister(near, &trackedConn{client: client, upstream: upstream}))

	prx.reconcile([]string{midAddr, farAddr})
	require.True(t, near.draining.Load())

	counts := pickCounts(t, prx)
	assert.Equal(t, pickRounds, counts[mid.addr], "the next tier takes every pick, the one below it none")
}

func TestPickBackend_Latency_UnsampledBackendIsPickable(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, midAddr, 0)
	addSampledBackend(prx, farAddr, 60*time.Millisecond)

	counts := pickCounts(t, prx)
	assert.Positive(t, counts[midAddr], "a new backend must not be starved before its first sample")
	assert.Positive(t, counts[nearAddr])
	assert.Zero(t, counts[farAddr])
}

func TestPickBackend_RandomMode_IgnoresLatency(t *testing.T) {
	prx := newTestProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	far := addSampledBackend(prx, farAddr, 60*time.Millisecond)

	counts := pickCounts(t, prx)
	assert.Positive(t, counts[nearAddr])
	assert.Positive(t, counts[farAddr], "random mode picks among every pickable backend")

	_, sampled := smoothedRTT(prx, far)
	assert.False(t, sampled, "random mode keeps no latency state")

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, "extractedprism_upstream_rtt_seconds{")
	assert.NotContains(t, body, "extractedprism_upstream_latency_tier{")
}

func TestRecordRTT_FirstSampleSeedsThenSmooths(t *testing.T) {
	prx := newLatencyProxy(t)

	bck := addSampledBackend(prx, nearAddr, 10*time.Millisecond)

	rtt, sampled := smoothedRTT(prx, bck)
	require.True(t, sampled)
	assert.Equal(t, 10*time.Millisecond, rtt, "the first sample is taken as is")

	prx.recordRTT(bck, 20*time.Millisecond)

	rtt, _ = smoothedRTT(prx, bck)
	want := 10*time.Millisecond + time.Duration(rttWeight*float64(10*time.Millisecond))
	assert.Equal(t, want, rtt, "later samples move the average by rttWeight")
}

func TestRecordRTT_HysteresisHoldsNearBoundary(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, 10*time.Millisecond)
	edge := addSampledBackend(prx, midAddr, 15*time.Millisecond)
	require.Equal(t, 0, tierOf(prx, edge))

	// Join limit 20ms, leave limit 25ms: samples hovering in between must
	// not move the backend in either direction.
	for range 50 {
		prx.recordRTT(edge, 22*time.Millisecond)
		require.Equal(t, 0, tierOf(prx, edge), "a member between the join and leave limits stays")
	}

	for range 50 {
		prx.recordRTT(edge, 30*time.Millisecond)
	}

	require.Equal(t, 1, tierOf(prx, edge), "a member above the leave limit leaves")

	for range 50 {
		prx.recordRTT(edge, 22*time.Millisecond)
		require.Equal(t, 1, tierOf(prx, edge), "a non-member between the limits stays out")
	}

	for range 50 {
		prx.recordRTT(edge, 15*time.Millisecond)
	}

	assert.Equal(t, 0, tierOf(prx, edge), "a non-member below the join limit joins")
}

func TestCheckOnce_SuccessfulDial_FeedsRTT(t *testing.T) {
	prx := newLatencyProxy(t)
	addr, _ := startEchoBackend(t)
	bck := addSampledBackend(prx, addr, 0)

	prx.checkOnce(t.Context(), bck)

	rtt, sampled := smoothedRTT(prx, bck)
	assert.True(t, sampled, "a successful health check dial is a sample")
	assert.Positive(t, rtt)
}

func TestCheckOnce_FailedDial_LeavesRTTUntouched(t *testing.T) {
	prx := newLatencyProxy(t)
	bck := addSampledBackend(prx, deadAddr(t), 0)

	prx.checkOnce(t.Context(), bck)

	_, sampled := smoothedRTT(prx, bck)
	assert.False(t, sampled, "a failed dial is not a sample")

	prx.recordRTT(bck, time.Millisecond)
	prx.checkOnce(t.Context(), bck)

	rtt, _ := smoothedRTT(prx, bck)
	assert.Equal(t, time.Millisecond, rtt, "a failed dial does not move the average")
}

func TestLatencyMetrics_ReflectRTTAndTier(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, farAddr, 60*time.Millisecond)

	body := scrapeInternal(t, prx.metrics)
	assert.Contains(t, body, `extractedprism_upstream_rtt_seconds{upstream="192.0.2.1:6443"} 0.001`)
	assert.Contains(t, body, `extractedprism_upstream_rtt_seconds{upstream="203.0.113.1:6443"} 0.06`)
	assert.Contains(t, body, `extractedprism_upstream_latency_tier{upstream="192.0.2.1:6443"} 0`)
	assert.Contains(t, body, `extractedprism_upstream_latency_tier{upstream="203.0.113.1:6443"} 1`)
}

func TestLatencyMetrics_DeletedOnBackendRemoval(t *testing.T) {
	prx := New(&Config{
		HealthTimeout:    50 * time.Millisecond,
		HealthInterval:   time.Hour,
		LatencySelection: true,
	}, zap.NewNop(), metrics.New())

	t.Cleanup(func() { _ = prx.Shutdown(context.Background()) })

	addSampledBackend(prx, nearAddr, time.Millisecond)
	far := addSampledBackend(prx, farAddr, 60*time.Millisecond)

	prx.reconcile([]string{nearAddr})

	require.Eventually(t, func() bool {
		prx.mu.RLock()
		defer prx.mu.RUnlock()

		_, ok := prx.backends[farAddr]

		return !ok
	}, 3*time.Second, 10*time.Millisecond, "a drain with no connections removes the backend")

	// A health check in flight during the removal reports now: it must not
	// resurrect the deleted series.
	prx.recordRTT(far, 70*time.Millisecond)

	body := scrapeInternal(t, prx.metrics)
	assert.NotContains(t, body, `extractedprism_upstream_rtt_seconds{upstream="203.0.113.1:6443"}`)
	assert.NotContains(t, body, `extractedprism_upstream_latency_tier{upstream="203.0.113.1:6443"}`)
	assert.Contains(t, body, `extractedprism_upstream_latency_tier{upstream="192.0.2.1:6443"} 0`)
}

// echoThroughProxy opens one connection through the proxy and proves it was
// served end to end.
func echoThroughProxy(t *testing.T, addr string) {
	t.Helper()

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	defer conn.Close()

	_, err = io.WriteString(conn, "ping")
	require.NoError(t, err)

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
}

func waitSampled(t *testing.T, prx *Proxy, addrs ...string) {
	t.Helper()

	require.Eventually(t, func() bool {
		prx.mu.RLock()
		defer prx.mu.RUnlock()

		for _, addr := range addrs {
			bck, ok := prx.backends[addr]
			if !ok || !bck.sampled {
				return false
			}
		}

		return true
	}, 3*time.Second, 10*time.Millisecond, "the first health checks did not record a sample")
}

func requireAccepts(t *testing.T, accepts *atomic.Int32, want int32, msg string) {
	t.Helper()

	assert.Equal(t, want, accepts.Load(), msg)
}

func TestStart_LatencySelection_RoutesToFirstTierThenFallsBack(t *testing.T) {
	nearUp, nearAccepts := startEchoBackend(t)
	farUp, farAccepts := startEchoBackend(t)

	prx := New(&Config{
		BindAddress:      "127.0.0.1",
		DialTimeout:      time.Second,
		HealthInterval:   time.Hour,
		HealthTimeout:    time.Second,
		DrainTimeout:     time.Hour,
		LatencySelection: true,
	}, zaptest.NewLogger(t), metrics.New())

	updates := make(chan []string, 1)
	require.NoError(t, prx.Start(t.Context(), updates))
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_ = prx.Shutdown(shutCtx)
	})

	updates <- []string{nearUp, farUp}

	waitSampled(t, prx, nearUp, farUp)

	prx.mu.RLock()
	far := prx.backends[farUp]
	prx.mu.RUnlock()

	// Loopback backends measure alike; push the far one into a lower tier.
	for range 30 {
		prx.recordRTT(far, time.Second)
	}

	// Each backend accepts exactly one health check dial before any proxied
	// connection; waiting for it keeps it out of the counts below.
	require.Eventually(t, func() bool { return nearAccepts.Load() == 1 && farAccepts.Load() == 1 },
		3*time.Second, 10*time.Millisecond)

	nearBefore, farBefore := nearAccepts.Load(), farAccepts.Load()

	for range 20 {
		echoThroughProxy(t, prx.Addr())
	}

	requireAccepts(t, farAccepts, farBefore, "no connection may reach the lower tier")
	requireAccepts(t, nearAccepts, nearBefore+20, "every connection goes to the first tier")

	updates <- []string{farUp}

	require.Eventually(t, func() bool {
		prx.mu.RLock()
		defer prx.mu.RUnlock()

		bck, ok := prx.backends[nearUp]

		return !ok || bck.draining.Load()
	}, 3*time.Second, 10*time.Millisecond, "the removal was not applied")

	nearBefore = nearAccepts.Load()

	for range 10 {
		echoThroughProxy(t, prx.Addr())
	}

	requireAccepts(t, nearAccepts, nearBefore, "a draining first tier gets no new connections")
	requireAccepts(t, farAccepts, farBefore+10, "the next tier takes over")
}

func TestPickBackend_Latency_PartialFirstTierLoss_KeepsTierBoundary(t *testing.T) {
	tests := []struct {
		name           string
		near, mid, far time.Duration
	}{
		{name: "relative bound", near: 40 * time.Millisecond, mid: 70 * time.Millisecond, far: 100 * time.Millisecond},
		{name: "absolute floor", near: 300 * time.Microsecond, mid: 2200 * time.Microsecond, far: 4 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prx := newLatencyProxy(t)

			near := addSampledBackend(prx, nearAddr, tt.near)
			mid := addSampledBackend(prx, midAddr, tt.mid)
			addSampledBackend(prx, farAddr, tt.far)

			prx.recordFailure(near, errors.New("refused"))
			prx.recordFailure(near, errors.New("refused"))
			require.False(t, near.healthy.Load())

			// A later sample rebuilds the tiers; the unhealthy member must
			// still anchor its tier.
			prx.recordRTT(mid, tt.mid)

			counts := pickCounts(t, prx)
			assert.Equal(t, pickRounds, counts[midAddr],
				"a pickable first tier member keeps the lower tier out, even after the best member is lost")
		})
	}
}

func TestPickBackend_Latency_WholeMultiMemberFirstTierLost_FallsBackToNextTier(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, time.Millisecond)
	nearToo := addSampledBackend(prx, near2, 1500*time.Microsecond)
	addSampledBackend(prx, midAddr, 20*time.Millisecond)
	addSampledBackend(prx, farAddr, 100*time.Millisecond)

	for _, bck := range []*backend{near, nearToo} {
		prx.recordFailure(bck, errors.New("refused"))
		prx.recordFailure(bck, errors.New("refused"))
	}

	counts := pickCounts(t, prx)
	assert.Equal(t, pickRounds, counts[midAddr], "the next tier takes every pick, the one below it none")
}

func TestRecordRTT_HysteresisHoldsInLowerTier(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, midAddr, 10*time.Millisecond)
	edge := addSampledBackend(prx, near2, 15*time.Millisecond)
	addSampledBackend(prx, farAddr, 100*time.Millisecond)

	prx.recordFailure(near, errors.New("refused"))
	prx.recordFailure(near, errors.New("refused"))
	require.Equal(t, 1, tierOf(prx, edge), "the second tier is anchored on mid")

	for range 50 {
		prx.recordRTT(edge, 22*time.Millisecond)
		require.Equal(t, 1, tierOf(prx, edge), "a second tier member between the join and leave limits stays")
	}
}

func TestNew_NilConfig_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "proxy.New: config must not be nil", func() {
		New(nil, zap.NewNop(), metrics.New())
	})
}

func TestRecordRTT_FirstSample_JudgedByJoinLimit(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, 10*time.Millisecond)
	fresh := addSampledBackend(prx, midAddr, 0)

	// Join limit 20ms, leave limit 25ms: membership granted for lack of a
	// sample is not a reason to apply the leave limit to the first one.
	prx.recordRTT(fresh, 22*time.Millisecond)

	assert.Equal(t, 1, tierOf(prx, fresh))
}

func TestPickBackend_Latency_PartialFirstTierDrain_KeepsTierBoundary(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, 40*time.Millisecond)
	addSampledBackend(prx, midAddr, 70*time.Millisecond)
	addSampledBackend(prx, farAddr, 100*time.Millisecond)

	// An open connection keeps the near backend draining for DrainTimeout.
	client, upstream := net.Pipe()
	require.True(t, prx.tryRegister(near, &trackedConn{client: client, upstream: upstream}))

	prx.reconcile([]string{midAddr, farAddr})
	require.True(t, near.draining.Load())

	counts := pickCounts(t, prx)
	assert.Equal(t, pickRounds, counts[midAddr],
		"a draining member keeps anchoring the tier while another member is pickable")
}

func TestPickBackend_Latency_UnsampledBackendDoesNotSuppressFallback(t *testing.T) {
	prx := newLatencyProxy(t)

	near := addSampledBackend(prx, nearAddr, time.Millisecond)
	addSampledBackend(prx, midAddr, 50*time.Millisecond)
	addSampledBackend(prx, near2, 51*time.Millisecond)

	prx.recordFailure(near, errors.New("refused"))
	prx.recordFailure(near, errors.New("refused"))

	addSampledBackend(prx, farAddr, 0)

	counts := pickCounts(t, prx)
	assert.Positive(t, counts[midAddr], "the next tier still takes picks next to an unsampled backend")
	assert.Positive(t, counts[near2], "the next tier still takes picks next to an unsampled backend")
	assert.Positive(t, counts[farAddr], "the unsampled backend shares the tier that takes picks")
}

func TestPickBackend_Latency_AnchorMemberHoveringAtBoundary_DoesNotFlapOthers(t *testing.T) {
	prx := newLatencyProxy(t)

	dead := addSampledBackend(prx, nearAddr, time.Millisecond)
	edge := addSampledBackend(prx, near2, 2500*time.Microsecond)
	addSampledBackend(prx, midAddr, 6*time.Millisecond)
	addSampledBackend(prx, farAddr, 50*time.Millisecond)

	prx.recordFailure(dead, errors.New("refused"))
	prx.recordFailure(dead, errors.New("refused"))

	// The edge average climbs to the 3.75ms leave limit of the dead
	// backend's tier and then hovers around it.
	samples := make([]time.Duration, 0, 42)
	samples = append(samples, 6*time.Millisecond, 6*time.Millisecond)

	for range 20 {
		samples = append(samples, 3600*time.Microsecond, 3900*time.Microsecond)
	}

	transitions := 0
	wasPicked := pickCounts(t, prx)[midAddr] > 0

	for _, sample := range samples {
		prx.recordRTT(edge, sample)

		picked := pickCounts(t, prx)[midAddr] > 0
		if picked != wasPicked {
			transitions++
		}

		wasPicked = picked
	}

	assert.LessOrEqual(t, transitions, 1, "a backend well inside its tier must not flap when another backend hovers at a boundary")
}

func TestRemoveBackend_RebuildsTiersWithoutIt(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, 40*time.Millisecond)
	addSampledBackend(prx, midAddr, 70*time.Millisecond)
	far := addSampledBackend(prx, farAddr, 100*time.Millisecond)
	require.Equal(t, 1, tierOf(prx, far))

	// No open connections: the drain completes and removes the backend.
	prx.reconcile([]string{midAddr, farAddr})

	require.Eventually(t, func() bool { return tierOf(prx, far) == 0 }, 3*time.Second, 10*time.Millisecond,
		"without the removed anchor, far is within the limit of the new best")
}

// holdAtLeaveBand puts edge in the tier of the 10ms anchor and walks its
// average up to about 24ms: above the 20ms join limit, below the 25ms leave
// limit, so only hysteresis keeps it in the tier.
func holdAtLeaveBand(t *testing.T, prx *Proxy) (*backend, *backend) {
	t.Helper()

	anchor := addSampledBackend(prx, nearAddr, 10*time.Millisecond)
	edge := addSampledBackend(prx, midAddr, 15*time.Millisecond)

	for range 50 {
		prx.recordRTT(edge, 24*time.Millisecond)
	}

	rtt, _ := smoothedRTT(prx, edge)
	require.Greater(t, rtt, 20*time.Millisecond)
	require.Equal(t, tierOf(prx, anchor), tierOf(prx, edge))

	return anchor, edge
}

func TestUpdateTiers_NewTierAbove_KeepsHysteresisMemberWithItsGroup(t *testing.T) {
	prx := newLatencyProxy(t)
	anchor, edge := holdAtLeaveBand(t, prx)

	addSampledBackend(prx, near2, 3*time.Millisecond)

	assert.Equal(t, 1, tierOf(prx, anchor), "a closer backend opens a tier above")
	assert.Equal(t, tierOf(prx, anchor), tierOf(prx, edge),
		"renumbered tiers must not split a member held by hysteresis from its group")
}

func TestUpdateTiers_TierAboveRemoved_KeepsHysteresisMemberWithItsGroup(t *testing.T) {
	prx := newLatencyProxy(t)

	top := addSampledBackend(prx, near2, 3*time.Millisecond)
	anchor, edge := holdAtLeaveBand(t, prx)
	require.Equal(t, 0, tierOf(prx, top))

	_, _, gen := top.startDrain()
	prx.removeBackend(top, gen)

	assert.Equal(t, 0, tierOf(prx, anchor))
	assert.Equal(t, tierOf(prx, anchor), tierOf(prx, edge),
		"renumbered tiers must not split a member held by hysteresis from its group")
}

func TestUpdateTiers_NewBestJoinsGroup_KeepsHysteresisMember(t *testing.T) {
	prx := newLatencyProxy(t)
	anchor, edge := holdAtLeaveBand(t, prx)

	// 9.9ms becomes the best of the same group: join limit 19.8ms, leave
	// limit 24.75ms, so the held member must stay.
	addSampledBackend(prx, near2, 9900*time.Microsecond)

	assert.Equal(t, 0, tierOf(prx, anchor))
	assert.Equal(t, tierOf(prx, anchor), tierOf(prx, edge),
		"a new best without a previous tier must not strip hysteresis from its group")
}

func TestUpdateTiers_MergingGroups_DoNotPullInNonMembers(t *testing.T) {
	prx := newLatencyProxy(t)

	anchor := addSampledBackend(prx, nearAddr, 10*time.Millisecond)
	joiner := addSampledBackend(prx, midAddr, 21*time.Millisecond)
	outsider := addSampledBackend(prx, farAddr, 24*time.Millisecond)
	require.Equal(t, 1, tierOf(prx, joiner))
	require.Equal(t, 1, tierOf(prx, outsider))

	for range 50 {
		prx.recordRTT(joiner, 19*time.Millisecond)
	}

	require.Equal(t, 0, tierOf(prx, joiner), "the joiner meets the 20ms join limit")
	assert.Equal(t, 1, tierOf(prx, outsider),
		"a backend above the join limit that was never in the tier must not ride in with its old group")
	assert.Equal(t, 0, tierOf(prx, anchor))
}

func TestRecordRTT_SingleOutlier_KeepsBackendInFirstTier(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	bck := addSampledBackend(prx, near2, time.Millisecond)

	// A lost SYN delays a connect by the TCP retransmission timeout.
	prx.recordRTT(bck, time.Second)

	assert.Equal(t, 0, tierOf(prx, bck), "one slow check must not move a backend to a far tier")
}

func TestRecordRTT_SustainedIncrease_LeavesFirstTierWithinThreeSamples(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)
	bck := addSampledBackend(prx, near2, time.Millisecond)

	for range 3 {
		prx.recordRTT(bck, 50*time.Millisecond)
	}

	assert.Equal(t, 1, tierOf(prx, bck), "a lasting latency increase must still move the backend")
}

func TestRecordRTT_SlowFirstSample_RecoversWithinOneSample(t *testing.T) {
	prx := newLatencyProxy(t)

	addSampledBackend(prx, nearAddr, time.Millisecond)

	// The first check of a starting pod can hit a retransmitted SYN; a
	// first sample has no average to cap it against.
	bck := addSampledBackend(prx, near2, time.Second)
	require.Equal(t, 1, tierOf(prx, bck))

	prx.recordRTT(bck, time.Millisecond)

	assert.Equal(t, 0, tierOf(prx, bck), "one normal sample must undo a slow first sample")
}
