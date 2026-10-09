package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestHandler_ExposesAllMetrics(t *testing.T) {
	m := metrics.New()

	m.ConnOpened()
	m.ConnOpened()
	m.ConnClosed()
	m.ConnError("192.0.2.1:6443")
	m.DiscoveryUpdate("static")
	m.DiscoveryError("kubernetes")
	m.SetUpstreams(2, 3)
	m.SetBackendHealth("192.0.2.1:6443", true)
	m.SetBackendHealth("192.0.2.2:6443", false)

	body := scrape(t, m)

	assert.Contains(t, body, "extractedprism_connections_active 1")
	assert.Contains(t, body, "extractedprism_connections_total 2")
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.1:6443"} 1`)
	assert.Contains(t, body, `extractedprism_discovery_updates_total{provider="static"} 1`)
	assert.Contains(t, body, `extractedprism_discovery_errors_total{provider="kubernetes"} 1`)
	assert.Contains(t, body, "extractedprism_upstreams_active 2")
	assert.Contains(t, body, "extractedprism_upstreams_total 3")
	assert.Contains(t, body, `extractedprism_health_check_status{upstream="192.0.2.1:6443"} 1`)
	assert.Contains(t, body, `extractedprism_health_check_status{upstream="192.0.2.2:6443"} 0`)
}

func TestMetrics_ZeroValuesBeforeAnyEvent(t *testing.T) {
	m := metrics.New()

	body := scrape(t, m)

	// Gauges and counters without labels must be exported at zero.
	assert.Contains(t, body, "extractedprism_connections_active 0")
	assert.Contains(t, body, "extractedprism_connections_total 0")
	assert.Contains(t, body, "extractedprism_upstreams_active 0")
	assert.Contains(t, body, "extractedprism_upstreams_total 0")

	// Labeled series must NOT exist before their first event: a pre-created
	// zero series for labels we have never seen would be a lie about state.
	assert.NotContains(t, body, "extractedprism_connection_errors_total{")
	assert.NotContains(t, body, "extractedprism_health_check_status{")
}

func TestConnOpenClose_Balances(t *testing.T) {
	m := metrics.New()

	m.ConnOpened()
	m.ConnClosed()

	body := scrape(t, m)
	assert.Contains(t, body, "extractedprism_connections_active 0")
	assert.Contains(t, body, "extractedprism_connections_total 1")
}

// The metrics layer is a thin wrapper and does NOT clamp: an unbalanced
// ConnClosed would surface as a negative gauge, making the caller's bug
// visible instead of masking it. The exactly-once contract is enforced
// structurally in the proxy layer (a connection is unregistered by map
// removal) and pinned by proxy tests.

func TestRemoveBackend_DeletesHealthSeries(t *testing.T) {
	m := metrics.New()

	m.SetBackendHealth("192.0.2.1:6443", true)
	m.RemoveBackend("192.0.2.1:6443")

	body := scrape(t, m)
	assert.NotContains(t, body, "extractedprism_health_check_status{")
}

func TestRemoveBackend_DeletesConnErrorSeries(t *testing.T) {
	m := metrics.New()

	m.ConnError("192.0.2.1:6443")
	m.ConnError("192.0.2.2:6443")
	m.RemoveBackend("192.0.2.1:6443")

	body := scrape(t, m)
	assert.NotContains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.1:6443"}`)
	assert.Contains(t, body, `extractedprism_connection_errors_total{upstream="192.0.2.2:6443"} 1`,
		"other upstreams keep their series")
}

func TestNew_IsolatedRegistries(t *testing.T) {
	// Two instances must not share state: no cross-talk and no duplicate
	// registration panic on the default registry.
	m1 := metrics.New()
	m2 := metrics.New()

	m1.ConnOpened()

	body1 := scrape(t, m1)
	body2 := scrape(t, m2)

	assert.Contains(t, body1, "extractedprism_connections_active 1")
	assert.Contains(t, body2, "extractedprism_connections_active 0")
}

func TestSetBackendRTT_ExportsSeconds(t *testing.T) {
	m := metrics.New()

	m.SetBackendRTT("192.0.2.1:6443", 1500*time.Microsecond)

	body := scrape(t, m)
	assert.Contains(t, body, `extractedprism_upstream_rtt_seconds{upstream="192.0.2.1:6443"} 0.0015`)
}

func TestSetBackendTier_ExportsTierIndex(t *testing.T) {
	m := metrics.New()

	m.SetBackendTier("192.0.2.1:6443", 0)
	m.SetBackendTier("192.0.2.2:6443", 2)

	body := scrape(t, m)
	assert.Contains(t, body, `extractedprism_upstream_latency_tier{upstream="192.0.2.1:6443"} 0`)
	assert.Contains(t, body, `extractedprism_upstream_latency_tier{upstream="192.0.2.2:6443"} 2`)
}

func TestRemoveBackend_DeletesLatencySeries(t *testing.T) {
	m := metrics.New()

	m.SetBackendRTT("192.0.2.1:6443", time.Millisecond)
	m.SetBackendTier("192.0.2.1:6443", 0)
	m.SetBackendRTT("192.0.2.2:6443", time.Millisecond)
	m.RemoveBackend("192.0.2.1:6443")

	body := scrape(t, m)
	assert.NotContains(t, body, `extractedprism_upstream_rtt_seconds{upstream="192.0.2.1:6443"}`)
	assert.NotContains(t, body, `extractedprism_upstream_latency_tier{upstream="192.0.2.1:6443"}`)
	assert.Contains(t, body, `extractedprism_upstream_rtt_seconds{upstream="192.0.2.2:6443"}`,
		"other upstreams keep their series")
}
