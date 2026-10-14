// Package metrics provides the Prometheus metric set for extractedprism.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "extractedprism"

	labelUpstream = "upstream"
	labelProvider = "provider"
)

// Metrics holds the extractedprism metric set. Each instance owns a private
// registry, so multiple instances (e.g. in tests) never share state.
type Metrics struct {
	registry *prometheus.Registry

	connActive       prometheus.Gauge
	connTotal        prometheus.Counter
	connErrors       *prometheus.CounterVec
	discoveryUpdates *prometheus.CounterVec
	discoveryErrors  *prometheus.CounterVec
	upstreamsActive  prometheus.Gauge
	upstreamsTotal   prometheus.Gauge
	backendHealth    *prometheus.GaugeVec
	backendRTT       *prometheus.GaugeVec
	backendTier      *prometheus.GaugeVec
	backendReady     *prometheus.GaugeVec
}

// New creates a Metrics with all series registered on a private registry.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		connActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_active",
			Help:      "Active TCP connections passing through the proxy.",
		}),
		connTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_total",
			Help:      "TCP connections established to upstreams since start.",
		}),
		connErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connection_errors_total",
			Help:      "Connection errors by upstream, or \"none\" when no upstream was available. Failures caused by proxy shutdown are not counted. The series of an upstream is deleted once it is drained and removed, and failures reported after that are not counted until the address is added again.",
		}, []string{labelUpstream}),
		discoveryUpdates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "discovery_updates_total",
			Help:      "Endpoint list updates received, by discovery provider.",
		}, []string{labelProvider}),
		discoveryErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "discovery_errors_total",
			Help:      "Discovery errors by provider: provider failures, failed Watch calls, watch error events and failed re-lists. A watch stream ending, including on a dropped connection, and 410 Gone expiry are not counted.",
		}, []string{labelProvider}),
		upstreamsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstreams_active",
			Help:      "Upstreams currently eligible for new connections (healthy and not draining).",
		}),
		//nolint:promlinter // the "_total" suffix on a gauge breaks Prometheus
		// naming convention, but the name is pinned by the metrics spec
		upstreamsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstreams_total",
			Help:      "Total known upstreams, including unhealthy and draining ones.",
		}),
		backendHealth: newUpstreamGaugeVec("health_check_status",
			"Upstream health state after the consecutive-failure threshold, fed by health checks and client dials (1 healthy, 0 unhealthy). Health checks stop while an upstream drains, so the value holds until it is removed or re-added."),
		backendRTT: newUpstreamGaugeVec("upstream_rtt_seconds",
			"Smoothed connect time of successful health checks, including name resolution for hostname endpoints. Only exported with --upstream-selection=latency, after the first successful check."),
		backendTier: newUpstreamGaugeVec("upstream_latency_tier",
			"Latency tier of the upstream, 0 being the closest group. New connections go to the lowest tier with a healthy, non-draining upstream (with --health-check=readyz, a ready one while any is ready), together with upstreams that have no sample yet. Only exported with --upstream-selection=latency, after the first successful check."),
		backendReady: newUpstreamGaugeVec("upstream_ready",
			"Upstream readiness after the consecutive-failure threshold of /readyz probes (1 ready, 0 not ready). New connections prefer ready upstreams, and an unready upstream loses its connections while another is ready. Only exported with --health-check=readyz, after the first probe."),
	}

	m.register()

	return m
}

func newUpstreamGaugeVec(name, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      name,
		Help:      help,
	}, []string{labelUpstream})
}

// Handler returns an HTTP handler exposing the registry in Prometheus format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ConnOpened records an accepted connection.
func (m *Metrics) ConnOpened() {
	m.connActive.Inc()
	m.connTotal.Inc()
}

// ConnClosed records a finished connection. Callers must guarantee
// one ConnClosed per ConnOpened; the proxy layer enforces this via
// connection registration.
func (m *Metrics) ConnClosed() {
	m.connActive.Dec()
}

// ConnError records a failed connection attempt for the given upstream.
// Use "none" when no upstream was available to dial.
func (m *Metrics) ConnError(upstream string) {
	m.connErrors.WithLabelValues(upstream).Inc()
}

// DiscoveryUpdate records an endpoint list update from the named provider.
func (m *Metrics) DiscoveryUpdate(provider string) {
	m.discoveryUpdates.WithLabelValues(provider).Inc()
}

// DiscoveryError records a failure of the named discovery provider.
func (m *Metrics) DiscoveryError(provider string) {
	m.discoveryErrors.WithLabelValues(provider).Inc()
}

// SetUpstreams sets the active (pickable) and total (known) upstream counts.
func (m *Metrics) SetUpstreams(active, total int) {
	m.upstreamsActive.Set(float64(active))
	m.upstreamsTotal.Set(float64(total))
}

// SetBackendHealth records the last health check result for an upstream.
func (m *Metrics) SetBackendHealth(upstream string, healthy bool) {
	value := 0.0
	if healthy {
		value = 1
	}

	m.backendHealth.WithLabelValues(upstream).Set(value)
}

// RemoveBackend deletes every per-upstream series of a removed upstream so
// dead series do not accumulate as endpoints change.
func (m *Metrics) RemoveBackend(upstream string) {
	m.backendHealth.DeleteLabelValues(upstream)
	m.connErrors.DeleteLabelValues(upstream)
	m.backendRTT.DeleteLabelValues(upstream)
	m.backendTier.DeleteLabelValues(upstream)
	m.backendReady.DeleteLabelValues(upstream)
}

// SetBackendRTT records the smoothed health check connect time of an upstream.
func (m *Metrics) SetBackendRTT(upstream string, rtt time.Duration) {
	m.backendRTT.WithLabelValues(upstream).Set(rtt.Seconds())
}

// SetBackendTier records the latency tier of an upstream.
func (m *Metrics) SetBackendTier(upstream string, tier int) {
	m.backendTier.WithLabelValues(upstream).Set(float64(tier))
}

// SetBackendReady records the readiness state of an upstream.
func (m *Metrics) SetBackendReady(upstream string, ready bool) {
	value := 0.0
	if ready {
		value = 1
	}

	m.backendReady.WithLabelValues(upstream).Set(value)
}

func (m *Metrics) register() {
	m.registry.MustRegister(
		m.connActive,
		m.connTotal,
		m.connErrors,
		m.discoveryUpdates,
		m.discoveryErrors,
		m.upstreamsActive,
		m.upstreamsTotal,
		m.backendHealth,
		m.backendRTT,
		m.backendTier,
		m.backendReady,
	)
}
