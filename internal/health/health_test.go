package health_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/lexfrei/extractedprism/internal/health"
)

type mockChecker struct {
	healthy bool
	err     error
	alive   bool
}

func (m *mockChecker) Healthy() (bool, error) {
	return m.healthy, m.err
}

func (m *mockChecker) Alive() bool {
	return m.alive
}

// mockLiveness implements only LivenessChecker, used to verify that
// /healthz uses the liveness parameter and not the checker parameter.
type mockLiveness struct {
	alive bool
}

func (m *mockLiveness) Alive() bool {
	return m.alive
}

func newTestLogger() *zap.Logger {
	return zap.NewNop()
}

func TestHealthz_Alive_Returns200(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok\n", rec.Body.String())
}

func TestHealthz_NotAlive_Returns503(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: false}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "not alive\n", rec.Body.String())
}

func TestHealthz_UsesSeparateLivenessChecker(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	liveness := &mockLiveness{alive: false}
	srv := health.NewServer("127.0.0.1", 0, checker, liveness, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"/healthz must use the liveness parameter, not the checker")
	assert.Equal(t, "not alive\n", rec.Body.String())
}

func TestHealthz_NotAlive_ContentType(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: false}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
}

func TestReadyz_HealthyReturns200(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok\n", rec.Body.String())
}

func TestReadyz_UnhealthyReturns503(t *testing.T) {
	checker := &mockChecker{healthy: false, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "not ready: health check failed\n", rec.Body.String())
}

func TestReadyz_ErrorReturns503WithGenericMessage(t *testing.T) {
	checker := &mockChecker{
		healthy: false,
		alive:   true,
		err:     errors.New("dial tcp 10.0.0.1:6443: connection refused"),
	}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "not ready\n", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "10.0.0.1",
		"response must not leak internal IP addresses")
	assert.NotContains(t, rec.Body.String(), "connection refused",
		"response must not leak internal error details")
}

func TestReadyz_ErrorLogsDetails(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	internalErr := errors.New("dial tcp 10.0.0.1:6443: connection refused")
	checker := &mockChecker{healthy: false, alive: true, err: internalErr}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, logger)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	require.Equal(t, 1, logs.Len(), "expected exactly one log entry")

	entry := logs.All()[0]
	assert.Equal(t, zap.WarnLevel, entry.Level)
	assert.Equal(t, "readiness check failed", entry.Message)

	errField := entry.ContextMap()["error"]
	assert.Contains(t, errField, "connection refused",
		"full error must be logged for debugging")
}

const expectedContentType = "text/plain; charset=utf-8"

func TestHealthz_ContentType(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
}

func TestReadyz_ContentType(t *testing.T) {
	tests := []struct {
		name    string
		checker *mockChecker
	}{
		{name: "healthy", checker: &mockChecker{healthy: true, alive: true}},
		{name: "unhealthy", checker: &mockChecker{healthy: false, alive: true}},
		{name: "error", checker: &mockChecker{healthy: false, alive: true, err: errors.New("fail")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := health.NewServer("127.0.0.1", 0, tt.checker, tt.checker, newTestLogger())

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	paths := []string{"/healthz", "/readyz"}
	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, path := range paths {
		for _, method := range methods {
			t.Run(path+"/"+method, func(t *testing.T) {
				req := httptest.NewRequest(method, path, nil)
				rec := httptest.NewRecorder()

				srv.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
				assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
				assert.Contains(t, rec.Body.String(), "method not allowed")
			})
		}
	}
}

func TestOptions_Returns204WithAllow(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	paths := []string{"/healthz", "/readyz"}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, path, nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
			assert.Empty(t, rec.Body.String())
		})
	}
}

func TestHead_Returns200(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	// Use httptest.Server for real HTTP round-trip so net/http
	// suppresses body for HEAD requests per RFC 9110.
	ts := httptest.NewServer(srv)
	defer ts.Close()

	paths := []string{"/healthz", "/readyz"}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			reqCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, ts.URL+path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			body, readErr := io.ReadAll(resp.Body)
			require.NoError(t, readErr)
			assert.Empty(t, body, "HEAD response must have no body")
		})
	}
}

func TestUnknownPathReturns404(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	paths := []string{"/", "/health", "/ready", "/metrics", "/foo"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestNewServer_SetsAllTimeouts(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	httpSrv := srv.HTTPServer()
	assert.NotZero(t, httpSrv.ReadHeaderTimeout, "ReadHeaderTimeout must be set")
	assert.NotZero(t, httpSrv.ReadTimeout, "ReadTimeout must be set")
	assert.NotZero(t, httpSrv.WriteTimeout, "WriteTimeout must be set")
	assert.NotZero(t, httpSrv.IdleTimeout, "IdleTimeout must be set")
	assert.Greater(t, httpSrv.ReadTimeout, httpSrv.ReadHeaderTimeout,
		"ReadTimeout must be greater than ReadHeaderTimeout")
}

func TestShutdown(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("127.0.0.1", 0, checker, checker, newTestLogger())

	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.Start(context.Background())
	}()

	// Poll until the server is listening.
	var addr string

	require.Eventually(t, func() bool {
		addr = srv.Addr()

		return addr != ""
	}, 2*time.Second, 10*time.Millisecond, "server did not start in time")

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/healthz", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "ok\n", string(body))

	// Shut down the server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = srv.Shutdown(ctx)
	require.NoError(t, err)

	// Start should have returned nil (graceful shutdown via ErrServerClosed).
	select {
	case startErr := <-errCh:
		assert.NoError(t, startErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Start to return")
	}
}

func TestStart_AcceptsContext(t *testing.T) {
	// Verify Start passes the context to Listen. Use a hostname
	// (not an IP) so Listen must perform DNS resolution, which
	// respects context cancellation. With a pre-cancelled context
	// the lookup returns "operation was canceled" immediately.
	// If Start ignores the context, DNS resolution would succeed
	// and the error would be "bind: can't assign requested address"
	// — a different error proving the context was not used.
	checker := &mockChecker{healthy: true, alive: true}
	srv := health.NewServer("localhost", 0, checker, checker, newTestLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := srv.Start(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewServer_NilChecker_Panics(t *testing.T) {
	liveness := &mockLiveness{alive: true}

	assert.Panics(t, func() {
		health.NewServer("127.0.0.1", 0, nil, liveness, newTestLogger())
	}, "NewServer must panic when checker is nil")
}

func TestNewServer_NilLiveness_Panics(t *testing.T) {
	checker := &mockChecker{healthy: true, alive: true}

	assert.Panics(t, func() {
		health.NewServer("127.0.0.1", 0, checker, nil, newTestLogger())
	}, "NewServer must panic when liveness is nil")
}
