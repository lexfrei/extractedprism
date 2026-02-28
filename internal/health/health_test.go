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
}

func (m *mockChecker) Healthy() (bool, error) {
	return m.healthy, m.err
}

func newTestLogger() *zap.Logger {
	return zap.NewNop()
}

func TestHealthz_Returns200(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok\n", rec.Body.String())
}

func TestReadyz_HealthyReturns200(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok\n", rec.Body.String())
}

func TestReadyz_UnhealthyReturns503(t *testing.T) {
	checker := &mockChecker{healthy: false}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "not ready: health check failed\n", rec.Body.String())
}

func TestReadyz_ErrorReturns503WithGenericMessage(t *testing.T) {
	checker := &mockChecker{
		healthy: false,
		err:     errors.New("dial tcp 10.0.0.1:6443: connection refused"),
	}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

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
	checker := &mockChecker{healthy: false, err: internalErr}
	srv := health.NewServer("127.0.0.1", 0, checker, logger)

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
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

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
		{name: "healthy", checker: &mockChecker{healthy: true}},
		{name: "unhealthy", checker: &mockChecker{healthy: false}},
		{name: "error", checker: &mockChecker{healthy: false, err: errors.New("fail")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := health.NewServer("127.0.0.1", 0, tt.checker, newTestLogger())

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
		})
	}
}

func TestHealthz_RejectsNonGetMethods(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/healthz", nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			assert.Contains(t, rec.Header().Get("Allow"), http.MethodGet)
			assert.Contains(t, rec.Header().Get("Allow"), http.MethodHead)
			assert.Equal(t, "method not allowed\n", rec.Body.String())
		})
	}
}

func TestReadyz_RejectsNonGetMethods(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/readyz", nil)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			assert.Contains(t, rec.Header().Get("Allow"), http.MethodGet)
			assert.Contains(t, rec.Header().Get("Allow"), http.MethodHead)
			assert.Equal(t, "method not allowed\n", rec.Body.String())
		})
	}
}

func TestHealthz_HeadMethod(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
}

func TestReadyz_HeadMethodHealthy(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodHead, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
}

func TestReadyz_HeadMethodUnhealthy(t *testing.T) {
	checker := &mockChecker{healthy: false}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	req := httptest.NewRequest(http.MethodHead, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestUnknownPathReturns404(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

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
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	httpSrv := srv.HTTPServer()
	assert.NotZero(t, httpSrv.ReadHeaderTimeout, "ReadHeaderTimeout must be set")
	assert.NotZero(t, httpSrv.ReadTimeout, "ReadTimeout must be set")
	assert.NotZero(t, httpSrv.WriteTimeout, "WriteTimeout must be set")
	assert.NotZero(t, httpSrv.IdleTimeout, "IdleTimeout must be set")
	assert.Greater(t, httpSrv.ReadTimeout, httpSrv.ReadHeaderTimeout,
		"ReadTimeout must be greater than ReadHeaderTimeout")
}

func TestShutdown(t *testing.T) {
	checker := &mockChecker{healthy: true}
	srv := health.NewServer("127.0.0.1", 0, checker, newTestLogger())

	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.Start()
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

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

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
