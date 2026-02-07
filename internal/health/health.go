package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

const readHeaderTimeout = 10 * time.Second

// Checker reports whether the system is healthy.
type Checker interface {
	Healthy() (bool, error)
}

// Server serves HTTP health-check endpoints.
type Server struct {
	httpServer *http.Server
	checker    Checker
	logger     *zap.Logger
	listener   net.Listener
}

// NewServer creates a health Server bound to the given address and port.
func NewServer(bindAddress string, port int, checker Checker, logger *zap.Logger) *Server {
	srv := &Server{
		checker: checker,
		logger:  logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealthz)
	mux.HandleFunc("/readyz", srv.handleReadyz)

	srv.httpServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", bindAddress, port),
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return srv
}

// ServeHTTP delegates to the internal mux so the server can be used with httptest.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpServer.Handler.ServeHTTP(w, r)
}

// Start begins listening and serving. It blocks until the server is shut down.
// Returns nil on graceful shutdown.
func (s *Server) Start() error {
	lc := net.ListenConfig{}

	var err error

	s.listener, err = lc.Listen(context.Background(), "tcp", s.httpServer.Addr)
	if err != nil {
		return errors.Wrap(err, "health server listen")
	}

	s.logger.Info("health server started", zap.String("addr", s.listener.Addr().String()))

	serveErr := s.httpServer.Serve(s.listener)
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}

	return errors.Wrap(serveErr, "health server serve")
}

// Shutdown gracefully stops the health server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("health server shutting down")

	shutdownErr := s.httpServer.Shutdown(ctx)
	if shutdownErr != nil {
		return errors.Wrap(shutdownErr, "health server shutdown")
	}

	return nil
}

// Addr returns the listener address once Start has been called.
// Returns an empty string if the server has not started.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok\n")
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	healthy, err := s.checker.Healthy()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "not ready: %s\n", err.Error())

		return
	}

	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "not ready: health check failed\n")

		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok\n")
}
