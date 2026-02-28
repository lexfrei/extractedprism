// Package server orchestrates the load balancer, discovery, and health subsystems.
package server

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/siderolabs/go-loadbalancer/controlplane"
	"github.com/siderolabs/go-loadbalancer/upstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/discovery"
	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
	"github.com/lexfrei/extractedprism/internal/discovery/merged"
	"github.com/lexfrei/extractedprism/internal/discovery/static"
	"github.com/lexfrei/extractedprism/internal/health"
)

const (
	keepAlivePeriod = 30 * time.Second
	tcpUserTimeout  = 30 * time.Second
	shutdownTimeout = 5 * time.Second
	defaultAPIPort  = "6443"

	// UpstreamChBuffer is the buffer size for the channel between the merged
	// discovery provider and the load balancer. A larger buffer absorbs bursts
	// of rapid endpoint updates so the provider does not block when the load
	// balancer is slow to reconcile routes.
	UpstreamChBuffer = 16
)

// healthServer abstracts the health HTTP server for testing.
type healthServer interface {
	Start() error
	Shutdown(ctx context.Context) error
}

// Option configures a Server.
type Option func(*Server)

// WithKubeClient injects a pre-built Kubernetes client for endpoint discovery.
func WithKubeClient(client kubernetes.Interface) Option {
	return func(srv *Server) {
		srv.kubeClient = client
	}
}

// WithHealthServer injects a custom health server implementation (for testing).
func WithHealthServer(hs healthServer) Option {
	return func(srv *Server) {
		srv.healthSrv = hs
	}
}

// Server ties together the load balancer, endpoint discovery, and health checking.
type Server struct {
	cfg        *config.Config
	logger     *zap.Logger
	lbHandle   *controlplane.LoadBalancer
	healthSrv  healthServer
	upstreamCh chan []string
	kubeClient kubernetes.Interface
}

// New creates a Server from the given config.
func New(cfg *config.Config, logger *zap.Logger, opts ...Option) (*Server, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	lbHandle, err := createLoadBalancer(cfg, logger)
	if err != nil {
		return nil, err
	}

	srv := &Server{
		cfg:        cfg,
		logger:     logger,
		lbHandle:   lbHandle,
		upstreamCh: make(chan []string, UpstreamChBuffer),
	}

	for _, opt := range opts {
		opt(srv)
	}

	if srv.healthSrv == nil {
		srv.healthSrv = health.NewServer(cfg.BindAddress, cfg.HealthPort, srv, logger)
	}

	return srv, nil
}

func createLoadBalancer(
	cfg *config.Config,
	logger *zap.Logger,
) (*controlplane.LoadBalancer, error) {
	lbHandle, err := controlplane.NewLoadBalancer(
		cfg.BindAddress, cfg.BindPort, logger,
		controlplane.WithDialTimeout(cfg.HealthTimeout),
		controlplane.WithKeepAlivePeriod(keepAlivePeriod),
		controlplane.WithTCPUserTimeout(tcpUserTimeout),
		controlplane.WithHealthCheckOptions(
			upstream.WithHealthcheckInterval(cfg.HealthInterval),
			upstream.WithHealthcheckTimeout(cfg.HealthTimeout),
		),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create load balancer")
	}

	return lbHandle, nil
}

// Healthy delegates to the load balancer health status.
func (srv *Server) Healthy() (bool, error) {
	healthy, err := srv.lbHandle.Healthy()
	if err != nil {
		return false, errors.Wrap(err, "load balancer health check")
	}

	return healthy, nil
}

// Run starts all subsystems and blocks until ctx is cancelled.
func (srv *Server) Run(ctx context.Context) error {
	err := srv.lbHandle.Start(srv.upstreamCh)
	if err != nil {
		return errors.Wrap(err, "start load balancer")
	}

	defer func() {
		shutErr := srv.lbHandle.Shutdown()
		if shutErr != nil {
			srv.logger.Warn("load balancer shutdown error", zap.Error(shutErr))
		}
	}()

	grp, ctx := errgroup.WithContext(ctx)

	grp.Go(func() error { return srv.runDiscovery(ctx) })
	grp.Go(func() error { return srv.runHealth(ctx) })

	waitErr := grp.Wait()
	if waitErr != nil {
		return errors.Wrap(waitErr, "server run")
	}

	return nil
}

func (srv *Server) runDiscovery(ctx context.Context) error {
	providers, err := srv.buildProviders()
	if err != nil {
		return err
	}

	mp := merged.NewMergedProvider(srv.logger, providers...)

	runErr := mp.Run(ctx, srv.upstreamCh)
	if runErr != nil {
		return errors.Wrap(runErr, "merged provider")
	}

	return nil
}

func (srv *Server) buildProviders() ([]discovery.EndpointProvider, error) {
	staticProv, err := static.NewStaticProvider(srv.cfg.Endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "create static provider")
	}

	providers := []discovery.EndpointProvider{staticProv}

	if srv.cfg.EnableDiscovery {
		kubeProv, kubeErr := srv.buildKubeProvider()
		if kubeErr != nil {
			srv.logger.Warn("kubernetes discovery unavailable, using static endpoints only",
				zap.Error(kubeErr))
		} else {
			providers = append(providers, kubeProv)
		}
	}

	return providers, nil
}

func (srv *Server) buildKubeProvider() (discovery.EndpointProvider, error) {
	client, err := srv.getKubeClient()
	if err != nil {
		return nil, err
	}

	apiPort := ExtractAPIPort(srv.cfg.Endpoints)

	return kubediscovery.NewProvider(client, srv.logger, apiPort), nil
}

func (srv *Server) getKubeClient() (kubernetes.Interface, error) {
	if srv.kubeClient != nil {
		return srv.kubeClient, nil
	}

	return buildInClusterClient(srv.cfg.Endpoints)
}

func buildInClusterClient(endpoints []string) (kubernetes.Interface, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "in-cluster config")
	}

	// Override host to bypass ClusterIP. extractedprism provides the LB
	// that CNI uses, so we cannot depend on cluster networking.
	if len(endpoints) > 0 {
		host := endpoints[0]
		if !strings.HasPrefix(host, "https://") {
			host = "https://" + host
		}

		restCfg.Host = host
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, errors.Wrap(err, "create kubernetes client")
	}

	return client, nil
}

// ExtractAPIPort returns the port from the first endpoint, or "6443" as default.
func ExtractAPIPort(endpoints []string) string {
	if len(endpoints) == 0 {
		return defaultAPIPort
	}

	_, port, err := net.SplitHostPort(endpoints[0])
	if err != nil {
		return defaultAPIPort
	}

	return port
}

func (srv *Server) runHealth(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.healthSrv.Start()
	}()

	select {
	case <-ctx.Done():
		return srv.shutdownHealth(ctx)
	case err := <-errCh:
		if err != nil {
			return errors.Wrap(err, "health server start")
		}

		return errors.New("health server exited unexpectedly")
	}
}

func (srv *Server) shutdownHealth(_ context.Context) error {
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	//nolint:contextcheck // parent ctx is done; fresh timeout needed for graceful shutdown
	shutErr := srv.healthSrv.Shutdown(shutCtx)
	if shutErr != nil {
		return errors.Wrap(shutErr, "health server shutdown")
	}

	return nil
}
