package server

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/proxy"
)

var errNoCACertificates = errors.New("health check CA file holds no PEM certificates")

// readinessCheck builds the /readyz probe from the config, or returns nil for
// TCP-only health checking. The CA is loaded once at startup. The client
// certificate and the token file are re-read on every probe, so rotation is
// picked up, and a credential that does not exist yet only fails probes: on a
// joining node the kubelet obtains its client certificate through this proxy.
func readinessCheck(cfg *config.Config) (*proxy.ReadinessCheck, error) {
	if cfg.HealthCheck != config.HealthCheckReadyz {
		return nil, nil //nolint:nilnil // nil check means TCP-only health checking
	}

	tlsCfg, err := readinessTLS(cfg)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		TLSClientConfig:   tlsCfg,
		DisableKeepAlives: true,
		// Probes target upstream IPs directly, never through a proxy.
		Proxy: nil,
	}

	check := &proxy.ReadinessCheck{Client: &http.Client{Transport: transport}}

	if cfg.HealthCheckTokenFile != "" {
		path := cfg.HealthCheckTokenFile
		check.Token = func() (string, error) { return readToken(path) }
	}

	return check, nil
}

func readinessTLS(cfg *config.Config) (*tls.Config, error) {
	caPEM, err := os.ReadFile(cfg.HealthCheckCAFile)
	if err != nil {
		return nil, errors.Wrap(err, "read health check CA file")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.Wrapf(errNoCACertificates, "%s", cfg.HealthCheckCAFile)
	}

	tlsCfg := &tls.Config{
		RootCAs:    pool,
		ServerName: cfg.HealthCheckServerName,
		MinVersion: tls.VersionTLS12,
	}

	if cfg.HealthCheckCertFile != "" {
		certFile, keyFile := cfg.HealthCheckCertFile, cfg.HealthCheckKeyFile
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pair, pairErr := tls.LoadX509KeyPair(certFile, keyFile)
			if pairErr != nil {
				return nil, errors.Wrap(pairErr, "load health check client certificate")
			}

			return &pair, nil
		}
	}

	return tlsCfg, nil
}

func readToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", errors.Wrap(err, "read health check token file")
	}

	return strings.TrimSpace(string(raw)), nil
}
