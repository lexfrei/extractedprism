package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lexfrei/extractedprism/internal/config"
)

func writeServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ca.crt")
	block := &pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(block), 0o600))

	return path
}

func readyzConfig(caFile string) *config.Config {
	cfg := config.NewBaseConfig()
	cfg.HealthCheck = config.HealthCheckReadyz
	cfg.HealthCheckCAFile = caFile

	return cfg
}

func TestReadinessCheck_TCPMode_ReturnsNil(t *testing.T) {
	check, err := readinessCheck(config.NewBaseConfig())

	require.NoError(t, err)
	assert.Nil(t, check)
}

func TestReadinessCheck_MissingCAFile_ReturnsError(t *testing.T) {
	_, err := readinessCheck(readyzConfig(filepath.Join(t.TempDir(), "absent.crt")))

	require.Error(t, err)
}

func TestReadinessCheck_CAWithoutCertificates_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))

	_, err := readinessCheck(readyzConfig(path))

	require.ErrorIs(t, err, errNoCACertificates)
}

func TestReadinessCheck_MissingClientKeyPair_FailsProbeNotStartup(t *testing.T) {
	clientCA := newTestCA(t)
	srv := newClientAuthServer(t, clientCA)

	cfg := readyzConfig(writeServerCA(t, srv))
	cfg.HealthCheckServerName = "example.com"
	cfg.HealthCheckCertFile = filepath.Join(t.TempDir(), "absent.pem")
	cfg.HealthCheckKeyFile = cfg.HealthCheckCertFile

	check, err := readinessCheck(cfg)
	require.NoError(t, err, "a client certificate that does not exist yet must not block startup")

	_, err = getStatus(t, check.Client, srv.URL+"/readyz")
	require.Error(t, err)
}

func TestReadinessCheck_ClientCertificateIsReloaded(t *testing.T) {
	clientCA := newTestCA(t)
	srv := newClientAuthServer(t, clientCA)

	certPath := filepath.Join(t.TempDir(), "client.pem")
	writeClientPair(t, clientCA, "first", certPath)

	cfg := readyzConfig(writeServerCA(t, srv))
	cfg.HealthCheckServerName = "example.com"
	cfg.HealthCheckCertFile = certPath
	cfg.HealthCheckKeyFile = certPath

	check, err := readinessCheck(cfg)
	require.NoError(t, err)

	for _, name := range []string{"first", "second"} {
		writeClientPair(t, clientCA, name, certPath)

		status, probeErr := getStatus(t, check.Client, srv.URL+"/readyz?expect="+name)
		require.NoError(t, probeErr)
		assert.Equal(t, http.StatusOK, status, "probe must present the certificate currently on disk (%s)", name)
	}
}

func TestReadinessCheck_VerifiesUpstreamAgainstCAAndServerName(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := readyzConfig(writeServerCA(t, srv))
	// httptest certificates are issued for example.com.
	cfg.HealthCheckServerName = "example.com"

	check, err := readinessCheck(cfg)
	require.NoError(t, err)

	status, err := getStatus(t, check.Client, srv.URL+"/readyz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)

	cfg.HealthCheckServerName = "kubernetes.default.svc"
	check, err = readinessCheck(cfg)
	require.NoError(t, err)

	_, err = getStatus(t, check.Client, srv.URL+"/readyz")
	require.Error(t, err, "a certificate not valid for the configured server name must be rejected")
}

func TestReadinessCheck_TokenFileIsReReadAndTrimmed(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)

	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("first\n"), 0o600))

	cfg := readyzConfig(writeServerCA(t, srv))
	cfg.HealthCheckTokenFile = tokenPath

	check, err := readinessCheck(cfg)
	require.NoError(t, err)
	require.NotNil(t, check.Token)

	token, err := check.Token()
	require.NoError(t, err)
	assert.Equal(t, "first", token)

	require.NoError(t, os.WriteFile(tokenPath, []byte("second"), 0o600))

	token, err = check.Token()
	require.NoError(t, err)
	assert.Equal(t, "second", token, "a rotated token must be picked up without a restart")
}

func TestReadinessCheck_NoTokenFile_SendsNoToken(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)

	check, err := readinessCheck(readyzConfig(writeServerCA(t, srv)))

	require.NoError(t, err)
	assert.Nil(t, check.Token)
}

func getStatus(t *testing.T, client *http.Client, url string) (int, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)

	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.Wrap(err, "probe readyz")
	}

	resp.Body.Close()

	return resp.StatusCode, nil
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &testCA{cert: cert, key: key, pool: pool}
}

// writeClientPair writes a certificate and its key into one PEM file, the
// layout of the kubelet's rotated client certificate.
func writeClientPair(t *testing.T, ca *testCA, commonName, path string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})...)
	require.NoError(t, os.WriteFile(path, out, 0o600))
}

// newClientAuthServer answers 200 only when the client certificate's common
// name equals the expect query parameter.
func newClientAuthServer(t *testing.T, clientCA *testCA) *httptest.Server {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.PeerCertificates[0].Subject.CommonName != r.URL.Query().Get("expect") {
			w.WriteHeader(http.StatusForbidden)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientCA.pool,
		MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv
}
