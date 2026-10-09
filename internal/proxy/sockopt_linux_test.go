//go:build linux

package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

func dialLoopback(t *testing.T) *net.TCPConn {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)

	return tcpConn
}

func readTCPUserTimeout(t *testing.T, conn *net.TCPConn) int {
	t.Helper()

	return readTCPIntOpt(t, conn, unix.TCP_USER_TIMEOUT)
}

func readTCPIntOpt(t *testing.T, conn *net.TCPConn, opt int) int {
	t.Helper()

	raw, err := conn.SyscallConn()
	require.NoError(t, err)

	var (
		value  int
		getErr error
	)

	require.NoError(t, raw.Control(func(fd uintptr) {
		value, getErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, opt)
	}))
	require.NoError(t, getErr)

	return value
}

func TestSetTCPUserTimeout_SetsMilliseconds(t *testing.T) {
	conn := dialLoopback(t)

	require.NoError(t, setTCPUserTimeout(conn, 1500*time.Millisecond))

	assert.Equal(t, 1500, readTCPUserTimeout(t, conn), "kernel option is in milliseconds")
}

func TestSetTCPUserTimeout_ZeroLeavesKernelDefault(t *testing.T) {
	conn := dialLoopback(t)

	require.NoError(t, setTCPUserTimeout(conn, 0))

	assert.Equal(t, 0, readTCPUserTimeout(t, conn), "zero timeout must not touch the socket option")
}

func TestProxy_AcceptedClientConn_GetsDeadPeerBounds(t *testing.T) {
	upstream, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { upstream.Close() })

	go func() {
		for {
			conn, acceptErr := upstream.Accept()
			if acceptErr != nil {
				return
			}

			go func() {
				defer conn.Close()

				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	prx := New(&Config{
		BindAddress: "127.0.0.1",
		DialTimeout: time.Second,
		// Distinct from Go's 15s keepalive defaults, so a side that silently
		// falls back to a default shows up as a mismatch.
		KeepAlivePeriod: 20 * time.Second,
		TCPUserTimeout:  1500 * time.Millisecond,
		HealthInterval:  time.Hour,
		HealthTimeout:   time.Second,
	}, zap.NewNop(), metrics.New())
	require.NoError(t, prx.Start(t.Context(), make(chan []string)))

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = prx.Shutdown(shutCtx)
	})

	bck := newTestBackend(upstream.Addr().String())

	prx.mu.Lock()
	prx.backends[bck.addr] = bck
	prx.mu.Unlock()

	client, err := new(net.Dialer).DialContext(t.Context(), "tcp", prx.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	var accepted, dialed *net.TCPConn

	require.Eventually(t, func() bool {
		bck.mu.Lock()
		defer bck.mu.Unlock()

		for tconn := range bck.conns {
			clientConn, clientOK := tconn.client.(*net.TCPConn)
			upstreamConn, upstreamOK := tconn.upstream.(*net.TCPConn)

			if clientOK && upstreamOK {
				accepted, dialed = clientConn, upstreamConn

				return true
			}
		}

		return false
	}, 3*time.Second, 10*time.Millisecond)

	// A dead client must not pin its upstream connection for the kernel's
	// retransmission timeout: the client side gets the same bound.
	assert.Equal(t, 1500, readTCPUserTimeout(t, accepted))

	// Keepalive on the client side matches the upstream side option by
	// option, compared against the real dialed socket rather than a constant.
	assert.Equal(t, 20, readTCPIntOpt(t, accepted, unix.TCP_KEEPIDLE), "keepalive idle follows the period")

	for _, opt := range []struct {
		name string
		id   int
	}{
		{name: "idle", id: unix.TCP_KEEPIDLE},
		{name: "interval", id: unix.TCP_KEEPINTVL},
		{name: "count", id: unix.TCP_KEEPCNT},
		{name: "user timeout", id: unix.TCP_USER_TIMEOUT},
	} {
		assert.Equal(t, readTCPIntOpt(t, dialed, opt.id), readTCPIntOpt(t, accepted, opt.id),
			"client and upstream sockets must match: %s", opt.name)
	}
}
