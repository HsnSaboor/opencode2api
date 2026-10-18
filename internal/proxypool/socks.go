package proxypool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Dialer opens TCP connections through the local sing-box SOCKS5 endpoint,
// authenticating as one profile node ("p<N>") so the multiplexer routes the
// connection through that node's outbound. It satisfies the signature an
// http.Transport DialContext expects.
type Dialer struct {
	// Addr is the multiplexer's SOCKS5 host:port (usually 127.0.0.1:20800).
	Addr string
	// User is the node's SOCKS5 username; Password is the shared pool
	// password (sing-box user passwords are set to "p" by BuildConfig).
	User     string
	Password string
	// Timeout bounds the whole handshake when the caller's context has no
	// deadline of its own. Zero means 10s.
	Timeout time.Duration
}

const socks5DialTimeout = 10 * time.Second

func (d *Dialer) dialTimeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return socks5DialTimeout
}

// DialContext connects to addr ("host:port") through the SOCKS5 proxy.
// The target host is sent as a domain name so the remote node resolves it,
// mirroring how a normal client would reach the upstream from that exit.
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("proxypool: unsupported network %q", network)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("proxypool: invalid address %q: %w", addr, err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return nil, fmt.Errorf("proxypool: invalid port %q", port)
	}

	dialer := &net.Dialer{}
	deadline := time.Now().Add(d.dialTimeout())
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	dialCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	conn, err := dialer.DialContext(dialCtx, "tcp", d.Addr)
	if err != nil {
		return nil, fmt.Errorf("proxypool: dial multiplexer %s: %w", d.Addr, err)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := socks5Handshake(conn, d.User, d.Password, host, portNum); err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Handshake done: clear the short deadline; request contexts bound the
	// connection from here on.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// socks5Handshake performs the SOCKS5 negotiation: method selection
// (username/password auth), RFC 1929 authentication, and one CONNECT.
func socks5Handshake(conn net.Conn, user, password, host string, port int) error {
	// Greeting: SOCKS5, one method, username/password (0x02).
	if _, err := conn.Write([]byte{0x05, 0x01, 0x02}); err != nil {
		return fmt.Errorf("proxypool: socks5 greeting: %w", err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("proxypool: socks5 method reply: %w", err)
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("proxypool: unexpected socks version 0x%02x", reply[0])
	}
	if reply[1] != 0x02 {
		return errors.New("proxypool: multiplexer requires no auth (unexpected)")
	}

	// RFC 1929: version 1, username, password.
	if len(user) > 255 || len(password) > 255 {
		return errors.New("proxypool: socks5 credentials exceed 255 bytes")
	}
	auth := make([]byte, 0, 3+len(user)+len(password))
	auth = append(auth, 0x01, byte(len(user)))
	auth = append(auth, user...)
	auth = append(auth, byte(len(password)))
	auth = append(auth, password...)
	if _, err := conn.Write(auth); err != nil {
		return fmt.Errorf("proxypool: socks5 auth write: %w", err)
	}
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("proxypool: socks5 auth reply: %w", err)
	}
	if reply[1] != 0x00 {
		return errors.New("proxypool: socks5 authentication failed")
	}

	// CONNECT: version, command, reserved, address type + address, port.
	connect := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			connect = append(connect, 0x01)
			connect = append(connect, v4...)
		} else {
			connect = append(connect, 0x04)
			connect = append(connect, ip.To16()...)
		}
	} else {
		if len(host) == 0 || len(host) > 255 {
			return fmt.Errorf("proxypool: target hostname %q out of range", host)
		}
		connect = append(connect, 0x03, byte(len(host)))
		connect = append(connect, host...)
	}
	connect = binary.BigEndian.AppendUint16(connect, uint16(port))
	if _, err := conn.Write(connect); err != nil {
		return fmt.Errorf("proxypool: socks5 connect write: %w", err)
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("proxypool: socks5 connect reply: %w", err)
	}
	if header[1] != 0x00 {
		return fmt.Errorf("proxypool: socks5 connect refused (code 0x%02x)", header[1])
	}
	var tail int
	switch header[3] {
	case 0x01:
		tail = 4 + 2
	case 0x03:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return fmt.Errorf("proxypool: socks5 bound address: %w", err)
		}
		tail = int(length[0]) + 2
	case 0x04:
		tail = 16 + 2
	default:
		return fmt.Errorf("proxypool: socks5 unknown address type 0x%02x", header[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, tail)); err != nil {
		return fmt.Errorf("proxypool: socks5 bound address read: %w", err)
	}
	return nil
}

// Client returns an *http.Client whose every connection goes through the
// multiplexer as node user. The caller owns the client and should call
// CloseIdleConnections when finished so keep-alive sockets do not outlive a
// probe.
func Client(d Dialer) *http.Client {
	transport := &http.Transport{
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          4,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 60 * time.Second}
}
