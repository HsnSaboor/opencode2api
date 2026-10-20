package proxypool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// writeProfile materializes an outbounds document in a temp dir.
func writeProfile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	return path
}

// The loader must admit exactly the outbounds that can serve as a general
// exit: direct/block/dns handlers, entries without an endpoint, and xhttp
// transports the installed sing-box cannot dial are skipped, while the
// surviving nodes take sequential indexes and p<N> users. A string port is
// accepted because profile converters emit both shapes.
func TestLoadProfileFiltersUnsupported(t *testing.T) {
	path := writeProfile(t, `{
	  "outbounds": [
	    {"type": "direct", "tag": "direct"},
	    {"type": "block", "tag": "block"},
	    {"type": "dns", "tag": "dns"},
	    {"type": "vless", "tag": "xhttp-node", "server": "a.example.com", "server_port": 443, "transport": {"type": "xhttp"}},
	    {"type": "vless", "tag": "vless-node", "server": "b.example.com", "server_port": 443},
	    {"type": "trojan", "tag": "no-server", "server_port": 443},
	    {"type": "shadowsocks", "tag": "ss-node", "server": "c.example.com", "server_port": 8388},
	    {"type": "vmess", "tag": "string-port", "server": "d.example.com", "server_port": "8443"},
	    {"type": "trojan", "tag": "zero-port", "server": "e.example.com", "server_port": 0}
	  ]
	}`)
	profile, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	wantTags := []string{"vless-node", "ss-node", "string-port"}
	if len(profile.Nodes) != len(wantTags) {
		t.Fatalf("nodes = %d, want %d (%+v)", len(profile.Nodes), len(wantTags), profile.Nodes)
	}
	for i, node := range profile.Nodes {
		if node.Tag != wantTags[i] {
			t.Errorf("node %d tag = %q, want %q", i, node.Tag, wantTags[i])
		}
		if node.Index != i {
			t.Errorf("node %d index = %d, want %d", i, node.Index, i)
		}
		if want := fmt.Sprintf("p%d", i); node.User != want {
			t.Errorf("node %d user = %q, want %q", i, node.User, want)
		}
	}
	if profile.Nodes[2].Port != 8443 {
		t.Errorf("string port parsed as %d, want 8443", profile.Nodes[2].Port)
	}
	if len(profile.Fingerprint) != 64 {
		t.Errorf("fingerprint = %q, want 64 hex chars", profile.Fingerprint)
	}
}

// BuildConfig must map every node to one SOCKS5 user and one auth_user
// routing rule, clone the raw outbound without domain_strategy (removed in
// sing-box 1.12+), and append the direct fallback outbound.
func TestBuildConfigMapsNodesToUsers(t *testing.T) {
	nodes := []Node{
		{Index: 0, Tag: "n0", Type: "vless", User: "p0", Server: "a.example.com", Port: 443, Raw: map[string]any{
			"type": "vless", "tag": "ignored", "server": "a.example.com", "server_port": 443.0,
			"domain_strategy": "prefer_ipv4", "uuid": "x",
		}},
		{Index: 1, Tag: "n1", Type: "trojan", User: "p1", Server: "b.example.com", Port: 8443, Raw: map[string]any{
			"type": "trojan", "server": "b.example.com", "server_port": 8443.0, "password": "x",
		}},
	}
	mux := NewMultiplexer(MultiplexerConfig{SocksPort: 21234, LogLevel: "debug"}, nodes, nil)
	raw, err := mux.BuildConfig("/tmp/proxypool-test.log")
	if err != nil {
		t.Fatalf("BuildConfig: %v", err)
	}
	var doc struct {
		Inbounds []struct {
			Type   string `json:"type"`
			Listen string `json:"listen"`
			Port   int    `json:"listen_port"`
			Users  []struct {
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"users"`
		} `json:"inbounds"`
		Outbounds []map[string]any `json:"outbounds"`
		Route     struct {
			Rules []struct {
				AuthUser []string `json:"auth_user"`
				Outbound string   `json:"outbound"`
			} `json:"rules"`
		} `json:"route"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	if len(doc.Inbounds) != 1 || doc.Inbounds[0].Type != "socks" || doc.Inbounds[0].Listen != "127.0.0.1" || doc.Inbounds[0].Port != 21234 {
		t.Fatalf("unexpected inbound: %+v", doc.Inbounds)
	}
	if len(doc.Inbounds[0].Users) != 2 {
		t.Fatalf("users = %d, want 2", len(doc.Inbounds[0].Users))
	}
	for i, user := range doc.Inbounds[0].Users {
		if want := fmt.Sprintf("p%d", i); user.Username != want || user.Password != ProxyPassword {
			t.Errorf("user %d = %q/%q, want %q/%q", i, user.Username, user.Password, want, ProxyPassword)
		}
	}
	if len(doc.Route.Rules) != 2 {
		t.Fatalf("rules = %d, want 2", len(doc.Route.Rules))
	}
	for i, rule := range doc.Route.Rules {
		if len(rule.AuthUser) != 1 || rule.AuthUser[0] != fmt.Sprintf("p%d", i) || rule.Outbound != fmt.Sprintf("n%d", i) {
			t.Errorf("rule %d = %+v, want p%d -> n%d", i, rule, i, i)
		}
	}
	if len(doc.Outbounds) != 3 {
		t.Fatalf("outbounds = %d, want 3 (2 nodes + direct)", len(doc.Outbounds))
	}
	if _, present := doc.Outbounds[0]["domain_strategy"]; present {
		t.Error("domain_strategy must be stripped from outbounds")
	}
	if doc.Outbounds[0]["tag"] != "n0" || doc.Outbounds[0]["type"] != "vless" {
		t.Errorf("outbound 0 = %+v, want tag n0 type vless", doc.Outbounds[0])
	}
	if doc.Outbounds[2]["type"] != "direct" || doc.Outbounds[2]["tag"] != "direct" {
		t.Errorf("fallback outbound = %+v, want direct", doc.Outbounds[2])
	}
}

// startEchoServer runs a TCP echo endpoint a SOCKS5 CONNECT can target.
func startEchoServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String()
}

// startSocksServer runs a minimal RFC 1929-authenticated SOCKS5 CONNECT
// server and reports the CONNECT targets it served.
func startSocksServer(t *testing.T, user, password string) (string, chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("socks listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	targets := make(chan string, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveSocks5(conn, user, password, targets)
		}
	}()
	return listener.Addr().String(), targets
}

func serveSocks5(conn net.Conn, user, password string, targets chan<- string) {
	defer conn.Close()
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil || header[0] != 0x05 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x02}); err != nil {
		return
	}
	auth := make([]byte, 2)
	if _, err := io.ReadFull(conn, auth); err != nil || auth[0] != 0x01 {
		return
	}
	name := make([]byte, int(auth[1]))
	if _, err := io.ReadFull(conn, name); err != nil {
		return
	}
	pwLen := make([]byte, 1)
	if _, err := io.ReadFull(conn, pwLen); err != nil {
		return
	}
	pass := make([]byte, int(pwLen[0]))
	if _, err := io.ReadFull(conn, pass); err != nil {
		return
	}
	if string(name) != user || string(pass) != password {
		_, _ = conn.Write([]byte{0x01, 0x01})
		return
	}
	if _, err := conn.Write([]byte{0x01, 0x00}); err != nil {
		return
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil || req[0] != 0x05 || req[1] != 0x01 {
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	case 0x03:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return
		}
		addr := make([]byte, int(length[0]))
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = string(addr)
	case 0x04:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	default:
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	target := net.JoinHostPort(host, strconv.Itoa(port))
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		_, _ = conn.Write([]byte{0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0}); err != nil {
		return
	}
	select {
	case targets <- target:
	default:
	}
	// Propagate the client's EOF to the upstream by closing it when the
	// client-to-upstream copy ends; without this the return copy would
	// block on an upstream that never learns the client left.
	go func() {
		defer upstream.Close()
		_, _ = io.Copy(upstream, conn)
	}()
	_, _ = io.Copy(conn, upstream)
}

// The dialer must negotiate username/password auth, CONNECT the requested
// host:port, and expose a working bidirectional stream.
func TestSocksDialerRoundTrip(t *testing.T) {
	echoAddr := startEchoServer(t)
	socksAddr, targets := startSocksServer(t, "p7", ProxyPassword)
	dialer := Dialer{Addr: socksAddr, User: "p7", Password: ProxyPassword, Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", echoAddr)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("echo = %q, want %q", buf, "hello")
	}
	select {
	case target := <-targets:
		if target != echoAddr {
			t.Fatalf("CONNECT target = %q, want %q", target, echoAddr)
		}
	case <-time.After(time.Second):
		t.Fatal("server saw no CONNECT")
	}
}

// A wrong password must fail the handshake instead of silently dialing.
func TestSocksDialerRejectsBadAuth(t *testing.T) {
	socksAddr, _ := startSocksServer(t, "p7", ProxyPassword)
	dialer := Dialer{Addr: socksAddr, User: "p7", Password: "wrong", Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := dialer.DialContext(ctx, "tcp", "127.0.0.1:1")
	if err == nil {
		t.Fatal("DialContext with wrong password succeeded")
	}
	if !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("error = %v, want authentication failure", err)
	}
}

// The address parser must reject non-tcp networks and malformed targets
// before touching the socket.
func TestSocksDialerRejectsBadTargets(t *testing.T) {
	dialer := Dialer{Addr: "127.0.0.1:1", User: "p0", Password: ProxyPassword}
	ctx := context.Background()
	if _, err := dialer.DialContext(ctx, "udp", "127.0.0.1:53"); err == nil {
		t.Fatal("udp dial accepted")
	}
	if _, err := dialer.DialContext(ctx, "tcp", "no-port"); err == nil {
		t.Fatal("port-less target accepted")
	}
	if _, err := dialer.DialContext(ctx, "tcp", "127.0.0.1:0"); err == nil {
		t.Fatal("zero port accepted")
	}
}

// NewMultiplexer applies defaults so a minimal config still produces a
// runnable command line.
func TestMultiplexerDefaults(t *testing.T) {
	mux := NewMultiplexer(MultiplexerConfig{SocksPort: 21000}, nil, nil)
	if mux.cfg.Binary != "sing-box" || mux.cfg.LogLevel != "warn" {
		t.Fatalf("defaults = %q/%q, want sing-box/warn", mux.cfg.Binary, mux.cfg.LogLevel)
	}
	if got := mux.SocksAddr(); got != "127.0.0.1:21000" {
		t.Fatalf("SocksAddr = %q", got)
	}
}

// A supervised spawn must prepare its own state dir and config: the gateway
// starts the multiplexer through Supervise without ever calling Start, so a
// spawn that cannot write its log would otherwise fail forever.
func TestSpawnPreparesStateDir(t *testing.T) {
	dir := t.TempDir()
	nodes := []Node{{Index: 0, User: "p0", Type: "vless", Server: "a.example.com", Port: 443, Raw: map[string]any{
		"type": "vless", "server": "a.example.com", "server_port": 443.0,
	}}}
	mux := NewMultiplexer(MultiplexerConfig{Binary: "/bin/true", SocksPort: 21399, StateDir: dir}, nodes, nil)
	err := mux.spawn(context.Background())
	if err == nil {
		t.Fatal("spawn with a process that never binds the SOCKS port must fail")
	}
	if strings.Contains(err.Error(), "open log") {
		t.Fatalf("spawn did not prepare the state dir: %v", err)
	}
	if _, statErr := os.Stat(mux.configPath()); statErr != nil {
		t.Fatalf("config not written on spawn: %v", statErr)
	}
	if _, statErr := os.Stat(mux.logPath()); statErr != nil {
		t.Fatalf("log not created on spawn: %v", statErr)
	}
}
