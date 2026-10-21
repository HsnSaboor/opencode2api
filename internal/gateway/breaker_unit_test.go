package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

func restoreBreakerWaitLimit(t *testing.T) {
	t.Helper()
	old := anonymousBreakerWaitLimit
	t.Cleanup(func() { anonymousBreakerWaitLimit = old })
}

func TestAnonymousBreakerTripsAfterConsecutiveTransportFailures(t *testing.T) {
	pool := &anonymousPool{}
	for i := 0; i < anonymousBreakerFailures; i++ {
		if !pool.AllowAttempt() {
			t.Fatalf("AllowAttempt() = false after %d failures, want true until the threshold", i)
		}
		pool.RecordTransportFailure()
	}
	if pool.AllowAttempt() {
		t.Fatal("AllowAttempt() = true after the failure threshold, want false")
	}
	if got := pool.breakerFailures.Load(); got != 0 {
		t.Fatalf("breakerFailures = %d after tripping, want 0 (half-open needs a fresh streak)", got)
	}
	// The cooldown window is a unix-nano deadline: push it into the past
	// instead of sleeping.
	pool.breakerUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if !pool.AllowAttempt() {
		t.Fatal("AllowAttempt() = false after the cooldown, want true")
	}
	// A half-open probe starts from zero: two fresh failures must not reopen
	// the breaker.
	pool.RecordTransportFailure()
	pool.RecordTransportFailure()
	if !pool.AllowAttempt() {
		t.Fatal("AllowAttempt() = false after two fresh failures, want true until the threshold")
	}
}

func TestAnonymousBreakerSuccessResets(t *testing.T) {
	pool := &anonymousPool{}
	pool.RecordTransportFailure()
	pool.RecordTransportFailure()
	pool.RecordTransportSuccess()
	if got := pool.breakerFailures.Load(); got != 0 {
		t.Fatalf("breakerFailures = %d after a success, want 0", got)
	}
	// One failure after a success must not trip: the streak restarted.
	pool.RecordTransportFailure()
	if !pool.AllowAttempt() {
		t.Fatal("AllowAttempt() = false after success plus one failure, want true")
	}
	// A success also closes an already open breaker.
	pool.breakerUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool.RecordTransportSuccess()
	if !pool.AllowAttempt() {
		t.Fatal("AllowAttempt() = false after success closed the breaker, want true")
	}
}

// breakerTestGateway builds an anonymous-enabled gateway whose lanes share a
// single upstream. The health-check slot of every lane is claimed so a
// transport failure never spawns the asynchronous URL verification: these
// tests exercise the breaker, not the network.
func breakerTestGateway(t *testing.T, upstream string, lanes int) *Gateway {
	t.Helper()
	addresses := make([]string, lanes)
	for i := range addresses {
		addresses[i] = "direct"
	}
	transports, err := newTransportPool(addresses, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("transport pool: %v", err)
	}
	for _, egress := range transports.snapshot() {
		egress.checking.Store(true)
	}
	nodes, err := newNodePool([]string{"oc_sk_test"}, transports, time.Second)
	if err != nil {
		t.Fatalf("node pool: %v", err)
	}
	return &Gateway{
		cfg:        config.Config{Retry: config.RetryConfig{MaxAttempts: 1}, Performance: testPerf()},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		transports: transports,
		channels: map[config.Tier]*channel{
			config.TierZen: {tier: config.TierZen, upstream: upstream, opencode: true, nodes: nodes},
		},
		anonymous: newAnonymousPool(true, transports, time.Second, nil),
	}
}

// droppingServer accepts requests and closes the connection before writing a
// single response byte. The client sees a transport error with no response,
// the same shape as a dead egress lane, without any timeout.
func droppingServer(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("response writer does not support hijacking")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(server.Close)
	return server
}

func breakerTestRequest() (models.Route, map[config.Tier][]byte, identity.RequestIDs) {
	route := models.Route{
		ID: "zen-model-free", Tier: config.TierZen, Protocol: wire.Chat,
		Protocols:         map[config.Tier]wire.Protocol{config.TierZen: wire.Chat},
		AnonymousFallback: true,
		KeyTiers:          []config.Tier{config.TierZen},
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"zen-model-free","messages":[{"role":"user","content":"hi"}]}`),
	}
	ids := identity.RequestIDs{Request: "req_breaker", Session: "sess_breaker"}
	return route, bodies, ids
}

// A mid-scan trip must wait out the breaker cooldown and then continue with a
// half-open probe instead of aborting the scan with zero further attempts.
func TestAnonymousScanWaitsWhenBreakerOpens(t *testing.T) {
	restoreBreakerWaitLimit(t)
	anonymousBreakerWaitLimit = 50 * time.Millisecond

	var hits atomic.Int32
	server := droppingServer(t, &hits)
	g := breakerTestGateway(t, server.URL, 4)
	route, bodies, ids := breakerTestRequest()

	resp, err, attempts := g.doAnonymousUpstream(context.Background(), route, bodies, ids, 0, true)
	if err != nil {
		t.Fatalf("err = %v, want the retryable pool 503", err)
	}
	if resp == nil {
		t.Fatal("resp = nil, want the retryable 503")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after the half-open probe failed", resp.StatusCode)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4 (three failures, then one half-open probe)", attempts)
	}
	if got := hits.Load(); got != 4 {
		t.Fatalf("upstream hits = %d, want 4 (the probe must reach a lane)", got)
	}
}

// A request that starts while the breaker is open waits for the cooldown and
// probes the pool instead of failing immediately with zero attempts.
func TestAnonymousScanProbesWhileBreakerOpen(t *testing.T) {
	restoreBreakerWaitLimit(t)
	anonymousBreakerWaitLimit = 60 * time.Millisecond

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)
	g := breakerTestGateway(t, server.URL, 4)
	g.anonymous.breakerUntil.Store(time.Now().Add(time.Minute).UnixNano())
	route, bodies, ids := breakerTestRequest()

	started := time.Now()
	resp, err, attempts := g.doAnonymousUpstream(context.Background(), route, bodies, ids, 0, true)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("err = %v, want a successful probe", err)
	}
	if resp == nil {
		t.Fatal("resp = nil, want the probed lane's response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 from the half-open probe", resp.StatusCode)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
	if elapsed < anonymousBreakerWaitLimit {
		t.Fatalf("elapsed = %v, want at least %v (the probe must wait for the breaker)", elapsed, anonymousBreakerWaitLimit)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("elapsed = %v, want a bounded wait", elapsed)
	}
	if !g.anonymous.AllowAttempt() {
		t.Fatal("breaker stayed open after a successful probe, want closed")
	}
}
