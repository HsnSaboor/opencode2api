package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// hedgeRewriteTransport sends requests addressed to the fake channel upstream
// to a per-lane httptest server, so each lane can answer independently without
// changing the production request URL.
type hedgeRewriteTransport struct {
	base   *http.Transport
	target *url.URL
}

func (t hedgeRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	return t.base.RoundTrip(clone)
}

func hedgeClient(t *testing.T, server *httptest.Server) *http.Client {
	t.Helper()
	base, ok := server.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("httptest client transport is not *http.Transport")
	}
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	return &http.Client{Transport: hedgeRewriteTransport{base: base, target: target}}
}

func hedgeRoute() models.Route {
	return models.Route{ID: "hedge-test-model", Tier: config.TierZen, Protocol: wire.Chat}
}

func hedgeBody() []byte {
	return []byte(`{"model":"hedge-test-model","stream":true}`)
}

// hedgeGateway binds one key per lane to the supplied fake servers. The
// channel upstream is deliberately unroutable: every lane client rewrites the
// request to its own server, which proves the scheduler (not DNS) decides
// which key answers.
func hedgeGateway(t *testing.T, servers []*httptest.Server, maxAttempts int) (*Gateway, *nodePool) {
	t.Helper()
	addresses := make([]string, len(servers))
	keys := make([]string, len(servers))
	for i := range servers {
		addresses[i] = "direct4"
		keys[i] = fmt.Sprintf("oc_sk_hedge_%d", i)
	}
	transports, err := newTransportPool(addresses, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("transport pool: %v", err)
	}
	for i, server := range servers {
		egress := transports.snapshot()[i]
		egress.client = hedgeClient(t, server)
		// A 5xx verdict can trigger an asynchronous lane health check. Claim
		// the slot so this unit test never reaches the real health URL.
		egress.checking.Store(true)
	}
	pool, err := newNodePool(keys, transports, time.Second)
	if err != nil {
		t.Fatalf("node pool: %v", err)
	}
	catalog := models.NewCatalog(config.TierZen, []config.Tier{config.TierZen}, nil)
	g := &Gateway{
		cfg:        config.Config{Retry: config.RetryConfig{MaxAttempts: maxAttempts}, Performance: testPerf()},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		transports: transports,
		catalog:    catalog,
		channels: map[config.Tier]*channel{
			config.TierZen: {
				tier:     config.TierZen,
				upstream: "http://example.invalid",
				opencode: true,
				nodes:    pool,
			},
		},
	}
	return g, pool
}

func restoreKeyHedgeDelay(t *testing.T) {
	t.Helper()
	old := keyHedgeDelay
	t.Cleanup(func() { keyHedgeDelay = old })
}

func TestKeyHedgeFastWinnerCancelsSlowAttempt(t *testing.T) {
	restoreKeyHedgeDelay(t)
	keyHedgeDelay = 50 * time.Millisecond

	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slowStarted <- struct{}{}:
		default:
		}
		// Hang at the transport level until the test releases it. The client
		// cancel closes the request; the release only keeps httptest.Server
		// from waiting on this handler forever.
		<-releaseSlow
	}))
	defer slow.Close()
	// Registered after slow.Close, so it runs first and lets the blocked
	// handler return before the server waits for it.
	defer close(releaseSlow)

	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer fast.Close()

	g, pool := hedgeGateway(t, []*httptest.Server{slow, fast, fast}, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := time.Now()
	resp, err, attempts := g.doKeyUpstream(ctx, hedgeRoute(), map[config.Tier][]byte{config.TierZen: hedgeBody()}, identity.RequestIDs{Request: "req-hedge-fast"}, 0, true)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("doKeyUpstream: %v", err)
	}
	if resp == nil {
		t.Fatal("doKeyUpstream returned a nil response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read winner body: %v", err)
	}
	if !strings.Contains(string(body), `"ok"`) {
		t.Fatalf("winner body = %q, want SSE content", body)
	}
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("time to first response = %v, want < 1.5s", elapsed)
	}
	if attempts < 2 {
		t.Fatalf("attempts = %d, want the hedged second key to have launched", attempts)
	}
	select {
	case <-slowStarted:
	default:
		t.Fatal("slow key was never attempted")
	}
	if got := pool.nodes[0].cooldownUntil.Load(); got != 0 {
		t.Fatalf("cancelled slow key cooldownUntil = %d, want 0", got)
	}
}

func TestKeyHedgeAllAttemptsFail(t *testing.T) {
	restoreKeyHedgeDelay(t)
	keyHedgeDelay = 10 * time.Millisecond

	var calls atomic.Int32
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	g, _ := hedgeGateway(t, []*httptest.Server{failing, failing, failing}, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err, attempts := g.doKeyUpstream(ctx, hedgeRoute(), map[config.Tier][]byte{config.TierZen: hedgeBody()}, identity.RequestIDs{Request: "req-hedge-fail"}, 0, true)
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3", got)
	}
	if resp == nil {
		if err == nil {
			t.Fatal("all attempts failed with neither a response nor an error")
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 from the last attempt", resp.StatusCode)
	}
	if _, readErr := io.ReadAll(resp.Body); readErr != nil {
		t.Fatalf("last failed response body is unreadable: %v", readErr)
	}
}

func TestKeyHedgeSingleAttemptNeverArmsTimer(t *testing.T) {
	restoreKeyHedgeDelay(t)
	keyHedgeDelay = time.Millisecond

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	g, _ := hedgeGateway(t, []*httptest.Server{server, server, server}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err, attempts := g.doKeyUpstream(ctx, hedgeRoute(), map[config.Tier][]byte{config.TierZen: hedgeBody()}, identity.RequestIDs{Request: "req-hedge-one"}, 0, true)
	if err != nil {
		t.Fatalf("doKeyUpstream: %v", err)
	}
	if resp == nil {
		t.Fatal("doKeyUpstream returned a nil response")
	}
	resp.Body.Close()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestKeyHedgeDoesNotDuplicateFastAttempt(t *testing.T) {
	restoreKeyHedgeDelay(t)
	keyHedgeDelay = time.Second

	var calls atomic.Int32
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer fast.Close()

	g, _ := hedgeGateway(t, []*httptest.Server{fast, fast, fast}, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := time.Now()
	resp, err, attempts := g.doKeyUpstream(ctx, hedgeRoute(), map[config.Tier][]byte{config.TierZen: hedgeBody()}, identity.RequestIDs{Request: "req-hedge-single"}, 0, true)
	if err != nil {
		t.Fatalf("doKeyUpstream: %v", err)
	}
	if resp == nil {
		t.Fatal("doKeyUpstream returned a nil response")
	}
	defer resp.Body.Close()
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("fast winner took %v, want the hedge timer not to fire", elapsed)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (hedge delay exceeds the fast response)", attempts)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}
