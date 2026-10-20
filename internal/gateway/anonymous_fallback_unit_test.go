package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// An AnonymousFallback route must try the authenticated Zen tier first and
// reach the public credential only after the keyed plan is exhausted. Here
// the single key answers 429, so the request must land on the anonymous lane
// and report the anonymous route back (the non-streaming client depends on
// route.Anonymous to collapse the forced SSE stream).
func TestAnonymousFallbackAfterKeyedExhaustion(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		seen = append(seen, auth)
		mu.Unlock()
		if auth == "Bearer "+anonymousZenKey {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	transports, err := newTransportPool([]string{"direct"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("transport pool: %v", err)
	}
	// Claim the lane's health-check slot so the 429 below cannot spawn the
	// asynchronous egress verification against the real health URL: the test
	// must observe lane state, not a network round trip.
	transports.snapshot()[0].checking.Store(true)
	nodes, err := newNodePool([]string{"oc_sk_test"}, transports, time.Second)
	if err != nil {
		t.Fatalf("node pool: %v", err)
	}
	catalog := models.NewCatalog(config.TierZen, []config.Tier{config.TierZen}, nil)
	g := &Gateway{
		cfg:        config.Config{Retry: config.RetryConfig{MaxAttempts: 1}, Performance: testPerf()},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		transports: transports,
		channels: map[config.Tier]*channel{
			config.TierZen: {tier: config.TierZen, upstream: server.URL, opencode: true, nodes: nodes},
		},
		anonymous: newAnonymousPool(true, transports, time.Second, nil),
		catalog:   catalog,
	}
	route := models.Route{
		ID: "zen-model-free", Tier: config.TierZen, Protocol: wire.Chat,
		Protocols:         map[config.Tier]wire.Protocol{config.TierZen: wire.Chat},
		AnonymousFallback: true,
		KeyTiers:          []config.Tier{config.TierZen},
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"zen-model-free","messages":[{"role":"user","content":"hi"}]}`),
	}
	ids := identity.RequestIDs{Request: "req_fallback_test", Session: "sess_fallback_test"}
	resp, servingRoute, attempts, err := g.doUpstreamTiers(context.Background(), route, bodies, ids, 0, true)
	if err != nil {
		t.Fatalf("doUpstreamTiers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !servingRoute.Anonymous {
		t.Fatalf("serving route = %+v, want the anonymous fallback", servingRoute)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one keyed 429 plus one anonymous success)", attempts)
	}
	mu.Lock()
	order := append([]string(nil), seen...)
	mu.Unlock()
	if len(order) != 2 || order[0] != "Bearer oc_sk_test" || order[1] != "Bearer "+anonymousZenKey {
		t.Fatalf("attempt order = %v, want keyed Zen first, anonymous second", order)
	}
	if nodes.nodes[0].cooldownUntil.Load() <= time.Now().UnixNano() {
		t.Fatal("a 429'd Zen key was not cooled")
	}
}

// A keyed tier whose only lane is benched must not burn request attempts on
// a lane the pool already knows answers FreeTier verdicts: the cursor's
// cooldown fallback would hand the benched node back anyway, so the keyed
// loop must detect that and hand the request straight to the anonymous
// bottom layer, which serves from the healthy lane.
func TestParkedKeyLanesSkipToAnonymousLayer(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		seen = append(seen, auth)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	transports, err := newTransportPool([]string{"direct", "direct"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("transport pool: %v", err)
	}
	// Claim the parked lane's health-check slot so no asynchronous egress
	// verification runs against the real health URL.
	transports.snapshot()[0].checking.Store(true)
	nodes, err := newNodePool([]string{"oc_sk_test"}, transports, time.Second)
	if err != nil {
		t.Fatalf("node pool: %v", err)
	}
	// The single key binds lane 0, which then parks. Lane 1 stays healthy
	// for the anonymous fallback to serve from.
	transports.snapshot()[0].freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	catalog := models.NewCatalog(config.TierZen, []config.Tier{config.TierZen}, nil)
	g := &Gateway{
		cfg:        config.Config{Retry: config.RetryConfig{MaxAttempts: 1}, Performance: testPerf()},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		transports: transports,
		channels: map[config.Tier]*channel{
			config.TierZen: {tier: config.TierZen, upstream: server.URL, opencode: true, nodes: nodes},
		},
		anonymous: newAnonymousPool(true, transports, time.Second, nil),
		catalog:   catalog,
	}
	route := models.Route{
		ID: "zen-model-free", Tier: config.TierZen, Protocol: wire.Chat,
		Protocols:         map[config.Tier]wire.Protocol{config.TierZen: wire.Chat},
		AnonymousFallback: true,
		KeyTiers:          []config.Tier{config.TierZen},
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"zen-model-free","messages":[{"role":"user","content":"hi"}]}`),
	}
	ids := identity.RequestIDs{Request: "req_parked_skip", Session: "sess_parked_skip"}
	resp, servingRoute, attempts, err := g.doUpstreamTiers(context.Background(), route, bodies, ids, 0, true)
	if err != nil {
		t.Fatalf("doUpstreamTiers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !servingRoute.Anonymous {
		t.Fatalf("serving route = %+v, want the anonymous fallback", servingRoute)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (anonymous only; the parked key lane must be skipped)", attempts)
	}
	mu.Lock()
	order := append([]string(nil), seen...)
	mu.Unlock()
	if len(order) != 1 || order[0] != "Bearer "+anonymousZenKey {
		t.Fatalf("attempt order = %v, want only the anonymous attempt", order)
	}
}
