package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestDrainStateWrap pins the two halves of the deploy-time contract: while the
// process is still serving, requests pass through with the in-flight count
// held open for the drain to wait on; once draining starts, new requests are
// answered with a retryable 503 instead of a refused connection.
func TestDrainStateWrap(t *testing.T) {
	var draining atomic.Bool
	var inflight atomic.Int64
	state := &drainState{draining: &draining, inflight: &inflight}

	var served atomic.Int64
	seenInflight := atomic.Int64{}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenInflight.Store(inflight.Load())
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	handler := state.wrap(next)

	normal := httptest.NewRecorder()
	handler.ServeHTTP(normal, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if normal.Code != http.StatusOK {
		t.Fatalf("serving status = %d, want 200", normal.Code)
	}
	if got := served.Load(); got != 1 {
		t.Fatalf("served = %d, want 1", got)
	}
	if got := seenInflight.Load(); got != 1 {
		t.Fatalf("in-flight during handler = %d, want 1", got)
	}
	if got := inflight.Load(); got != 0 {
		t.Fatalf("in-flight after handler = %d, want 0", got)
	}

	draining.Store(true)
	blocked := httptest.NewRecorder()
	handler.ServeHTTP(blocked, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if blocked.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining status = %d, want 503", blocked.Code)
	}
	if got := blocked.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	if got := blocked.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if body := blocked.Body.String(); !strings.Contains(body, "service_unavailable") || !strings.Contains(body, "restarting") {
		t.Fatalf("body = %q, want service_unavailable restarting", body)
	}
	if got := served.Load(); got != 1 {
		t.Fatalf("served = %d, want 1 (draining request must not reach the handler)", got)
	}
	if got := inflight.Load(); got != 0 {
		t.Fatalf("in-flight after draining request = %d, want 0", got)
	}
}
