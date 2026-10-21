package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
)

// realContentGateway builds a one-tier gateway whose keyed lanes point at the
// supplied fake servers, with the handler dependencies (rotation, retry budget)
// wired so handleInference can be exercised directly.
func realContentGateway(t *testing.T, servers []*httptest.Server, maxAttempts int) *Gateway {
	t.Helper()
	g, _ := hedgeGateway(t, servers, maxAttempts)
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatalf("rotation manager: %v", err)
	}
	g.rotation = manager
	g.cfg.Retry.TimeoutSeconds = 10
	g.cfg.Anonymous = false
	g.order = []config.Tier{config.TierZen}
	return g
}

func chatRequest(stream bool) *http.Request {
	streamField := ""
	if stream {
		streamField = `,"stream":true`
	}
	body := `{"model":"hedge-test-model","messages":[{"role":"user","content":"hi"}]` + streamField + `}`
	return httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
}

func TestNonStreamingRetriesEmpty2xxForRealContent(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"choices":[]}`)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"real answer"}}]}`)
	}))
	defer server.Close()

	g := realContentGateway(t, []*httptest.Server{server, server, server}, 3)
	rec := httptest.NewRecorder()
	g.handleInference(wire.Chat)(rec, chatRequest(false))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "real answer") {
		t.Fatalf("body = %q, want the second key's real content", rec.Body.String())
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("upstream calls = %d, want at least 2 (the empty 2xx must be retried)", got)
	}
}

func TestNonStreamingAllEmpty2xxNeverReturns200(t *testing.T) {
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}))
	defer empty.Close()

	g := realContentGateway(t, []*httptest.Server{empty, empty, empty}, 3)
	rec := httptest.NewRecorder()
	g.handleInference(wire.Chat)(rec, chatRequest(false))

	if rec.Code == http.StatusOK {
		t.Fatalf("status = 200, want a non-2xx error; body = %s", rec.Body.String())
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 after every 2xx was empty", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "error") {
		t.Fatalf("body = %q, want an error envelope", rec.Body.String())
	}
}

func TestNonStreamingErrorEnvelopeIsRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"error":{"message":"quota exhausted"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"served"}}]}`)
	}))
	defer server.Close()

	g := realContentGateway(t, []*httptest.Server{server, server, server}, 3)
	rec := httptest.NewRecorder()
	g.handleInference(wire.Chat)(rec, chatRequest(false))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "served") {
		t.Fatalf("body = %q, want the retry's content", rec.Body.String())
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("upstream calls = %d, want at least 2 (the error envelope must be retried)", got)
	}
}

func TestStreamingRetriesRoleOnlyStream(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n")
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"second stream\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	g := realContentGateway(t, []*httptest.Server{server, server, server}, 3)
	rec := httptest.NewRecorder()
	g.handleInference(wire.Chat)(rec, chatRequest(true))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "second stream") {
		t.Fatalf("body = %q, want the second stream's content", body)
	}
	if strings.Contains(body, `"role":"assistant"`) {
		t.Fatalf("body = %q, want the role-only first stream discarded", body)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("upstream calls = %d, want at least 2 (the role-only stream must be retried)", got)
	}
}

func TestStreamingAllRoleOnlyStreamsNeverReturns200(t *testing.T) {
	roleOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer roleOnly.Close()

	g := realContentGateway(t, []*httptest.Server{roleOnly, roleOnly, roleOnly}, 3)
	rec := httptest.NewRecorder()
	g.handleInference(wire.Chat)(rec, chatRequest(true))

	if rec.Code == http.StatusOK {
		t.Fatalf("status = 200, want a non-2xx error; body = %s", rec.Body.String())
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 after every stream was role-only", rec.Code)
	}
}
