package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// The free-tier network block surfaced as "we've detected abusive traffic
// from this IP address" is exit-attributable, exactly like the older
// FreeTierError/Abuse Traffic markers.
func TestClassifyForbiddenAbusiveTraffic(t *testing.T) {
	const snippet = `{"error":{"message":"OpenCode's free models aren't available from your current network because we've detected abusive traffic from this IP address."}}`
	if got := classifyForbidden(snippet); got != forbiddenFreeTier {
		t.Fatalf("classifyForbidden = %q, want %q", got, forbiddenFreeTier)
	}
}

// The terminal FreeTier suppressor must recognize the abuse body while
// leaving it readable for callers that still forward the response.
func TestFreeTierForbiddenRestoresBody(t *testing.T) {
	const body = `{"error":{"message":"we've detected abusive traffic from this IP address"}}`
	resp := &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(body))}
	if !freeTierForbidden(resp) {
		t.Fatal("abusive traffic 403 must classify as FreeTier")
	}
	restored, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if string(restored) != body {
		t.Fatalf("body = %q, want the original payload", restored)
	}
	account := &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"error":"user_blocked"}`))}
	if freeTierForbidden(account) {
		t.Fatal("account block must not be suppressed as FreeTier")
	}
}
