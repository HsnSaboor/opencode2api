package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A FreeTier/abuse verdict on the direct class must bench every non-proxy
// lane, not just the address that answered: the upstream flags the host's
// datacenter prefixes as a unit.
func TestDirectEgressHardParkSkipsNonProxyLanes(t *testing.T) {
	transports := &transportPool{}
	pool := &anonymousPool{enabled: true, transports: transports}
	direct := laneAt(0, "2001:db8:1::1")
	proxy := proxyAt(1)
	directNode := &anonymousNode{egress: direct}
	proxyNode := &anonymousNode{egress: proxy}
	pool.nodes = []*anonymousNode{directNode, proxyNode}

	transports.ParkDirectEgress(time.Hour)
	if !transports.DirectEgressParked() {
		t.Fatal("direct egress not parked after ParkDirectEgress")
	}

	cursor := pool.CursorFor("")
	if node := cursor.Next(); node != proxyNode {
		t.Fatalf("parked cursor first lane = %v, want the proxy lane", node)
	}
	if node := cursor.Next(); node != nil {
		t.Fatalf("parked cursor served direct lane %v", node)
	}

	// Expiry re-admits the class; the next verdict re-parks it.
	transports.directParkUntil.Store(time.Now().Add(-time.Second).UnixNano())
	seen := map[*anonymousNode]bool{}
	cursor = pool.CursorFor("")
	for node := cursor.Next(); node != nil; node = cursor.Next() {
		seen[node] = true
	}
	if !seen[directNode] || !seen[proxyNode] {
		t.Fatalf("expired cursor served %v, want both lanes", seen)
	}
}

func TestAnonymousUnavailableResponseIsRetryable(t *testing.T) {
	resp := anonymousUnavailableResponse(anonymousRetryAfter)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "warming up") {
		t.Fatalf("body = %q, want the warming-up message", body)
	}
}
