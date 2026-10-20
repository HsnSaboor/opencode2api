package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The "direct4" lane keeps the unbound dialer (kernel picks the source) but
// must be dialable end to end: this guards the tcp4-forcing wrapper against
// accidentally pinning a local address or breaking the transport wiring.
func TestDirect4LaneDialerIsUnbound(t *testing.T) {
	dialer, err := egressDialer("direct4", testPerf())
	if err != nil {
		t.Fatalf("egressDialer: %v", err)
	}
	if dialer.LocalAddr != nil {
		t.Fatalf("direct4 lane bound LocalAddr %v, want nil", dialer.LocalAddr)
	}
}

func TestDirect4LaneReachesLocalIPv4Server(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	lane := pool.snapshot()[0]
	if lane.address != "direct4" {
		t.Fatalf("lane address = %q, want direct4", lane.address)
	}
	resp, err := lane.client.Get(srv.URL)
	if err != nil {
		t.Fatalf("lane request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("lane response = %d %q, want 200 \"ok\"", resp.StatusCode, body)
	}
}
