package gateway

import "testing"

// The free-tier network block surfaced as "we've detected abusive traffic
// from this IP address" is exit-attributable, exactly like the older
// FreeTierError/Abuse Traffic markers.
func TestClassifyForbiddenAbusiveTraffic(t *testing.T) {
	const snippet = `{"error":{"message":"OpenCode's free models aren't available from your current network because we've detected abusive traffic from this IP address."}}`
	if got := classifyForbidden(snippet); got != forbiddenFreeTier {
		t.Fatalf("classifyForbidden = %q, want %q", got, forbiddenFreeTier)
	}
}
