package gateway

import (
	"net/http"
	"testing"
)

// A scan that only surfaced exit-attributable FreeTier verdicts and then
// transport failures must report a retryable pool outage, not a 502: the
// verdicts parked lanes, so the pool is flagged and the client should back
// off and retry instead of treating the request as broken.
func TestAnonymousScanDegraded(t *testing.T) {
	forbidden := &http.Response{StatusCode: http.StatusForbidden}
	cases := []struct {
		name  string
		last  *http.Response
		parks int
		want  bool
	}{
		{name: "transport failure after FreeTier parks is retryable", parks: 1, want: true},
		{name: "forwardable response keeps normal handling", last: forbidden, parks: 3, want: false},
		{name: "transport failure without FreeTier parks keeps the bad gateway", parks: 0, want: false},
	}
	for _, tc := range cases {
		if got := anonymousScanDegraded(tc.last, tc.parks); got != tc.want {
			t.Fatalf("%s: anonymousScanDegraded = %v, want %v", tc.name, got, tc.want)
		}
	}
}
