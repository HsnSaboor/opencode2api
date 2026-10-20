package gateway

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"
)

// forbiddenVerdict classifies one upstream 403 body snippet. The taxonomy
// mirrors zen-proxy's classify_forbidden (upstream.py): the match strings
// are the literal markers the upstream embeds in its error bodies.
type forbiddenVerdict string

const (
	// forbiddenAccount means the identity itself is blocked. Rotating egress
	// cannot fix it; the request must fail fast without burning more lanes.
	forbiddenAccount forbiddenVerdict = "account_blocked"
	// forbiddenFreeTier means the exit IP is throttled. The flag is
	// exit-attributable and temporary; park the lane and rotate silently.
	forbiddenFreeTier forbiddenVerdict = "freitier_blocked"
	// forbiddenRegion means the exit's /64 is geo-blocked. Cool the subnet
	// and rotate.
	forbiddenRegion forbiddenVerdict = "region_blocked"
	// forbiddenBare is a 403 with none of the known markers. It keeps the
	// existing failure path: cool the lane, rotate.
	forbiddenBare forbiddenVerdict = "http_403"
)

// classifyForbidden maps a 403 body snippet to its verdict. Order matters:
// the first marker present wins, exactly as in zen-proxy.
func classifyForbidden(snippet string) forbiddenVerdict {
	switch {
	case strings.Contains(snippet, "user_blocked"):
		return forbiddenAccount
	case strings.Contains(snippet, "FreeTierError"),
		strings.Contains(snippet, "Abuse Traffic"),
		strings.Contains(snippet, "Detect Abuse Traffic"),
		strings.Contains(snippet, "abuse_traffic"),
		// The user-visible free-tier network block reads "we've detected
		// abusive traffic from this IP address"; it is the same
		// exit-attributable class as the other FreeTier markers.
		strings.Contains(snippet, "abusive traffic"),
		strings.Contains(snippet, "Abusive Traffic"):
		return forbiddenFreeTier
	case strings.Contains(snippet, "RegionError"):
		return forbiddenRegion
	default:
		return forbiddenBare
	}
}

// peekForbiddenSnippet reads up to limit bytes of a 403 body for verdict
// classification and restores the body so the response stays usable as the
// returned lastResponse. Callers drain-and-close the previous lastResponse
// before each attempt, so resp.Body is always the live attempt body here.
func peekForbiddenSnippet(resp *http.Response, limit int) string {
	if resp == nil || resp.Body == nil || limit <= 0 {
		return ""
	}
	// Read the whole body: error payloads are small, and restoring only
	// the peek window would truncate the response for the caller that
	// returns it as lastResponse. Cap the classify snippet separately.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if err != nil {
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		return ""
	}
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	if len(raw) > limit {
		return string(raw[:limit])
	}
	return string(raw)
}

// forbiddenCooldown mirrors zen-proxy's region handling scale: a corroborated
// /64 region block cools the subnet for one hour.
const forbiddenRegionCooldown = time.Hour
