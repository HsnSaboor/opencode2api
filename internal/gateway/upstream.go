package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

// DebugKeyOverride pins one inference request to a single configured upstream
// key so the Playground can exercise that key in isolation.
type DebugKeyOverride struct {
	Tier  config.Tier
	KeyID string
}

type debugKeyOverrideContextKey struct{}

type diagnosticContextKey struct{}

func WithDiagnosticRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, diagnosticContextKey{}, true)
}

func isDiagnosticRequest(ctx context.Context) bool {
	diagnostic, _ := ctx.Value(diagnosticContextKey{}).(bool)
	return diagnostic
}

func WithDebugKeyOverride(ctx context.Context, override DebugKeyOverride) context.Context {
	return context.WithValue(WithDiagnosticRequest(ctx), debugKeyOverrideContextKey{}, override)
}

func debugKeyOverrideFrom(ctx context.Context) (DebugKeyOverride, bool) {
	override, ok := ctx.Value(debugKeyOverrideContextKey{}).(DebugKeyOverride)
	return override, ok && override.KeyID != ""
}

func (g *Gateway) doUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs) (*http.Response, models.Route, error) {
	resp, effectiveRoute, attempts, err := g.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if _, selected := debugKeyOverrideFrom(ctx); selected {
		return resp, effectiveRoute, err
	}
	if err != nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
		return resp, effectiveRoute, err
	}
	origBody := resp.Body
	errBody, readErr := io.ReadAll(io.LimitReader(origBody, 1<<20))
	// The original network body is always closed here: the retry path below
	// reuses the connection, otherwise downstream receives a fresh in-memory
	// reader over the cached bytes.
	httpx.DrainAndClose(origBody)
	if readErr != nil {
		resp.Body = io.NopCloser(bytes.NewReader(errBody))
		return resp, effectiveRoute, nil
	}
	// Restore the body so downstream error handling still sees the original
	// payload when no retry happens below.
	resp.Body = io.NopCloser(bytes.NewReader(errBody))
	// Log a capped snippet of the upstream error body so the 400 class is
	// visible without dumping full payloads. Bodies stay out of attempt
	// records; the hub redactor scrubs configured secrets from log output.
	if len(errBody) > 0 {
		redactor := config.NewSecretRedactor()
		redactor.Replace(g.cfg)
		snippet := redactor.String(string(errBody))
		if len(snippet) > 240 {
			snippet = snippet[:240]
		}
		g.logger.Info("upstream error returned or retried", "component", "upstream", "event", "upstream_error_body", "request_id", ids.Request, "model", route.ID, "status", resp.StatusCode, "snippet", snippet)
	}
	if !isStaleReasoningReference(errBody) {
		return resp, effectiveRoute, nil
	}
	stripped, changed := stripStaleReasoningInputs(effectiveRoute, bodies)
	if !changed {
		return resp, effectiveRoute, nil
	}
	// The referenced reasoning items belong to an upstream chain this session
	// can no longer address (e.g. an interrupted stream). Replay once without
	// them instead of failing the client request outright. Attempt numbering
	// continues from the first round so monitoring never shows duplicate attempt
	// numbers for one request.
	//
	// The client's session is deliberately preserved: it carries key/egress
	// affinity and upstream session correlation. Minting a fresh session made
	// the replay land on a different node with a cold prompt cache and detached
	// the reply from the chain the client still holds; the stale references are
	// already removed from the payload itself.
	g.logger.Info("retrying upstream without stale reasoning references", "component", "upstream", "event", "reasoning_reference_retry", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "attempt_offset", attempts)
	retryResp, retryRoute, _, retryErr := g.doUpstreamTiers(ctx, effectiveRoute, stripped, ids, attempts)
	if retryErr != nil || retryResp == nil || retryResp.StatusCode/100 != 2 {
		retryStatus := 0
		if retryResp != nil {
			retryStatus = retryResp.StatusCode
			httpx.DrainAndClose(retryResp.Body)
		}
		g.logger.Warn("reasoning reference retry failed; returning original error", "component", "upstream", "event", "reasoning_reference_retry_failed", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "error", retryErr, "retry_status", retryStatus)
		fallback := *resp
		fallback.Body = io.NopCloser(bytes.NewReader(errBody))
		return &fallback, effectiveRoute, nil
	}
	return retryResp, retryRoute, nil
}

// isStaleReasoningReference reports whether an upstream 400 body describes a
// reasoning item/reference the server no longer recognizes, such as
// "Referenced reasoning item 'rs_...' was not found or has expired".
// Generic validation errors that merely mention reasoning (e.g. "unknown
// reasoning field") must NOT match, so both the target phrase and the
// gone/expired marker are required.
func isStaleReasoningReference(body []byte) bool {
	text := strings.ToLower(string(body))
	if !strings.Contains(text, "reasoning item") && !strings.Contains(text, "reasoning reference") {
		return false
	}
	for _, marker := range []string{"not found", "expir", "does not exist", "no longer"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// stripStaleReasoningInputs removes server-issued reasoning references from
// Responses-protocol upstream payloads: replayed "reasoning" input items and
// any previous_response_id chain link. Other tiers/protocols are passed
// through untouched. It reports whether any payload actually changed.
func stripStaleReasoningInputs(route models.Route, bodies map[config.Tier][]byte) (map[config.Tier][]byte, bool) {
	changed := false
	out := make(map[config.Tier][]byte, len(bodies))
	for tier, body := range bodies {
		if len(body) == 0 || route.ProtocolFor(tier) != wire.Responses {
			out[tier] = body
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			out[tier] = body
			continue
		}
		tierChanged := false
		if _, ok := payload["previous_response_id"]; ok {
			delete(payload, "previous_response_id")
			tierChanged = true
		}
		if raw, ok := payload["input"].([]any); ok {
			kept := make([]any, 0, len(raw))
			for _, item := range raw {
				if m, ok := item.(map[string]any); ok && jsonutil.StringAt(m, "type") == "reasoning" {
					tierChanged = true
					continue
				}
				kept = append(kept, item)
			}
			if tierChanged {
				payload["input"] = kept
			}
		}
		if !tierChanged {
			out[tier] = body
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			out[tier] = body
			continue
		}
		out[tier] = encoded
		changed = true
	}
	return out, changed
}

func (g *Gateway) doUpstreamTiers(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) (*http.Response, models.Route, int, error) {
	var lastResponse *http.Response
	var lastErr error
	effectiveRoute := route
	attempts := attemptOffset
	g.dumpOutboundBodies(route, bodies, ids, attemptOffset)
	if override, selected := debugKeyOverrideFrom(ctx); selected {
		resp, err, used := g.doSelectedKeyUpstream(ctx, route, bodies, ids, override, attemptOffset)
		return resp, route, used, err
	}
	if route.Anonymous {
		resp, err, used := g.doAnonymousUpstream(ctx, route, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, route, attempts, nil
		}
		lastResponse, lastErr = resp, err
		if len(route.KeyTiers) > 0 {
			g.logger.Debug("anonymous request did not succeed; entering preferred key tiers", "component", "upstream", "event", "anonymous_fallback", "request_id", ids.Request, "attempts", attempts, "key_tiers", route.KeyTiers)
		}
	}
	if ctx.Err() != nil {
		// The anonymous phase already consumed the request budget. Entering the
		// authenticated tiers here would only fire instant attempts against a
		// dead context, cooling keys that were never really tried.
		if lastResponse != nil {
			return lastResponse, effectiveRoute, attempts, nil
		}
		if lastErr == nil {
			lastErr = ctx.Err()
		}
		return nil, effectiveRoute, attempts, lastErr
	}

	keyTiers := route.KeyTiers
	if !route.Anonymous && len(keyTiers) == 0 && g.channel(route.Tier) != nil {
		keyTiers = []config.Tier{route.Tier}
	}
	for _, tier := range keyTiers {
		if lastResponse != nil {
			httpx.DrainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		keyRoute := route
		keyRoute.Tier = tier
		keyRoute.Anonymous = false
		keyRoute.Protocol = route.ProtocolFor(tier)
		effectiveRoute = keyRoute
		resp, err, used := g.doKeyUpstream(ctx, keyRoute, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, keyRoute, attempts, nil
		}
		lastResponse, lastErr = resp, err
	}
	if route.AnonymousFallback && ctx.Err() == nil {
		// The authenticated plan is exhausted (429s, blocks, outages), so the
		// public credential gets the request instead of failing on the last
		// keyed verdict: the anonymous lane is the route's last-resort layer.
		if lastResponse != nil {
			httpx.DrainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		anonymousRoute := route
		anonymousRoute.Tier = config.TierZen
		anonymousRoute.Protocol = route.ProtocolFor(config.TierZen)
		anonymousRoute.Anonymous = true
		anonymousRoute.AnonymousFallback = false
		effectiveRoute = anonymousRoute
		g.logger.Debug("authenticated tiers exhausted; entering the anonymous bottom layer", "component", "upstream", "event", "anonymous_bottom_layer", "request_id", ids.Request, "attempts", attempts)
		resp, err, used := g.doAnonymousUpstream(ctx, anonymousRoute, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, anonymousRoute, attempts, nil
		}
		lastResponse, lastErr = resp, err
	}
	if lastResponse != nil {
		return lastResponse, effectiveRoute, attempts, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no usable upstream route")
	}
	return nil, effectiveRoute, attempts, lastErr
}

// freitierScanLimit bounds how many lanes one request may park on a FreeTier
// verdict before the scan concludes the verdict is request-scoped. The cursor
// interleaves fate classes (configured prefixes and proxy exits), so ten
// parks cover every independent class a healthy-sized pool holds; a wave that
// flags one prefix — or one day's worth of proxy nodes — cannot exhaust the
// budget before an independent lane is tried. When the cap is reached anyway,
// a canary probe through a verified exit decides whether the verdicts
// described the request (parks restored, request fails) or the exits (parks
// stand, request fails), so the pool survives global rejections intact.
const freitierScanLimit = 10

// freitierScanExtendedLimit is the second, one-shot budget a scan may spend
// when the canary proves the gate still serves the canonical profile from a
// verified exit. A served canary means the FreeTier verdicts were lane-scoped,
// not a wave: the sampled lanes were genuinely flagged, but working lanes
// remain, and the capped scan would otherwise fail a request the pool can
// serve. The extended budget walks past the flagged prefix classes into the
// independent exits; when it is spent without a 2xx the scan still fails fast,
// so a total wave costs at most one canary round and one budget per request.
const freitierScanExtendedLimit = 24

// anonymousRetryAfter is the delay advertised when the anonymous tier has no
// usable egress. Proxy lanes are seeded before the multiplexer answers SOCKS,
// so the first requests after a restart can fail with transport errors; a
// short retry lets the client wait for the pool to warm up instead of
// surfacing an abuse verdict from a direct lane.
const anonymousRetryAfter = 10 * time.Second

// anonymousUnavailableResponse builds the retryable 503 the anonymous tier
// returns when its egress is not ready. Direct lanes are hard-parked, so
// there is nothing healthy to fall through to; a 503 with Retry-After tells
// clients to try again instead of forwarding an upstream 403 that reads like
// an account-level refusal.
func anonymousUnavailableResponse() *http.Response {
	body := []byte(`{"error":{"type":"service_unavailable","message":"anonymous egress pool is warming up; retry shortly"}}`)
	header := make(http.Header, 2)
	header.Set("Content-Type", "application/json")
	header.Set("Retry-After", strconv.Itoa(int(anonymousRetryAfter/time.Second)))
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

// anonymousEgressDown reports whether the anonymous tier depends on the proxy
// multiplexer and the multiplexer is not accepting connections. Direct lanes
// are hard-parked whenever the pool is enabled, so a down multiplexer means
// there is no selectable anonymous egress at all.
func (g *Gateway) anonymousEgressDown() bool {
	return g != nil && g.proxies != nil && g.proxies.mux != nil && !g.proxies.mux.Ready()
}

// doAnonymousUpstream tries every currently available egress lane at most
// once. Transport failures and retryable responses (5xx, 401/403/429)
// advance to the next lane; a deterministic non-retryable 4xx ends the scan
// immediately (mirroring the key path) because no other IP will change the
// verdict. Only a successful response ends the anonymous phase; exhausting
// the lane cursor returns control to the preferred authenticated tiers.
func (g *Gateway) doAnonymousUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	cursor := g.anonymous.CursorFor(ids.Session)
	limit := g.anonymous.Len()
	attempts := 0
	freitierParks := 0
	// scanLimit is the FreeTier park budget for this request. It starts at
	// the base cap and extends once when the canary proves the gate still
	// serves (see freitierScanExtendedLimit).
	scanLimit := freitierScanLimit
	// requestParks records the lanes this request parked on FreeTier
	// verdicts. If the scan's cap is reached and the canary shows the
	// verdicts were request-scoped, these parks are restored: they were
	// charged to exits the gate never actually refused.
	var requestParks []*egressTransport
	body := bodies[config.TierZen]
	if len(body) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0
	}
	if g.anonymousEgressDown() {
		g.logger.Warn("anonymous egress is not ready; returning a retryable 503", "component", "upstream", "event", "anonymous_egress_unavailable", "request_id", ids.Request, "retry_after_s", int(anonymousRetryAfter/time.Second))
		return anonymousUnavailableResponse(), nil, 0
	}
	// The anonymous free tier serves SSE only — non-streaming wire bodies are
	// rejected with 403 FreeTierError — so streaming is forced here and the
	// gateway collapses the stream back when the client asked for plain JSON.
	// The same shaping stamps the gate's required tool profile (see
	// shapeAnonymousBody). Key tiers keep their original bodies.
	body = shapeAnonymousBody(body, route.ProtocolFor(config.TierZen))
	for attempts < limit {
		if ctx.Err() != nil {
			// The request-level budget is already gone. Stop scanning instead
			// of firing attempts that cannot succeed and then recording them
			// as failures against lanes that were never really tried.
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			break
		}
		node := cursor.Next()
		if node == nil {
			break
		}
		attempts++
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Attempts = attemptOffset + attempts
			meta.Tier = string(config.TierZen)
		}
		if lastResponse != nil {
			httpx.DrainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		req, err := newUpstreamRequest(ctx, g.channel(config.TierZen).upstream, true, route.Protocol, body, ids, anonymousZenKey)
		if err != nil {
			return nil, err, attempts
		}
		setRequestCredential(ctx, config.TierZen, "anonymous", "anonymous", true, node.egress)
		started := time.Now()
		resp, err := doInferenceAttempt(node.egress.client, req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second)
		duration := time.Since(started)
		if ctx.Err() != nil {
			// The parent budget expired while this attempt was in flight. Its
			// outcome says nothing about this lane, so record nothing and stop
			// scanning.
			lastResponse, lastErr = resp, err
			if lastErr == nil && lastResponse == nil {
				lastErr = ctx.Err()
			}
			break
		}
		g.observeAnonymousResult(ctx, node, resp, err)
		g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attempts, "anonymous", "anonymous", true, node.egress, resp, err, duration)
		// A 403 carries an exit-attributable verdict in its body (zen-proxy
		// taxonomy): account blocks fail fast, FreeTier flags park the lane,
		// region blocks cool the /64, bare 403s keep the failure path.
		// The snippet peek restores the body, so lastResponse stays usable.
		if err == nil && resp.StatusCode == http.StatusForbidden {
			verdict := classifyForbidden(peekForbiddenSnippet(resp, 2048))
			g.logger.Info("anonymous upstream returned 403", "component", "upstream", "event", "anonymous_forbidden", "request_id", ids.Request, "attempt", attempts, "verdict", string(verdict), "egress", config.RedactURL(node.egress.address), "duration_ms", duration.Milliseconds())
			if verdict == forbiddenAccount {
				lastResponse = resp
				lastErr = err
				break
			}
			if verdict == forbiddenFreeTier {
				if node.egress.proxy == nil {
					if !g.transports.DirectEgressParked() {
						g.logger.Info("direct egress class hard-parked after a FreeTier verdict", "component", "upstream", "event", "direct_egress_hard_parked", "request_id", ids.Request, "egress", config.RedactURL(node.egress.address), "duration", directEgressHardPark.String())
					}
					g.transports.ParkDirectEgress(directEgressHardPark)
				}
				g.parkEgressFreitier(node.egress)
				requestParks = append(requestParks, node.egress)
				if prefix, strikes, cooling := g.transports.StrikePrefix(node.egress.address); prefix != "" {
					g.logger.Info("anonymous FreeTier verdict attributed to configured prefix", "component", "upstream", "event", "anonymous_prefix_strike", "request_id", ids.Request, "prefix", prefix, "strikes", strikes, "cooling", cooling)
				}
				freitierParks++
				// Cap the FreeTier scan. A gate wave answers every lane, so
				// sampling the whole proxy pool (laneTarget, up to 200) only
				// delays the canary and the client's eventual failure. The
				// cursor interleaves fate classes, so freitierScanLimit parks
				// already cover each class before the canary runs.
				if freitierParks >= scanLimit {
					// The fate-interleaved scan has now sampled every class
					// the pool holds — the configured prefixes and the proxy
					// exits — without a success. Ask a verified exit whether
					// the gate still serves the canonical request profile: a
					// refused canary means the verdict describes the request,
					// not the exits, so the parks this request charged are
					// restored and the wave may leave every lane selectable
					// for the requests that follow.
					rejected, decisive := g.proxyGateRejectsRequest(ctx)
					if decisive && rejected {
						for _, parked := range requestParks {
							g.anonymous.UnparkFreitier(parked)
						}
						g.logger.Warn("anonymous FreeTier verdict repeated across fate classes; restoring request-scoped parks", "component", "upstream", "event", "anonymous_freitier_request_scoped", "request_id", ids.Request, "attempts", attempts, "parks", len(requestParks))
						lastResponse = resp
						lastErr = err
						break
					}
					if decisive && scanLimit < freitierScanExtendedLimit {
						// A verified exit served the canonical profile, so
						// the gate is up and the verdicts are lane-scoped:
						// extend the budget once and keep walking towards the
						// lanes the flag wave did not cover. Failing here
						// would abandon a request the pool can still answer.
						scanLimit = freitierScanExtendedLimit
						g.logger.Info("anonymous FreeTier verdicts are lane-scoped; extending the scan", "component", "upstream", "event", "anonymous_freitier_scan_extended", "request_id", ids.Request, "attempts", attempts, "parks", freitierParks, "limit", scanLimit)
					} else {
						// No verified exit could serve the canonical profile,
						// or the extended budget is spent: the gate rejects
						// this request from every lane, and another park
						// would drain the pool for a decision no lane can
						// change.
						g.logger.Warn("anonymous FreeTier verdict repeated across lanes; failing fast", "component", "upstream", "event", "anonymous_freitier_scan_capped", "request_id", ids.Request, "attempts", attempts, "limit", scanLimit)
						lastResponse = resp
						lastErr = err
						break
					}
				}
			}
			if verdict == forbiddenRegion {
				g.transports.CoolSubnet(node.egress.address, forbiddenRegionCooldown)
				g.transports.pruneSubnetCooldowns()
			}
			lastResponse = resp
			lastErr = err
			continue
		}
		if err == nil && resp.StatusCode/100 == 2 {
			g.transports.ClearPrefix(node.egress.address)
			g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "egress", config.RedactURL(node.egress.address), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
			return resp, nil, attempts
		}
		lastResponse = resp
		lastErr = err
		// Deterministic client errors (never 401/403/429, which stay
		// retryable so per-lane IP-reputation scanning is preserved) end
		// the scan: no other lane will change the verdict. Lanes are not
		// poisoned by this — observeAnonymousResult only cools lanes on
		// cooldownWorthy outcomes, which already exclude these 4xx.
		if isNonRetryableClientResponse(resp, err) {
			g.logger.Debug("anonymous upstream rejected a non-retryable request", "component", "upstream", "event", "anonymous_attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "egress", config.RedactURL(node.egress.address), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
			break
		}
		if err != nil {
			g.logger.Debug("anonymous transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "egress", config.RedactURL(node.egress.address), "duration_ms", duration.Milliseconds(), "error", err)
		} else {
			g.logger.Debug("anonymous upstream returned an error response; trying the next lane", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "egress", config.RedactURL(node.egress.address), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	if ctx.Err() != nil {
		// Preserve the request-budget outcome: callers map a deadline to a
		// typed timeout, not to a retryable pool-warming error.
		if lastErr == nil {
			lastErr = ctx.Err()
		}
		return nil, lastErr, attempts
	}
	if attempts == 0 || g.anonymousEgressDown() {
		g.logger.Warn("anonymous egress has no usable lanes; returning a retryable 503", "component", "upstream", "event", "anonymous_egress_unavailable", "request_id", ids.Request, "attempts", attempts, "retry_after_s", int(anonymousRetryAfter/time.Second))
		return anonymousUnavailableResponse(), nil, attempts
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy anonymous egress lanes available")
	}
	return nil, lastErr, attempts
}

// anonymousReadTool is the one tool the free-tier gate requires on agent
// traffic. The gate inspects only the function name, so a name-only
// declaration suffices and is the least intrusive thing to add to a foreign
// harness's tool wall.
var anonymousReadTool = []string{"read"}

// anonymousTitleMarker identifies OpenCode's internal title-generation
// profile inside a system prompt. The free-tier gate matches that profile
// fuzzily and rejects it when tools are present, so shaping must leave
// tool-less title requests exactly as they are.
const anonymousTitleMarker = "title generator"

// shapeAnonymousBody prepares a free-tier wire body for the anonymous lane.
// The upstream serves SSE only, so streaming is forced and Chat usage
// reporting enabled; the gateway collapses the stream back when the client
// asked for plain JSON. Tool shaping makes the body recognizable to the
// free-tier gate, which admits exactly two profiles:
//
//   - agent traffic: a non-empty tools list containing a function named
//     "read" — declared tools are preserved and a missing "read" is appended;
//   - the title generator: no tools at all.
//
// Everything else is answered with 403 FreeTierError, so tool-less requests
// that are not the title profile are stamped with a minimal "read"
// declaration. System One payloads are decision requests, not agent traffic,
// so they are forwarded verbatim; injecting streaming would make the upstream
// reject them. Bodies that already satisfy the shape (or are not JSON
// objects) are returned unchanged.
func shapeAnonymousBody(body []byte, protocol wire.Protocol) []byte {
	if protocol == wire.SystemOne {
		return body
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}
	changed := false
	if streaming, ok := payload["stream"].(bool); !ok || !streaming {
		payload["stream"] = true
		changed = true
	}
	if ensureAnonymousChatUsage(payload, protocol) {
		changed = true
	}
	if ensureAnonymousTools(payload, protocol) {
		changed = true
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return encoded
}

// ensureAnonymousChatUsage keeps token usage available when a non-streaming
// request is forced onto the Chat SSE path. OpenAI-compatible Chat streams
// require stream_options.include_usage for the final usage event.
func ensureAnonymousChatUsage(payload map[string]any, protocol wire.Protocol) bool {
	if protocol != wire.Chat {
		return false
	}
	options, ok := payload["stream_options"].(map[string]any)
	if !ok {
		payload["stream_options"] = map[string]any{"include_usage": true}
		return true
	}
	includeUsage, ok := options["include_usage"].(bool)
	if ok && includeUsage {
		return false
	}
	options["include_usage"] = true
	return true
}

// ensureAnonymousTools makes a payload recognizable as free-tier agent
// traffic: the tools list must exist, be non-empty, and contain a "read"
// function. It reports whether the payload changed. The title generator's
// tool-less profile is never given tools — the gate rejects that combination
// outright — and a title-profile request that somehow already carries tools
// is left untouched, because the official client never emits that shape and
// rewriting caller semantics would be worse than the upstream verdict.
func ensureAnonymousTools(payload map[string]any, protocol wire.Protocol) bool {
	raw, exists := payload["tools"]
	items, isSlice := raw.([]any)
	if !exists || (isSlice && len(items) == 0) {
		if anonymousTitleProfile(payload, protocol) {
			return false
		}
		payload["tools"] = anonymousToolset(protocol, anonymousReadTool)
		return true
	}
	if !isSlice {
		return false
	}
	for _, item := range items {
		if anonymousToolName(protocol, item) == "read" {
			return false
		}
	}
	if anonymousTitleProfile(payload, protocol) {
		return false
	}
	payload["tools"] = append(items, anonymousTool(protocol, "read"))
	return true
}

// anonymousTitleProfile reports whether the payload is OpenCode's internal
// title-generation request. The gate's own matching is loose (prompt
// mutations pass), so a marker search is a faithful approximation — and
// conservative in the right direction: agent payloads always carry tools and
// a harness system prompt that never contains the marker. Only system-side
// prompts are inspected; a user message that happens to mention titles cannot
// misclassify the request.
func anonymousTitleProfile(payload map[string]any, protocol wire.Protocol) bool {
	prompt := anonymousSystemPrompt(payload, protocol)
	return prompt != "" && strings.Contains(strings.ToLower(prompt), anonymousTitleMarker)
}

// anonymousSystemPrompt extracts the system/instruction text from the
// protocol-specific shapes a wire body can carry.
func anonymousSystemPrompt(payload map[string]any, protocol wire.Protocol) string {
	if protocol == wire.Anthropic {
		return textContent(payload["system"])
	}
	if protocol == wire.Responses {
		if text := textContent(payload["instructions"]); text != "" {
			return text
		}
	}
	for _, field := range []string{"messages", "input"} {
		for _, raw := range jsonutil.SliceAt(payload, field) {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if role := jsonutil.StringAt(item, "role"); role != "system" && role != "developer" {
				continue
			}
			if text := textContent(item["content"]); text != "" {
				return text
			}
		}
	}
	return ""
}

// textContent flattens the string-or-parts content shapes Chat, Anthropic,
// and Responses use for system prompts and messages.
func textContent(raw any) string {
	switch value := raw.(type) {
	case string:
		return value
	case []any:
		var b strings.Builder
		for _, part := range value {
			switch item := part.(type) {
			case string:
				b.WriteString(item)
			case map[string]any:
				b.WriteString(jsonutil.StringAt(item, "text"))
			}
		}
		return b.String()
	}
	return ""
}

func anonymousToolName(protocol wire.Protocol, item any) string {
	entry, ok := item.(map[string]any)
	if !ok {
		return ""
	}
	if protocol == wire.Chat {
		return jsonutil.StringAt(entry, "function", "name")
	}
	return jsonutil.StringAt(entry, "name")
}

func anonymousToolset(protocol wire.Protocol, names []string) []any {
	if names == nil {
		names = anonymousReadTool
	}
	tools := make([]any, 0, len(names))
	for _, name := range names {
		tools = append(tools, anonymousTool(protocol, name))
	}
	return tools
}

func anonymousTool(protocol wire.Protocol, name string) map[string]any {
	description := "Agent tool " + name
	parameters := map[string]any{"type": "object", "properties": map[string]any{}}
	switch protocol {
	case wire.Chat:
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        name,
				"description": description,
				"parameters":  parameters,
			},
		}
	case wire.Anthropic:
		return map[string]any{
			"name":         name,
			"description":  description,
			"input_schema": parameters,
		}
	default:
		return map[string]any{
			"type":        "function",
			"name":        name,
			"description": description,
			"parameters":  parameters,
		}
	}
}

// requestBodyDumpLimit caps how much of an outbound body is written to the log.
const requestBodyDumpLimit = 64 << 10

// dumpOutboundBodies records the exact bytes about to be sent upstream when
// logging.dump_request_bodies is enabled. It exists so retry and translation
// problems (stale rs_* references, re-encoded history, tier re-encoding) can be
// diagnosed without patching the binary. Credentials travel in headers and never
// in the body. The logger redacts configured secrets, but conversation content
// can still be sensitive. Reference IDs are retained for replay diagnostics.
func (g *Gateway) dumpOutboundBodies(route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) {
	if !g.cfg.Logging.DumpRequestBodies || len(bodies) == 0 {
		return
	}
	phase := "initial"
	if attemptOffset > 0 {
		phase = "retry"
	}
	tiers := make([]string, 0, len(bodies))
	for tier := range bodies {
		tiers = append(tiers, string(tier))
	}
	sort.Strings(tiers)
	for _, name := range tiers {
		tier := config.Tier(name)
		body := bodies[tier]
		if len(body) == 0 {
			continue
		}
		sum := sha256.Sum256(body)
		truncated := len(body) > requestBodyDumpLimit
		payload := body
		if truncated {
			payload = body[:requestBodyDumpLimit]
		}
		g.logger.Debug("outbound upstream request body", "component", "upstream", "event", "upstream_request_body",
			"request_id", ids.Request, "model", route.ID, "tier", tier, "protocol", route.ProtocolFor(tier),
			"phase", phase, "bytes", len(body), "sha256", hex.EncodeToString(sum[:8]),
			"truncated", truncated, "body", string(payload))
	}
}

// shapeKeyBody forces OpenCode key-tier free-model wire bodies onto the SSE
// transport the free tier serves, mirroring the anonymous lane. Paid models
// keep their original bodies. Provider channels are never shaped: the free
// gate's profiles are an OpenCode contract, and a provider that happens to
// name a model "free" must not inherit them. It reports whether the body
// changed so the gateway can collapse the SSE stream a non-streaming client
// receives back.
func (g *Gateway) shapeKeyBody(channel *channel, body []byte, route models.Route) ([]byte, bool) {
	if !channel.opencode || !g.catalog.IsFreeModel(route.ID) {
		return body, false
	}
	shaped := shapeAnonymousBody(body, route.ProtocolFor(channel.tier))
	return shaped, !bytes.Equal(shaped, body)
}

// key. It performs no failover at all — no anonymous lane, no other key and no
// other tier — so the outcome describes that one key. It also leaves pool state
// untouched on purpose: a diagnostic request must never cool a production key or
// evict a lane, which is exactly what made a bad Playground request degrade
// live traffic before.
func (g *Gateway) doSelectedKeyUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, override DebugKeyOverride, attemptOffset int) (*http.Response, error, int) {
	channel := g.channel(override.Tier)
	if channel == nil {
		return nil, fmt.Errorf("selected %s key tier is no longer configured", override.Tier), 0
	}
	node := channel.nodes.NodeByID(override.KeyID)
	if node == nil {
		return nil, fmt.Errorf("selected %s key is no longer configured", override.Tier), 0
	}
	body := bodies[override.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", override.Tier), 0
	}
	if shaped, changed := g.shapeKeyBody(channel, body, route); changed {
		body = shaped
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Shaped = true
		}
	}
	egress := channel.nodes.Egress(node)
	if egress == nil {
		return nil, errors.New("selected upstream key has no egress binding"), 0
	}
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.Attempts = attemptOffset + 1
		meta.Tier = string(override.Tier)
	}
	keyID := config.KeyDisplayID(node.key)
	setRequestCredential(ctx, override.Tier, keyID, "key", false, egress)
	req, err := newUpstreamRequest(ctx, channel.upstream, channel.opencode, route.ProtocolFor(override.Tier), body, ids, node.key)
	if err != nil {
		return nil, err, 0
	}
	started := time.Now()
	resp, err := doInferenceAttempt(egress.client, req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second)
	duration := time.Since(started)
	g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+1, keyID, "key", false, egress, resp, err, duration)
	if err != nil {
		return nil, err, 1
	}
	return resp, nil, 1
}

func (g *Gateway) doKeyUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	channel := g.channel(route.Tier)
	if channel == nil {
		return nil, fmt.Errorf("no %s tier is configured", route.Tier), 0
	}
	nodes := channel.nodes
	cursor := nodes.CursorFor(ids.Session)
	if nodes.Len() == 0 {
		return nil, fmt.Errorf("no %s nodes configured", route.Tier), 0
	}
	attempts := 0
	body := bodies[route.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", route.Tier), 0
	}
	if shaped, changed := g.shapeKeyBody(channel, body, route); changed {
		body = shaped
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Shaped = true
		}
	}
	for attempts < g.cfg.Retry.MaxAttempts {
		if ctx.Err() != nil {
			// Same guard as the anonymous loop: an exhausted request budget
			// must not be spent rotating keys that were never really tried.
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			break
		}
		node := cursor.Next()
		if node == nil {
			break
		}
		// Cursor.Next only returns a benched node when every lane in the
		// pool is benched (its cooldown fallback ran out of unbenched
		// candidates). Attempting it would spend the request budget on
		// the lane's own known verdict — a parked FreeTier lane answers
		// the same 403 again — so hand the request to the layers below.
		if nodes.laneParked(node) {
			break
		}
		attempts++
		// Keep the request-level trace synchronized with the attempt that is
		// about to be sent. Only the redacted key suffix is retained.
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Attempts = attemptOffset + attempts
			meta.Tier = string(route.Tier)
		}
		if lastResponse != nil {
			httpx.DrainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		req, err := newUpstreamRequest(ctx, channel.upstream, channel.opencode, route.Protocol, body, ids, node.key)
		if err != nil {
			return nil, err, attempts
		}
		egress := nodes.Egress(node)
		if egress == nil {
			lastErr = errors.New("upstream key has no egress binding")
			break
		}
		keyID := config.KeyDisplayID(node.key)
		setRequestCredential(ctx, route.Tier, keyID, "key", false, egress)
		attemptStarted := time.Now()
		resp, err := doInferenceAttempt(egress.client, req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second)
		attemptDuration := time.Since(attemptStarted)
		if ctx.Err() != nil {
			// The request budget expired while this attempt was in flight. A
			// cancelled context says nothing about the key or the lane: without
			// this guard the instant DeadlineExceeded would cool a healthy key
			// and evict a healthy lane from the pool.
			lastResponse, lastErr = resp, err
			if lastErr == nil && lastResponse == nil {
				lastErr = ctx.Err()
			}
			break
		}
		g.observeKeyResult(ctx, channel, node, egress, resp, err)
		g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attempts, keyID, "key", false, egress, resp, err, attemptDuration)
		// Same 403 taxonomy as the anonymous loop: account blocks fail the
		// tier fast, FreeTier flags park the lane, region blocks cool the
		// /64. MarkFailure already cooled the lane via cooldownWorthy; the
		// park/cool here is the additional exit-attributable signal. It is an
		// OpenCode gate contract: provider verdicts never touch lane state.
		if channel.opencode && err == nil && resp.StatusCode == http.StatusForbidden {
			verdict := classifyForbidden(peekForbiddenSnippet(resp, 2048))
			g.logger.Info("upstream returned 403", "component", "upstream", "event", "attempt_forbidden", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "verdict", string(verdict), "egress", config.RedactURL(egress.address), "duration_ms", attemptDuration.Milliseconds())
			if verdict == forbiddenAccount {
				return resp, nil, attempts
			}
			if verdict == forbiddenFreeTier {
				g.parkEgressFreitier(egress)
			}
			if verdict == forbiddenRegion {
				g.transports.CoolSubnet(egress.address, forbiddenRegionCooldown)
				g.transports.pruneSubnetCooldowns()
			}
			lastResponse = resp
			lastErr = err
			continue
		}
		if err == nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("upstream accepted request", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "egress", config.RedactURL(egress.address), "status", resp.StatusCode, "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		// Request-shape errors are deterministic and must leave this tier without
		// rotating through unrelated keys. The outer route may still try the next
		// tier in prefer order. Authentication, throttling, server, and transport
		// failures remain retryable inside this tier.
		if isNonRetryableClientResponse(resp, err) {
			g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "status", resp.StatusCode, "egress", config.RedactURL(egress.address), "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		lastResponse = resp
		lastErr = err
		if err != nil {
			g.logger.Debug("upstream transport attempt failed", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "egress", config.RedactURL(egress.address), "duration_ms", attemptDuration.Milliseconds(), "error", err)
		} else {
			g.logger.Debug("upstream returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "status", resp.StatusCode, "egress", config.RedactURL(egress.address), "duration_ms", attemptDuration.Milliseconds())
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	return nil, lastErr, attempts
}

// Diagnostics use the same routing and transports as production, but must not
// update cooldowns, health, or bindings, including after successful attempts.
func (g *Gateway) observeKeyResult(ctx context.Context, channel *channel, node *upstreamNode, egress *egressTransport, resp *http.Response, err error) {
	if isDiagnosticRequest(ctx) {
		return
	}
	status := upstreamStatus(resp)
	if !channel.opencode {
		// A provider shares the egress lanes with OpenCode traffic but its
		// verdicts describe the key, not the exit: cool the key, never the
		// lane.
		if err == nil && status/100 == 2 || isNonRetryableClientResponse(resp, err) {
			channel.nodes.MarkSuccess(node)
			return
		}
		channel.nodes.MarkFailure(node, resp, err)
		return
	}
	egressFailed := g.syncEgressResult(ctx, egress, status, err)
	if err == nil && status/100 == 2 || isNonRetryableClientResponse(resp, err) {
		channel.nodes.MarkSuccess(node)
		return
	}
	if !egressFailed || channel.nodes.Egress(node) == egress {
		channel.nodes.MarkFailure(node, resp, err)
	}
}

func (g *Gateway) observeAnonymousResult(ctx context.Context, node *anonymousNode, resp *http.Response, err error) {
	if isDiagnosticRequest(ctx) {
		return
	}
	status := upstreamStatus(resp)
	g.syncEgressResult(ctx, node.egress, status, err)
	if err == nil && status/100 == 2 {
		g.anonymous.MarkSuccess(node)
	} else {
		g.anonymous.MarkFailure(node, resp, err)
	}
}

func upstreamStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func setRequestCredential(ctx context.Context, tier config.Tier, keyID, channel string, anonymous bool, egress *egressTransport) {
	meta := telemetry.MetaFromContext(ctx)
	if meta == nil {
		return
	}
	meta.Tier = string(tier)
	meta.KeyID = keyID
	meta.Channel = channel
	meta.Anonymous = anonymous
	meta.Egress = ""
	if egress != nil {
		meta.Egress = config.RedactURL(egress.address)
	}
}

func requestCredential(ctx context.Context) (string, string, bool) {
	meta := telemetry.MetaFromContext(ctx)
	if meta == nil {
		return "", "", false
	}
	return meta.KeyID, meta.Channel, meta.Anonymous
}

func (g *Gateway) recordUpstreamAttempt(ctx context.Context, route models.Route, ids identity.RequestIDs, attempt int, keyID, channel string, anonymous bool, egress *egressTransport, resp *http.Response, err error, duration time.Duration) {
	status := upstreamStatus(resp)
	success := err == nil && status >= 200 && status < 300
	outcome := "retryable_failure"
	if success {
		outcome = "success"
	} else if err != nil {
		outcome = "transport_error"
	} else if isNonRetryableClientResponse(resp, nil) {
		outcome = "rejected"
	}
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.AttemptOutcome = outcome
		meta.Protocol = route.Protocol
		// Append a per-attempt leg (lane:status:ms) so the request log
		// shows the whole attempt chain at a glance. Anonymous attempts
		// carry the egress identity: WARP/proxy lanes use their node tag
		// (e.g. anon.cf-warp-9), direct lanes use anon.direct. This makes
		// 429s attributable to a specific exit without guessing.
		label := keyID
		if anonymous {
			label = "anon"
			if egress != nil {
				if egress.proxy != nil && egress.proxy.Tag != "" {
					label += "." + egress.proxy.Tag
				} else {
					label += ".direct"
				}
			}
		}
		leg := label + ":"
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				leg += "timeout"
			} else {
				leg += "transport"
			}
		} else {
			leg += strconv.Itoa(status)
		}
		leg += ":" + strconv.FormatInt(max(duration.Milliseconds(), 0), 10) + "ms"
		meta.Legs = append(meta.Legs, leg)
	}
	if g.monitor == nil {
		return
	}
	egressName := "unavailable"
	if egress != nil {
		egressName = config.RedactURL(egress.address)
	}
	g.monitor.RecordAttempt(telemetry.UpstreamAttempt{
		Time: time.Now().UTC(), RequestID: ids.Request, Model: route.ID, Tier: string(route.Tier), Attempt: attempt,
		KeyID: keyID, Channel: channel, Anonymous: anonymous, Egress: egressName, Status: status,
		DurationMS: max(duration.Milliseconds(), 0), Success: success, Outcome: outcome,
	})
}

// upstreamPath returns the protocol endpoint under an upstream base. OpenCode
// inference bases use the vendor-scoped contract from wire.Path; providers
// are plain OpenAI-compatible APIs that keep the /v1/* routes.
func upstreamPath(protocol wire.Protocol, opencode bool) string {
	if opencode {
		return wire.Path(protocol)
	}
	switch protocol {
	case wire.Responses:
		return "/v1/responses"
	case wire.Anthropic:
		return "/v1/messages"
	case wire.SystemOne:
		return "/v1/systemone"
	default:
		return "/v1/chat/completions"
	}
}

func newUpstreamRequest(ctx context.Context, upstream string, opencode bool, protocol wire.Protocol, body []byte, ids identity.RequestIDs, key string) (*http.Request, error) {
	endpoint := strings.TrimRight(upstream, "/") + upstreamPath(protocol, opencode)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// The header set mirrors the official CLI's model requests
	// (packages/core/src/session/model-request.ts): Accept: */*, the
	// opencode/<channel>/<version>/cli UA, and the same canonical session on
	// every affinity alias — the free-tier gate accepts any of
	// x-opencode-session / x-session-affinity / x-session-id, and provider
	// prompt caches key off them. The current contract reads no
	// x-opencode-request header, so none is sent.
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", httpx.UserAgent())
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("x-opencode-session", ids.Session)
	req.Header.Set("x-opencode-session-id", ids.Session)
	req.Header.Set("x-session-affinity", ids.Session)
	req.Header.Set("X-Session-Id", ids.Session)
	req.Header.Set("x-opencode-project", ids.Project)
	if ids.ParentSession != "" {
		req.Header.Set("x-parent-session-id", ids.ParentSession)
		req.Header.Set("x-opencode-parent-session-id", ids.ParentSession)
	}
	if protocol == wire.Anthropic {
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14")
	} else {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return req, nil
}

func isNonRetryableClientResponse(resp *http.Response, err error) bool {
	return err == nil && resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests
}
