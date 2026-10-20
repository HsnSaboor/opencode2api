package gateway

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// rotationMembership ranks every Go-servable model into the two rotation
// groups from the upstream catalog's published prices. The most capable
// models (highest published output price) form SOTA; the rest, cheapest
// first, form the value group. Nothing about a model is compiled in: a new
// flagship joins SOTA, a repriced model changes groups, and a retired model
// drops out on its own.
//
// The split is by price because the catalog publishes no capability score and
// price is the operator-visible proxy for it. Within a group, ties break on
// input price, then context window, then release date, then ID, so ranking is
// deterministic across refreshes.
func (g *Gateway) rotationMembership() rotation.Membership {
	models := g.catalog.List()
	type candidate struct {
		id         string
		inputCost  float64
		outputCost float64
		context    int
		released   string
	}
	candidates := make([]candidate, 0, len(models))
	for _, model := range models {
		route, err := g.catalog.RouteForTier(model, config.TierGo, g.keyAvailability())
		if err != nil || route.Protocol == wire.SystemOne {
			continue
		}
		md := g.catalog.MetadataForTier(model, config.TierGo)
		item := candidate{id: model, context: md.ContextWindow, released: md.ReleaseDate}
		if md.InputCost != nil {
			item.inputCost = *md.InputCost
		}
		if md.OutputCost != nil {
			item.outputCost = *md.OutputCost
		}
		candidates = append(candidates, item)
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		if a.outputCost != b.outputCost {
			return cmp.Compare(b.outputCost, a.outputCost)
		}
		if a.inputCost != b.inputCost {
			return cmp.Compare(b.inputCost, a.inputCost)
		}
		if a.context != b.context {
			return cmp.Compare(b.context, a.context)
		}
		if a.released != b.released {
			return cmp.Compare(b.released, a.released)
		}
		return strings.Compare(a.id, b.id)
	})
	size := min(max(g.cfg.Rotation.SOTASize, 1), len(candidates))
	membership := rotation.Membership{
		SOTA:  make([]string, 0, size),
		Sweet: make([]string, 0, len(candidates)-size),
	}
	for i, item := range candidates {
		if i < size {
			membership.SOTA = append(membership.SOTA, item.id)
			continue
		}
		membership.Sweet = append(membership.Sweet, item.id)
	}
	// The value group reads cheapest-first, which is the opposite of the
	// capability ordering the sort produced.
	slices.Reverse(membership.Sweet)
	return membership
}

func (g *Gateway) syncRotation() {
	g.rotation.Sync(g.rotationMembership())
}
func (m *RuntimeManager) RotationSnapshot() rotation.Snapshot {
	r := m.current.Load()
	r.gateway.syncRotation()
	return m.rotation.Snapshot()
}

// RotationAliases lists the rotation group aliases that currently resolve to
// at least one Go-servable model, in group order. The WebUI offers them as the
// stable model name to put in a client configuration.
func (m *RuntimeManager) RotationAliases() []string {
	runtime := m.current.Load()
	runtime.gateway.syncRotation()
	snapshot := m.rotation.Snapshot()
	aliases := make([]string, 0, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		for _, model := range group.Order {
			if route, err := runtime.gateway.catalog.RouteForTier(model, config.TierGo, runtime.gateway.keyAvailability()); err == nil && route.Protocol != wire.SystemOne {
				aliases = append(aliases, group.Alias)
				break
			}
		}
	}
	return aliases
}
func (m *RuntimeManager) SelectRotation(id, model string) error {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	m.current.Load().gateway.syncRotation()
	return m.rotation.Manual(id, model)
}

// probeEffortLadder orders the levels to try for an auto-effort probe, from
// the model's own declared ladder. The probe's goal is the highest level the
// model accepts, so the ladder is reversed from strongest to weakest and ends
// with "none" (the already-succeeded baseline) so a model that accepts no
// effort at all is recorded as such rather than left unknown.
//
// A model that declares no named effort (toggle or budget-only) has no ladder
// to search: the baseline result stands as "none".
func probeEffortLadder(declared []string) []string {
	rank := func(level string) int {
		switch level {
		case "max":
			return 5
		case "xhigh":
			return 4
		case "high":
			return 3
		case "medium":
			return 2
		case "low":
			return 1
		case "minimal":
			return 0
		default:
			return -1
		}
	}
	levels := make([]string, 0, len(declared))
	for _, level := range declared {
		if rank(level) >= 0 {
			levels = append(levels, level)
		}
	}
	// "none" is the baseline the probe already ran successfully; it must come
	// last so it is only recorded when every real level was rejected.
	levels = append(levels, "none")
	slices.SortStableFunc(levels, func(a, b string) int {
		if a == "none" {
			return 1
		}
		if b == "none" {
			return -1
		}
		return cmp.Compare(rank(b), rank(a))
	})
	return levels
}

// Explicit quota markers only: ordinary provider errors must keep their budget.
func quotaError(status int, body string) bool {
	if status == 429 {
		return true
	}
	body = strings.ToLower(body)
	for _, marker := range []string{"insufficient_quota", "quota_exceeded", "quota exceeded", "quota exhausted", "rate_limit", "rate limit", "rate-limit", "usage_limit", "usage limit", "insufficient balance", "credit balance is too low", "额度不足", "配额已用尽"} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

type groupAttempt struct {
	err              error
	quota, committed bool
	pause            time.Duration
	reason           string
}

func (g *Gateway) handleRotation(w http.ResponseWriter, r *http.Request, external wire.Protocol, payload map[string]any, id, alias string) {
	g.syncRotation()
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
	defer cancel()
	ids := identity.DeriveRequestIDs(r, payload)
	meta := telemetry.MetaFromRequest(r)
	if meta != nil {
		meta.Request = ids.Request
		meta.ModelAlias = alias
	}
	visited := map[string]bool{}
	ordinaryModels, total := 0, 0
	for ctx.Err() == nil {
		token, ok := g.rotation.Select(id, visited)
		if !ok {
			break
		}
		visited[token.Model] = true
		route, err := g.catalog.RouteForTier(token.Model, config.TierGo, g.keyAvailability())
		if err != nil {
			continue
		}
		counted := false
		for n := 1; n <= 3 && ctx.Err() == nil; n++ {
			total++
			if meta != nil {
				meta.Model = token.Model
				meta.Tier = "go"
				meta.Protocol = route.Protocol
				meta.Attempts = total
				meta.Usage = wire.Usage{}
				meta.UsageReported = false
			}
			result := g.rotationAttempt(ctx, w, r, external, payload, alias, route, ids, total)
			outcome := result.reason
			if result.err == nil {
				outcome = "success"
			}
			var clientWrite *wire.ClientWriteError
			canceled := r.Context().Err() != nil || errors.As(result.err, &clientWrite)
			if canceled {
				outcome = "client_canceled"
				if meta != nil {
					meta.Outcome = outcome
				}
			}
			g.rotation.Record(rotation.Attempt{Request: ids.Request, Alias: alias, Model: token.Model, Number: total, Outcome: outcome})
			if state := g.rotation.Snapshot(); state.PersistenceError != "" {
				g.logger.Error("rotation state persistence failed", "component", "rotation", "event", "rotation_persistence_failed")
			}
			g.logger.Info("model rotation attempt", "component", "rotation", "event", "rotation_attempt", "request_id", ids.Request, "alias", alias, "model", token.Model, "attempt", total, "reason", outcome)
			if canceled {
				return
			}
			if result.err == nil {
				return
			}
			if result.committed {
				if meta != nil {
					meta.Outcome = "stream_error"
				}
				pause := 15 * time.Second
				if result.quota {
					pause = result.pause
				}
				g.rotation.Fail(token, pause, result.reason)
				return
			}
			if ctx.Err() != nil {
				g.rotation.Fail(token, 15*time.Second, "request_timeout")
				break
			}
			if result.quota {
				g.rotation.Fail(token, result.pause, result.reason)
				break
			}
			if !counted {
				ordinaryModels++
				counted = true
			}
			if n == 3 {
				g.rotation.Fail(token, 15*time.Second, result.reason)
			}
		}
		if ordinaryModels >= 3 {
			break
		}
	}
	if r.Context().Err() != nil {
		return
	}
	status := http.StatusServiceUnavailable
	message := "该分组暂无可用模型（已暂停、下架、额度不足或重试次数已用尽），请稍后重试"
	if ctx.Err() != nil {
		status = http.StatusGatewayTimeout
		message = "分组请求整体超时"
	}
	wire.WriteError(w, external, status, message, "rotation_unavailable", ids.Request)
}

func (g *Gateway) rotationAttempt(parent context.Context, w http.ResponseWriter, original *http.Request, external wire.Protocol, payload map[string]any, alias string, route models.Route, ids identity.RequestIDs, attempt int) groupAttempt {
	result := groupAttempt{reason: "upstream_error", pause: 60 * time.Second}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	seconds := g.rotation.Snapshot().FirstOutputSeconds
	timer := time.AfterFunc(time.Duration(seconds)*time.Second, cancel)
	defer timer.Stop()
	copyPayload := make(map[string]any, len(payload))
	for k, v := range payload {
		copyPayload[k] = v
	}
	copyPayload["model"] = route.ID
	bodies, err := g.prepareRouteBodies(external, route, copyPayload)
	if err != nil {
		result.err = err
		result.reason = "request_conversion_error"
		return result
	}
	goChannel := g.channel(config.TierGo)
	body, shaped := g.shapeKeyBody(goChannel, bodies[config.TierGo], route)
	cursor := goChannel.nodes.CursorFor(ids.Session)
	var node *upstreamNode
	// Each group attempt is exactly one HTTP call. Key selection may change,
	// but there is no nested retry, stale-reasoning replay, or model-wide key ban.
	for n := 0; n < attempt; n++ {
		node = cursor.Next()
	}
	if node == nil {
		result.err = errors.New("no Go key")
		return result
	}
	egress := goChannel.nodes.Egress(node)
	if egress == nil {
		result.err = errors.New("no egress lane")
		return result
	}
	req, err := newUpstreamRequest(ctx, goChannel.upstream, goChannel.opencode, route.Protocol, body, ids, node.key)
	if err != nil {
		result.err = err
		return result
	}
	// A reused connection must not cause net/http to implicitly replay POSTs.
	req.GetBody = nil
	client := *egress.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	setRequestCredential(parent, config.TierGo, config.KeyDisplayID(node.key), "key", false, egress)
	started := time.Now()
	resp, err := client.Do(req)
	g.recordUpstreamAttempt(parent, route, ids, attempt, config.KeyDisplayID(node.key), "key", false, egress, resp, err, time.Since(started))
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		result.err = err
		result.reason = "transport_error"
		if ctx.Err() != nil {
			result.reason = "first_output_timeout"
		}
		return result
	}
	// Absent/invalid Retry-After keeps the 60s default; an explicit zero
	// ("0" or an HTTP date at/before now) means retry immediately.
	if pause, ok := parseRetryAfterPresent(resp.Header.Get("Retry-After")); ok {
		result.pause = pause
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		result.err = fmt.Errorf("upstream HTTP %d", resp.StatusCode)
		result.quota = quotaError(resp.StatusCode, string(b))
		result.reason = fmt.Sprintf("http_%d", resp.StatusCode)
		if result.quota {
			result.reason = "quota_or_rate_limit"
		}
		return result
	}
	stream := jsonutil.BoolAt(payload, "stream")
	ready := func() error {
		if !timer.Stop() || ctx.Err() != nil {
			return context.DeadlineExceeded
		}
		w.Header().Set("X-Resolved-Model", route.ID)
		w.Header().Set("x-request-id", ids.Request)
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		return nil
	}
	meta := telemetry.MetaFromContext(parent)
	if stream {
		if meta != nil {
			meta.Stream = true
		}
		if g.monitor != nil {
			g.monitor.BeginStream()
			defer g.monitor.EndStream()
		}
		usage, reported, committed, err := wire.RotationStream(original.Context(), w, resp.Body, route.Protocol, external, alias, ready)
		if meta != nil {
			meta.Usage = usage
			meta.UsageReported = reported
		}
		result.err = err
		result.committed = committed
		result.reason = "stream_error"
		var failure *wire.StreamFailure
		if errors.As(err, &failure) {
			result.quota = quotaError(0, failure.Type+" "+failure.Message)
		}
		if result.quota {
			result.reason = "quota_or_rate_limit"
		} else if !committed && ctx.Err() != nil {
			result.reason = "first_output_timeout"
		}
		return result
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (64<<20)+1))
	if err == nil && len(b) > 64<<20 {
		err = errors.New("response exceeds size limit")
	}
	if err != nil {
		result.err = err
		result.reason = "response_read_error"
		return result
	}
	if !timer.Stop() || ctx.Err() != nil {
		result.err = context.DeadlineExceeded
		result.reason = "first_output_timeout"
		return result
	}
	if shaped {
		b, err = wire.CollapseStream(bytes.NewReader(b), route.Protocol, route.ID)
	}
	var document map[string]any
	if err == nil {
		err = json.Unmarshal(b, &document)
	}
	if err == nil && document == nil {
		err = errors.New("empty upstream response")
	}
	if err == nil && document["error"] != nil {
		result.quota = quotaError(0, string(b))
		err = errors.New("upstream error envelope")
	}
	if err == nil {
		valid := false
		switch route.Protocol {
		case wire.Chat:
			_, valid = document["choices"].([]any)
		case wire.Anthropic:
			_, valid = document["content"].([]any)
		case wire.Responses:
			_, valid = document["output"].([]any)
		}
		if !valid {
			err = errors.New("invalid response schema")
		}
	}
	if err == nil && external != route.Protocol {
		b, err = wire.ConvertResponse(route.Protocol, external, b)
	}
	if err != nil {
		result.err = err
		result.reason = "response_conversion_error"
		if result.quota {
			result.reason = "quota_or_rate_limit"
		}
		return result
	}
	if meta != nil {
		meta.Usage, meta.UsageReported = wire.ResponseUsage(external, b)
	}
	document = nil
	if err = json.Unmarshal(b, &document); err != nil {
		result.err = err
		result.reason = "response_conversion_error"
		return result
	}
	document["model"] = alias
	b, err = json.Marshal(document)
	if err != nil {
		result.err = err
		return result
	}
	w.Header().Set("X-Resolved-Model", route.ID)
	w.Header().Set("x-request-id", ids.Request)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(b)
	if err != nil {
		result.err = &wire.ClientWriteError{Err: err}
	}
	result.committed = true
	return result
}
