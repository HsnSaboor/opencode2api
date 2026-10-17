package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	modelcatalog "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

const (
	egressHealthCheckURL      = "https://cloudflare.com/cdn-cgi/trace"
	egressHealthCheckInterval = 15 * time.Minute
	egressHealthCheckTimeout  = 10 * time.Second
	// egressWarmTimeout bounds the whole startup pre-dial across all
	// lanes; each lane additionally caps its own check at
	// egressHealthCheckTimeout, so this is the outer budget only.
	egressWarmTimeout = 10 * time.Second
	// prefixProbationInterval is how often the gateway re-tests a cooled
	// egress prefix whose cooldown window has expired. The probe request is
	// one minimal completion; a still-flagged prefix re-cools for another
	// full window, so a blocked prefix costs a few probes per hour while
	// recovery to a healthy rotation is automatic.
	prefixProbationInterval = 5 * time.Minute
	prefixProbationTimeout  = 30 * time.Second
)

// syncEgressResult updates lane health from real traffic. Only timeouts and
// connection refusals mark a lane unavailable. Other errors and 4xx/5xx
// responses trigger a neutral URL check without being treated as lane failure.
func (g *Gateway) syncEgressResult(ctx context.Context, egress *egressTransport, status int, err error) bool {
	if egress == nil {
		return false
	}
	if isEgressFailure(err) {
		g.rebindFailedEgress(egress)
		g.verifyEgressAfterError(ctx, egress, status)
		return true
	}
	if status >= 200 && status < 400 {
		wasHealthy := egress.healthy.Swap(true)
		if !wasHealthy {
			g.restoreEgress(egress)
		}
		return false
	}
	if err != nil {
		g.verifyEgressAfterError(ctx, egress, status)
		return false
	}
	if status >= 400 && status < 600 {
		g.verifyEgressAfterError(ctx, egress, status)
	}
	return false
}

func (g *Gateway) verifyEgressAfterError(ctx context.Context, egress *egressTransport, status int) {
	if !egress.checking.CompareAndSwap(false, true) {
		return
	}
	// The client request may finish or be cancelled while the verification is
	// running. Keep its values but give the lane check an independent timeout.
	checkCtx := context.WithoutCancel(ctx)
	go func() {
		result := g.transports.checkClaimedEgress(checkCtx, egress, egressHealthCheckURL, egressHealthCheckTimeout)
		g.applyEgressHealthResult(result, "upstream HTTP response", status)
	}()
}

func (g *Gateway) rebindFailedEgress(egress *egressTransport) (zenMoved, goMoved int) {
	if egress == nil {
		return 0, 0
	}
	wasHealthy := egress.healthy.Swap(false)
	return g.rebindUnavailableEgress(egress, wasHealthy)
}

func (g *Gateway) rebindUnavailableEgress(egress *egressTransport, wasHealthy bool) (zenMoved, goMoved int) {
	zenMoved = g.zenNodes.RebindEgress(egress.index)
	goMoved = g.goNodes.RebindEgress(egress.index)
	if wasHealthy || zenMoved+goMoved > 0 {
		g.logger.Warn("egress lane became unavailable", "component", "egress", "event", "egress_unavailable", "egress", config.RedactURL(egress.address), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved)
	}
	return zenMoved, goMoved
}

func (g *Gateway) restoreEgress(egress *egressTransport) (zenMoved, goMoved int) {
	if egress == nil {
		return 0, 0
	}
	zenMoved = g.zenNodes.RestoreEgress(egress.index)
	goMoved = g.goNodes.RestoreEgress(egress.index)
	if zenMoved+goMoved > 0 {
		g.logger.Info("egress connectivity restored", "component", "egress", "event", "egress_restored", "egress", config.RedactURL(egress.address), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved)
	}
	return zenMoved, goMoved
}

func (g *Gateway) StartEgressHealthChecks(ctx context.Context) {
	check := func() {
		results := g.transports.CheckHealth(ctx, egressHealthCheckURL, egressHealthCheckTimeout)
		for _, result := range results {
			g.applyEgressHealthResult(result, "scheduled health check", 0)
		}
	}
	go func() {
		ticker := time.NewTicker(egressHealthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				check()
			}
		}
	}()
}

func (g *Gateway) applyEgressHealthResult(result egressHealthResult, source string, upstreamStatus int) {
	if result.err == nil {
		if !result.wasHealthy {
			g.restoreEgress(result.egress)
		}
		g.logger.Debug("egress health check passed", "component", "egress", "event", "health_check_passed", "source", source, "upstream_status", upstreamStatus, "egress", config.RedactURL(result.egress.address))
		return
	}
	if !result.failed {
		g.logger.Debug("egress health check was inconclusive", "component", "egress", "event", "health_check_inconclusive", "source", source, "upstream_status", upstreamStatus, "egress", config.RedactURL(result.egress.address), "error", result.err)
		return
	}
	if g.transports.hasHealthy() {
		zenMoved, goMoved := g.rebindUnavailableEgress(result.egress, result.wasHealthy)
		if result.wasHealthy || zenMoved+goMoved > 0 {
			g.logger.Warn("egress health check failed", "component", "egress", "event", "health_check_failed", "source", source, "upstream_status", upstreamStatus, "egress", config.RedactURL(result.egress.address), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved, "error", result.err)
			return
		}
	}
	g.logger.Debug("egress health check is still failing", "component", "egress", "event", "health_check_still_failing", "source", source, "upstream_status", upstreamStatus, "egress", config.RedactURL(result.egress.address), "error", result.err)
}

// startPrefixProbation re-tests FreeTier-cooled prefixes on a timer so a
// prefix that was blocked upstream rejoins the rotation as soon as the flag
// clears, without waiting for incidental traffic. It runs beside the model
// refresh loop and stops with the same context.
func (g *Gateway) startPrefixProbation(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(prefixProbationInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.probeCoolingPrefixes(ctx)
			}
		}
	}()
}

// probeCoolingPrefixes sends one minimal gated request from each cooled
// prefix whose window has expired. A success clears the cooldown (the prefix
// is serving traffic again); a repeated FreeTier verdict restarts it. Other
// outcomes — including transport errors, which say nothing about the flag —
// leave the state untouched.
func (g *Gateway) probeCoolingPrefixes(ctx context.Context) {
	if !g.cfg.Anonymous {
		return
	}
	cooling := g.transports.CoolingPrefixes()
	if len(cooling) == 0 {
		return
	}
	model := g.probationModel()
	if model == "" {
		return
	}
	now := time.Now()
	for prefix, until := range cooling {
		if ctx.Err() != nil {
			return
		}
		if now.Before(until) {
			continue
		}
		g.probeCoolingPrefix(ctx, prefix, model)
	}
}

// probationModel picks the free model a probation probe asks for: any free
// model exercises the same FreeTier gate, and the request is capped at a
// single token.
func (g *Gateway) probationModel() string {
	for _, model := range g.catalog.List() {
		if g.catalog.IsFreeModel(model) {
			return model
		}
	}
	return ""
}

func (g *Gateway) probeCoolingPrefix(ctx context.Context, prefix, model string) {
	addrs, err := config.SampleFreshEgress([]string{prefix}, nil, 1, nil)
	if err != nil || len(addrs) == 0 {
		g.logger.Debug("egress prefix probation could not sample an address", "component", "egress", "event", "prefix_probation_sample_failed", "prefix", prefix, "error", err)
		return
	}
	address := addrs[0]
	egress, err := g.transports.buildLane(0, address)
	if err != nil {
		g.logger.Debug("egress prefix probation could not build a lane", "component", "egress", "event", "prefix_probation_lane_failed", "prefix", prefix, "egress", config.RedactURL(address), "error", err)
		return
	}
	body, err := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []any{map[string]any{"role": "user", "content": "ping"}},
		"max_tokens": 1,
		"stream":     true,
	})
	if err != nil {
		return
	}
	// Probation traffic carries the same fingerprint as production traffic:
	// the verdict is only meaningful when the request itself would pass the
	// gate from a healthy prefix.
	body = shapeAnonymousBody(body, wire.Chat)
	ids := identity.RequestIDs{
		Session: identity.CanonicalSessionID("probation:" + prefix),
		Project: identity.ProjectID(""),
	}
	probeCtx, cancel := context.WithTimeout(ctx, prefixProbationTimeout)
	defer cancel()
	req, err := newUpstreamRequest(probeCtx, g.cfg.Upstream.Zen, wire.Chat, body, ids, anonymousZenKey)
	if err != nil {
		return
	}
	started := time.Now()
	resp, err := egress.client.Do(req)
	duration := time.Since(started)
	if err != nil {
		g.logger.Debug("egress prefix probation request failed", "component", "egress", "event", "prefix_probation_transport_failed", "prefix", prefix, "egress", config.RedactURL(address), "duration_ms", duration.Milliseconds(), "error", err)
		return
	}
	defer httpx.DrainAndClose(resp.Body)
	if resp.StatusCode == http.StatusForbidden && classifyForbidden(peekForbiddenSnippet(resp, 2048)) == forbiddenFreeTier {
		g.transports.reCoolPrefix(config.CanonicalizePrefix(prefix))
		g.logger.Info("egress prefix probation still blocked", "component", "egress", "event", "prefix_probation_blocked", "prefix", prefix, "egress", config.RedactURL(address), "duration_ms", duration.Milliseconds())
		return
	}
	if resp.StatusCode/100 == 2 {
		g.transports.ClearPrefix(address)
		g.logger.Info("egress prefix probation succeeded; prefix rejoined the rotation", "component", "egress", "event", "prefix_probation_recovered", "prefix", prefix, "egress", config.RedactURL(address), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
		return
	}
	g.logger.Debug("egress prefix probation was inconclusive", "component", "egress", "event", "prefix_probation_inconclusive", "prefix", prefix, "egress", config.RedactURL(address), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
}

func (g *Gateway) StartModelRefresh(ctx context.Context) {
	g.startPrefixProbation(ctx)
	refresh := func() {
		var zen, goModels []string
		var capabilities modelcatalog.Capabilities
		var capabilitiesErr error
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); zen = g.refreshZen(ctx) }()
		go func() { defer wg.Done(); goModels = g.refreshTier(ctx, g.cfg.Upstream.Go, g.goNodes) }()
		go func() {
			defer wg.Done()
			capabilityCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			capabilities, capabilitiesErr = g.refreshProtocolCapabilities(capabilityCtx)
		}()
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
		if capabilitiesErr != nil {
			g.logger.Warn("OpenCode capability catalog refresh failed", "component", "models", "event", "capability_refresh_failed", "error", capabilitiesErr)
		}
		if zen != nil || goModels != nil {
			g.catalog.ReplaceWithCapabilities(zen, goModels, capabilities.Protocols, capabilities.Unsupported, capabilities.Metadata)
			g.syncRotation()
			if ctx.Err() == nil {
				if err := g.catalog.SaveCache(); err != nil {
					g.logger.Warn("model catalog cache write failed", "component", "models", "event", "catalog_cache_write_failed", "error", err)
				}
			}
			g.logger.Info("model catalog refreshed", "component", "models", "event", "catalog_refreshed", "models", len(g.catalog.List()))
		}
	}
	go func() {
		refresh()
		ticker := time.NewTicker(time.Duration(g.cfg.Models.RefreshSeconds) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

func (g *Gateway) refreshProtocolCapabilities(ctx context.Context) (modelcatalog.Capabilities, error) {
	items := g.transports.snapshot()
	if len(items) == 0 {
		return modelcatalog.FetchCapabilities(ctx, &http.Client{Timeout: 30 * time.Second}, modelcatalog.CapabilitiesURL)
	}
	var lastErr error
	for _, egress := range items {
		if egress == nil || !egress.healthy.Load() {
			continue
		}
		capabilities, err := modelcatalog.FetchCapabilities(ctx, egress.client, modelcatalog.CapabilitiesURL)
		if err == nil {
			return capabilities, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy egress lane available for OpenCode capability catalog")
	}
	return modelcatalog.Capabilities{}, lastErr
}

func (g *Gateway) refreshZen(ctx context.Context) []string {
	if models := g.refreshTier(ctx, g.cfg.Upstream.Zen, g.zenNodes); models != nil {
		return models
	}
	if !g.cfg.Anonymous {
		return nil
	}
	return g.refreshAnonymousTier(ctx, g.cfg.Upstream.Zen)
}

func (g *Gateway) refreshAnonymousTier(ctx context.Context, base string) []string {
	cursor := g.anonymous.CursorFor("")
	limit := g.anonymous.Len()
	for attempt := 1; attempt <= limit; attempt++ {
		node := cursor.Next()
		if node == nil {
			break
		}
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		models, status, err := modelcatalog.FetchModels(refreshCtx, node.egress.client, base, anonymousZenKey)
		g.syncEgressResult(refreshCtx, node.egress, status, err)
		cancel()
		if err == nil {
			g.anonymous.MarkSuccess(node)
			return models
		}
		g.anonymous.MarkFailure(node, nil, err)
		g.logger.Debug("anonymous model catalog refresh attempt failed", "component", "models", "event", "anonymous_refresh_attempt_failed", "upstream", config.RedactURL(base), "attempt", attempt, "egress", config.RedactURL(node.egress.address), "error", err)
	}
	g.logger.Warn("anonymous model catalog refresh failed", "component", "models", "event", "anonymous_refresh_failed", "upstream", config.RedactURL(base))
	return nil
}

func (g *Gateway) refreshTier(ctx context.Context, base string, nodes *nodePool) []string {
	cursor := nodes.Cursor()
	for attempt := 0; attempt < g.cfg.Retry.MaxAttempts; attempt++ {
		node := cursor.Next()
		if node == nil {
			return nil
		}
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		egress := nodes.Egress(node)
		if egress == nil {
			cancel()
			return nil
		}
		models, status, err := modelcatalog.FetchModels(refreshCtx, egress.client, base, node.key)
		g.syncEgressResult(refreshCtx, egress, status, err)
		cancel()
		if err == nil {
			nodes.MarkSuccess(node)
			return models
		}
		nodes.MarkFailure(node, nil, err)
		g.logger.Debug("model catalog refresh attempt failed", "component", "models", "event", "refresh_attempt_failed", "upstream", config.RedactURL(base), "attempt", attempt+1, "error", err)
	}
	g.logger.Warn("model catalog refresh failed", "component", "models", "event", "refresh_failed", "upstream", config.RedactURL(base))
	return nil
}
