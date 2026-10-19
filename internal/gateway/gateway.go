// Package gateway routes inference requests through managed upstream pools.
package gateway

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

const maxRequestBody = 32 << 20

const anonymousZenKey = "public"

// channel is one authenticated upstream family: a base URL with its own key
// pool and wire dialect. Zen and Go are the built-in OpenCode channels; every
// configured provider is one more channel under its own name.
type channel struct {
	tier     config.Tier
	upstream string
	// opencode marks an OpenCode inference upstream: vendor-scoped protocol
	// paths, the free-tier request shaping, and egress verdicts shared with
	// the anonymous lane. Provider channels are plain OpenAI-compatible APIs
	// that reuse the same pool machinery without the gate semantics.
	opencode bool
	// home declares which lane class this channel's key pool treats as
	// home for its keys: dedicated source-address lanes (one /64 per key)
	// by default, proxy exits whenever no dedicated lane is vacant. Zen
	// sets homeDedicatedOnly: its keys ride their own /64 lanes, and no
	// proxy exit is ever applied to keyed free-tier traffic.
	home egressHome
	// suffix is appended to every discovered model ID before it enters the
	// catalog, so a provider whose free routes carry a name suffix is
	// addressable by the exact ID its upstream accepts.
	suffix string
	nodes  *nodePool
}

type Gateway struct {
	rotation     *rotation.Manager
	cfg          config.Config
	logger       *slog.Logger
	transports   *transportPool
	channels     map[config.Tier]*channel
	order        []config.Tier
	anonymous    *anonymousPool
	catalog      *models.Catalog
	monitor      *telemetry.Monitor
	availability *models.AvailabilityStore
	// proxies is nil unless the proxy pool is configured: it owns the
	// candidate node set, verification state, and multiplexer process.
	proxies *proxyManager
	// proxyCanary caches the most recent gate-canary verdict (see
	// proxyGateRejectsRequest): one canonical-profile probe per TTL decides
	// whether a burst of FreeTier verdicts describes the requests or the
	// exits, on both the request and verification paths.
	proxyCanaryMu sync.Mutex
	proxyCanaryAt time.Time
	proxyCanaryOK bool
}

func New(cfg config.Config, logger *slog.Logger, monitor *telemetry.Monitor) (*Gateway, error) {
	startup := time.Now()
	cfg.Rotation.Defaults()
	rotationManager, err := rotation.New("", cfg.Rotation)
	if err != nil {
		return nil, err
	}
	addresses := cfg.EgressAddresses()
	prefixes := cfg.Egress.Prefixes
	prefixWeights := cfg.Egress.PrefixWeights
	if cfg.Egress.IPv4Only || !cfg.Egress.EnableIPv6 {
		prefixes = nil
		prefixWeights = nil
	}
	transports, err := newTransportPool(addresses, cfg.Performance, cfg.Performance.AttemptTimeout(time.Duration(cfg.Retry.TimeoutSeconds)*time.Second), prefixes, prefixWeights)
	if err != nil {
		return nil, err
	}
	if logger != nil {
		// The sampled pool is fresh per process start, so one startup line
		// names what this instance actually rotates. Full literals stay at
		// debug: a pool of 128 addresses would spam the default log level.
		logger.Info("egress pool ready", "component", "egress", "event", "egress_pool_ready", "prefixes", prefixes, "pool_size", len(addresses), "setup_ms", time.Since(startup).Milliseconds())
		logger.Debug("egress pool addresses", "component", "egress", "event", "egress_pool_addresses", "addresses", addresses)
	}
	// Pre-dial lanes in the background against the same neutral target the
	// scheduled health checks use, so the first client request usually finds
	// a warm TLS route. Warming must not gate startup: a few dead addresses
	// used to stretch the listener's arrival to egressWarmTimeout, which is
	// exactly the restart window clients saw as an outage.
	go func() {
		started := time.Now()
		warmCtx, warmCancel := context.WithTimeout(context.Background(), egressWarmTimeout)
		defer warmCancel()
		results := transports.warmLanes(warmCtx, egressHealthCheckURL, egressHealthCheckTimeout)
		warmed := 0
		for _, result := range results {
			if result.err == nil {
				warmed++
			}
		}
		if logger != nil {
			logger.Info("egress pool warmed", "component", "egress", "event", "egress_pool_warmed", "warmed", warmed, "total", len(results), "duration_ms", time.Since(started).Milliseconds())
		}
	}()
	cooldown := time.Duration(cfg.Performance.FailureCooldownSeconds) * time.Second
	tiers := cfg.Tiers()
	channels := make(map[config.Tier]*channel, len(tiers))
	for _, tier := range tiers {
		nodes, err := newNodePool(cfg.TierKeys(tier), transports, cooldown)
		if err != nil {
			return nil, fmt.Errorf("%s node pool: %w", tier, err)
		}
		home := homeDedicated
		if tier == config.TierZen {
			// Zen keys ride their dedicated /64 lanes only; no proxy
			// exit ever carries keyed free-tier traffic (see channel.home).
			home = homeDedicatedOnly
		}
		channels[tier] = &channel{
			tier:     tier,
			upstream: cfg.TierUpstream(tier),
			opencode: tier == config.TierZen || tier == config.TierGo,
			home:     home,
			suffix:   cfg.TierModelSuffix(tier),
			nodes:    nodes,
		}
	}
	catalog := models.NewCatalog(cfg.Prefer, tiers, cfg.Models.Protocols)
	catalog.SetRefreshInterval(time.Duration(cfg.Models.RefreshSeconds) * time.Second)
	gateway := &Gateway{
		cfg:        cfg,
		rotation:   rotationManager,
		logger:     logger,
		transports: transports,
		channels:   channels,
		order:      tiers,
		catalog:    catalog,
		monitor:    monitor,
	}
	// The proxy pool is attached before the anonymous pool snapshots the
	// lane list so seeded proxy lanes become anonymous nodes like any
	// other; later admissions append via anonymousPool.addNode.
	if cfg.Egress.Proxies.Enabled {
		manager, proxyErr := newProxyManager(cfg.Egress.Proxies, logger)
		if proxyErr != nil {
			return nil, fmt.Errorf("proxy pool: %w", proxyErr)
		}
		gateway.proxies = manager
		transports.proxies = manager
		seeded := gateway.seedProxyLanes()
		if logger != nil {
			logger.Info("proxy pool ready", "component", "egress", "event", "proxy_pool_ready", "nodes", len(manager.profile.Nodes), "lanes", seeded, "count", manager.cfg.Count, "socks_port", manager.cfg.SocksPort, "state_file", manager.statePath, "startup_ms", time.Since(startup).Milliseconds())
		}
	}
	if cfg.Anonymous && cfg.Egress.Proxies.Enabled {
		// The free tier now rejects the host's datacenter prefixes as abusive
		// as a class. Bench them from the start instead of waiting for the
		// first request to discover the block and briefly surface its 403.
		transports.ParkDirectEgress(directEgressHardPark)
		if logger != nil {
			logger.Info("direct egress lanes hard-parked for anonymous traffic", "component", "egress", "event", "direct_egress_hard_parked", "duration", directEgressHardPark.String())
		}
	}
	gateway.anonymous = newAnonymousPool(cfg.Anonymous, transports, cooldown, prefixes)
	// Key pools bound before proxy lanes seeded, so spread them now: the
	// seeded lanes would otherwise idle keyless while one address carries
	// every account of a keyed provider.
	gateway.rebalanceEgressKeys()
	return gateway, nil
}

// channel returns the authenticated channel for one tier, or nil when the
// tier is not configured.
func (g *Gateway) channel(tier config.Tier) *channel {
	return g.channels[tier]
}

// orderedChannels returns every channel in configuration order (zen, go,
// then providers).
func (g *Gateway) orderedChannels() []*channel {
	channels := make([]*channel, 0, len(g.order))
	for _, tier := range g.order {
		if channel := g.channels[tier]; channel != nil {
			channels = append(channels, channel)
		}
	}
	return channels
}

// keyAvailability reports which tiers hold at least one key. It is the single
// routing input for tier selection, so a tier's eligibility always matches
// the pool that would serve it.
func (g *Gateway) keyAvailability() map[config.Tier]bool {
	availability := make(map[config.Tier]bool, len(g.channels))
	for tier, channel := range g.channels {
		availability[tier] = channel.nodes.Len() > 0
	}
	return availability
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", g.authenticate(g.handleModels))
	mux.HandleFunc("POST /v1/chat/completions", g.authenticate(g.handleInference(wire.Chat)))
	mux.HandleFunc("POST /v1/responses", g.authenticate(g.handleInference(wire.Responses)))
	mux.HandleFunc("POST /v1/messages", g.authenticate(g.handleInference(wire.Anthropic)))
	mux.HandleFunc("POST /v1/systemone", g.authenticate(g.handleSystemOne))
	mux.HandleFunc("GET /healthz", g.handleHealth)
	// /health is the conventional readiness path monitors probe; keep it as
	// an alias so a 404 never reads as "gateway down".
	mux.HandleFunc("GET /health", g.handleHealth)
	return telemetry.Recover(g.logger, mux)
}

func (g *Gateway) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		candidates := []string{strings.TrimSpace(r.Header.Get("x-api-key"))}
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			candidates = append(candidates, strings.TrimSpace(auth[7:]))
		}
		valid := false
		for _, key := range g.cfg.ServerKeys {
			if !g.cfg.ServerKeyEnabled(key) {
				continue
			}
			for _, candidate := range candidates {
				if len(candidate) == len(key) && subtle.ConstantTimeCompare([]byte(candidate), []byte(key)) == 1 {
					valid = true
				}
			}
		}
		if !valid {
			protocol := wire.Chat
			if r.URL.Path == "/v1/messages" {
				protocol = wire.Anthropic
			}
			wire.WriteError(w, protocol, http.StatusUnauthorized, "invalid local API key", "authentication_error", "")
			return
		}
		next(w, r)
	}
}

func (g *Gateway) handleInference(external wire.Protocol) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, "request body is too large or unreadable", "invalid_request_error", "")
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, "request body must be a JSON object", "invalid_request_error", "")
			return
		}
		model := jsonutil.StringAt(payload, "model")
		meta := telemetry.MetaFromRequest(r)
		if meta != nil {
			meta.Model = model
		}
		if model == "" {
			wire.WriteError(w, external, http.StatusBadRequest, "model is required", "invalid_request_error", "model")
			return
		}
		if id := g.rotation.GroupID(model); id != "" {
			g.handleRotation(w, r, external, payload, id, model)
			return
		}
		if !g.catalog.Supported(model) {
			wire.WriteError(w, external, http.StatusBadRequest, "the model uses an upstream protocol that opencode2api does not expose", "invalid_request_error", "model")
			return
		}
		route, err := g.catalog.Route(model, g.keyAvailability(), g.cfg.Anonymous)
		if override, selected := debugKeyOverrideFrom(r.Context()); selected {
			// A per-key diagnostic must not silently be served by another key,
			// another tier or the anonymous lane.
			route, err = g.catalog.RouteForTier(model, override.Tier, g.keyAvailability())
		}
		if err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "model")
			return
		}
		if meta != nil {
			meta.Tier = string(route.Tier)
			meta.Protocol = route.Protocol
		}
		// A System One model has no message-shaped equivalent, so a decision
		// payload submitted on a message endpoint is relayed verbatim instead of
		// being converted. This keeps the model reachable for clients that can
		// only address /v1/chat/completions or /v1/responses — for example a
		// gateway whose OpenAI platform pins every request to Responses.
		if route.Protocol == wire.SystemOne {
			if len(jsonutil.MapAt(payload, "questions")) == 0 || payload["state"] == nil {
				wire.WriteError(w, external, http.StatusBadRequest, "this model uses System One decisions, not chat; send state and typed questions to /v1/systemone", "invalid_request_error", "model")
				return
			}
			g.forwardSystemOne(w, r, body, payload, model, route)
			return
		}
		bodies, err := g.prepareRouteBodies(external, route, payload)
		if err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "")
			return
		}
		ids := identity.DeriveRequestIDs(r, payload)
		if meta != nil {
			meta.Request = ids.Request
		}
		stream := jsonutil.BoolAt(payload, "stream")
		requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
		defer cancel()
		resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids)
		if err != nil {
			finalTier := route.Tier
			if meta != nil && meta.Tier != "" {
				finalTier = config.Tier(meta.Tier)
			}
			keyID, channel, anonymous := requestCredential(requestCtx)
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			// An exhausted request budget is a timeout, not a bad gateway: the
			// distinction matters to clients that retry on 502.
			if errors.Is(err, context.DeadlineExceeded) {
				wire.WriteError(w, external, http.StatusGatewayTimeout, "upstream request timed out", "upstream_timeout", ids.Request)
				return
			}
			wire.WriteError(w, external, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
			return
		}
		defer resp.Body.Close()
		if meta != nil {
			meta.Tier = string(upstreamRoute.Tier)
			meta.Protocol = upstreamRoute.Protocol
		}
		w.Header().Set("x-request-id", ids.Request)
		if resp.StatusCode/100 != 2 {
			copyErrorResponse(w, external, resp, ids.Request)
			return
		}
		if stream {
			if meta != nil {
				meta.Stream = true
			}
			if g.monitor != nil {
				g.monitor.BeginStream()
				defer g.monitor.EndStream()
			}
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(resp.StatusCode)
			var usage wire.Usage
			var usageReported bool
			if external == upstreamRoute.Protocol {
				usage, usageReported, err = wire.ForwardStream(r.Context(), w, resp.Body, upstreamRoute.Protocol, model)
			} else {
				usage, usageReported, err = wire.TranscodeStream(r.Context(), w, resp.Body, upstreamRoute.Protocol, external, model)
			}
			if meta != nil {
				meta.Usage, meta.UsageReported = usage, usageReported
				if err != nil {
					meta.Outcome = "stream_error"
					if wire.ClientCanceled(r.Context(), err) {
						meta.Outcome = "client_canceled"
					}
				}
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				g.logger.Warn("downstream stream ended with an error", "component", "stream", "event", "stream_failed", "request_id", ids.Request, "model", model, "tier", upstreamRoute.Tier, "error", err)
			}
			return
		}
		responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		if err != nil {
			wire.WriteError(w, external, http.StatusBadGateway, "failed to read upstream response", "upstream_error", ids.Request)
			return
		}
		if upstreamRoute.Anonymous || (meta != nil && meta.Shaped) {
			// The anonymous lane and key-tier shaped requests were
			// force-streamed (see shapeAnonymousBody); collapse the events
			// back into the single document this non-streaming client
			// asked for.
			collapsed, err := wire.CollapseStream(bytes.NewReader(responseBody), upstreamRoute.Protocol, model)
			if err != nil {
				g.logger.Warn("anonymous stream collapse failed", "component", "conversion", "event", "anonymous_collapse_failed", "request_id", ids.Request, "model", model, "source_protocol", upstreamRoute.Protocol, "error", err)
				wire.WriteError(w, external, http.StatusBadGateway, "unsupported upstream response", "upstream_error", ids.Request)
				return
			}
			responseBody = collapsed
		}
		if usage, reported := wire.ResponseUsage(upstreamRoute.Protocol, responseBody); meta != nil {
			meta.Usage, meta.UsageReported = usage, reported
		}
		if external != upstreamRoute.Protocol {
			responseBody, err = wire.ConvertResponse(upstreamRoute.Protocol, external, responseBody)
			if err != nil {
				g.logger.Warn("response protocol conversion failed", "component", "conversion", "event", "response_conversion_failed", "request_id", ids.Request, "model", model, "source_protocol", upstreamRoute.Protocol, "target_protocol", external, "error", err)
				wire.WriteError(w, external, http.StatusBadGateway, "unsupported upstream response", "upstream_error", ids.Request)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(responseBody)
	}
}

// handleSystemOne serves System One decision requests on their own endpoint.
// Only models whose upstream protocol is System One can be served here; a chat
// or responses model belongs on its own endpoint, where the bridge can convert
// it.
func (g *Gateway) handleSystemOne(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "request body is too large or unreadable", "invalid_request_error", "")
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "request body must be a JSON object", "invalid_request_error", "")
		return
	}
	model := jsonutil.StringAt(payload, "model")
	if meta := telemetry.MetaFromRequest(r); meta != nil {
		meta.Model = model
	}
	if model == "" {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "model is required", "invalid_request_error", "model")
		return
	}
	if !g.catalog.Supported(model) {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "the model uses an upstream protocol that opencode2api does not expose", "invalid_request_error", "model")
		return
	}
	route, err := g.catalog.Route(model, g.keyAvailability(), g.cfg.Anonymous)
	if err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, err.Error(), "invalid_request_error", "model")
		return
	}
	if route.Protocol != wire.SystemOne {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, fmt.Sprintf("the model does not use the %s protocol", wire.SystemOne), "invalid_request_error", "model")
		return
	}
	g.forwardSystemOne(w, r, body, payload, model, route)
}

// forwardSystemOne relays a System One decision payload verbatim to the
// upstream systemone endpoint and returns the typed answer document unchanged.
// The payload pairs a free-form state with typed questions, which no
// message-shaped upstream protocol accepts, so it is never translated. Answers
// are non-streaming today; a streaming upstream reply is still relayed.
func (g *Gateway) forwardSystemOne(w http.ResponseWriter, r *http.Request, body []byte, payload map[string]any, model string, route models.Route) {
	meta := telemetry.MetaFromRequest(r)
	if meta != nil {
		meta.Model = model
		meta.Tier = string(route.Tier)
		meta.Protocol = route.Protocol
	}
	// One verbatim body serves every tier: a decision payload has no per-tier
	// encoding, so no protocol conversion is attempted.
	bodies := make(map[config.Tier][]byte, len(route.KeyTiers)+1)
	bodies[route.Tier] = body
	for _, tier := range route.KeyTiers {
		bodies[tier] = body
	}
	ids := identity.DeriveRequestIDs(r, payload)
	if meta != nil {
		meta.Request = ids.Request
	}
	requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
	defer cancel()
	resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids)
	if err != nil {
		finalTier := route.Tier
		if meta != nil && meta.Tier != "" {
			finalTier = config.Tier(meta.Tier)
		}
		keyID, channel, anonymous := requestCredential(requestCtx)
		g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
		if errors.Is(err, context.DeadlineExceeded) {
			wire.WriteError(w, wire.SystemOne, http.StatusGatewayTimeout, "upstream request timed out", "upstream_timeout", ids.Request)
			return
		}
		wire.WriteError(w, wire.SystemOne, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
		return
	}
	defer resp.Body.Close()
	if meta != nil {
		meta.Tier = string(upstreamRoute.Tier)
		meta.Protocol = upstreamRoute.Protocol
	}
	w.Header().Set("x-request-id", ids.Request)
	if resp.StatusCode/100 != 2 {
		copyErrorResponse(w, wire.SystemOne, resp, ids.Request)
		return
	}
	if contentType := resp.Header.Get("Content-Type"); strings.HasPrefix(contentType, "text/event-stream") {
		if meta != nil {
			meta.Stream = true
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadGateway, "failed to read upstream response", "upstream_error", ids.Request)
		return
	}
	if usage, reported := wire.ResponseUsage(upstreamRoute.Protocol, responseBody); meta != nil {
		meta.Usage, meta.UsageReported = usage, reported
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(responseBody)
}

func (g *Gateway) prepareRouteBodies(from wire.Protocol, route models.Route, input map[string]any) (map[config.Tier][]byte, error) {
	tiers := make([]config.Tier, 0, len(route.KeyTiers)+1)
	seen := make(map[config.Tier]bool, len(route.KeyTiers)+1)
	addTier := func(tier config.Tier) {
		if _, known := g.channels[tier]; !known || seen[tier] {
			return
		}
		seen[tier] = true
		tiers = append(tiers, tier)
	}
	addTier(route.Tier)
	for _, tier := range route.KeyTiers {
		addTier(tier)
	}
	if len(tiers) == 0 {
		return nil, errors.New("no usable upstream tier")
	}
	bodies := make(map[config.Tier][]byte, len(tiers))
	for _, tier := range tiers {
		protocol := route.ProtocolFor(tier)
		tierInput := input
		if from == wire.Responses {
			if tools, ok := sanitizeToolsForUpstream(input, protocol); ok {
				tierInput = withReplacedTools(input, tools)
			}
		}
		target := wire.Target{InterleavedField: g.catalog.MetadataForTier(route.ID, tier).InterleavedField}
		upstreamPayload, err := wire.PrepareRequest(from, protocol, tierInput, target)
		if err != nil {
			if tier != route.Tier {
				// A fallback tier may use a stricter wire format than the
				// preferred tier. Do not reject a request before the preferred
				// upstream has even been tried; that tier is attempted only if
				// the request actually falls back.
				g.logger.Debug("skipping fallback tier with incompatible request shape", "component", "conversion", "event", "fallback_tier_skipped", "model", jsonutil.StringAt(input, "model"), "tier", tier, "protocol", protocol, "error", err)
				continue
			}
			return nil, fmt.Errorf("prepare %s upstream request: %w", tier, err)
		}
		if effort := g.resolvedEffort(jsonutil.StringAt(upstreamPayload, "model"), tier, protocol); effort != "" {
			wire.ForcedEffort(protocol, upstreamPayload, effort)
		}
		encoded, err := json.Marshal(upstreamPayload)
		if err != nil {
			return nil, errors.New("request contains unsupported JSON values")
		}
		bodies[tier] = encoded
	}
	return bodies, nil
}

func copyErrorResponse(w http.ResponseWriter, protocol wire.Protocol, resp *http.Response, requestID string) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	message := http.StatusText(resp.StatusCode)
	var value map[string]any
	if json.Unmarshal(body, &value) == nil {
		message = jsonutil.FirstString(jsonutil.StringAt(value, "error", "message"), jsonutil.StringAt(value, "message"), message)
	}
	wire.WriteError(w, protocol, resp.StatusCode, message, "upstream_error", requestID)
}
