// Package models discovers model capabilities, pricing, and available routes.
package models

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

type Route struct {
	ID       string
	Tier     config.Tier
	Protocol wire.Protocol
	// Protocols is the native protocol for each possible upstream tier. Zen
	// and Go intentionally do not share one global protocol: OpenCode's
	// catalog currently exposes, for example, MiniMax through Chat on Zen and
	// Messages on Go. The request must therefore be re-encoded when a retry
	// crosses tiers.
	Protocols map[config.Tier]wire.Protocol
	Anonymous bool
	// AnonymousFallback marks a keyed route that keeps the public credential
	// as its last-resort layer: the request is tried on the authenticated
	// tiers first and only enters the anonymous lane when every keyed tier
	// has been exhausted. It is set for OpenCode free models whenever a Zen
	// key pool is configured, so real API keys (per-account quota) outrank
	// the shared public credential (per-IP quota).
	AnonymousFallback bool
	// KeyTiers is the ordered authenticated fallback plan. Anonymous requests
	// always start on Zen, then enter this list when the public credential does
	// not succeed.
	KeyTiers []config.Tier
}

type RouteDiagnostic struct {
	Model             string                        `json:"model"`
	RequestedProtocol wire.Protocol                 `json:"requested_protocol,omitempty"`
	NativeProtocol    wire.Protocol                 `json:"native_protocol"`
	NativeProtocols   map[config.Tier]wire.Protocol `json:"native_protocols,omitempty"`
	ProtocolSource    string                        `json:"protocol_source"`
	// Available reports, per configured tier, whether that tier advertises the
	// model. Zen and Go keep their historical JSON keys ("zen"/"go"); provider
	// tiers appear under their own names.
	Available            map[string]bool   `json:"available_tiers"`
	Tier                 config.Tier       `json:"tier,omitempty"`
	Anonymous            bool              `json:"anonymous"`
	KeyID                string            `json:"key_id,omitempty"`
	Channel              string            `json:"channel,omitempty"`
	Attempts             int               `json:"attempts,omitempty"`
	KeyTiers             []config.Tier     `json:"key_tiers,omitempty"`
	AnonymousEligibility AnonymousDecision `json:"anonymous_eligibility"`
	RouteError           string            `json:"route_error,omitempty"`
}

type Catalog struct {
	mu sync.RWMutex
	// advertised maps every tier to the model IDs its upstream reported. The
	// built-in tiers are keyed by TierZen/TierGo; each provider uses its own
	// name as the tier key.
	advertised map[config.Tier]map[string]bool
	// order is the canonical tier order (zen, go, then providers). Routing
	// consults it whenever several tiers serve the same model.
	order     []config.Tier
	protocols map[string]wire.Protocol
	// nativeProtocols is populated from OpenCode's public model capability
	// catalog. protocols remains the user-configured override map.
	nativeProtocols map[config.Tier]map[string]wire.Protocol
	unsupported     map[config.Tier]map[string]bool
	modelMeta       map[config.Tier]map[string]Metadata
	updatedAt       time.Time
	prefer          config.Tier
	pricing         *PricingStore
	availability    *AvailabilityStore
	cachePath       string
	cacheSource     string
	stale           bool
	refreshAfter    time.Duration
}

type CatalogSnapshot struct {
	Tiers       map[string]int `json:"tiers"`
	Total       int            `json:"total"`
	Exposed     int            `json:"exposed"`
	UpdatedAt   time.Time      `json:"updated_at,omitempty"`
	CacheSource string         `json:"cache_source,omitempty"`
	Stale       bool           `json:"stale"`
}

func NewCatalog(prefer config.Tier, tiers []config.Tier, overrides map[string]string) *Catalog {
	protocols := make(map[string]wire.Protocol, len(overrides))
	for model, protocol := range overrides {
		protocols[model] = wire.Protocol(protocol)
	}
	order := append([]config.Tier(nil), tiers...)
	return &Catalog{
		advertised:      newTierSets(order),
		order:           order,
		protocols:       protocols,
		nativeProtocols: newTierMaps[wire.Protocol](order),
		unsupported:     newTierMaps[bool](order),
		prefer:          prefer,
		cacheSource:     "none",
	}
}

// newTierSets builds one empty model set per tier.
func newTierSets(tiers []config.Tier) map[config.Tier]map[string]bool {
	sets := make(map[config.Tier]map[string]bool, len(tiers))
	for _, tier := range tiers {
		sets[tier] = map[string]bool{}
	}
	return sets
}

// newTierMaps builds one empty per-tier value map per tier.
func newTierMaps[V any](tiers []config.Tier) map[config.Tier]map[string]V {
	maps := make(map[config.Tier]map[string]V, len(tiers))
	for _, tier := range tiers {
		maps[tier] = map[string]V{}
	}
	return maps
}

// TierList reports the configured tier order, including tiers that never
// advertised a model (a configured provider is a tier from construction on,
// so its pool, status and key diagnostics exist before first discovery).
func (c *Catalog) TierList() []config.Tier {
	return append([]config.Tier(nil), c.order...)
}

// SetPricingStore connects routing decisions to the shared pricing metadata.
func (c *Catalog) SetPricingStore(store *PricingStore) {
	c.mu.Lock()
	c.pricing = store
	c.mu.Unlock()
}

func (c *Catalog) SetAvailabilityStore(store *AvailabilityStore) {
	c.mu.Lock()
	c.availability = store
	c.mu.Unlock()
}

// MetadataSnapshot reports pricing freshness without exposing the store.
func (c *Catalog) MetadataSnapshot() MetadataSnapshot {
	c.mu.RLock()
	store := c.pricing
	c.mu.RUnlock()
	if store == nil {
		return MetadataSnapshot{}
	}
	return store.Snapshot()
}

func (c *Catalog) SetCachePath(path string) {
	c.mu.Lock()
	c.cachePath = path
	c.mu.Unlock()
}

func (c *Catalog) SetRefreshInterval(interval time.Duration) {
	c.mu.Lock()
	c.refreshAfter = interval
	c.mu.Unlock()
}

// ReplaceWithCapabilities installs one discovery round. models holds the
// per-tier model lists just discovered; a nil list keeps that tier's previous
// models, so one provider's failed refresh cannot blank its catalog. native,
// unsupported, and metadata follow the same optional update rule.
func (c *Catalog) ReplaceWithCapabilities(models map[config.Tier][]string, native map[config.Tier]map[string]wire.Protocol, unsupported map[config.Tier]map[string]bool, metadata map[config.Tier]map[string]Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for tier, ids := range models {
		if ids == nil {
			continue
		}
		if _, known := c.advertised[tier]; !known {
			continue
		}
		c.advertised[tier] = toSet(ids)
	}
	if native != nil {
		for tier, protocols := range native {
			if _, known := c.nativeProtocols[tier]; !known {
				continue
			}
			c.nativeProtocols[tier] = cloneProtocols(protocols)
		}
	}
	if unsupported != nil {
		for tier, values := range unsupported {
			if _, known := c.unsupported[tier]; !known {
				continue
			}
			c.unsupported[tier] = cloneBools(values)
		}
	}
	if metadata != nil {
		c.modelMeta = cloneModelMeta(metadata)
	}
	c.updatedAt = time.Now().UTC()
	c.cacheSource = "live"
	c.stale = false
}

func (c *Catalog) CopyState(source *Catalog) {
	if source == nil {
		return
	}
	source.mu.RLock()
	advertised := newTierSets(c.order)
	for tier, ids := range source.advertised {
		target, known := advertised[tier]
		if !known {
			continue
		}
		for model, available := range ids {
			target[model] = available
		}
	}
	native := newTierMaps[wire.Protocol](c.order)
	unsupported := newTierMaps[bool](c.order)
	for tier, protocols := range source.nativeProtocols {
		if target, known := native[tier]; known {
			for model, protocol := range protocols {
				target[model] = protocol
			}
		}
	}
	for tier, values := range source.unsupported {
		if target, known := unsupported[tier]; known {
			for model, value := range values {
				target[model] = value
			}
		}
	}
	meta := cloneModelMeta(source.modelMeta)
	updatedAt := source.updatedAt
	cacheSource := source.cacheSource
	stale := source.stale
	source.mu.RUnlock()
	c.mu.Lock()
	c.advertised, c.nativeProtocols, c.unsupported, c.updatedAt = advertised, native, unsupported, updatedAt
	c.modelMeta = meta
	c.cacheSource, c.stale = cacheSource, stale
	c.mu.Unlock()
}

func (c *Catalog) Route(model string, hasKeys map[config.Tier]bool, hasAnonymous bool) (Route, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routeLocked(model, hasKeys, hasAnonymous)
}

func (c *Catalog) routeLocked(model string, hasKeys map[config.Tier]bool, hasAnonymous bool) (Route, error) {
	if c.availability != nil && c.availability.Disabled(model) {
		return Route{}, fmt.Errorf("model %q is disabled after a failed availability check; restore it in the WebUI or wait for the next probe", model)
	}
	keyTiers := c.keyTierOrderLocked(model, hasKeys)

	zenModel := model
	if strings.HasSuffix(model, ":free") {
		zenModel = strings.TrimSuffix(model, ":free") + "-free"
	}
	zenAdvertised := (c.advertised[config.TierZen] != nil && (c.advertised[config.TierZen][model] || c.advertised[config.TierZen][zenModel]))

	decision := c.anonymousDecision(model)
	if !decision.Allowed && zenModel != model {
		decision = c.anonymousDecision(zenModel)
	}
	anonymousEligible := hasAnonymous && decision.Allowed &&
		(c.protocols[model] != "" || c.protocols[zenModel] != "" || !c.unsupported[config.TierZen][model] || !c.unsupported[config.TierZen][zenModel]) &&
		(c.zenGoPendingLocked() || zenAdvertised || c.advertised[config.TierGo][model] || c.advertised[config.TierGo][zenModel])

	zenKeyed := false
	for _, tier := range keyTiers {
		if tier == config.TierZen {
			zenKeyed = true
			break
		}
	}
	if anonymousEligible && (zenKeyed || len(keyTiers) > 0) {
		protocols := c.protocolsForLocked(model, keyTiers, true)
		return Route{ID: model, Tier: keyTiers[0], Protocol: protocols[keyTiers[0]], Protocols: protocols, AnonymousFallback: true, KeyTiers: keyTiers}, nil
	}
	if anonymousEligible {
		protocols := c.protocolsForLocked(model, keyTiers, true)
		return Route{ID: model, Tier: config.TierZen, Protocol: protocols[config.TierZen], Protocols: protocols, Anonymous: true, KeyTiers: keyTiers}, nil
	}
	if len(keyTiers) > 0 {
		protocols := c.protocolsForLocked(model, keyTiers, false)
		return Route{ID: model, Tier: keyTiers[0], Protocol: protocols[keyTiers[0]], Protocols: protocols, KeyTiers: keyTiers}, nil
	}
	return Route{}, fmt.Errorf("model %q is not served by any configured tier", model)
}

// tierPendingLocked reports whether a tier has not reported a model list yet.
// Before a tier's first successful discovery nothing can contradict it, which
// keeps that tier's keys usable through a transient refresh failure.
func (c *Catalog) tierPendingLocked(tier config.Tier) bool {
	return len(c.advertised[tier]) == 0
}

// zenGoPendingLocked reports whether neither OpenCode channel has reported a
// model list yet. Anonymous eligibility counts Zen and Go only: the public
// lane dials Zen, so provider discovery must never widen it.
func (c *Catalog) zenGoPendingLocked() bool {
	return len(c.advertised[config.TierZen]) == 0 && len(c.advertised[config.TierGo]) == 0
}

func (r Route) ProtocolFor(tier config.Tier) wire.Protocol {
	if protocol := r.Protocols[tier]; protocol != "" {
		return protocol
	}
	return r.Protocol
}

func (c *Catalog) protocolsForLocked(model string, keyTiers []config.Tier, includeZen bool) map[config.Tier]wire.Protocol {
	protocols := make(map[config.Tier]wire.Protocol, len(keyTiers)+1)
	if includeZen {
		protocols[config.TierZen] = c.protocolForLocked(model, config.TierZen)
	}
	for _, tier := range keyTiers {
		protocols[tier] = c.protocolForLocked(model, tier)
	}
	return protocols
}

func (c *Catalog) protocolForLocked(model string, tier config.Tier) wire.Protocol {
	if protocol := c.protocols[model]; protocol != "" {
		return protocol
	}
	if protocol := c.nativeProtocols[tier][model]; protocol != "" {
		return protocol
	}
	equiv := equivalentFreeModel(model)
	if equiv != model {
		if protocol := c.protocols[equiv]; protocol != "" {
			return protocol
		}
		if protocol := c.nativeProtocols[tier][equiv]; protocol != "" {
			return protocol
		}
	}
	// The OpenCode capability catalog is authoritative when available. Chat is
	// the only safe protocol-neutral fallback for an ID that has just appeared
	// in /v1/models but is not present in the capability snapshot yet.
	return wire.Chat
}

// keyTierOrderLocked builds an authenticated route in prefer order: the
// preferred tier first, then every other configured tier in canonical order.
// A tier is included only when it has a key and serves the model. Before a
// tier's first successful catalog refresh it stays usable, so temporary
// discovery failures do not take that tier offline.
func (c *Catalog) keyTierOrderLocked(model string, hasKeys map[config.Tier]bool) []config.Tier {
	equiv := equivalentFreeModel(model)
	available := func(tier config.Tier) bool {
		pending := c.tierPendingLocked(tier)
		if !hasKeys[tier] {
			return false
		}
		if pending {
			return true
		}
		advertised := c.advertised[tier][model] || (equiv != model && c.advertised[tier][equiv])
		if !advertised {
			return false
		}
		return c.tierSupportedLocked(model, tier) || (equiv != model && c.tierSupportedLocked(equiv, tier))
	}
	order := make([]config.Tier, 0, len(c.order))
	if c.prefer != "" {
		order = append(order, c.prefer)
	}
	for _, tier := range c.order {
		if tier != c.prefer {
			order = append(order, tier)
		}
	}
	result := make([]config.Tier, 0, len(order))
	for _, tier := range order {
		if available(tier) {
			result = append(result, tier)
		}
	}
	return result
}

// RouteForTier builds a route pinned to one tier, used by the Playground's
// per-key diagnostics. It never selects the anonymous lane and never adds a
// fallback tier: the operator asked to exercise one configured key, so the
// result describes that key and nothing else.
func (c *Catalog) RouteForTier(model string, tier config.Tier, hasKeys map[config.Tier]bool) (Route, error) {
	if !hasKeys[tier] {
		return Route{}, fmt.Errorf("no %s key is configured", tier)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.availability != nil && c.availability.Disabled(model) {
		return Route{}, fmt.Errorf("model %q is disabled after a failed availability check", model)
	}
	if !c.tierPendingLocked(tier) && !c.advertised[tier][model] {
		return Route{}, fmt.Errorf("model %q is not available in the selected %s key tier", model, tier)
	}
	if !c.tierSupportedLocked(model, tier) {
		return Route{}, fmt.Errorf("model %q uses an upstream protocol that is not available on the selected %s key tier", model, tier)
	}
	protocol := c.protocolForLocked(model, tier)
	return Route{
		ID: model, Tier: tier, Protocol: protocol,
		Protocols: map[config.Tier]wire.Protocol{tier: protocol},
		KeyTiers:  []config.Tier{tier},
	}, nil
}

func (c *Catalog) anonymousDecision(model string) AnonymousDecision {
	if c.pricing != nil {
		return c.pricing.Decide(model)
	}
	return AnonymousDecision{Allowed: isFreeModel(model), Source: "name_fallback_metadata_pending"}
}

// IsFreeModel reports whether the catalog considers the model free-tier
// (pricing metadata, falling back to the -free name convention). Key-tier
// shaping uses it so free models without "free" in the name are covered.
func (c *Catalog) IsFreeModel(model string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.anonymousDecision(model).Allowed
}

func (c *Catalog) Diagnostic(model string, requested wire.Protocol, hasKeys map[config.Tier]bool, hasAnonymous bool) RouteDiagnostic {
	c.mu.RLock()
	configured, explicit := c.protocols[model]
	available := make(map[string]bool, len(c.order))
	nativeProtocols := make(map[config.Tier]wire.Protocol, len(c.order))
	known := false
	for _, tier := range c.order {
		available[string(tier)] = c.advertised[tier][model]
		nativeProtocols[tier] = c.protocolForLocked(model, tier)
		if _, ok := c.nativeProtocols[tier][model]; ok {
			known = true
		}
	}
	prefer, order := c.prefer, c.order
	c.mu.RUnlock()
	source := "configured"
	if !explicit {
		source = "default"
		if known {
			source = "upstream"
		}
	}
	protocol := configured
	if protocol == "" {
		// Route() below selects the preferred available tier. This is only the
		// fallback shown when no route can currently be built.
		protocol = nativeProtocols[prefer]
		if protocol == "" && len(order) > 0 {
			protocol = nativeProtocols[order[0]]
		}
	}
	diagnostic := RouteDiagnostic{
		Model: model, RequestedProtocol: requested, NativeProtocol: protocol, NativeProtocols: nativeProtocols, ProtocolSource: source,
		Available: available, AnonymousEligibility: c.anonymousDecision(model),
	}
	route, err := c.Route(model, hasKeys, hasAnonymous)
	if err != nil {
		diagnostic.RouteError = err.Error()
		return diagnostic
	}
	diagnostic.NativeProtocol = route.Protocol
	diagnostic.NativeProtocols = route.Protocols
	diagnostic.Tier, diagnostic.Anonymous = route.Tier, route.Anonymous
	diagnostic.KeyTiers = append([]config.Tier(nil), route.KeyTiers...)
	return diagnostic
}

func equivalentFreeModel(model string) string {
	if strings.HasSuffix(model, ":free") {
		return strings.TrimSuffix(model, ":free") + "-free"
	}
	if strings.HasSuffix(model, "-free") {
		return strings.TrimSuffix(model, "-free") + ":free"
	}
	return model
}

func isFreeModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "free")
}

func (c *Catalog) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.modelIDsLocked()
	models := ids[:0]
	for _, model := range ids {
		if c.supportedLocked(model) {
			models = append(models, model)
		}
	}
	return models
}

func (c *Catalog) modelIDsLocked() []string {
	seen := make(map[string]bool)
	for _, tier := range c.order {
		for model := range c.advertised[tier] {
			seen[model] = true
		}
	}
	return sortedSetKeys(seen)
}

func (c *Catalog) Snapshot() CatalogSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked(c.modelIDsLocked())
}

// AvailableModels provides discovery and readiness with the same route
// filtering, including configured key tiers and anonymous eligibility.
func (c *Catalog) AvailableModels(hasKeys map[config.Tier]bool, hasAnonymous bool) ([]Route, CatalogSnapshot) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.modelIDsLocked()
	routes := make([]Route, 0, len(ids))
	for _, model := range ids {
		if !c.supportedLocked(model) {
			continue
		}
		if route, err := c.routeLocked(model, hasKeys, hasAnonymous); err == nil {
			routes = append(routes, route)
		}
	}
	snapshot := c.snapshotLocked(ids)
	snapshot.Exposed = len(routes)
	return routes, snapshot
}

func (c *Catalog) snapshotLocked(ids []string) CatalogSnapshot {
	exposed := 0
	for _, model := range ids {
		if c.supportedLocked(model) {
			exposed++
		}
	}
	stale := c.stale
	if !c.updatedAt.IsZero() && c.refreshAfter > 0 {
		stale = stale || time.Since(c.updatedAt) > max(2*c.refreshAfter, time.Minute)
	}
	tiers := make(map[string]int, len(c.order))
	for _, tier := range c.order {
		tiers[string(tier)] = len(c.advertised[tier])
	}
	return CatalogSnapshot{
		Tiers: tiers, Total: len(ids), Exposed: exposed,
		UpdatedAt: c.updatedAt, CacheSource: c.cacheSource, Stale: stale,
	}
}

func (c *Catalog) Supported(model string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.supportedLocked(model)
}

// MetadataForTier returns the rich per-model metadata (context window,
// reasoning, tool call, modalities) captured from the opencode catalog for
// the tier that will actually serve the request. Same-named models can carry
// different limits per tier, so callers must pass route.Tier — never a
// tier-blind lookup. Anonymous routes always resolve to TierZen, which keeps
// the keyless path on Zen metadata. Limits the catalog omitted are filled from
// models.dev; capability flags always come from the catalog. The zero value is
// returned for models neither source describes (or before the first refresh).
func (c *Catalog) MetadataForTier(model string, tier config.Tier) Metadata {
	c.mu.RLock()
	md := c.modelMeta[tier][model]
	equiv := equivalentFreeModel(model)
	if md.ContextWindow == 0 && equiv != model {
		md = c.modelMeta[tier][equiv]
	}
	pricing := c.pricing
	c.mu.RUnlock()
	if pricing == nil {
		return md
	}
	price, ok := pricing.Price(model)
	if !ok {
		return md
	}
	// OpenCode's capability catalog is authoritative. Use models.dev limits
	// only to fill fields it omitted so discovery clients still get per-model
	// context and output limits.
	if md.ContextWindow == 0 {
		md.ContextWindow = price.ContextWindow
	}
	if md.MaxInput == 0 {
		md.MaxInput = price.MaxInput
	}
	if md.MaxOutput == 0 {
		md.MaxOutput = price.MaxOutput
	}
	// DeepSeek V4.1 Flash Free empirically accepts 1M (1,048,576) context tokens upstream
	if model == "deepseek-v4.1-flash-free" {
		md.ContextWindow = 1048576
		md.MaxOutput = 384000
	}
	return md
}

func (c *Catalog) supportedLocked(model string) bool {
	equiv := equivalentFreeModel(model)
	for _, tier := range c.order {
		if !c.tierPendingLocked(tier) && !c.advertised[tier][model] && (equiv == model || !c.advertised[tier][equiv]) {
			continue
		}
		if c.tierSupportedLocked(model, tier) || (equiv != model && c.tierSupportedLocked(equiv, tier)) {
			return true
		}
	}
	return false
}

func (c *Catalog) tierSupportedLocked(model string, tier config.Tier) bool {
	if c.protocols[model] != "" {
		return true
	}
	if c.unsupported[tier][model] {
		return false
	}
	if c.nativeProtocols[tier][model] != "" {
		return true
	}
	// A tier that has not reported a model list yet has no upstream capability
	// snapshot to contradict a configured key, so retain the pre-refresh
	// compatibility behavior for that tier alone.
	return c.tierPendingLocked(tier)
}

func toSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[item] = true
	}
	return out
}

func cloneProtocols(source map[string]wire.Protocol) map[string]wire.Protocol {
	result := make(map[string]wire.Protocol, len(source))
	for model, protocol := range source {
		result[model] = protocol
	}
	return result
}

func cloneBools(source map[string]bool) map[string]bool {
	result := make(map[string]bool, len(source))
	for model, value := range source {
		result[model] = value
	}
	return result
}

func cloneModelMeta(source map[config.Tier]map[string]Metadata) map[config.Tier]map[string]Metadata {
	result := make(map[config.Tier]map[string]Metadata, len(source))
	for tier, models := range source {
		meta := make(map[string]Metadata, len(models))
		for id, md := range models {
			meta[id] = md
		}
		result[tier] = meta
	}
	return result
}

func sortedSetKeys(source map[string]bool) []string {
	result := make([]string, 0, len(source))
	for model, available := range source {
		if available {
			result = append(result, model)
		}
	}
	sort.Strings(result)
	return result
}

func cloneTierProtocols(source map[config.Tier]map[string]wire.Protocol) map[config.Tier]map[string]wire.Protocol {
	result := make(map[config.Tier]map[string]wire.Protocol, len(source))
	for tier, protocols := range source {
		result[tier] = cloneProtocols(protocols)
	}
	return result
}

func cloneTierBools(source map[config.Tier]map[string]bool) map[config.Tier]map[string]bool {
	result := make(map[config.Tier]map[string]bool, len(source))
	for tier, models := range source {
		result[tier] = cloneBools(models)
	}
	return result
}
