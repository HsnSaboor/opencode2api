package models

import (
	"path/filepath"
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

var testTiers = []config.Tier{config.TierZen, config.TierGo, config.Tier("tokenharbor")}

// A provider tier routes like any other key tier: its advertised models
// resolve to it, its own list gates RouteForTier, and the diagnostic carries
// one availability entry per configured tier.
func TestProviderTierRouting(t *testing.T) {
	catalog := NewCatalog(config.TierGo, testTiers, nil)
	catalog.ReplaceWithCapabilities(
		map[config.Tier][]string{
			config.TierZen:             {},
			config.TierGo:              {},
			config.Tier("tokenharbor"): {"th-model"},
		},
		map[config.Tier]map[string]wire.Protocol{
			config.Tier("tokenharbor"): {"th-model": wire.Chat},
		},
		nil, nil,
	)
	hasKeys := map[config.Tier]bool{config.Tier("tokenharbor"): true}
	route, err := catalog.Route("th-model", hasKeys, false)
	if err != nil || route.Tier != config.Tier("tokenharbor") || route.Protocol != wire.Chat {
		t.Fatalf("Route(th-model) = %+v, %v", route, err)
	}
	if len(route.KeyTiers) != 1 || route.KeyTiers[0] != config.Tier("tokenharbor") {
		t.Fatalf("KeyTiers = %v", route.KeyTiers)
	}
	if _, err := catalog.Route("th-model", nil, false); err == nil {
		t.Fatal("Route succeeded without a provider key")
	}
	if _, err := catalog.RouteForTier("th-model", config.Tier("tokenharbor"), hasKeys); err != nil {
		t.Fatalf("RouteForTier: %v", err)
	}
	if _, err := catalog.RouteForTier("zen-model", config.Tier("tokenharbor"), hasKeys); err == nil {
		t.Fatal("RouteForTier served a model the provider does not advertise")
	}
	diagnostic := catalog.Diagnostic("th-model", "", hasKeys, false)
	if !diagnostic.Available["tokenharbor"] || diagnostic.Available["go"] {
		t.Fatalf("diagnostic availability = %v", diagnostic.Available)
	}
	if diagnostic.Tier != config.Tier("tokenharbor") || diagnostic.RouteError != "" {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

// The anonymous lane stays Zen-only: a provider model never rides the public
// credential, even when the provider is the only tier advertising it.
func TestAnonymousLaneStaysZenOnly(t *testing.T) {
	catalog := NewCatalog(config.TierGo, testTiers, nil)
	catalog.ReplaceWithCapabilities(
		map[config.Tier][]string{
			config.TierZen:             {"zen-model"},
			config.TierGo:              {},
			config.Tier("tokenharbor"): {"th-model:free"},
		},
		map[config.Tier]map[string]wire.Protocol{
			config.Tier("tokenharbor"): {"th-model:free": wire.Chat},
		},
		nil, nil,
	)
	route, err := catalog.Route("th-model:free", map[config.Tier]bool{config.Tier("tokenharbor"): true}, true)
	if err != nil || route.Anonymous || route.Tier != config.Tier("tokenharbor") {
		t.Fatalf("Route(th-model:free) = %+v, %v", route, err)
	}
}

// A tier whose discovery has not succeeded yet stays routable: a transient
// refresh failure must not take the keys the operator just configured
// offline.
func TestPendingTierStaysRoutable(t *testing.T) {
	catalog := NewCatalog(config.TierGo, testTiers, nil)
	catalog.ReplaceWithCapabilities(map[config.Tier][]string{config.TierZen: {"zen-model"}}, nil, nil, nil)
	route, err := catalog.Route("th-model", map[config.Tier]bool{config.Tier("tokenharbor"): true}, false)
	if err != nil || route.Tier != config.Tier("tokenharbor") {
		t.Fatalf("pending provider did not stay routable: %+v, %v", route, err)
	}
}

// Prefer reorders the fallback plan across every tier, providers included.
func TestPreferOrdersProviderFirst(t *testing.T) {
	catalog := NewCatalog(config.Tier("tokenharbor"), testTiers, nil)
	catalog.ReplaceWithCapabilities(
		map[config.Tier][]string{
			config.TierGo:              {"shared"},
			config.Tier("tokenharbor"): {"shared"},
		},
		map[config.Tier]map[string]wire.Protocol{
			config.TierGo:              {"shared": wire.Chat},
			config.Tier("tokenharbor"): {"shared": wire.Chat},
		},
		nil, nil,
	)
	hasKeys := map[config.Tier]bool{config.TierGo: true, config.Tier("tokenharbor"): true}
	route, err := catalog.Route("shared", hasKeys, false)
	if err != nil || route.Tier != config.Tier("tokenharbor") {
		t.Fatalf("Route(shared) = %+v, %v", route, err)
	}
	if len(route.KeyTiers) != 2 || route.KeyTiers[0] != config.Tier("tokenharbor") || route.KeyTiers[1] != config.TierGo {
		t.Fatalf("KeyTiers = %v", route.KeyTiers)
	}
	routes, snapshot := catalog.AvailableModels(hasKeys, false)
	if len(routes) != 1 || routes[0].ID != "shared" {
		t.Fatalf("AvailableModels = %+v", routes)
	}
	if snapshot.Tiers["tokenharbor"] != 1 || snapshot.Tiers["go"] != 1 || snapshot.Tiers["zen"] != 0 {
		t.Fatalf("snapshot tiers = %v", snapshot.Tiers)
	}
}

// The disk cache round-trips provider discovery, so a restart keeps serving
// provider models before the first refresh completes.
func TestCatalogCacheRoundTripsProviderTiers(t *testing.T) {
	catalog := NewCatalog(config.TierGo, testTiers, nil)
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalog.SetCachePath(path)
	catalog.ReplaceWithCapabilities(
		map[config.Tier][]string{
			config.TierZen:             {"zen-model"},
			config.Tier("tokenharbor"): {"th-model"},
		},
		map[config.Tier]map[string]wire.Protocol{
			config.Tier("tokenharbor"): {"th-model": wire.Chat},
		},
		nil, nil,
	)
	if err := catalog.SaveCache(); err != nil {
		t.Fatalf("SaveCache: %v", err)
	}
	restored := NewCatalog(config.TierGo, testTiers, nil)
	if err := restored.LoadCache(path); err != nil {
		t.Fatalf("LoadCache: %v", err)
	}
	route, err := restored.Route("th-model", map[config.Tier]bool{config.Tier("tokenharbor"): true}, false)
	if err != nil || route.Tier != config.Tier("tokenharbor") {
		t.Fatalf("restored route = %+v, %v", route, err)
	}
	if _, snapshot := restored.AvailableModels(nil, false); snapshot.Tiers["tokenharbor"] != 1 || snapshot.Tiers["zen"] != 1 {
		t.Fatalf("restored snapshot tiers = %v", snapshot.Tiers)
	}
}
