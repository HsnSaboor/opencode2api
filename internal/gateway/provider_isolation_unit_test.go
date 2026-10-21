package gateway

import (
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

func TestProviderLaneParkedIsolation(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	// Park the lane with FreeTier verdict
	parked := pool.snapshot()[0]
	parked.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())

	// OpenCode pool should respect the park
	zenNodes, err := newNodePool([]string{"zen_key"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool(zen): %v", err)
	}
	zenNodes.opencode = true
	if !zenNodes.laneParked(zenNodes.nodes[0]) {
		t.Fatal("expected zen node on parked lane to be laneParked")
	}

	// Provider pool must NOT be affected by OpenCode free-tier lane parks
	providerNodes, err := newNodePool([]string{"provider_key"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool(provider): %v", err)
	}
	providerNodes.opencode = false
	if providerNodes.laneParked(providerNodes.nodes[0]) {
		t.Fatal("expected provider node to NOT be laneParked even if lane has OpenCode freitierUntil")
	}
}

func TestPrepareRouteBodiesAnonymousFallback(t *testing.T) {
	g := &Gateway{
		channels: map[config.Tier]*channel{
			config.TierZen:             {tier: config.TierZen},
			config.Tier("tokenharbor"): {tier: config.Tier("tokenharbor")},
		},
	}
	route := models.Route{
		ID:                "deepseek-v4.1-flash:free",
		Tier:              config.Tier("tokenharbor"),
		KeyTiers:          []config.Tier{config.Tier("tokenharbor")},
		Protocols:         map[config.Tier]wire.Protocol{config.Tier("tokenharbor"): wire.Chat, config.TierZen: wire.Chat},
		AnonymousFallback: true,
	}
	input := map[string]any{"model": "deepseek-v4.1-flash:free"}
	bodies, err := g.prepareRouteBodies(wire.Chat, route, input)
	if err != nil {
		t.Fatalf("prepareRouteBodies failed: %v", err)
	}
	if _, ok := bodies[config.TierZen]; !ok {
		t.Fatal("expected bodies to contain config.TierZen when AnonymousFallback is true")
	}
}
