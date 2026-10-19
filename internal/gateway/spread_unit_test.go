package gateway

import (
	"testing"
	"time"

	"opencode2api/internal/config"
)

// Every channel's key pool binds independently at construction, so two
// providers' first keys land on the same lanes even though neither pool is
// crowded on its own. The cross-channel spread must give every keyed account
// a lane of its own while empty lanes remain, preferring dedicated address
// lanes over proxy exits.
func TestSpreadKeysAcrossChannels(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	pool.items = append(pool.items, laneAt(1, "2001:db8:1::5"), laneAt(2, "2001:db8:2::9"), laneAt(3, "2001:db8:3::7"), proxyAt(4))
	zenNodes, err := newNodePool([]string{"z0", "z1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool(zen): %v", err)
	}
	thNodes, err := newNodePool([]string{"t0", "t1", "t2"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool(tokenharbor): %v", err)
	}
	gateway := &Gateway{
		transports: pool,
		order:      []config.Tier{config.TierZen, config.Tier("tokenharbor")},
		channels: map[config.Tier]*channel{
			config.TierZen:             {tier: config.TierZen, nodes: zenNodes},
			config.Tier("tokenharbor"): {tier: config.Tier("tokenharbor"), nodes: thNodes},
		},
	}
	if shared := sharedLaneKeys(gateway); shared < 2 {
		t.Fatalf("fixture shares %d keys on lanes, want at least 2", shared)
	}
	if moved := gateway.spreadKeysAcrossLanes(); moved < 2 {
		t.Fatalf("spreadKeysAcrossLanes moved %d keys, want at least 2", moved)
	}
	seen := map[int]int{}
	for _, channel := range gateway.orderedChannels() {
		for _, node := range channel.nodes.nodes {
			seen[int(node.egressIndex.Load())]++
		}
	}
	for lane, count := range seen {
		if count != 1 {
			t.Fatalf("lane %d holds %d keys across channels, want 1", lane, count)
		}
	}
	if moved := gateway.spreadKeysAcrossLanes(); moved != 0 {
		t.Fatalf("second spread moved %d keys, want 0", moved)
	}
}

// sharedLaneKeys counts keys beyond the first that sit on lanes carrying
// keys from more than one channel.
func sharedLaneKeys(g *Gateway) int {
	total := map[int]int{}
	for _, channel := range g.orderedChannels() {
		for _, node := range channel.nodes.nodes {
			total[int(node.egressIndex.Load())]++
		}
	}
	shared := 0
	for _, count := range total {
		if count > 1 {
			shared += count - 1
		}
	}
	return shared
}
