package gateway

import (
	"testing"
	"time"
)

func TestAnonymousPoolBestCoolingNode(t *testing.T) {
	transports := &transportPool{}
	pool := &anonymousPool{enabled: true, transports: transports}

	warpLane := proxyAt(0)
	warpLane.proxy.Tag = "cf-warp-1"
	chkLane := proxyAt(1)
	chkLane.proxy.Tag = "chk-1"
	directLane := laneAt(2, "2001:db8:1::1")

	warpNode := &anonymousNode{egress: warpLane}
	chkNode := &anonymousNode{egress: chkLane}
	directNode := &anonymousNode{egress: directLane}

	now := time.Now()
	// Both warp and chk are cooling
	warpNode.cooldownUntil.Store(now.Add(10 * time.Minute).UnixNano())
	chkNode.cooldownUntil.Store(now.Add(5 * time.Minute).UnixNano())
	directNode.cooldownUntil.Store(now.Add(1 * time.Minute).UnixNano())

	pool.nodes = []*anonymousNode{directNode, chkNode, warpNode}

	// 1. When direct egress is parked, directNode is skipped.
	transports.ParkDirectEgress(time.Hour)
	// WARP is preferred over chk proxy even if chk has a shorter cooldown.
	best := pool.BestCoolingNode(transports)
	if best != warpNode {
		t.Fatalf("best cooling node = %v, want warpNode (WARP preferred over chk)", best)
	}

	// 2. If warpNode has a FreeTier flag active, it must be skipped and chkNode selected.
	warpLane.freitierUntil.Store(now.Add(time.Hour).UnixNano())
	best = pool.BestCoolingNode(transports)
	if best != chkNode {
		t.Fatalf("best cooling node after warp parked = %v, want chkNode", best)
	}

	// 3. If all selectable nodes are parked, return nil.
	chkLane.freitierUntil.Store(now.Add(time.Hour).UnixNano())
	best = pool.BestCoolingNode(transports)
	if best != nil {
		t.Fatalf("best cooling node when all parked = %v, want nil", best)
	}
}
