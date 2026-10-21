package gateway

import (
	"fmt"
	"testing"

	"opencode2api/internal/proxypool"
)

func warpTestLane(tag string) *anonymousNode {
	egress := &egressTransport{proxy: &proxypool.Node{Tag: tag}}
	egress.healthy.Store(true)
	return &anonymousNode{egress: egress}
}

// TestAnonymousCursorLeadsWithWarp pins the anonymous attempt hierarchy:
// the first lane must be a WARP exit, and the first few lanes must
// interleave WARP with public chk exits instead of walking every chk lane
// before reaching a WARP one.
func TestAnonymousCursorLeadsWithWarp(t *testing.T) {
	nodes := make([]*anonymousNode, 0, 6)
	for i := 0; i < 3; i++ {
		nodes = append(nodes, warpTestLane(fmt.Sprintf("chk-%d", i)))
	}
	for i := 0; i < 3; i++ {
		nodes = append(nodes, warpTestLane(fmt.Sprintf("cf-warp-%d", i+1)))
	}

	pool := &anonymousPool{nodes: nodes}
	cursor := pool.CursorFor("")
	if len(cursor.nodes) != len(nodes) {
		t.Fatalf("cursor has %d lanes, want %d", len(cursor.nodes), len(nodes))
	}
	if !isWarpLane(cursor.nodes[0]) {
		t.Fatalf("first lane = %q, want a cf-warp lane", cursor.nodes[0].egress.proxy.Tag)
	}
	if isWarpLane(cursor.nodes[1]) {
		t.Fatalf("second lane = %q, want a public chk lane after the WARP head", cursor.nodes[1].egress.proxy.Tag)
	}
	if !isWarpLane(cursor.nodes[2]) {
		t.Fatalf("third lane = %q, want the next WARP lane interleaved", cursor.nodes[2].egress.proxy.Tag)
	}
	seen := make(map[*anonymousNode]bool, len(cursor.nodes))
	for _, node := range cursor.nodes {
		if seen[node] {
			t.Fatalf("lane %q appears twice in the cursor", node.egress.proxy.Tag)
		}
		seen[node] = true
	}
}

func TestFateKeySeparatesWarpFromPublicProxies(t *testing.T) {
	pool := &anonymousPool{}
	if got := pool.fateKey(&egressTransport{proxy: &proxypool.Node{Tag: "cf-warp-7"}}); got != "warp" {
		t.Fatalf("fateKey(cf-warp-7) = %q, want warp", got)
	}
	if got := pool.fateKey(&egressTransport{proxy: &proxypool.Node{Tag: "chk-7"}}); got != "proxy" {
		t.Fatalf("fateKey(chk-7) = %q, want proxy", got)
	}
}

// TestWarpHeadIsSkippedWhenNoWarpLaneExists proves the new head does not
// invent a lane or panic on a pool made only of public proxies.
func TestWarpHeadWithoutWarpLanes(t *testing.T) {
	nodes := []*anonymousNode{
		warpTestLane("chk-1"),
		warpTestLane("chk-2"),
	}
	pool := &anonymousPool{nodes: nodes}
	cursor := pool.CursorFor("")
	if len(cursor.nodes) != 2 {
		t.Fatalf("cursor has %d lanes, want 2", len(cursor.nodes))
	}
	if isWarpLane(cursor.nodes[0]) {
		t.Fatal("cursor led with WARP in a pool without WARP lanes")
	}
}
