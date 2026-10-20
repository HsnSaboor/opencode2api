package gateway

import (
	"fmt"
	"testing"
	"time"
)

// The anonymous scan is per-IP quota-bound: a request that opens on a
// flagged lane pays a 403 and a park before reaching a working egress. The
// cursor must therefore lead with the lanes whose 2xx arrived most recently
// — fall back to the fate-interleaved order, and serve every lane exactly
// once — so a request opens on an egress the pool just proved works.
func TestAnonymousCursorLeadsWithRecentSuccesses(t *testing.T) {
	pool := &anonymousPool{enabled: true}
	a, b, c := laneAt(0, "2001:db8:1::1"), laneAt(1, "2001:db8:2::1"), laneAt(2, "2001:db8:3::1")
	na, nb, nc := &anonymousNode{egress: a}, &anonymousNode{egress: b}, &anonymousNode{egress: c}
	pool.nodes = []*anonymousNode{na, nb, nc}

	drain := func(affinity string) []*anonymousNode {
		cursor := pool.CursorFor(affinity)
		var out []*anonymousNode
		for node := cursor.Next(); node != nil; node = cursor.Next() {
			out = append(out, node)
		}
		return out
	}

	// A cold pool still serves every lane exactly once in rotation order.
	if got := drain("s1"); len(got) != 3 {
		t.Fatalf("cold cursor served %d lanes, want 3", len(got))
	}

	// The most recent success leads; older successes follow newest first.
	pool.MarkSuccess(nb)
	got := drain("s1")
	if len(got) != 3 || got[0] != nb {
		t.Fatalf("cursor order = %v, want the last success first", got)
	}
	pool.MarkSuccess(nc)
	got = drain("s2")
	if len(got) != 3 || got[0] != nc || got[1] != nb {
		t.Fatalf("cursor order = %v, want recent successes newest-first", got)
	}

	// Re-marking a lane moves it to the head without duplicating it.
	pool.MarkSuccess(nc)
	got = drain("s3")
	if len(got) != 3 || got[0] != nc {
		t.Fatalf("re-marked cursor order = %v, want the re-marked lane first and no duplicates", got)
	}
	seen := map[*anonymousNode]int{}
	for _, node := range got {
		seen[node]++
	}
	for _, node := range []*anonymousNode{na, nb, nc} {
		if seen[node] != 1 {
			t.Fatalf("lane %p served %d times, want exactly once", node, seen[node])
		}
	}

	// A parked recent lane is skipped even at the head: the upstream
	// verdict outranks the recency hint.
	pool.MarkSuccess(na)
	a.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	got = drain("s4")
	if len(got) != 2 || got[0] != nc || got[1] != nb {
		t.Fatalf("cursor order with a parked recent lane = %v, want the unparked recent lanes", got)
	}
}

// A prefix the gate has flagged as a unit is drained after every independent
// class: its sibling lanes can only answer the same 403, so they must not
// stand between the scan and a lane the wave did not cover.
func TestAnonymousInterleaveDrainsCoolingPrefixesLast(t *testing.T) {
	const prefix = "2001:db8:1::/48"
	transports := &transportPool{prefixes: []string{prefix}}
	pool := &anonymousPool{enabled: true, prefixes: []string{prefix}, transports: transports}
	first := testFateLane(0, "198.51.100.7", true)
	dead := testFateLane(1, "2001:db8:1::5", false)
	other := testFateLane(2, "198.51.100.8", true)
	nodes := []*anonymousNode{{egress: first}, {egress: dead}, {egress: other}}

	// While the prefix is selectable the classes interleave: the prefix
	// lane sits between the two proxy lanes.
	ordered := interleaveOrder(pool, nodes, 0)
	if len(ordered) != 3 || ordered[1].egress != dead {
		t.Fatalf("uncooled order = %v, want the prefix lane interleaved", ordered)
	}

	// Two FreeTier verdicts without a success cool the prefix; its lanes
	// drain after the independent classes instead of interleaving.
	transports.StrikePrefix("2001:db8:1::5")
	if _, _, cooling := transports.StrikePrefix("2001:db8:1::5"); !cooling {
		t.Fatal("prefix did not cool after the strike limit")
	}
	ordered = interleaveOrder(pool, nodes, 0)
	if len(ordered) != 3 {
		t.Fatalf("cooled order served %d lanes, want all 3", len(ordered))
	}
	if ordered[len(ordered)-1].egress != dead {
		t.Fatalf("cooled order = %v, want the cooling prefix last", ordered)
	}
}

// A prefix that cools mid-scan must stop consuming attempts: the cursor
// defers its remaining lanes behind every live lane, yet still reaches them
// when nothing else is selectable.
func TestAnonymousCursorDefersCoolingPrefixMidScan(t *testing.T) {
	const prefix = "2001:db8:1::/48"
	transports := &transportPool{prefixes: []string{prefix}}
	pool := &anonymousPool{enabled: true, prefixes: []string{prefix}, transports: transports}
	dead := testFateLane(0, "2001:db8:1::5", false)
	live := testFateLane(1, "198.51.100.7", true)
	deadNode := &anonymousNode{egress: dead}
	liveNode := &anonymousNode{egress: live}
	pool.nodes = []*anonymousNode{deadNode, liveNode}

	// The cursor snapshot predates the cooldown, as in a scan whose own
	// FreeTier verdicts cool the prefix.
	cursor := pool.CursorFor("")
	transports.StrikePrefix("2001:db8:1::5")
	if _, _, cooling := transports.StrikePrefix("2001:db8:1::5"); !cooling {
		t.Fatal("prefix did not cool after the strike limit")
	}

	var got []*anonymousNode
	for node := cursor.Next(); node != nil; node = cursor.Next() {
		got = append(got, node)
	}
	if len(got) != 2 {
		t.Fatalf("cursor served %d lanes, want all 2 (cooling lanes defer, never drop)", len(got))
	}
	if got[0].egress != live || got[1].egress != dead {
		t.Fatalf("cursor order = %v, want the live lane before the cooling prefix", got)
	}

	// With no live lane left the deferred lanes are still served: a
	// cooldown is a priority, not a blacklist.
	only := &anonymousPool{enabled: true, prefixes: []string{prefix}, transports: transports}
	only.nodes = []*anonymousNode{deadNode}
	cursor = only.CursorFor("")
	if node := cursor.Next(); node != deadNode {
		t.Fatalf("all-cooling cursor = %v, want the deferred lane", node)
	}
}

// The recent-success memory is bounded: the oldest wins fall off, so a scan
// left without a working recent lane still reaches the interleaved
// full-pool order quickly.
func TestAnonymousRecentSuccessesAreCapped(t *testing.T) {
	pool := &anonymousPool{enabled: true}
	var nodes []*anonymousNode
	for i := 0; i < recentSuccessLimit+3; i++ {
		node := &anonymousNode{egress: laneAt(i, fmt.Sprintf("2001:db8:%x::1", i))}
		nodes = append(nodes, node)
		pool.nodes = append(pool.nodes, node)
	}
	for _, node := range nodes {
		pool.MarkSuccess(node)
	}
	if len(pool.recent) != recentSuccessLimit {
		t.Fatalf("recent list holds %d lanes, want %d", len(pool.recent), recentSuccessLimit)
	}
	last := nodes[len(nodes)-1]
	if pool.recent[0] != last {
		t.Fatalf("recent head = %p, want the last success %p", pool.recent[0], last)
	}
}
