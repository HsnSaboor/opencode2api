package gateway

import (
	"fmt"
	"testing"
	"time"

	"opencode2api/internal/proxypool"
)

// testFateLane builds a synthetic lane without dialers: fate accounting and
// selection only read the address, the proxy binding, and the park and
// health atomics.
func testFateLane(index int, address string, proxy bool) *egressTransport {
	lane := &egressTransport{index: index, address: address}
	if proxy {
		node := proxypool.Node{Index: index, Type: "vless", User: fmt.Sprintf("p%d", index)}
		lane.proxy = &node
	}
	lane.healthy.Store(true)
	return lane
}

// testFatePool builds a selection pool over synthetic lanes. The transport
// pool is deliberately nil: selection and fate accounting never dial, and a
// nil transport pool keeps ParkFreitier from sampling real replacement
// addresses inside the unit test.
func testFatePool(prefixes []string, lanes ...*egressTransport) *anonymousPool {
	pool := &anonymousPool{
		enabled:  true,
		prefixes: append([]string(nil), prefixes...),
	}
	for _, lane := range lanes {
		pool.nodes = append(pool.nodes, &anonymousNode{egress: lane})
	}
	return pool
}

// A scan must reach an independent fate class (another prefix, or a proxy
// exit) within its first few attempts instead of walking a contiguous run of
// lanes that one upstream block may have covered.
func TestInterleaveByFateCoversClassesEarly(t *testing.T) {
	prefixes := []string{"2001:db8:aaaa::/48", "2001:db8:bbbb::/48", "2001:db8:cccc::/48"}
	lanes := []*egressTransport{
		testFateLane(0, "2001:db8:aaaa::1", false),
		testFateLane(1, "2001:db8:aaaa::2", false),
		testFateLane(2, "2001:db8:aaaa::3", false),
		testFateLane(3, "2001:db8:bbbb::1", false),
		testFateLane(4, "2001:db8:bbbb::2", false),
		testFateLane(5, "2001:db8:cccc::1", false),
		testFateLane(6, "2001:db8:cccc::2", false),
		testFateLane(7, "2001:db8:cccc::3", false),
		testFateLane(8, "2001:db8:cccc::4", false),
		testFateLane(9, "2001:db8:cccc::5", false),
		testFateLane(10, "proxy:10", true),
		testFateLane(11, "proxy:11", true),
	}
	pool := testFatePool(prefixes, lanes...)
	nodes := pool.snapshotNodes()
	if len(nodes) != len(lanes) {
		t.Fatalf("snapshot = %d nodes, want %d", len(nodes), len(lanes))
	}
	ordered := interleaveByFate(pool, nodes, 3)
	if ordered[0] != nodes[3] {
		t.Fatal("the affinity-chosen lane must stay first")
	}
	// Every lane appears exactly once.
	seen := map[*egressTransport]int{}
	for _, node := range ordered {
		seen[node.egress]++
	}
	for _, lane := range lanes {
		if seen[lane] != 1 {
			t.Fatalf("lane %s appears %d times", lane.address, seen[lane])
		}
	}
	// The first four attempts cover four distinct fate classes: the leading
	// lane's class plus up to three more in round-robin order.
	classes := map[string]bool{}
	for _, node := range ordered[:4] {
		classes[pool.fateKey(node.egress)] = true
	}
	if len(classes) != 4 {
		t.Fatalf("first four attempts cover %d classes (%v), want 4", len(classes), classes)
	}
	if !classes["proxy"] {
		t.Fatalf("proxy exits not reached inside the first four attempts: %v", classes)
	}
}

// Addressing classes are the configured prefixes; proxy lanes share one class
// and are never counted against an address prefix.
func TestFateKeyClasses(t *testing.T) {
	pool := testFatePool([]string{"2001:db8:aaaa::/48"})
	address := testFateLane(0, "2001:db8:aaaa:0:1::9", false)
	proxy := testFateLane(1, "proxy:1", true)
	other := testFateLane(2, "2001:db8:zzzz::1", false)
	direct := testFateLane(3, "direct", false)
	if got := pool.fateKey(address); got != "2001:db8:aaaa::/48" {
		t.Fatalf("address key = %q", got)
	}
	if got := pool.fateKey(proxy); got != "proxy" {
		t.Fatalf("proxy key = %q", got)
	}
	if got := pool.fateKey(other); got != "direct" {
		t.Fatalf("unconfigured address key = %q", got)
	}
	if got := pool.fateKey(direct); got != "direct" {
		t.Fatalf("direct key = %q", got)
	}
	if got := pool.fateKey(nil); got != "none" {
		t.Fatalf("nil key = %q", got)
	}
}

// Next must skip lanes that are unhealthy, cooling, or parked, and resume
// visiting them once the state clears.
func TestCursorSkipsUnselectableLanes(t *testing.T) {
	lanes := []*egressTransport{
		testFateLane(0, "2001:db8:aaaa::1", false),
		testFateLane(1, "2001:db8:bbbb::1", false),
		testFateLane(2, "2001:db8:cccc::1", false),
	}
	pool := testFatePool([]string{"2001:db8:aaaa::/48", "2001:db8:bbbb::/48", "2001:db8:cccc::/48"}, lanes...)
	lanes[0].healthy.Store(false)
	pool.nodes[1].cooldownUntil.Store(time.Now().Add(time.Minute).UnixNano())
	pool.ParkFreitier(lanes[2])
	cursor := pool.CursorFor("ses_cursor")
	if node := cursor.Next(); node != nil {
		t.Fatalf("cursor returned %s with every lane unselectable", node.egress.address)
	}
	// Clearing each state must make its lane discoverable again.
	lanes[0].healthy.Store(true)
	pool.nodes[1].cooldownUntil.Store(0)
	pool.UnparkFreitier(lanes[2])
	cursor = pool.CursorFor("ses_cursor")
	found := map[string]bool{}
	for node := cursor.Next(); node != nil; node = cursor.Next() {
		found[node.egress.address] = true
	}
	if len(found) != len(lanes) {
		t.Fatalf("recovered lanes = %v, want all %d", found, len(lanes))
	}
}

// A request-scoped FreeTier verdict restores the lanes it parked: the band
// must be selectable again immediately, without waiting out the park window.
func TestUnparkFreitierRestoresSelection(t *testing.T) {
	lane := testFateLane(0, "2001:db8:aaaa::1", false)
	pool := testFatePool([]string{"2001:db8:aaaa::/48"}, lane)
	pool.ParkFreitier(lane)
	if !freitierParked(lane) {
		t.Fatal("ParkFreitier did not park the lane")
	}
	cursor := pool.CursorFor("")
	if node := cursor.Next(); node != nil {
		t.Fatal("parked lane still selectable")
	}
	pool.UnparkFreitier(lane)
	if freitierParked(lane) {
		t.Fatal("UnparkFreitier did not clear the park")
	}
	cursor = pool.CursorFor("")
	if node := cursor.Next(); node == nil || node.egress != lane {
		t.Fatal("unparked lane not selectable")
	}
}
