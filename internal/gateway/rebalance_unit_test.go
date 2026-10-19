package gateway

import (
	"fmt"
	"testing"
	"time"

	"opencode2api/internal/proxypool"
)

// Keys bind when their pool is built, but proxy lanes seed after key pools
// and keep filling afterwards. Without a rebalance pass every key stays
// stacked on the lanes that existed at construction, leaving one egress
// address answering for a whole keyed provider while the proxy exits idle.
func TestRebalanceSpreadsKeysOntoLaterLanes(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1", "k2", "k3"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	if got := nodes.bindingCount[0]; got != 4 {
		t.Fatalf("construction bindingCount[0] = %d, want 4", got)
	}
	for i := 1; i <= 4; i++ {
		lane := &egressTransport{index: i, address: fmt.Sprintf("proxy:%d", i), proxy: &proxypool.Node{}}
		lane.healthy.Store(true)
		pool.items = append(pool.items, lane)
	}
	if moved := nodes.RebalanceEgress(nil, false); moved != 4 {
		t.Fatalf("RebalanceEgress moved %d, want 4", moved)
	}
	if got := nodes.bindingCount[0]; got != 0 {
		t.Fatalf("direct lane still holds %d keys, want 0", got)
	}
	seen := make(map[int]int, 4)
	for _, node := range nodes.nodes {
		index := int(node.egressIndex.Load())
		if node.preferredEgress != index {
			t.Fatalf("node %d preferredEgress = %d, egressIndex = %d", node.index, node.preferredEgress, index)
		}
		if egress := nodes.Egress(node); egress == nil || egress.proxy == nil {
			t.Fatalf("node %d is not on a proxy lane after rebalance", node.index)
		}
		seen[index]++
	}
	for lane, count := range seen {
		if count != 1 {
			t.Fatalf("lane %d holds %d keys, want 1", lane, count)
		}
	}
	if moved := nodes.RebalanceEgress(nil, false); moved != 0 {
		t.Fatalf("second RebalanceEgress moved %d, want 0", moved)
	}
	// Re-homed keys must not snap back when the original lane recovers.
	if moved := nodes.RestoreEgress(0); moved != 0 {
		t.Fatalf("RestoreEgress moved %d keys back to the direct lane, want 0", moved)
	}
}

func TestRebalanceSkipsUnhealthyLanes(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	sick := &egressTransport{index: 1, address: "proxy:1", proxy: &proxypool.Node{}}
	sick.healthy.Store(false)
	well := &egressTransport{index: 2, address: "proxy:2", proxy: &proxypool.Node{}}
	well.healthy.Store(true)
	pool.items = append(pool.items, sick, well)
	if moved := nodes.RebalanceEgress(nil, false); moved != 1 {
		t.Fatalf("RebalanceEgress moved %d, want 1", moved)
	}
	if got := nodes.bindingCount[0]; got != 1 {
		t.Fatalf("direct lane count = %d, want 1 (last resort)", got)
	}
	if got := nodes.bindingCount[1]; got != 0 {
		t.Fatalf("unhealthy lane received %d keys, want 0", got)
	}
	if got := nodes.bindingCount[2]; got != 1 {
		t.Fatalf("healthy lane holds %d keys, want 1", got)
	}
}

// laneAt and proxyAt build the lane fixtures the placement tests stack after
// the direct lane a fresh transport pool always holds at index zero.
func laneAt(index int, address string) *egressTransport {
	lane := &egressTransport{index: index, address: address}
	lane.healthy.Store(true)
	return lane
}

func proxyAt(index int) *egressTransport {
	lane := &egressTransport{index: index, address: fmt.Sprintf("proxy:%d", index), proxy: &proxypool.Node{}}
	lane.healthy.Store(true)
	return lane
}

// Dedicated source-address lanes are the preferred home for keys: each draw
// carries its own network identity, so accounts spread across them answer
// from distinct addresses. Proxy exits are the next choice and the host's
// own route the last resort.
func TestRebalancePrefersDedicatedLanes(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	pool.items = append(pool.items, laneAt(1, "2001:db8:1::5"), laneAt(2, "2001:db8:2::9"), proxyAt(3))
	if moved := nodes.RebalanceEgress(nil, false); moved != 2 {
		t.Fatalf("RebalanceEgress moved %d, want 2", moved)
	}
	if got := nodes.bindingCount[0]; got != 0 {
		t.Fatalf("direct lane still holds %d keys, want 0", got)
	}
	if nodes.bindingCount[1] != 1 || nodes.bindingCount[2] != 1 {
		t.Fatalf("dedicated lanes hold %d/%d keys, want 1/1", nodes.bindingCount[1], nodes.bindingCount[2])
	}
	if got := nodes.bindingCount[3]; got != 0 {
		t.Fatalf("proxy lane holds %d keys while dedicated lanes idle, want 0", got)
	}
}

// Benched lanes — parked by a FreeTier flag or cooled under a region block —
// are never chosen as a key's home: selection prefers to skip them, so a key
// placed there would sit out until the bench expires. The host's route keeps
// its lone key when the only alternatives are benched, and keys drift back
// onto a dedicated lane as soon as it rejoins selection.
func TestRebalanceAvoidsBenchedLanes(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	parked := laneAt(1, "2001:db8:1::5")
	parked.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	cooled := laneAt(2, "2001:db8:2::9")
	pool.items = append(pool.items, parked, cooled, proxyAt(3))
	pool.CoolSubnet(cooled.address, time.Hour)
	if moved := nodes.RebalanceEgress(nil, false); moved != 1 {
		t.Fatalf("RebalanceEgress moved %d, want 1", moved)
	}
	if got := nodes.bindingCount[0]; got != 1 {
		t.Fatalf("direct lane holds %d keys, want 1 (last resort)", got)
	}
	if nodes.bindingCount[1] != 0 || nodes.bindingCount[2] != 0 {
		t.Fatalf("benched lanes hold %d/%d keys, want 0/0", nodes.bindingCount[1], nodes.bindingCount[2])
	}
	if got := nodes.bindingCount[3]; got != 1 {
		t.Fatalf("proxy lane holds %d keys, want 1", got)
	}
	// Once the park expires the lane rejoins selection and takes the key
	// that was left on the host's address.
	parked.freitierUntil.Store(0)
	if moved := nodes.RebalanceEgress(nil, false); moved != 1 {
		t.Fatalf("RebalanceEgress after park expiry moved %d, want 1", moved)
	}
	if got := nodes.bindingCount[1]; got != 1 {
		t.Fatalf("unparked dedicated lane holds %d keys, want 1", got)
	}
	if got := nodes.bindingCount[0]; got != 0 {
		t.Fatalf("direct lane still holds %d keys after the park expired, want 0", got)
	}
}

// A lane failure moves keys to an unbenched alternative — even onto company
// (a proxy lane that already carries a key) — never onto a benched lane;
// when every alternative is benched the keys stay put rather than parking
// themselves.
func TestRebindEgressAvoidsBenchedLanes(t *testing.T) {
	pool, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	parked := laneAt(1, "2001:db8:1::5")
	parked.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool.items = append(pool.items, parked, proxyAt(2))
	if moved := nodes.RebindEgress(0, false); moved != 2 {
		t.Fatalf("RebindEgress moved %d, want 2", moved)
	}
	if got := nodes.bindingCount[1]; got != 0 {
		t.Fatalf("parked lane received %d keys, want 0", got)
	}
	if got := nodes.bindingCount[2]; got != 2 {
		t.Fatalf("proxy lane holds %d keys, want 2", got)
	}

	// A preferProxy pool looks for a vacant proxy exit before the first
	// empty dedicated lane, mirroring RebalanceEgress's home order.
	pool3, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes3, err := newNodePool([]string{"k0"}, pool3, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	pool3.items = append(pool3.items, laneAt(1, "2001:db8:5::5"), proxyAt(2))
	if moved := nodes3.RebindEgress(0, true); moved != 1 {
		t.Fatalf("preferProxy RebindEgress moved %d, want 1", moved)
	}
	if got := int(nodes3.nodes[0].egressIndex.Load()); got != 2 {
		t.Fatalf("preferProxy rebind egressIndex = %d, want 2 (the exit)", got)
	}

	pool2, err := newTransportPool([]string{"direct4"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes2, err := newNodePool([]string{"k0", "k1"}, pool2, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	parked2 := laneAt(1, "2001:db8:3::5")
	parked2.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool2.items = append(pool2.items, parked2)
	if moved := nodes2.RebindEgress(0, false); moved != 0 {
		t.Fatalf("RebindEgress moved %d keys onto a parked lane, want 0", moved)
	}
	if got := nodes2.bindingCount[0]; got != 2 {
		t.Fatalf("keys left the failed lane: bindingCount[0] = %d, want 2", got)
	}
}

// A key bound to a lane that gets benched loses its home: cursors skip
// parked lanes for selection, so the key would sit out every request until
// the bench expires. The rebalance pass moves it onto a vacant healthy home
// the moment one exists.
func TestRebalanceMovesBenchedKeyOntoHealthyLane(t *testing.T) {
	pool, err := newTransportPool([]string{"2001:db8:1::5", "2001:db8:2::9"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	// k0 binds lane 0, k1 lane 1; then lane 1 parks on a FreeTier verdict.
	pool.items[1].freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool.items = append(pool.items, laneAt(2, "2001:db8:3::7"))
	if moved := nodes.RebalanceEgress(nil, false); moved != 1 {
		t.Fatalf("RebalanceEgress moved %d keys, want 1 (the benched key)", moved)
	}
	if got := nodes.bindingCount[1]; got != 0 {
		t.Fatalf("parked lane holds %d keys, want 0", got)
	}
	if got := nodes.bindingCount[2]; got != 1 {
		t.Fatalf("healthy lane holds %d keys, want 1", got)
	}
	if got := int(nodes.nodes[1].egressIndex.Load()); got != 2 {
		t.Fatalf("benched key egressIndex = %d, want 2", got)
	}
	if moved := nodes.RebalanceEgress(nil, false); moved != 0 {
		t.Fatalf("second RebalanceEgress moved %d keys, want 0", moved)
	}
}

// Zen keys invert the home order: the free-tier gate rejects the configured
// prefixes as a class, so a healthy dedicated lane is not a working home for
// free models and the pass drains Zen keys onto proxy exits while vacant
// exits wait. With no exit vacancy the healthy dedicated keys stay put — the
// pass never churns them between equivalent lanes — and a benched exit hands
// its key to the next vacant one.
func TestRebalanceZenKeysHomeOnProxyExits(t *testing.T) {
	pool, err := newTransportPool([]string{"2001:db8:1::5", "2001:db8:2::9"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	nodes, err := newNodePool([]string{"k0", "k1"}, pool, time.Second)
	if err != nil {
		t.Fatalf("newNodePool: %v", err)
	}
	// Both keys start on dedicated lanes; without exits they must stay.
	if moved := nodes.RebalanceEgress(nil, true); moved != 0 {
		t.Fatalf("RebalanceEgress without exits moved %d keys, want 0", moved)
	}
	pool.items = append(pool.items, proxyAt(2))
	if moved := nodes.RebalanceEgress(nil, true); moved != 1 {
		t.Fatalf("RebalanceEgress moved %d keys, want 1 (first exit vacancy)", moved)
	}
	if got := nodes.bindingCount[2]; got != 1 {
		t.Fatalf("proxy exit holds %d keys, want 1", got)
	}
	if got := int(nodes.nodes[0].egressIndex.Load()); got != 2 {
		t.Fatalf("drained key egressIndex = %d, want 2 (the exit)", got)
	}
	// A healthy dedicated key never churns while the exit vacancy is gone.
	if moved := nodes.RebalanceEgress(nil, true); moved != 0 {
		t.Fatalf("second RebalanceEgress moved %d keys, want 0", moved)
	}
	// The exit parks: its key moves to the next vacant exit.
	pool.items[2].freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool.items = append(pool.items, proxyAt(3))
	if moved := nodes.RebalanceEgress(nil, true); moved != 1 {
		t.Fatalf("RebalanceEgress after exit park moved %d keys, want 1", moved)
	}
	if got := nodes.bindingCount[2]; got != 0 {
		t.Fatalf("parked exit holds %d keys, want 0", got)
	}
	if got := nodes.bindingCount[3]; got != 1 {
		t.Fatalf("replacement exit holds %d keys, want 1", got)
	}
}
