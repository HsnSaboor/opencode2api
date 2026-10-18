package gateway

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/proxypool"
)

// testProxyManager builds a manager with synthetic nodes and pre-verified
// indexes, without a multiplexer or profile file.
func testProxyManager(t *testing.T, nodeCount int, verified ...int) *proxyManager {
	t.Helper()
	nodes := make([]proxypool.Node, nodeCount)
	for i := range nodes {
		nodes[i] = proxypool.Node{Index: i, Tag: fmt.Sprintf("n%d", i), Type: "vless", User: fmt.Sprintf("p%d", i)}
	}
	manager := &proxyManager{
		cfg:     config.ProxyPoolConfig{Enabled: true, Count: 4, SocksPort: 21800},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		profile: &proxypool.Profile{Path: "test", Nodes: nodes, Fingerprint: "fp"},
		state:   make([]proxyNodeState, nodeCount),
		wake:    make(chan struct{}, 1),
	}
	for _, index := range verified {
		manager.state[index].verified = true
		manager.state[index].lastVerified = time.Now()
	}
	return manager
}

// take/release must claim each verified node exactly once, skip unverified
// ones, and return a released node to the pool.
func TestProxyManagerTakeRelease(t *testing.T) {
	manager := testProxyManager(t, 3, 0, 1)
	first, ok := manager.take()
	if !ok || first.Index != 0 {
		t.Fatalf("take = %+v/%v, want node 0", first, ok)
	}
	if !manager.state[0].inUse {
		t.Fatal("claimed node must be marked in use")
	}
	second, ok := manager.take()
	if !ok || second.Index != 1 {
		t.Fatalf("take = %+v/%v, want node 1", second, ok)
	}
	if _, ok := manager.take(); ok {
		t.Fatal("take returned an unverified node")
	}
	manager.release(0)
	if manager.state[0].inUse {
		t.Fatal("released node still in use")
	}
	again, ok := manager.take()
	if !ok || again.Index != 0 {
		t.Fatalf("take after release = %+v/%v, want node 0", again, ok)
	}
}

// A cooling node must be invisible to both the lane pool and the probe
// sweep until its window expires.
func TestProxyManagerCoolingSkipsNode(t *testing.T) {
	manager := testProxyManager(t, 2)
	manager.state[0].coolingUntil = time.Now().Add(time.Minute)
	candidate, ok := manager.nextCandidate()
	if !ok || candidate.Index != 1 {
		t.Fatalf("candidate = %+v/%v, want node 1 (node 0 cooling)", candidate, ok)
	}
	manager.completeVerify(1, proxyOutcomeInconclusive)
	manager.state[0].coolingUntil = time.Now().Add(-time.Second)
	candidate, ok = manager.nextCandidate()
	if !ok || candidate.Index != 0 {
		t.Fatalf("candidate after expiry = %+v/%v, want node 0", candidate, ok)
	}
}

// Verdicts must land as state: verified promotes, FreTier/rejected cool with
// escalation, inconclusive leaves the node untouched for a retry.
func TestProxyCompleteVerifyVerdicts(t *testing.T) {
	manager := testProxyManager(t, 3)
	manager.completeVerify(0, proxyOutcomeFreitier)
	if manager.state[0].verified {
		t.Fatal("freitier verdict must not verify a node")
	}
	if manager.state[0].failures != 1 {
		t.Fatalf("failures = %d, want 1", manager.state[0].failures)
	}
	if until := time.Until(manager.state[0].coolingUntil); until < 29*time.Minute {
		t.Fatalf("cooling window = %v, want ~%v", until, proxyFreitierCooldownBase)
	}
	manager.completeVerify(0, proxyOutcomeFreitier)
	if until := time.Until(manager.state[0].coolingUntil); until < 59*time.Minute {
		t.Fatalf("second strike window = %v, want ~%v", until, 2*proxyFreitierCooldownBase)
	}
	manager.completeVerify(1, proxyOutcomeVerified)
	if !manager.state[1].verified || manager.state[1].failures != 0 || !manager.state[1].coolingUntil.IsZero() {
		t.Fatalf("verified state = %+v", manager.state[1])
	}
	manager.completeVerify(2, proxyOutcomeInconclusive)
	if manager.state[2].failures != 0 || !manager.state[2].coolingUntil.IsZero() || manager.state[2].probing {
		t.Fatalf("inconclusive changed state: %+v", manager.state[2])
	}
	if _, ok := manager.take(); !ok {
		t.Fatal("verified node must be takeable")
	}
}

// Cooldowns must be bounded: dead exits escalate to the cap, gate-blocked
// exits stop at the freitier cap.
func TestProxyCooldownBounds(t *testing.T) {
	if got := proxyCooldownFor(proxyOutcomeDead, 1); got != proxyDeadCooldownBase {
		t.Fatalf("dead base = %v, want %v", got, proxyDeadCooldownBase)
	}
	if got := proxyCooldownFor(proxyOutcomeDead, 99); got != proxyDeadCooldownCap {
		t.Fatalf("dead cap = %v, want %v", got, proxyDeadCooldownCap)
	}
	if got := proxyCooldownFor(proxyOutcomeFreitier, 1); got != proxyFreitierCooldownBase {
		t.Fatalf("freitier base = %v, want %v", got, proxyFreitierCooldownBase)
	}
	if got := proxyCooldownFor(proxyOutcomeFreitier, 99); got != proxyFreitierCooldownCap {
		t.Fatalf("freitier cap = %v, want %v", got, proxyFreitierCooldownCap)
	}
	if got := proxyCooldownFor(proxyOutcomeRateLimited, 1); got != proxyRateLimitCooldownBase {
		t.Fatalf("rate-limited base = %v, want %v", got, proxyRateLimitCooldownBase)
	}
	if got := proxyCooldownFor(proxyOutcomeRejected, 1); got != proxyRejectedCooldownBase {
		t.Fatalf("rejected base = %v, want %v", got, proxyRejectedCooldownBase)
	}
}

// A proxy park's replacement lane must come from the verified node set, bind
// the node, and leave the node busy for a second lane.
func TestReplenishProxyLaneBindsNode(t *testing.T) {
	manager := testProxyManager(t, 2, 0, 1)
	pool, err := newTransportPool([]string{"direct"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	pool.proxies = manager
	lane := pool.replenishProxyLane(nil)
	if lane == nil {
		t.Fatal("replenish returned no lane")
	}
	if lane.proxy == nil || lane.proxy.Index != 0 {
		t.Fatalf("lane proxy binding = %+v, want node 0", lane.proxy)
	}
	if lane.address != "proxy:0" {
		t.Fatalf("lane address = %q, want proxy:0", lane.address)
	}
	if got := len(pool.snapshot()); got != 2 {
		t.Fatalf("pool size = %d, want 2", got)
	}
	if !manager.state[0].inUse {
		t.Fatal("bound node must stay in use")
	}
	if got := manager.availableVerified(); got != 1 {
		t.Fatalf("available = %d, want 1", got)
	}
}

// A failed lane build (pool cap) must release the claimed node so a later
// park can still use it.
func TestReplenishProxyLaneReleasesOnCap(t *testing.T) {
	manager := testProxyManager(t, 2, 0, 1)
	pool, err := newTransportPool([]string{"direct"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	pool.proxies = manager
	pool.maxLanes = 1
	if lane := pool.replenishProxyLane(nil); lane != nil {
		t.Fatalf("replenish at cap returned %+v, want nil", lane)
	}
	if got := manager.availableVerified(); got != 2 {
		t.Fatalf("available after cap failure = %d, want 2 (node released)", got)
	}
}

// A parked proxy lane must draw a proxy replacement through the anonymous
// pool, not an IPv6 address lane.
func TestMaybeReplenishProxyBranch(t *testing.T) {
	manager := testProxyManager(t, 1, 0)
	pool, err := newTransportPool([]string{"direct"}, testPerf(), 0, nil, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	pool.proxies = manager
	anon := newAnonymousPool(true, pool, time.Second, nil)
	node := manager.profile.Nodes[0]
	parked := &egressTransport{address: "proxy:0", proxy: &node}
	anon.maybeReplenish(parked)
	nodes := anon.snapshotNodes()
	if len(nodes) != 2 {
		t.Fatalf("anonymous nodes = %d, want 2 (initial + proxy replacement)", len(nodes))
	}
	last := nodes[len(nodes)-1].egress
	if last.proxy == nil || last.address != "proxy:0" {
		t.Fatalf("replacement lane = %+v, want proxy:0", last)
	}
}

// Persisted state must round-trip verified and cooling verdicts, drop the
// transient in-use binding, and refuse a foreign profile fingerprint.
func TestProxyStateRoundTrip(t *testing.T) {
	manager := testProxyManager(t, 3, 0)
	manager.statePath = filepath.Join(t.TempDir(), "pool", "proxy-state.json")
	manager.state[1].verified = true
	manager.state[2].failures = 3
	manager.state[2].coolingUntil = time.Now().Add(time.Hour)
	manager.state[0].inUse = true
	manager.dirty = true
	manager.saveState()

	fresh := testProxyManager(t, 3)
	fresh.statePath = manager.statePath
	fresh.loadState()
	if !fresh.state[0].verified || !fresh.state[1].verified {
		t.Fatalf("verified flags lost: %+v / %+v", fresh.state[0], fresh.state[1])
	}
	if fresh.state[0].inUse {
		t.Fatal("in-use binding must not persist across restarts")
	}
	if fresh.state[2].failures != 3 {
		t.Fatalf("failures = %d, want 3", fresh.state[2].failures)
	}
	if !fresh.state[2].coolingUntil.After(time.Now()) {
		t.Fatalf("cooling window lost: %v", fresh.state[2].coolingUntil)
	}

	other := testProxyManager(t, 3)
	other.statePath = manager.statePath
	other.profile = &proxypool.Profile{Path: "test", Nodes: other.profile.Nodes, Fingerprint: "other"}
	other.loadState()
	if stats := other.stats(); stats.Verified != 0 || stats.Cooling != 0 {
		t.Fatalf("foreign fingerprint loaded state: %+v", stats)
	}
}

// The stats view drives the resource snapshot: verified/available/cooling/
// pending must partition the node set along the documented lines.
func TestProxyStatsPartition(t *testing.T) {
	manager := testProxyManager(t, 4, 0, 1)
	manager.state[0].inUse = true
	manager.state[2].failures = 1
	manager.state[2].coolingUntil = time.Now().Add(time.Minute)
	stats := manager.stats()
	if stats.Nodes != 4 || stats.Verified != 2 || stats.Available != 1 || stats.Cooling != 1 || stats.Pending != 1 {
		t.Fatalf("stats = %+v, want nodes 4 verified 2 available 1 cooling 1 pending 1", stats)
	}
}

// Verified nodes are re-tested once their verdict is stale, and a failed
// re-verify retires the admission instead of keeping a dead node "ready".
func TestProxyReverifyStaleVerdicts(t *testing.T) {
	manager := testProxyManager(t, 1, 0)
	if _, ok := manager.nextCandidate(); ok {
		t.Fatal("freshly verified node must not be re-probed")
	}
	manager.state[0].lastVerified = time.Now().Add(-2 * proxyReverifyInterval)
	candidate, ok := manager.nextCandidate()
	if !ok || candidate.Index != 0 {
		t.Fatalf("stale verified node = %+v/%v, want node 0", candidate, ok)
	}
	manager.completeVerify(0, proxyOutcomeFreitier)
	if manager.state[0].verified {
		t.Fatal("failed re-verify must retire the admission")
	}
	if !manager.state[0].coolingUntil.After(time.Now()) {
		t.Fatal("failed re-verify must cool the node")
	}
}

// staleVerified drives the sweep when lanes and reserve are otherwise full.
func TestProxyStaleVerifiedCount(t *testing.T) {
	manager := testProxyManager(t, 3, 0, 1)
	if got := manager.staleVerified(); got != 0 {
		t.Fatalf("stale = %d, want 0", got)
	}
	manager.state[0].inUse = true
	manager.state[1].lastVerified = time.Time{}
	if got := manager.staleVerified(); got != 1 {
		t.Fatalf("stale = %d, want 1 (in-use node excluded)", got)
	}
}

// A canary-confirmed gate rejection pauses candidate sweeps so failing
// probes cannot charge healthy nodes; the hold expires on its own and never
// shrinks under a later, shorter request.
func TestProxyManagerGateHold(t *testing.T) {
	manager := testProxyManager(t, 2)
	if held, _ := manager.sweepHold(); held {
		t.Fatal("fresh manager must not be held")
	}
	manager.holdSweep(60 * time.Millisecond)
	if held, until := manager.sweepHold(); !held || until.IsZero() {
		t.Fatal("holdSweep did not register a hold")
	}
	if _, ok := manager.nextCandidate(); ok {
		t.Fatal("held sweep must not hand out candidates")
	}
	manager.holdSweep(time.Second)
	manager.holdSweep(10 * time.Millisecond)
	if _, until := manager.sweepHold(); time.Until(until) < 500*time.Millisecond {
		t.Fatal("a shorter hold must not shorten an existing one")
	}
	time.Sleep(70 * time.Millisecond)
	if held, _ := manager.sweepHold(); !held {
		t.Fatal("extended hold expired early")
	}
	manager.mu.Lock()
	manager.gateHoldUntil = time.Now().Add(-time.Second)
	manager.mu.Unlock()
	if held, _ := manager.sweepHold(); held {
		t.Fatal("expired hold still reported")
	}
	if _, ok := manager.nextCandidate(); !ok {
		t.Fatal("sweep did not resume after the hold expired")
	}
}

// The canary probes the most recently verified node: it is the best
// available evidence of what the gate currently admits.
func TestProxyManagerControlNodePrefersFreshVerified(t *testing.T) {
	manager := testProxyManager(t, 3, 0, 1, 2)
	manager.state[0].lastVerified = time.Now().Add(-3 * time.Hour)
	manager.state[1].lastVerified = time.Now().Add(-2 * time.Hour)
	manager.state[2].lastVerified = time.Now().Add(-time.Minute)
	node, ok := manager.controlNode()
	if !ok || node.Index != 2 {
		t.Fatalf("control = %+v/%v, want node 2", node, ok)
	}
	if manager.state[2].inUse {
		t.Fatal("control probe must not claim the node")
	}
	if _, ok := testProxyManager(t, 2).controlNode(); ok {
		t.Fatal("a manager without verified nodes has no control")
	}
}

// Gate policy verdicts are canary-checked before they are charged to a node;
// transport deaths are not, because a request-scoped wave cannot make an
// exit unreachable.
func TestProxyGateFailureClassification(t *testing.T) {
	for _, outcome := range []proxyProbeOutcome{proxyOutcomeFreitier, proxyOutcomeRateLimited, proxyOutcomeRejected} {
		if !proxyGateFailure(outcome) {
			t.Fatalf("%v must be canary-checked", outcome)
		}
	}
	for _, outcome := range []proxyProbeOutcome{proxyOutcomeVerified, proxyOutcomeDead, proxyOutcomeInconclusive} {
		if proxyGateFailure(outcome) {
			t.Fatalf("%v must not be canary-checked", outcome)
		}
	}
}
