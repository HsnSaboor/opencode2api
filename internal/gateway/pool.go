package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	rand "math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/proxypool"
)

// egressTransport is one upstream lane bound to a single source address. The
// address is either "direct" (no explicit bind, kernel-chosen family),
// "direct4" (no explicit bind, forced IPv4) or an IPv6 literal sampled from
// the configured egress prefixes; every lane dials straight to the upstream,
// so rotating lanes rotates the visible egress. A proxy lane ("proxy:<node>")
// instead dials the loopback SOCKS5 multiplexer as one profile node, so its
// visible egress is that node's exit IP; apart from the dialer the lane is
// identical — same selection, parks, health, and rotation.
type egressTransport struct {
	index   int
	address string
	client  *http.Client
	// proxy marks a multiplexed proxy lane and carries its node binding.
	// Set at construction and immutable afterwards.
	proxy    *proxypool.Node
	healthy  atomic.Bool
	checking atomic.Bool
	// freitierUntil parks a FreeTier-flagged exit while the upstream flag
	// clears (UnixNano wall clock, 0 = not parked). totalFreitiers counts
	// lifetime flags and is diagnostic only. freitierParks counts consecutive
	// parks with no success in between, which sets the escalating park
	// duration (see freitierParkFor). Park state lives on the lane (not the
	// anonymous node) so the key path shares the same enforcement.
	freitierUntil  atomic.Int64
	totalFreitiers atomic.Uint32
	freitierParks  atomic.Int32
}

type transportPool struct {
	items []*egressTransport
	// subnetMu guards subnetCooldowns, which cool whole /64s after a
	// corroborated region block. Per-lane checks (freitierUntil) stay
	// lock-free atomics; only the rare region verdict takes this mutex.
	subnetMu        sync.Mutex
	subnetCooldowns map[string]int64
	// replenishMu serializes on-demand lane replacement so concurrent
	// freitier parks cannot overshoot the pool cap. The hot selection
	// paths never take it.
	replenishMu sync.Mutex
	// perf, headerTimeout, prefixes, and weights carry the construction
	// inputs a replacement lane needs: a fresh lane must dial with the same
	// budgets as its siblings and land with the same prefix mix.
	perf          config.PerformanceConfig
	headerTimeout time.Duration
	prefixes      []string
	weights       map[string]int
	// proxies hands out verified nodes for proxy lanes and is nil when the
	// proxy pool is disabled. Set once at construction.
	proxies *proxyManager
	// prefixMu guards prefixStrikes/prefixCooling, which track FreeTier
	// verdicts per configured prefix: a prefix whose lanes keep answering
	// FreeTier is blocked upstream as a unit and leaves replenishment
	// sampling until its cooldown expires. Only the rare verdict path takes
	// this mutex.
	prefixMu      sync.Mutex
	prefixStrikes map[string]int
	prefixCooling map[string]int64
	// directParkUntil hard-parks every non-proxy ("direct") lane after an
	// upstream FreeTier/abuse verdict. Direct lanes draw on the host's
	// datacenter prefixes, which the free tier refuses wholesale; keeping
	// them selectable burns scan budget and can surface the upstream
	// "abusive traffic" 403 when no proxy lane is ready. Set at startup when
	// the proxy pool is enabled and refreshed by each fresh verdict.
	directParkUntil atomic.Int64
	// maxLanes caps total lanes (initial + replacements) so a sustained
	// flag storm cannot grow the pool without bound. Zero means the
	// constructor default below.
	maxLanes int
}

// maxPoolLanes caps the live lane count including dynamic replacements:
// twice the sampled pool, clamped to the validated config ceiling, plus
// headroom for proxy lanes. The /48 holds 65535 usable /64s, so the cap is a
// memory/socket guard, not address-space exhaustion.
const maxPoolLanes = 512

// anonymousPool gives the shared OpenCode "public" credential an independent
// cooldown per egress address. Unlike key nodes, anonymous nodes are never
// rebound: changing egress address is the failover mechanism because the free
// tier rate-limits by source IP.
type anonymousPool struct {
	nodes    []*anonymousNode
	next     atomic.Uint64
	cooldown time.Duration
	// breakerFailures counts consecutive pure-transport failures (an error
	// with no response headers) across requests; reaching
	// anonymousBreakerFailures opens the breaker. breakerUntil holds the end
	// of the open window as unix nanoseconds, zero when closed.
	breakerFailures atomic.Uint32
	breakerUntil    atomic.Int64
	// enabled mirrors the anonymous tier switch: when false the pool holds
	// no nodes even though the lanes exist for key tiers.
	enabled bool
	// transports is the lane source new nodes draw from when the pool
	// replenishes a parked lane. It aliases the gateway's shared pool.
	transports *transportPool
	// prefixes are the configured egress CIDRs used to sample replacement
	// lanes. Stored once at construction; never mutated afterwards.
	prefixes []string
	// recent lists the lanes whose 2xx arrived most recently, newest first,
	// capped at recentSuccessLimit. Cursors lead scans with them so a
	// request opens on an egress that answered lately — the anonymous scan
	// is per-IP quota-bound and otherwise half its requests open on a
	// flagged lane — while the interleaved fate order still covers every
	// lane once whenever a recent lane stops answering.
	recent []*anonymousNode
	// mu guards nodes and recent, which grow and reorder while selection
	// cursors range them. Read locks are slice-header-sized critical
	// sections only; no I/O happens under them.
	mu sync.RWMutex
}

type anonymousNode struct {
	egress   *egressTransport
	failures atomic.Uint32
	// total429s counts 429s since the last success on this lane and drives
	// the escalating 429 backoff. MarkSuccess clears it so a recovered lane
	// starts again at the base delay.
	total429s     atomic.Uint32
	cooldownUntil atomic.Int64
}

type anonymousCursor struct {
	pool   *anonymousPool
	nodes  []*anonymousNode
	start  int
	offset int
	// deferred holds lanes whose prefix was cooling when the scan reached
	// them. They are retried only after every live lane, so a blocked prefix
	// that cools mid-scan cannot keep eating attempts, while a pool whose
	// every lane is cooling still gets tried instead of failing outright.
	deferred []*anonymousNode
}

// cooldownFor converts a consecutive-failure count into an exponential
// backoff rooted at the configured base: base * 2^n capped at 8x, stretched
// to honor any Retry-After the upstream returned.
func cooldownFor(base time.Duration, failures uint32, retryAfter time.Duration) time.Duration {
	if failures == 0 {
		// Defensive: failures-1 on a uint32 zero underflows to MaxUint32.
		// All MarkFailure callers pass failures.Add(1) (>= 1), so this only
		// fires on direct misuse; clamping to 1 keeps the shift in range.
		failures = 1
	}
	delay := base * time.Duration(1<<min(failures-1, 3))
	if retryAfter > delay {
		delay = retryAfter
	}
	return delay
}

// cooldown429Cap bounds the calculated 429 park. Free-tier quotas reset on
// short rolling windows, so a multi-hour park outlives the upstream outage:
// nine keys limited within one minute stayed benched for eight hours and
// left the pool with a single key and no hedge. Retry-After can still ask
// for a longer park and is honored by storeCooldownDelay.
const cooldown429Cap = 30 * time.Minute

// cooldown429For returns the escalating 429 backoff for the n-th 429 on a
// lane (n >= 1): 5min * 3^(n-1), capped at cooldown429Cap. The first offense
// parks the lane briefly; repeat offenders back off to the cap. MarkSuccess
// clears the 429 history so a key that serves again starts at the base.
func cooldown429For(prior429s uint32) time.Duration {
	if prior429s == 0 {
		prior429s = 1
	}
	shift := min(prior429s-1, 5)
	delay := 5 * time.Minute
	for range shift {
		delay *= 3
	}
	return min(delay, cooldown429Cap)
}

// storeCooldownDelay records an explicit delay starting now, stretched to
// honor any Retry-After the upstream returned. Unlike storeCooldownUntil it
// applies no consecutive-failure multiplier: callers pass a fully computed
// delay such as the escalating 429 backoff.
func storeCooldownDelay(target *atomic.Int64, delay, retryAfter time.Duration) {
	if retryAfter > delay {
		delay = retryAfter
	}
	target.Store(time.Now().Add(delay).UnixNano())
}

// storeCooldownUntil records a base-backed backoff for failures consecutive
// failures starting now. The target holds a wall-clock UnixNano timestamp,
// which stays comparable across Store/Load pairs.
func storeCooldownUntil(target *atomic.Int64, base time.Duration, failures uint32, retryAfter time.Duration) {
	target.Store(time.Now().Add(cooldownFor(base, failures, retryAfter)).UnixNano())
}

func startForAffinity(next *atomic.Uint64, affinity string, size int) int {
	if size <= 0 {
		return 0
	}
	if affinity == "" {
		return int((next.Add(1) - 1) % uint64(size))
	}
	return int(config.StableIndex(affinity) % uint64(size))
}

func newAnonymousPool(enabled bool, transports *transportPool, cooldown time.Duration, prefixes []string) *anonymousPool {
	pool := &anonymousPool{cooldown: cooldown, enabled: enabled, transports: transports, prefixes: append([]string(nil), prefixes...)}
	if !enabled || transports == nil {
		return pool
	}
	pool.nodes = make([]*anonymousNode, 0, len(transports.items))
	// Proxy lanes lead: multiplexed exits carry independent reputation and
	// are the only anonymous egress the free tier currently serves.
	for _, egress := range transports.items {
		if egress.proxy != nil {
			pool.nodes = append(pool.nodes, &anonymousNode{egress: egress})
		}
	}
	if transports.proxies != nil {
		// When the proxy pool is configured, the direct datacenter lanes stay
		// in the shared transport pool for keyed traffic but never enter the
		// anonymous rotation. The free tier rejects those prefixes wholesale,
		// so adding them would only burn scan attempts (and can expose the
		// upstream "abusive traffic" 403). The hard park remains the safety
		// net for deployments that run without a proxy pool.
		return pool
	}
	// Without a proxy pool, address lanes are the only anonymous egress.
	for _, egress := range transports.items {
		if egress.proxy == nil {
			pool.nodes = append(pool.nodes, &anonymousNode{egress: egress})
		}
	}
	return pool
}

func (p *anonymousPool) Len() int {
	if p == nil {
		return 0
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.nodes)
}

func (p *anonymousPool) CursorFor(affinity string) anonymousCursor {
	if p == nil {
		return anonymousCursor{pool: p}
	}
	p.mu.RLock()
	nodes := append([]*anonymousNode(nil), p.nodes...)
	recent := append([]*anonymousNode(nil), p.recent...)
	p.mu.RUnlock()
	if len(nodes) == 0 {
		return anonymousCursor{pool: p}
	}
	// The scan leads with the pool's most recent successes — the free tier
	// is per-IP quota-bound, and a request that opens on a lane the pool
	// has not just heard from pays attempt after attempt rediscovering a
	// working egress — then walks the fate-interleaved order: one class per
	// configured prefix, plus one shared by the multiplexed proxy exits, so
	// a flag or outage that covers one class cannot swallow a whole
	// request's attempts. The affinity-chosen lane stays first among the
	// rest, so prompt caches and per-session placement keep working; every
	// lane still appears exactly once.
	return anonymousCursor{pool: p, nodes: interleaveByFate(p, nodes, startForAffinity(&p.next, affinity, len(nodes)), recent)}
}

// isWarpLane reports whether a lane is a local Cloudflare WARP exit. WARP
// exits are multiplexed separately from the public chk proxies and have a
// much better anonymous success rate, so the cursor leads with one.
func isWarpLane(node *anonymousNode) bool {
	return node != nil && node.egress != nil && node.egress.proxy != nil &&
		strings.HasPrefix(node.egress.proxy.Tag, "cf-warp-")
}

// interleaveByFate reorders one cursor snapshot: one WARP lane first, then
// the given lead lanes (the pool's recent successes, most recent first), then
// the affinity-chosen lane, then the remaining lanes round-robin by fate class.
// With the pool's typical composition (many address lanes behind one
// tunnel, a handful of independent proxy exits) a contiguous walk could
// spend every attempt of a request inside a single blocked prefix; the
// interleaved order instead samples another prefix or a proxy node within
// its first few attempts. Every lane appears exactly once.
func interleaveByFate(pool *anonymousPool, nodes []*anonymousNode, start int, lead []*anonymousNode) []*anonymousNode {
	size := len(nodes)
	if size < 2 {
		return nodes
	}
	// WARP exits are the healthiest anonymous egress class in production
	// (82% 2xx over 24h versus 42% for public chk exits). Start the fate
	// round-robin on a WARP lane so the scan tries it first and then keeps
	// alternating classes; starting on a chk lane and moving one WARP lane to
	// the front would leave two chk lanes back to back. The cursor still skips
	// the WARP lane when it is cooling or parked.
	for index, node := range nodes {
		if isWarpLane(node) {
			start = index
			break
		}
	}
	ordered := interleaveOrder(pool, nodes, start)
	out := make([]*anonymousNode, 0, size)
	seen := make(map[*anonymousNode]bool, len(nodes))
	if len(ordered) > 0 && isWarpLane(ordered[0]) {
		seen[ordered[0]] = true
		out = append(out, ordered[0])
	}
	// Recent successes follow. They keep a request on an exit that answered
	// lately without letting the lead displace the WARP head start.
	for _, node := range lead {
		if node == nil || seen[node] {
			continue
		}
		seen[node] = true
		out = append(out, node)
	}
	for _, node := range ordered {
		if !seen[node] {
			out = append(out, node)
		}
	}
	return out
}

// interleaveOrder is the fate-class round-robin behind interleaveByFate.
func interleaveOrder(pool *anonymousPool, nodes []*anonymousNode, start int) []*anonymousNode {
	size := len(nodes)
	if size < 2 {
		return nodes
	}
	if start < 0 || start >= size {
		start = 0
	}
	first := nodes[start]
	order := make([]string, 0, 8)
	buckets := make(map[string][]*anonymousNode, 8)
	for offset := 1; offset < size; offset++ {
		node := nodes[(start+offset)%size]
		key := pool.fateKey(node.egress)
		if _, seen := buckets[key]; !seen {
			order = append(order, key)
		}
		buckets[key] = append(buckets[key], node)
	}
	out := make([]*anonymousNode, 0, size)
	out = append(out, first)
	// The leading lane's class moves to the end of the round-robin: the
	// attempts that follow sample the other classes first, so a flag over
	// the leading lane's class cannot shadow them in the first round.
	firstKey := pool.fateKey(first.egress)
	lead := -1
	for i, key := range order {
		if key == firstKey {
			lead = i
			break
		}
	}
	if lead >= 0 {
		order = append(order[lead+1:], order[:lead+1]...)
	} else {
		order = append(order, firstKey)
	}
	// Classes the gate has cooled from repeated FreeTier verdicts drain last.
	// The flag covers a prefix as a unit, so its sibling lanes would only pay
	// 403s ahead of the independent classes; keeping them reachable (rather
	// than skipping them) preserves capacity when a cooldown is stale. Within
	// each phase the order stays round-robin, so no single class can swallow
	// the scan.
	live := make([]string, 0, len(order))
	cooling := make([]string, 0, len(order))
	for _, key := range order {
		if pool != nil && pool.transports != nil && pool.transports.prefixCooled(key) {
			cooling = append(cooling, key)
			continue
		}
		live = append(live, key)
	}
	for _, phase := range [][]string{live, cooling} {
		for len(out) < size {
			progress := false
			for _, key := range phase {
				bucket := buckets[key]
				if len(bucket) == 0 {
					continue
				}
				out = append(out, bucket[0])
				buckets[key] = bucket[1:]
				progress = true
			}
			if !progress {
				break
			}
		}
	}
	return out
}

// fateKey names the upstream fate a lane shares with its siblings: the
// configured prefix for address lanes, one shared class for every
// multiplexed proxy exit (they are individually addressed, but jointly a
// separate fate from the tunnel prefixes), and "direct" for everything else.
// Nil-safe: a nil pool has no prefixes and a nil lane matches nothing.
func (p *anonymousPool) fateKey(egress *egressTransport) string {
	if egress == nil {
		return "none"
	}
	if egress.proxy != nil {
		// WARP exits are a distinct fate from public proxies: the two
		// classes fail independently, so interleave them rather than
		// treating every proxy as one bucket.
		if strings.HasPrefix(egress.proxy.Tag, "cf-warp-") {
			return "warp"
		}
		return "proxy"
	}
	if p != nil {
		if key := config.PrefixKeyFor(p.prefixes, egress.address); key != "" {
			return key
		}
	}
	return "direct"
}

// prefixCooling reports whether the configured prefix holding the lane is
// cooling from repeated FreeTier verdicts. Proxy lanes and addresses outside
// the configured prefixes never cool: their fate is their own exit.
func (p *anonymousPool) prefixCooling(egress *egressTransport) bool {
	if p == nil || p.transports == nil || egress == nil {
		return false
	}
	key := config.PrefixKeyFor(p.prefixes, egress.address)
	if key == "" {
		return false
	}
	return p.transports.prefixCooled(key)
}

// directClassParked reports whether the node belongs to the hard-parked
// direct egress class. The park covers every non-proxy lane, so the cursor
// drops them from the scan instead of deferring them: unlike a prefix
// cooldown, this is a class-wide verdict, and retrying a sibling only buys
// another upstream rejection.
func (c *anonymousCursor) directClassParked(node *anonymousNode) bool {
	return node != nil && node.egress != nil && node.egress.proxy == nil &&
		c.pool != nil && c.pool.transports != nil && c.pool.transports.DirectEgressParked()
}

// Next visits each healthy, non-cooling, non-parked egress lane at most once
// per cursor. Lanes of a prefix cooling from repeated FreeTier verdicts are
// deferred to the end of the scan: the flag covers the prefix as a unit, so
// its sibling lanes would only pay another 403 ahead of independent classes,
// but they stay reachable in case the whole pool is cooling.
func (c *anonymousCursor) Next() *anonymousNode {
	if len(c.nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	for c.offset < len(c.nodes) {
		node := c.nodes[(c.start+c.offset)%len(c.nodes)]
		c.offset++
		if !node.egress.healthy.Load() || node.cooldownUntil.Load() > now || node.egress.freitierUntil.Load() > now {
			continue
		}
		if c.directClassParked(node) {
			continue
		}
		if c.pool != nil && c.pool.prefixCooling(node.egress) {
			c.deferred = append(c.deferred, node)
			continue
		}
		return node
	}
	for len(c.deferred) > 0 {
		node := c.deferred[0]
		c.deferred = c.deferred[1:]
		if node == nil || !node.egress.healthy.Load() || node.cooldownUntil.Load() > now || node.egress.freitierUntil.Load() > now {
			continue
		}
		if c.directClassParked(node) {
			continue
		}
		return node
	}
	return nil
}

func (p *anonymousPool) MarkSuccess(node *anonymousNode) {
	if node == nil {
		return
	}
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
	node.total429s.Store(0)
	if node.egress != nil {
		// A served request proves the exit is not flagged: reset the FreeTier
		// park escalation so a later flag starts at the short base again.
		node.egress.freitierParks.Store(0)
	}
	if p == nil {
		return
	}
	// Remember the lane as the most recent success: the next scans lead
	// with it, so a request opens on an egress the pool just proved works
	// instead of paying another flagged lane's 403. The list is a bounded
	// MRU — re-marking a lane moves it to the head without duplicating it.
	p.mu.Lock()
	next := make([]*anonymousNode, 0, recentSuccessLimit)
	next = append(next, node)
	for _, seen := range p.recent {
		if seen == node || len(next) >= recentSuccessLimit {
			continue
		}
		next = append(next, seen)
	}
	p.recent = next
	p.mu.Unlock()
}

func (p *anonymousPool) MarkFailure(node *anonymousNode, resp *http.Response, err error) {
	if node == nil {
		return
	}
	if !cooldownWorthy(resp, err) {
		return
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		storeCooldownDelay(&node.cooldownUntil, cooldown429For(node.total429s.Add(1)), retryAfterOf(resp))
		node.failures.Add(1)
		return
	}
	storeCooldownUntil(&node.cooldownUntil, p.cooldown, node.failures.Add(1), retryAfterOf(resp))
}

// anonymousBreakerFailures is the number of consecutive no-response
// transport failures across requests that opens the anonymous egress
// breaker. A live lane answers with response headers in well under a second;
// only a dead egress path (hanging connect or TLS, or an exit dropping
// requests before any byte) produces a run of attempts with no response at
// all. Without the breaker those attempts consume the whole request budget
// and the client sees a timeout instead of a retryable pool outage.
const anonymousBreakerFailures = 3

// anonymousBreakerCooldown is how long new anonymous requests fail fast once
// the breaker opens. It is deliberately short: a recovering egress path
// should not stay unserved, and the half-open probe needs three fresh
// failures, so a stale counter cannot reopen it. The tradeoff of opening
// wrongly is a few requests answered with the retryable 503 instead of
// waiting on a slow but live lane.
const anonymousBreakerCooldown = 10 * time.Second

// AllowAttempt reports whether the anonymous egress path may start a new
// upstream attempt. False means the breaker is open from a run of
// transport-level failures with no response.
func (p *anonymousPool) AllowAttempt() bool {
	if p == nil {
		return true
	}
	until := p.breakerUntil.Load()
	return until == 0 || time.Now().UnixNano() >= until
}

// BreakerRemaining reports how long the breaker stays open, or zero when it
// is closed. It lets a request wait out the cooldown before probing instead
// of failing immediately with zero attempts.
func (p *anonymousPool) BreakerRemaining() time.Duration {
	if p == nil {
		return 0
	}
	until := p.breakerUntil.Load()
	if until == 0 {
		return 0
	}
	remaining := time.Until(time.Unix(0, until))
	if remaining < 0 {
		return 0
	}
	return remaining
}

// RecordTransportFailure counts a no-response transport failure. The third
// consecutive failure opens the breaker for anonymousBreakerCooldown and
// resets the counter, so the next probe window needs three fresh failures
// rather than inheriting the previous run.
func (p *anonymousPool) RecordTransportFailure() {
	if p == nil {
		return
	}
	if p.breakerFailures.Add(1) < anonymousBreakerFailures {
		return
	}
	p.breakerFailures.Store(0)
	p.breakerUntil.Store(time.Now().Add(anonymousBreakerCooldown).UnixNano())
}

// RecordTransportSuccess clears the failure streak and any open breaker: a
// served lane proves the egress path works.
func (p *anonymousPool) RecordTransportSuccess() {
	if p == nil {
		return
	}
	p.breakerFailures.Store(0)
	p.breakerUntil.Store(0)
}

// recentSuccessLimit bounds the anonymous pool's recent-success memory: the
// head lanes lead every scan, the oldest is dropped. Large enough to spread
// a burst over several exits instead of hammering one IP's quota, small
// enough that a scan left without a winner still reaches the interleaved
// full-pool order quickly.
const recentSuccessLimit = 6

// freitierCooldown parks a FreeTier-flagged exit while the upstream flag
// clears. It mirrors zen-proxy's mark_freitier_blocked: per-lane only, never
// subnet-wide, health and failure counters untouched.
const freitierCooldown = 30 * time.Minute

// directEgressHardPark is the park applied to the whole non-proxy lane class
// when the free tier answers it with a FreeTier/abuse verdict. It is far
// longer than the per-lane freitierCooldown because the verdict describes the
// host's datacenter prefixes, not one address: the flag has to clear upstream
// before any sibling /64 will serve again.
const directEgressHardPark = 12 * time.Hour

// freitierParkBase is the first FreeTier park applied to a lane. Repeated
// parks with no success in between double it up to freitierCooldown. A flat
// freitierCooldown let a single flag wave bench every current exit for the
// full window, which starved anonymous selection and produced a retryable
// 503 for the whole period; a short first park leaves room for a transient
// wave to pass, and the streak still keeps genuinely blocked exits out of
// rotation.
const freitierParkBase = 3 * time.Minute

// freitierParkFor returns the escalating park for the n-th consecutive park
// on a lane (n >= 1): freitierParkBase * 2^min(n-1, 4), capped at
// freitierCooldown. The streak resets on any success or restored park, so a
// verdict that described the request rather than the exit does not escalate.
func freitierParkFor(parks int32) time.Duration {
	if parks < 1 {
		parks = 1
	}
	shift := min(parks-1, 4)
	delay := freitierParkBase
	for range shift {
		delay *= 2
	}
	return min(delay, freitierCooldown)
}

// Prefix-level cooling: one FreeTier 403 parks a single lane, but a prefix
// whose lanes keep answering FreeTier is blocked upstream as a unit — a
// rotated /48 that had been used for abuse answered 403 FreeTierError from
// every /64 tried while sibling routed /64s on the same tunnel answered 200.
// After prefixFreeTierStrikeLimit strikes with no success in between, the
// whole prefix leaves replenishment sampling for prefixFreeTierCooldown;
// expiry (or a successful fallback lane) re-admits it.
const (
	prefixFreeTierStrikeLimit = 2
	prefixFreeTierCooldown    = 15 * time.Minute
)

// ParkFreitier parks the lane after an upstream FreeTier 403 and grows a
// fresh replacement lane on an unused /64, keeping the pool's selectable
// count stable through a flag storm. The lane keeps its health and failure
// counters; it simply sits out selection until the park expires. Growth is
// capped (see maxPoolLanes): at the cap the park stands alone and selection
// degrades gracefully as before. prefixes threads the gateway's configured
// egress prefixes through; an empty list disables regrowth but never the
// park itself.
func (p *anonymousPool) ParkFreitier(egress *egressTransport) {
	if egress == nil {
		return
	}
	egress.totalFreitiers.Add(1)
	egress.freitierUntil.Store(time.Now().Add(freitierParkFor(egress.freitierParks.Add(1))).UnixNano())
	p.maybeReplenish(egress)
}

// UnparkFreitier returns a lane parked on a FreeTier verdict to selection.
// The request path restores parks when the verdict is request-scoped (see
// gateway.doAnonymousUpstream): the gate kept refusing a request its own
// canonical profile showed it serves, so the exits were never the problem,
// and benching them would only shrink capacity for every later request.
func (p *anonymousPool) UnparkFreitier(egress *egressTransport) {
	if egress == nil {
		return
	}
	// The verdict was request-scoped, so it is not evidence against the exit:
	// clear the park and the escalation streak with it.
	egress.freitierParks.Store(0)
	egress.freitierUntil.Store(0)
}

// replenishPrefixes returns the configured egress CIDRs available for
// replenishment sampling. FreeTier-cooled prefixes are held back so a blocked
// prefix does not keep minting doomed lanes; the healthy list falls back to
// the full set when every prefix is cooling, so recovery stays possible.
// Nil-safe for a nil pool.
func (p *anonymousPool) replenishPrefixes() []string {
	if p == nil || p.transports == nil {
		return nil
	}
	return p.transports.HealthyPrefixes(p.prefixes)
}

// maybeReplenish appends one fresh node+lane when the pool is below cap. It
// runs on the request path (a FreeTier park just fired), so sampling failure
// is silent: the park still holds, and the next park retries regrowth. A
// parked proxy lane draws its replacement from the verified node set instead
// of sampling an IPv6 address.
func (p *anonymousPool) maybeReplenish(parked *egressTransport) {
	if p == nil || p.transports == nil {
		return
	}
	if parked != nil && parked.proxy == nil && p.transports.DirectEgressParked() {
		// The direct class is benched as a unit; minting a fresh prefix lane
		// would only add an unselectable node.
		return
	}
	var lane *egressTransport
	if parked != nil && parked.proxy != nil {
		lane = p.transports.replenishProxyLane(parked)
	} else {
		prefixes := p.replenishPrefixes()
		if len(prefixes) == 0 {
			return
		}
		lane = p.transports.replenishLane(prefixes, parked)
	}
	if lane == nil {
		return
	}
	p.addNode(lane)
}

// addNode appends one lane to rotation. Called on the request path (a park
// just fired) and by the proxy fill loop, so it is lock-guarded and does no
// I/O.
func (p *anonymousPool) addNode(lane *egressTransport) {
	if p == nil || lane == nil || !p.enabled {
		return
	}
	p.mu.Lock()
	p.nodes = append(p.nodes, &anonymousNode{egress: lane})
	p.mu.Unlock()
}

// freitierParked reports whether the lane is currently parked by a FreeTier
// flag. Nil-safe: a nil lane is never selectable anyway.
func freitierParked(egress *egressTransport) bool {
	return egress != nil && egress.freitierUntil.Load() > time.Now().UnixNano()
}

// dedicatedLane reports whether the lane egresses through a pinned source
// address — an address drawn from a configured egress prefix — rather than
// the host's own route ("direct"/"direct4") or a proxy exit. Dedicated lanes
// are the preferred home for keys: each draw carries its own network
// identity upstream, so accounts spread across them answer from distinct
// addresses instead of stacking behind one.
func dedicatedLane(egress *egressTransport) bool {
	if egress == nil || egress.proxy != nil {
		return false
	}
	return egress.address != "direct" && egress.address != "direct4"
}

// directLane reports whether the lane dials from the host's own address.
// That lane is the worst home for a keyed account: every account it carries
// answers upstream from the same host address, which is what providers flag
// as many accounts sharing one network address.
func directLane(egress *egressTransport) bool {
	return egress != nil && egress.proxy == nil && (egress.address == "direct" || egress.address == "direct4")
}

// CoolSubnet cools the /64 holding addr for d. It mirrors zen-proxy's
// subnet-level region handling: the flag is corroborated upstream by the
// caller deciding to cool, and the map stays small because keys are /64s of
// the configured prefixes.
func (p *transportPool) CoolSubnet(addr string, d time.Duration) {
	if p == nil || addr == "" || d <= 0 {
		return
	}
	ip := net.ParseIP(addr)
	if ip == nil {
		return
	}
	key := ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
	p.subnetMu.Lock()
	if p.subnetCooldowns == nil {
		p.subnetCooldowns = make(map[string]int64)
	}
	p.subnetCooldowns[key] = time.Now().Add(d).UnixNano()
	p.subnetMu.Unlock()
}

// subnetCooled reports whether addr's /64 is currently cooled. Direct and
// non-IP lanes never cool.
func (p *transportPool) subnetCooled(addr string) bool {
	if p == nil || addr == "" {
		return false
	}
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}
	key := ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
	p.subnetMu.Lock()
	until, ok := p.subnetCooldowns[key]
	p.subnetMu.Unlock()
	return ok && until > time.Now().UnixNano()
}

// pruneSubnetCooldowns drops expired entries so the map cannot grow past the
// subnets ever cooled. Callers run this on the rare region path, not hot.
func (p *transportPool) pruneSubnetCooldowns() {
	if p == nil {
		return
	}
	now := time.Now().UnixNano()
	p.subnetMu.Lock()
	for key, until := range p.subnetCooldowns {
		if until <= now {
			delete(p.subnetCooldowns, key)
		}
	}
	p.subnetMu.Unlock()
}

// StrikePrefix attributes a FreeTier 403 to the configured prefix holding
// addr. It returns the prefix key (empty when addr maps to none), the running
// strike count, and whether the prefix is cooling as of this verdict — either
// this strike started the cooldown or one was already running.
func (p *transportPool) StrikePrefix(addr string) (string, int, bool) {
	if p == nil {
		return "", 0, false
	}
	key := config.PrefixKeyFor(p.prefixes, addr)
	if key == "" {
		return "", 0, false
	}
	now := time.Now().UnixNano()
	p.prefixMu.Lock()
	defer p.prefixMu.Unlock()
	if p.prefixCooling[key] > now {
		return key, p.prefixStrikes[key], true
	}
	if p.prefixStrikes == nil {
		p.prefixStrikes = make(map[string]int)
	}
	if p.prefixCooling == nil {
		p.prefixCooling = make(map[string]int64)
	}
	p.prefixStrikes[key]++
	if p.prefixStrikes[key] >= prefixFreeTierStrikeLimit {
		delete(p.prefixStrikes, key)
		p.prefixCooling[key] = now + int64(prefixFreeTierCooldown)
		return key, prefixFreeTierStrikeLimit, true
	}
	return key, p.prefixStrikes[key], false
}

// reCoolPrefix restarts a cooled prefix's cooldown window. The probation
// probe calls it when a re-tested prefix is still answering FreeTier, so the
// prefix stays out of sampling for another full cooldown instead of being
// retried on every probation tick.
func (p *transportPool) reCoolPrefix(key string) {
	if p == nil || key == "" {
		return
	}
	p.prefixMu.Lock()
	if p.prefixCooling == nil {
		p.prefixCooling = make(map[string]int64)
	}
	p.prefixCooling[key] = time.Now().Add(prefixFreeTierCooldown).UnixNano()
	delete(p.prefixStrikes, key)
	p.prefixMu.Unlock()
}

// CoolingPrefixes returns the prefixes currently tracked as FreeTier-cooled
// together with the end of their cooldown. The set is bounded by the
// configured prefix count.
func (p *transportPool) CoolingPrefixes() map[string]time.Time {
	if p == nil {
		return nil
	}
	p.prefixMu.Lock()
	defer p.prefixMu.Unlock()
	out := make(map[string]time.Time, len(p.prefixCooling))
	for key, until := range p.prefixCooling {
		if until > 0 {
			out[key] = time.Unix(0, until)
		}
	}
	return out
}

// ParkDirectEgress hard-parks every non-proxy lane for d. Direct lanes are
// the host's own route and addresses sampled from the configured datacenter
// prefixes; the free tier flags those ranges as abusive as a class, so one
// verdict benches all of them instead of letting each sibling pay the same
// 403. Proxy lanes are unaffected: their exits carry independent reputation
// and keep serving anonymous traffic.
func (p *transportPool) ParkDirectEgress(d time.Duration) {
	if p == nil || d <= 0 {
		return
	}
	p.directParkUntil.Store(time.Now().Add(d).UnixNano())
}

// DirectEgressParked reports whether the non-proxy lane class is hard-parked.
func (p *transportPool) DirectEgressParked() bool {
	return p != nil && p.directParkUntil.Load() > time.Now().UnixNano()
}

// prefixCooled reports whether the canonical prefix key is cooling from
// repeated FreeTier verdicts. Selection drains a blocked prefix's siblings
// after the independent fate classes (see interleaveOrder); probation
// re-admits the prefix once its cooldown expires (see probeCoolingPrefixes).
func (p *transportPool) prefixCooled(key string) bool {
	if p == nil || key == "" {
		return false
	}
	now := time.Now().UnixNano()
	p.prefixMu.Lock()
	defer p.prefixMu.Unlock()
	return p.prefixCooling[key] > now
}

// ClearPrefix retires any strikes and cooldown for the prefix holding addr:
// the prefix just served a request, so it is not blocked. Callers pass the
// address of the lane that produced the 2xx; a nil pool or an address outside
// the configured prefixes is a no-op.
func (p *transportPool) ClearPrefix(addr string) {
	if p == nil {
		return
	}
	key := config.PrefixKeyFor(p.prefixes, addr)
	if key == "" {
		return
	}
	p.prefixMu.Lock()
	delete(p.prefixStrikes, key)
	delete(p.prefixCooling, key)
	p.prefixMu.Unlock()
}

// HealthyPrefixes filters prefixes down to those not cooling from FreeTier
// verdicts. An empty result falls back to the full list: sampling must keep
// drawing somewhere for a cooled prefix to get a chance to prove itself (a
// successful request clears the cooldown), otherwise a total outage would
// never recover.
func (p *transportPool) HealthyPrefixes(prefixes []string) []string {
	if p == nil || len(prefixes) == 0 {
		return prefixes
	}
	now := time.Now().UnixNano()
	p.prefixMu.Lock()
	defer p.prefixMu.Unlock()
	healthy := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		key := config.CanonicalizePrefix(prefix)
		if key == "" || p.prefixCooling[key] <= now {
			healthy = append(healthy, prefix)
		}
	}
	if len(healthy) == 0 {
		return prefixes
	}
	return healthy
}

func (p *transportPool) hasHealthy() bool {
	for _, egress := range p.snapshot() {
		if egress.healthy.Load() {
			return true
		}
	}
	return false
}

// selectableCounts reports pool totals split the way operators think about
// an outage: transport-healthy lanes vs lanes parked by a FreeTier flag.
// cooling counts anonymous-lane failure backoffs and needs the node list the
// caller already holds; passing it in keeps this method free of a back-pointer.
func (p *transportPool) selectableCounts(nodes []*anonymousNode) (total, healthy, parked, cooling int) {
	snap := p.snapshot()
	now := time.Now().UnixNano()
	directParked := p.DirectEgressParked()
	for _, egress := range snap {
		if egress.healthy.Load() {
			healthy++
		}
		if egress.freitierUntil.Load() > now || (directParked && egress.proxy == nil) {
			parked++
		}
	}
	for _, node := range nodes {
		if node != nil && node.cooldownUntil.Load() > now {
			cooling++
		}
	}
	return len(snap), healthy, parked, cooling
}

// snapshotNodes returns a point-in-time copy of the node list for status
// views. Selection cursors carry their own snapshot (see CursorFor), but
// longer-lived readers must not observe a mid-append slice.
func (p *anonymousPool) snapshotNodes() []*anonymousNode {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]*anonymousNode(nil), p.nodes...)
}

// newTransportPool builds one HTTP transport per egress address. Idle
// connection budgets are divided across the pool so the configured totals
// stay global instead of multiplying per lane; a zero MaxConnsPerHost keeps
// its unlimited meaning.
func newTransportPool(addresses []string, cfg config.PerformanceConfig, responseHeaderTimeout time.Duration, prefixes []string, weights map[string]int) (*transportPool, error) {
	if len(addresses) == 0 {
		return nil, errors.New("at least one egress address is required")
	}
	weightCopy := make(map[string]int, len(weights))
	for prefix, weight := range weights {
		weightCopy[prefix] = weight
	}
	p := &transportPool{
		items:         make([]*egressTransport, 0, len(addresses)),
		perf:          cfg,
		headerTimeout: responseHeaderTimeout,
		prefixes:      append([]string(nil), prefixes...),
		weights:       weightCopy,
		maxLanes:      maxPoolLanes,
	}
	for _, address := range addresses {
		egress, err := p.buildLane(len(p.items), address)
		if err != nil {
			return nil, err
		}
		p.items = append(p.items, egress)
	}
	return p, nil
}

// buildLane constructs one bound lane with the pool's shared budgets. It is
// the single construction site for both the initial pool and dynamic
// replacements, so per-lane timeouts and idle budgets cannot drift apart.
func (p *transportPool) buildLane(index int, address string) (*egressTransport, error) {
	lanes := max(len(p.items)+1, 1)
	idle := max(p.perf.MaxIdleConns/lanes, 1)
	idlePerHost := max(p.perf.MaxIdleConnsPerHost/lanes, 1)
	maxPerHost := p.perf.MaxConnsPerHost
	if maxPerHost > 0 {
		maxPerHost = max(maxPerHost/lanes, 1)
	}
	dialer, err := egressDialer(address, p.perf)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = idle
	transport.MaxIdleConnsPerHost = idlePerHost
	transport.MaxConnsPerHost = maxPerHost
	transport.IdleConnTimeout = time.Duration(p.perf.IdleConnTimeoutSeconds) * time.Second
	transport.ResponseHeaderTimeout = p.headerTimeout
	transport.ForceAttemptHTTP2 = true
	if address == "direct4" {
		// Force the IPv4 family: the kernel would otherwise prefer the (for
		// the free tier, blocked) IPv6 route, and a connect that succeeds at
		// the TCP layer never falls back to IPv4.
		transport.DialContext = func(ctx context.Context, _, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp4", addr)
		}
	} else {
		transport.DialContext = dialer.DialContext
	}
	egress := &egressTransport{index: index, address: address, client: &http.Client{Transport: transport}}
	egress.healthy.Store(true)
	return egress, nil
}

// replenishLane samples one fresh address on an unused /64 and appends it
// as a new lane, returning the lane for immediate use. It is called when a
// lane is parked by a FreeTier flag: the replacement keeps the pool's
// selectable count stable instead of shrinking toward zero under a flag
// storm. Growth is capped at maxLanes; at the cap the park stands alone.
// Replacement lanes start healthy and warm on demand like any other lane.
// Sampling honors the pool's prefix weights so replacements keep the
// primary/reserve mix instead of drifting toward a uniform draw.
func (p *transportPool) replenishLane(prefixes []string, parked *egressTransport) *egressTransport {
	if p == nil || len(prefixes) == 0 {
		return nil
	}
	p.replenishMu.Lock()
	defer p.replenishMu.Unlock()
	if p.maxLanes > 0 && len(p.items) >= p.maxLanes {
		return nil
	}
	exclude := make(map[string]struct{}, len(p.items)+1)
	for _, egress := range p.items {
		if key := config.SubnetKeyFor(prefixes, egress.address); key != "" {
			exclude[key] = struct{}{}
		}
	}
	if parked != nil {
		if key := config.SubnetKeyFor(prefixes, parked.address); key != "" {
			exclude[key] = struct{}{}
		}
	}
	addrs, err := config.SampleFreshEgress(prefixes, p.weights, 1, exclude)
	if err != nil || len(addrs) == 0 {
		return nil
	}
	egress, err := p.buildLane(len(p.items), addrs[0])
	if err != nil {
		return nil
	}
	p.items = append(p.items, egress)
	return egress
}

// replenishProxyLane replaces a parked proxy lane with a fresh verified node
// from the proxy manager. The parked lane's node stays bound to it (it comes
// back when the park expires, and two lanes must never share one exit IP),
// so the replacement is a genuinely new node. When none is verified ready
// the manager is nudged to sweep; sampling failure is silent like the
// address path, and the next park retries.
func (p *transportPool) replenishProxyLane(parked *egressTransport) *egressTransport {
	if p == nil || p.proxies == nil {
		return nil
	}
	_ = parked
	node, ok := p.proxies.take()
	if !ok {
		p.proxies.requestVerify()
		return nil
	}
	lane, err := p.addProxyLane(node)
	if err != nil {
		p.proxies.release(node.Index)
		return nil
	}
	return lane
}

// addProxyLane builds and appends one proxy lane under the replenish lock,
// enforcing the shared lane cap so address and proxy growth cannot overshoot
// it together.
func (p *transportPool) addProxyLane(node proxypool.Node) (*egressTransport, error) {
	if p == nil {
		return nil, errors.New("no transport pool")
	}
	p.replenishMu.Lock()
	defer p.replenishMu.Unlock()
	if p.maxLanes > 0 && len(p.items) >= p.maxLanes {
		return nil, fmt.Errorf("egress pool is at its %d lane cap", p.maxLanes)
	}
	lane, err := p.buildProxyLane(len(p.items), node)
	if err != nil {
		return nil, err
	}
	p.items = append(p.items, lane)
	return lane, nil
}

// proxyLaneCount reports how many live lanes rotate proxy exits.
func (p *transportPool) proxyLaneCount() int {
	if p == nil {
		return 0
	}
	count := 0
	for _, egress := range p.snapshot() {
		if egress.proxy != nil {
			count++
		}
	}
	return count
}

// buildProxyLane constructs one multiplexed proxy lane with the pool's
// shared budgets, mirroring buildLane so proxy and address lanes stay
// uniform. The dialer reaches the loopback SOCKS5 endpoint as the node's
// user; the node's exit IP is what the upstream sees.
func (p *transportPool) buildProxyLane(index int, node proxypool.Node) (*egressTransport, error) {
	if p.proxies == nil {
		return nil, errors.New("proxy pool is not configured")
	}
	lanes := max(len(p.items)+1, 1)
	idle := max(p.perf.MaxIdleConns/lanes, 1)
	idlePerHost := max(p.perf.MaxIdleConnsPerHost/lanes, 1)
	maxPerHost := p.perf.MaxConnsPerHost
	if maxPerHost > 0 {
		maxPerHost = max(maxPerHost/lanes, 1)
	}
	dialer := &proxypool.Dialer{
		Addr:     p.proxies.socksAddr(),
		User:     node.User,
		Password: proxypool.ProxyPassword,
		Timeout:  time.Duration(p.perf.ConnectTimeoutSeconds) * time.Second,
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = idle
	transport.MaxIdleConnsPerHost = idlePerHost
	transport.MaxConnsPerHost = maxPerHost
	transport.IdleConnTimeout = time.Duration(p.perf.IdleConnTimeoutSeconds) * time.Second
	transport.ResponseHeaderTimeout = p.headerTimeout
	transport.ForceAttemptHTTP2 = true
	transport.DialContext = dialer.DialContext
	nodeBinding := node
	egress := &egressTransport{
		index:   index,
		address: fmt.Sprintf("proxy:%d", node.Index),
		client:  &http.Client{Transport: transport},
		proxy:   &nodeBinding,
	}
	egress.healthy.Store(true)
	return egress, nil
}

// snapshot returns a point-in-time copy of the lane list for iteration.
// Readers (health loops, capability refresh, status views, cursors) must use
// a snapshot instead of ranging p.items: the pool grows by replacement, and
// ranging a slice while it appends races and skews index math. Snapshots
// share the lane pointers, so health and park state stay live.
func (p *transportPool) snapshot() []*egressTransport {
	if p == nil {
		return nil
	}
	p.replenishMu.Lock()
	defer p.replenishMu.Unlock()
	return append([]*egressTransport(nil), p.items...)
}

// egressDialer binds a lane to its source address. "direct" and "direct4"
// leave the local address unset so the kernel picks the route default (the
// caller forces the family for "direct4"); any other value must be an IP
// literal assigned (or routed) on this host, otherwise the first dial fails
// and health tracking takes the lane out of rotation.
func egressDialer(address string, cfg config.PerformanceConfig) (*net.Dialer, error) {
	dialer := &net.Dialer{
		Timeout:   time.Duration(cfg.ConnectTimeoutSeconds) * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if address == "direct" || address == "direct4" {
		return dialer, nil
	}
	ip := net.ParseIP(strings.TrimSpace(address))
	if ip == nil {
		return nil, fmt.Errorf("invalid egress address %q: must be an IP literal or \"direct\"", address)
	}
	dialer.LocalAddr = &net.TCPAddr{IP: ip}
	return dialer, nil
}

type egressHealthResult struct {
	egress     *egressTransport
	err        error
	failed     bool
	wasHealthy bool
}

// CheckHealth concurrently rechecks only lanes already marked unhealthy.
// Healthy lanes are skipped before a check is claimed. Any HTTP response
// from the test URL proves that the route is reachable; only a network-level
// failure keeps the lane unhealthy.
func (p *transportPool) CheckHealth(ctx context.Context, target string, timeout time.Duration) []egressHealthResult {
	items := p.snapshot()
	results := make(chan egressHealthResult, len(items))
	checks := 0
	for _, egress := range items {
		if egress.healthy.Load() || !egress.checking.CompareAndSwap(false, true) {
			continue
		}
		// A real request may have restored the lane between the first health
		// read and claiming this check.
		if egress.healthy.Load() {
			egress.checking.Store(false)
			continue
		}
		checks++
		go func() {
			results <- p.checkClaimedEgress(ctx, egress, target, timeout)
		}()
	}
	out := make([]egressHealthResult, 0, checks)
	for range checks {
		out = append(out, <-results)
	}
	return out
}

// egressWarmConcurrency bounds the background startup pre-dial. A 128-lane
// pool used to fan out every TLS handshake at once; sixteen at a time keeps
// the restart CPU/network spike small without stretching the warm window.
const egressWarmConcurrency = 16

// warmLanes pre-dials every lane against the health-check target so TLS
// and routing are established before the first client request. Failures
// never fail startup: lanes stay marked healthy and warm on demand via the
// regular health path. The wait is bounded by timeout per lane plus the
// caller's context; no goroutine outlives the return.
func (p *transportPool) warmLanes(ctx context.Context, target string, timeout time.Duration) []egressHealthResult {
	items := p.snapshot()
	results := make(chan egressHealthResult, len(items))
	checks := 0
	sem := make(chan struct{}, egressWarmConcurrency)
	for _, egress := range items {
		if !egress.checking.CompareAndSwap(false, true) {
			continue
		}
		checks++
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			results <- p.checkClaimedEgress(ctx, egress, target, timeout)
		}()
	}
	out := make([]egressHealthResult, 0, checks)
	for range checks {
		result := <-results
		if result.failed {
			// Startup pre-dial is latency-only: a warm miss never evicts
			// a lane. Real traffic and the scheduled checks own health;
			// the lane stays in rotation and warms on demand.
			result.egress.healthy.Store(true)
		}
		out = append(out, result)
	}
	return out
}

// checkClaimedEgress performs a check after the caller has acquired checking.
func (p *transportPool) checkClaimedEgress(ctx context.Context, egress *egressTransport, target string, timeout time.Duration) egressHealthResult {
	defer egress.checking.Store(false)
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, target, nil)
	if err == nil {
		req.Header.Set("User-Agent", httpx.UserAgent())
		resp, requestErr := egress.client.Do(req)
		err = requestErr
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
		}
	}
	result := egressHealthResult{egress: egress, err: err, wasHealthy: egress.healthy.Load()}
	if err == nil {
		result.wasHealthy = egress.healthy.Swap(true)
	} else if isEgressFailure(err) {
		result.failed = true
		result.wasHealthy = egress.healthy.Swap(false)
	}
	return result
}

// isEgressFailure deliberately recognizes only failures that say the egress
// route is unavailable. HTTP responses and unrelated transport/protocol
// errors must not evict a lane. Addresses the host cannot bind (yet) read the
// same as a dead route, and health checks restore the lane once it binds.
func isEgressFailure(err error) bool {
	if errors.Is(err, errFirstEventTimeout) {
		return false // The upstream accepted HTTP; slow generation is not a broken lane.
	}
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EADDRNOTAVAIL) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// upstreamNode keeps a key stable while allowing its egress binding to change
// atomically when a lane becomes unavailable.
type upstreamNode struct {
	key             string
	index           int
	preferredEgress int
	egressIndex     atomic.Int64
	failures        atomic.Uint32
	// total429s counts 429s since the last success on this key and drives
	// the escalating 429 backoff. MarkSuccess clears it so a recovered key
	// starts again at the base delay.
	// 429 backoff. It is never reset by success, mirroring the zen pool.
	total429s     atomic.Uint32
	cooldownUntil atomic.Int64
}

type nodePool struct {
	nodes        []*upstreamNode
	transports   *transportPool
	next         atomic.Uint64
	cooldown     time.Duration
	bindingsMu   sync.Mutex
	bindingCount []int
}

// newNodePool distributes keys over egress lanes in round-robin order. When
// there are fewer keys than lanes, the remaining lanes are intentionally idle
// and can take over a key immediately if an active lane fails.
func newNodePool(keys []string, transports *transportPool, cooldown time.Duration) (*nodePool, error) {
	if transports == nil || len(transports.items) == 0 {
		return nil, fmt.Errorf("at least one egress transport is required")
	}
	pool := &nodePool{
		nodes:        make([]*upstreamNode, 0, len(keys)),
		transports:   transports,
		cooldown:     cooldown,
		bindingCount: make([]int, len(transports.items)),
	}
	if len(keys) == 0 {
		return pool, nil
	}
	for i, key := range keys {
		egressIndex := i % len(transports.items)
		node := &upstreamNode{key: key, index: i, preferredEgress: egressIndex}
		node.egressIndex.Store(int64(egressIndex))
		pool.nodes = append(pool.nodes, node)
		pool.bindingCount[egressIndex]++
	}
	return pool, nil
}

// RestoreEgress moves keys that were originally assigned to a recovered lane
// back to it. Lanes without an original key simply become healthy failover
// candidates again.
func (p *nodePool) RestoreEgress(recoveredEgress int) int {
	if p == nil || p.transports == nil {
		return 0
	}
	n := len(p.transports.snapshot())
	if recoveredEgress < 0 || recoveredEgress >= n {
		return 0
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()
	p.ensureBindingsLocked(n)

	moved := 0
	for _, node := range p.nodes {
		current := int(node.egressIndex.Load())
		if node.preferredEgress != recoveredEgress || current == recoveredEgress {
			continue
		}
		if current >= 0 && current < len(p.bindingCount) {
			p.bindingCount[current]--
		}
		p.bindingCount[recoveredEgress]++
		node.egressIndex.Store(int64(recoveredEgress))
		node.failures.Store(0)
		node.cooldownUntil.Store(0)
		moved++
	}
	return moved
}

func (p *nodePool) Len() int { return len(p.nodes) }

// NodeByID resolves a node by its stable fingerprint. Callers only ever hold
// the fingerprint, never the key value, so the lookup cannot leak a secret.
func (p *nodePool) NodeByID(id string) *upstreamNode {
	if p == nil || id == "" {
		return nil
	}
	for _, node := range p.nodes {
		if config.Fingerprint(node.key) == id {
			return node
		}
	}
	return nil
}

func (p *nodePool) Egress(node *upstreamNode) *egressTransport {
	if p == nil || node == nil || p.transports == nil {
		return nil
	}
	items := p.transports.snapshot()
	index := int(node.egressIndex.Load())
	if index < 0 || index >= len(items) {
		return nil
	}
	return items[index]
}

// egressHome declares which lane class a keyed pool treats as home for its
// keys. Dedicated source-address lanes — one /64 per key — are the default
// home; proxy exits are used whenever no dedicated lane is vacant. Zen sets
// homeDedicatedOnly so no proxy exit ever carries keyed free-tier traffic.
// homeProxyFirst inverts the default order for deployments whose upstream
// gate rejects the configured prefixes as a class while exits keep
// answering.
type egressHome int

const (
	homeDedicated egressHome = iota
	homeProxyFirst
	homeDedicatedOnly
)

// RebindEgress moves every key currently using failedEgress to the
// least-loaded healthy lane. Empty lanes are selected in configuration order;
// otherwise one of the least-loaded lanes is chosen at random. If every
// alternative is currently unhealthy, it still attempts the least-loaded
// alternative. Benched lanes — parked by a FreeTier flag or cooled under a
// region block — are never chosen, since selection would only skip them.
// Candidates follow the pool's home order (see egressHome): homeProxyFirst
// looks for a vacant proxy exit before the first dedicated lane;
// homeDedicatedOnly accepts dedicated lanes only, so a dead lane never hands
// its key to an exit.
func (p *nodePool) RebindEgress(failedEgress int, home egressHome) int {
	if p == nil || p.transports == nil {
		return 0
	}
	n := len(p.transports.snapshot())
	if n < 2 || failedEgress < 0 || failedEgress >= n {
		return 0
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()
	p.ensureBindingsLocked(n)

	moved := 0
	for _, node := range p.nodes {
		if int(node.egressIndex.Load()) != failedEgress {
			continue
		}
		target := -1
		switch home {
		case homeProxyFirst:
			// The pool's working home class is the proxy exits (see
			// RebalanceEgress): prefer a vacant exit over the first
			// dedicated lane, which would answer the next request with
			// the same FreeTier verdict that parked its predecessors.
			target = p.replacementLocked(failedEgress, true, func(e *egressTransport) bool { return e.proxy != nil })
			if target < 0 {
				target = p.replacementLocked(failedEgress, true, nil)
			}
		case homeDedicatedOnly:
			target = p.replacementLocked(failedEgress, true, dedicatedLane)
		default:
			target = p.replacementLocked(failedEgress, true, nil)
		}
		if target < 0 {
			if home == homeDedicatedOnly {
				target = p.replacementLocked(failedEgress, false, dedicatedLane)
			} else {
				target = p.replacementLocked(failedEgress, false, nil)
			}
		}
		if target < 0 {
			continue
		}
		p.bindingCount[failedEgress]--
		p.bindingCount[target]++
		node.egressIndex.Store(int64(target))
		node.failures.Store(0)
		node.cooldownUntil.Store(0)
		moved++
	}
	return moved
}

// RebalanceEgress spreads keys from lanes carrying several of them onto
// empty healthy lanes and gives every key a home of the pool's home class
// (see egressHome): dedicated source-address lanes by default, proxy exits
// when homeProxyFirst is set. A second pass covers the leftover homes: keys
// sitting on a lane parked by a FreeTier flag or cooled under a region block
// move first — a cursor skips such lanes for everything else, so the key
// would sit out traffic entirely — then keys riding the host's own route
// after a failover, then keys on a healthy lane of the non-preferred class
// while a vacant preferred lane waits. homeDedicatedOnly pools never land a
// key on an exit, and never chase a benched key between dedicated lanes
// either: every dedicated lane shares one upstream gate, so hopping to a
// vacant lane would only park a fresh lane with the same verdict. Benched
// lanes are never chosen as a home, and healthy keys only change class
// toward a vacant preferred lane, so each pass makes at most one hop per key
// and two lanes can never trade the same key back and forth. The occupied
// slice marks lanes held by another channel's keys: per-pool emptiness is
// not global emptiness, and a key moved onto a lane that already answers for
// another account would recreate the shared-address problem. Key pools bind
// against the lanes that exist when they are built, while proxy lanes seed
// afterwards and keep arriving until the pool reaches its target, so without
// this pass every account keeps answering from the egress address that
// happened to exist at boot. Moved keys adopt the target as their preferred
// lane so a later lane recovery does not drag them back into the crowd.
func (p *nodePool) RebalanceEgress(occupied []bool, home egressHome) int {
	if p == nil || p.transports == nil || len(p.nodes) == 0 {
		return 0
	}
	items := p.transports.snapshot()
	n := len(items)
	if n < 2 {
		return 0
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()
	p.ensureBindingsLocked(n)

	benched := func(egress *egressTransport) bool {
		return freitierParked(egress) || p.transports.subnetCooled(egress.address)
	}
	empty := func(want func(*egressTransport) bool) int {
		for i := 0; i < n; i++ {
			if i < len(occupied) && occupied[i] {
				continue
			}
			if p.bindingCount[i] != 0 || !items[i].healthy.Load() || benched(items[i]) {
				continue
			}
			if want == nil || want(items[i]) {
				return i
			}
		}
		return -1
	}
	withKeys := func(want func(*egressTransport) bool) int {
		for i := 0; i < n; i++ {
			if p.bindingCount[i] > 0 && (want == nil || want(items[i])) {
				return i
			}
		}
		return -1
	}
	proxyLane := func(egress *egressTransport) bool { return egress.proxy != nil }
	pickEmpty := func(kind int) int {
		// 0: home class only; 1: home class, then the other; 2: any
		// lane. The home class follows the pool's home order (see
		// egressHome). A dedicated-only pool never falls past its home
		// class: no proxy exit may carry one of its keys.
		first, second := dedicatedLane, proxyLane
		if home == homeProxyFirst {
			first, second = proxyLane, dedicatedLane
		}
		if kind < 2 {
			if target := empty(first); target >= 0 {
				return target
			}
		}
		if kind == 0 || home == homeDedicatedOnly {
			return -1
		}
		if kind == 1 {
			return empty(second)
		}
		return empty(nil)
	}
	carrierOn := func(lane int) *upstreamNode {
		for _, node := range p.nodes {
			if int(node.egressIndex.Load()) == lane {
				return node
			}
		}
		return nil
	}
	moved := 0
	move := func(source, target int, carrier *upstreamNode) {
		p.bindingCount[source]--
		p.bindingCount[target]++
		carrier.egressIndex.Store(int64(target))
		carrier.preferredEgress = target
		moved++
	}

	// Pass one: no lane keeps a second key while an empty healthy lane can
	// take it.
	for range p.nodes {
		target := pickEmpty(1)
		if target < 0 {
			target = pickEmpty(2)
		}
		if target < 0 {
			break
		}
		source, sourceCount := -1, 1
		for i := 0; i < n; i++ {
			if p.bindingCount[i] > sourceCount {
				source, sourceCount = i, p.bindingCount[i]
			}
		}
		if source < 0 {
			break
		}
		carrier := carrierOn(source)
		if carrier == nil {
			break
		}
		move(source, target, carrier)
	}
	// Pass two: give every key a home that is not benched. Sources are
	// tried by urgency: keys on a benched lane first — cursors skip those
	// lanes, so the key would sit out traffic entirely — then keys riding
	// the host's own route after a failover, then keys on a healthy lane
	// of the non-preferred class while a vacant preferred lane waits.
	// Moves toward the non-preferred class happen only from a benched or
	// host-route lane, so two lanes can never trade the same key back and
	// forth across calls. A dedicated-only pool moves only host-route and
	// exit-bound keys (a legacy placement): a benched dedicated key stays
	// where it is, because every dedicated lane answers to the same
	// upstream gate.
	for range p.nodes {
		var target, source int
		switch home {
		case homeProxyFirst:
			// Proxy exits first: the zen free-tier gate rejects the
			// configured prefixes as a class while per-node exits keep
			// answering, so an exit is a key's working home whenever one
			// is vacant and unbenched.
			if target = empty(proxyLane); target >= 0 {
				if source = withKeys(benched); source < 0 {
					if source = withKeys(directLane); source < 0 {
						source = withKeys(dedicatedLane)
					}
				}
			} else if target = empty(dedicatedLane); target >= 0 {
				// No exit is vacant: only benched and host-route keys can
				// be helped; healthy dedicated keys stay put so an exit's
				// recovery cannot pull the pool into churn.
				if source = withKeys(benched); source < 0 {
					source = withKeys(directLane)
				}
			}
		case homeDedicatedOnly:
			if target = empty(dedicatedLane); target >= 0 {
				if source = withKeys(directLane); source < 0 {
					source = withKeys(proxyLane)
				}
			}
		default:
			if target = empty(dedicatedLane); target >= 0 {
				if source = withKeys(benched); source < 0 {
					if source = withKeys(directLane); source < 0 {
						source = withKeys(proxyLane)
					}
				}
			} else if target = empty(proxyLane); target >= 0 {
				// No dedicated lane is left: keep draining benched and
				// host-route keys onto proxy exits — one host address
				// answering for a whole account set is what a provider
				// sees as one network address.
				if source = withKeys(benched); source < 0 {
					source = withKeys(directLane)
				}
			}
		}
		if target < 0 || source < 0 {
			break
		}
		carrier := carrierOn(source)
		if carrier == nil {
			break
		}
		move(source, target, carrier)
	}
	return moved
}

// ensureBindingsLocked grows bindingCount to cover n lanes. Callers hold
// bindingsMu. Replacement lanes arrive after construction, so the count
// slice must track the pool, not the initial size.
func (p *nodePool) ensureBindingsLocked(n int) {
	for len(p.bindingCount) < n {
		p.bindingCount = append(p.bindingCount, 0)
	}
}

// bindingCounts returns a copy of the per-lane key counts sized to n lanes.
// Gateway-level placement uses it to see every channel's demand at once;
// per-channel rebalancing alone cannot know that another provider's key
// already sits on a lane this pool considers empty.
func (p *nodePool) bindingCounts(n int) []int {
	if p == nil {
		return make([]int, max(n, 0))
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()
	p.ensureBindingsLocked(n)
	out := make([]int, n)
	copy(out, p.bindingCount[:n])
	return out
}

// RelocateOne moves one of this pool's keys from lane src onto lane dst and
// marks dst as that key's preferred home, so a later lane recovery does not
// drag it back into the crowd. It reports false when the pool holds no key
// on src.
func (p *nodePool) RelocateOne(src, dst int) bool {
	if p == nil || src == dst || src < 0 || dst < 0 {
		return false
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()
	p.ensureBindingsLocked(max(src, dst) + 1)
	if p.bindingCount[src] == 0 {
		return false
	}
	for _, node := range p.nodes {
		if int(node.egressIndex.Load()) == src {
			p.bindingCount[src]--
			p.bindingCount[dst]++
			node.egressIndex.Store(int64(dst))
			node.preferredEgress = dst
			return true
		}
	}
	return false
}

// replacementLocked returns a home lane for a key leaving failedEgress:
// the first empty lane in the desired class when one exists, otherwise a
// random pick among the least-loaded lanes of that class. want filters the
// lane class (nil means any); benched lanes are never candidates.
func (p *nodePool) replacementLocked(failedEgress int, healthyOnly bool, want func(*egressTransport) bool) int {
	items := p.transports.snapshot()
	minimum := int(^uint(0) >> 1)
	candidates := make([]int, 0, len(items)-1)
	for i := range items {
		if i == failedEgress || healthyOnly && !items[i].healthy.Load() {
			continue
		}
		if want != nil && !want(items[i]) {
			continue
		}
		// A benched lane is not a home: selection prefers to skip parked
		// and region-cooled lanes, so a key moved there would sit out until
		// the bench expires.
		if freitierParked(items[i]) || p.transports.subnetCooled(items[i].address) {
			continue
		}
		count := 0
		if i < len(p.bindingCount) {
			count = p.bindingCount[i]
		}
		if count == 0 {
			return i
		}
		if count < minimum {
			minimum = count
			candidates = candidates[:0]
			candidates = append(candidates, i)
		} else if count == minimum {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return -1
	}
	return candidates[rand.IntN(len(candidates))]
}

type nodeCursor struct {
	pool *nodePool
	next int
}

// Cursor reserves a different starting node for each concurrent request.
// Selection is delayed until Next, so a node marked failed by the preceding
// attempt is immediately skipped. Both Cursor and Next allocate no memory.
func (p *nodePool) Cursor() nodeCursor {
	if len(p.nodes) == 0 {
		return nodeCursor{pool: p}
	}
	return nodeCursor{pool: p, next: int((p.next.Add(1) - 1) % uint64(len(p.nodes)))}
}

// CursorFor returns a cursor whose first choice is stable for the supplied
// affinity key. This keeps every turn in a conversation on the same upstream
// key while retaining Next's cooldown-aware failover behavior. Empty affinity
// keys keep the round-robin behavior used by background tasks.
func (p *nodePool) CursorFor(affinity string) nodeCursor {
	if len(p.nodes) == 0 {
		return nodeCursor{pool: p}
	}
	if affinity == "" {
		return p.Cursor()
	}
	return nodeCursor{pool: p, next: startForAffinity(&p.next, affinity, len(p.nodes))}
}

func (c *nodeCursor) Next() *upstreamNode {
	if c.pool == nil || len(c.pool.nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	choice := -1
	var earliest int64
	for offset := 0; offset < len(c.pool.nodes); offset++ {
		i := (c.next + offset) % len(c.pool.nodes)
		until := c.pool.nodes[i].cooldownUntil.Load()
		if until <= now && !c.pool.laneParked(c.pool.nodes[i]) {
			choice = i
			break
		}
		if choice == -1 || until < earliest {
			choice, earliest = i, until
		}
	}
	if choice < 0 {
		return nil
	}
	c.next = (choice + 1) % len(c.pool.nodes)
	return c.pool.nodes[choice]
}

// laneParked reports whether the egress lane bound to node is currently
// parked by a FreeTier flag or cooled by a region block. Parked lanes are
// preferred to be skipped; the earliest-cooldown fallback in Next may still
// return one when every lane is cooling, which preserves liveness exactly as
// before.
func (p *nodePool) laneParked(node *upstreamNode) bool {
	if p == nil || p.transports == nil || node == nil {
		return false
	}
	items := p.transports.snapshot()
	index := int(node.egressIndex.Load())
	if index < 0 || index >= len(items) {
		return false
	}
	egress := items[index]
	return freitierParked(egress) || p.transports.subnetCooled(egress.address)
}

func (p *nodePool) MarkSuccess(node *upstreamNode) {
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
	node.total429s.Store(0)
}

func (p *nodePool) MarkFailure(node *upstreamNode, resp *http.Response, err error) {
	if !cooldownWorthy(resp, err) {
		return
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		storeCooldownDelay(&node.cooldownUntil, cooldown429For(node.total429s.Add(1)), retryAfterOf(resp))
		node.failures.Add(1)
		return
	}
	storeCooldownUntil(&node.cooldownUntil, p.cooldown, node.failures.Add(1), retryAfterOf(resp))
}

// cooldownWorthy reports whether a response or transport error justifies a
// failure cooldown. Deterministic 4xx client responses (anything but auth,
// rate-limit, or 5xx class) end the tier without cooling the key.
func cooldownWorthy(resp *http.Response, err error) bool {
	return err != nil || resp == nil || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
}

func parseRetryAfter(value string) time.Duration {
	duration, ok := parseRetryAfterPresent(value)
	if !ok {
		return 0
	}
	return duration
}

// parseRetryAfterPresent distinguishes an absent or invalid Retry-After
// (false) from an explicit zero (true, 0): a "0" seconds value or an HTTP
// date at/before now means "retry immediately", while a missing or
// unparseable header means "no signal". Callers that need a nonzero default
// branch on the presence bit instead of the duration.
func parseRetryAfterPresent(value string) (time.Duration, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(trimmed); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	if when, err := http.ParseTime(trimmed); err == nil {
		return max(time.Until(when), 0), true
	}
	return 0, false
}

// retryAfterOf extracts any Retry-After delay from resp, returning 0 when the
// response carries none. Nil-safe: a nil response has no headers to read.
func retryAfterOf(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	return parseRetryAfter(resp.Header.Get("Retry-After"))
}

// BestCoolingNode returns the least-cooling eligible lane in the pool when
// every eligible lane is cooling, preferring WARP exits. It returns nil if
// the pool is empty or every lane is unhealthy, FreeTier-parked, or direct-parked.
func (p *anonymousPool) BestCoolingNode(transports *transportPool) *anonymousNode {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	nodes := append([]*anonymousNode(nil), p.nodes...)
	p.mu.RUnlock()
	if len(nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	directParked := transports != nil && transports.DirectEgressParked()
	var best *anonymousNode
	var earliest int64
	bestIsWarp := false

	for _, node := range nodes {
		if node == nil || node.egress == nil || !node.egress.healthy.Load() || node.egress.freitierUntil.Load() > now {
			continue
		}
		if directParked && node.egress.proxy == nil {
			continue
		}
		until := node.cooldownUntil.Load()
		warp := isWarpLane(node)
		if best == nil {
			best, earliest, bestIsWarp = node, until, warp
			continue
		}
		if warp && !bestIsWarp {
			best, earliest, bestIsWarp = node, until, true
			continue
		}
		if bestIsWarp && !warp {
			continue
		}
		if until < earliest {
			best, earliest = node, until
		}
	}
	return best
}
