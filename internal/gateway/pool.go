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
)

// egressTransport is one upstream lane bound to a single source address. The
// address is either "direct" (no explicit bind, kernel-chosen family),
// "direct4" (no explicit bind, forced IPv4) or an IPv6 literal sampled from
// the configured egress prefixes; every lane dials straight to the upstream,
// so rotating lanes rotates the visible egress.
type egressTransport struct {
	index    int
	address  string
	client   *http.Client
	healthy  atomic.Bool
	checking atomic.Bool
	// freitierUntil parks a FreeTier-flagged exit while the upstream flag
	// clears (UnixNano wall clock, 0 = not parked). totalFreitiers counts
	// lifetime flags and is diagnostic only. Park state lives on the lane
	// (not the anonymous node) so the key path shares the same enforcement.
	freitierUntil  atomic.Int64
	totalFreitiers atomic.Uint32
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
	// prefixMu guards prefixStrikes/prefixCooling, which track FreeTier
	// verdicts per configured prefix: a prefix whose lanes keep answering
	// FreeTier is blocked upstream as a unit and leaves replenishment
	// sampling until its cooldown expires. Only the rare verdict path takes
	// this mutex.
	prefixMu      sync.Mutex
	prefixStrikes map[string]int
	prefixCooling map[string]int64
	// maxLanes caps total lanes (initial + replacements) so a sustained
	// flag storm cannot grow the pool without bound. Zero means the
	// constructor default below.
	maxLanes int
}

// maxPoolLanes caps the live lane count including dynamic replacements:
// twice the sampled pool, clamped to the validated config ceiling. The
// /48 holds 65535 usable /64s, so the cap is a memory/socket guard, not
// address-space exhaustion.
const maxPoolLanes = 256

// anonymousPool gives the shared OpenCode "public" credential an independent
// cooldown per egress address. Unlike key nodes, anonymous nodes are never
// rebound: changing egress address is the failover mechanism because the free
// tier rate-limits by source IP.
type anonymousPool struct {
	nodes    []*anonymousNode
	next     atomic.Uint64
	cooldown time.Duration
	// transports is the lane source new nodes draw from when the pool
	// replenishes a parked lane. It aliases the gateway's shared pool.
	transports *transportPool
	// prefixes are the configured egress CIDRs used to sample replacement
	// lanes. Stored once at construction; never mutated afterwards.
	prefixes []string
	// mu guards nodes, which grows by replenishment while selection
	// cursors range it. Read locks are slice-header-sized critical
	// sections only; no I/O happens under them.
	mu sync.RWMutex
}

type anonymousNode struct {
	egress   *egressTransport
	failures atomic.Uint32
	// total429s counts lifetime 429s on this lane and drives the escalating
	// 429 backoff. It is never reset by success, mirroring the zen pool.
	total429s     atomic.Uint32
	cooldownUntil atomic.Int64
}

type anonymousCursor struct {
	pool   *anonymousPool
	nodes  []*anonymousNode
	start  int
	offset int
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

// cooldown429For returns the escalating zen-grade 429 backoff for the
// n-th 429 on a lane (n >= 1): 5min * 3^min(n-1,5), capped at 8h. The
// first offense parks the lane briefly; repeat offenders escalate to the
// full quota-reset window. Callers still stretch to Retry-After via
// storeCooldownDelay when the upstream asks for longer.
func cooldown429For(prior429s uint32) time.Duration {
	if prior429s == 0 {
		prior429s = 1
	}
	shift := min(prior429s-1, 5)
	delay := 5 * time.Minute
	for range shift {
		delay *= 3
	}
	return min(delay, 8*time.Hour)
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
	pool := &anonymousPool{cooldown: cooldown, transports: transports, prefixes: append([]string(nil), prefixes...)}
	if !enabled || transports == nil {
		return pool
	}
	pool.nodes = make([]*anonymousNode, 0, len(transports.items))
	for _, egress := range transports.items {
		pool.nodes = append(pool.nodes, &anonymousNode{egress: egress})
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
	p.mu.RUnlock()
	if len(nodes) == 0 {
		return anonymousCursor{pool: p}
	}
	return anonymousCursor{pool: p, nodes: nodes, start: startForAffinity(&p.next, affinity, len(nodes))}
}

// Next visits each healthy, non-cooling, non-parked egress address at most
// once per cursor.
func (c *anonymousCursor) Next() *anonymousNode {
	if len(c.nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	for c.offset < len(c.nodes) {
		node := c.nodes[(c.start+c.offset)%len(c.nodes)]
		c.offset++
		if node.egress.healthy.Load() && node.cooldownUntil.Load() <= now && node.egress.freitierUntil.Load() <= now {
			return node
		}
	}
	return nil
}

func (p *anonymousPool) MarkSuccess(node *anonymousNode) {
	if node == nil {
		return
	}
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
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

// freitierCooldown parks a FreeTier-flagged exit while the upstream flag
// clears. It mirrors zen-proxy's mark_freitier_blocked: per-lane only, never
// subnet-wide, health and failure counters untouched.
const freitierCooldown = 30 * time.Minute

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
	egress.freitierUntil.Store(time.Now().Add(freitierCooldown).UnixNano())
	p.maybeReplenish(egress)
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
// is silent: the park still holds, and the next park retries regrowth.
func (p *anonymousPool) maybeReplenish(parked *egressTransport) {
	if p == nil || p.transports == nil {
		return
	}
	prefixes := p.replenishPrefixes()
	if len(prefixes) == 0 {
		return
	}
	lane := p.transports.replenishLane(prefixes, parked)
	if lane == nil {
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
	for _, egress := range snap {
		if egress.healthy.Load() {
			healthy++
		}
		if egress.freitierUntil.Load() > now {
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

// warmLanes pre-dials every lane against the health-check target so TLS
// and routing are established before the first client request. Failures
// never fail startup: lanes stay marked healthy and warm on demand via the
// regular health path. The wait is bounded by timeout per lane plus the
// caller's context; no goroutine outlives the return.
func (p *transportPool) warmLanes(ctx context.Context, target string, timeout time.Duration) []egressHealthResult {
	items := p.snapshot()
	results := make(chan egressHealthResult, len(items))
	checks := 0
	for _, egress := range items {
		if !egress.checking.CompareAndSwap(false, true) {
			continue
		}
		checks++
		go func() {
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
	// total429s counts lifetime 429s on this key and drives the escalating
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

// RebindEgress moves every key currently using failedEgress to the
// least-loaded healthy lane. Empty lanes are selected in configuration order;
// otherwise one of the least-loaded lanes is chosen at random. If every
// alternative is currently unhealthy, it still attempts the least-loaded
// alternative.
func (p *nodePool) RebindEgress(failedEgress int) int {
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
		target := p.replacementLocked(failedEgress, true)
		if target < 0 {
			target = p.replacementLocked(failedEgress, false)
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

// ensureBindingsLocked grows bindingCount to cover n lanes. Callers hold
// bindingsMu. Replacement lanes arrive after construction, so the count
// slice must track the pool, not the initial size.
func (p *nodePool) ensureBindingsLocked(n int) {
	for len(p.bindingCount) < n {
		p.bindingCount = append(p.bindingCount, 0)
	}
}

func (p *nodePool) replacementLocked(failedEgress int, healthyOnly bool) int {
	items := p.transports.snapshot()
	minimum := int(^uint(0) >> 1)
	candidates := make([]int, 0, len(items)-1)
	for i := range items {
		if i == failedEgress || healthyOnly && !items[i].healthy.Load() {
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
