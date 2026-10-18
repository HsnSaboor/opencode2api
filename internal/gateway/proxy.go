package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/proxypool"
)

// The proxy pool mirrors the address pool's rotation semantics with one
// difference: a lane's "address" is a verified profile node reached through
// the shared sing-box multiplexer, and only nodes that pass the same gate
// production traffic faces are admitted. Verification is continuous because
// the gate's verdicts drift: a node blocked today answers 200 tomorrow and a
// passing node can be flagged an hour later.
const (
	// proxyVerifyInterval paces candidate sweeps. Together with
	// verify_concurrency it bounds how hard the pool probes the gate.
	proxyVerifyInterval = 3 * time.Second
	// proxyFillInterval paces lane growth toward the configured count.
	proxyFillInterval = 5 * time.Second
	// proxyProbeTimeout bounds one verification probe end to end.
	proxyProbeTimeout = 45 * time.Second
	// proxyProbeDialTimeout bounds just the SOCKS5 handshake to the
	// loopback multiplexer.
	proxyProbeDialTimeout = 10 * time.Second
	// proxyWarmReserve is how many verified-but-unused nodes the sweep tries
	// to keep ready, so a park can swap in a fresh exit instantly.
	proxyWarmReserve = 8
	// proxyReverifyInterval re-tests verified-but-unused nodes on a slow
	// cadence: profile nodes die and get flagged, so a verdict older than
	// this window is treated as a candidate again (zen-proxy revalidated
	// daily; public nodes churn faster, so this runs every six hours).
	proxyReverifyInterval = 6 * time.Hour
	// proxyCanaryTTL bounds how long one canary verdict is trusted; a single
	// canonical-profile probe per window is enough to tell a request-scoped
	// gate rejection from an exit-scoped one even under load.
	proxyCanaryTTL = 30 * time.Second
	// proxyCanaryTimeout bounds the canary probe a failing request or
	// candidate runs. The canonical profile answers in seconds when the gate
	// has an opinion at all; nothing waits the full probe budget inline.
	proxyCanaryTimeout = 15 * time.Second
	// proxyGateHold pauses candidate sweeps after a canary proves the gate
	// refuses the canonical request profile everywhere. Probing through a
	// request-scoped wave would charge failures to healthy nodes, so the
	// sweep waits the wave out and resumes on its own.
	proxyGateHold = 3 * time.Minute
	// proxyStateSaveInterval paces state persistence.
	proxyStateSaveInterval = 15 * time.Second
	// Verification cooldowns. Dead exits retry soon and escalate with
	// repeated failures; gate-blocked exits sit out a park-length window
	// first and stretch to hours, so a sweep settles instead of hammering
	// thousands of blocked nodes every few minutes.
	proxyDeadCooldownBase      = 5 * time.Minute
	proxyFreitierCooldownBase  = 30 * time.Minute
	proxyRateLimitCooldownBase = 30 * time.Minute
	proxyRejectedCooldownBase  = 15 * time.Minute
	proxyDeadCooldownCap       = time.Hour
	proxyFreitierCooldownCap   = 2 * time.Hour
)

// proxyProbeOutcome is the verdict of one candidate verification probe.
type proxyProbeOutcome int

const (
	proxyOutcomeVerified proxyProbeOutcome = iota
	proxyOutcomeFreitier
	proxyOutcomeRateLimited
	proxyOutcomeDead
	proxyOutcomeRejected
	// proxyOutcomeInconclusive means the probe could not produce a verdict
	// (no model yet, request build failure, cancelled context). The node
	// returns to the candidate set without a cooldown.
	proxyOutcomeInconclusive
)

func (o proxyProbeOutcome) String() string {
	switch o {
	case proxyOutcomeVerified:
		return "verified"
	case proxyOutcomeFreitier:
		return "freitier"
	case proxyOutcomeRateLimited:
		return "rate_limited"
	case proxyOutcomeDead:
		return "dead"
	case proxyOutcomeRejected:
		return "rejected"
	default:
		return "inconclusive"
	}
}

// proxyNodeState is the pool's bookkeeping for one profile node.
type proxyNodeState struct {
	verified     bool
	inUse        bool
	probing      bool
	failures     int
	coolingUntil time.Time
	lastVerified time.Time
}

// proxyStateDoc is the persisted shape of the pool's node state. It is keyed
// by the profile fingerprint so a swapped profile starts cold instead of
// inheriting verdicts for unrelated nodes.
type proxyStateDoc struct {
	Version     int              `json:"version"`
	Fingerprint string           `json:"fingerprint"`
	Verified    []int            `json:"verified"`
	Cooling     map[string]int64 `json:"cooling,omitempty"`
	Failures    map[string]int   `json:"failures,omitempty"`
}

const proxyStateVersion = 1

// proxyManager owns the candidate node set behind the multiplexer: which
// nodes passed the gate, which are bound to a lane, and which are cooling
// after a failed probe. The gateway asks it for nodes (take), returns them
// when a lane cannot be built (release), and reports probe verdicts
// (completeVerify). The manager never calls back into the transport pool, so
// lock ordering stays one-way.
type proxyManager struct {
	cfg       config.ProxyPoolConfig
	logger    *slog.Logger
	profile   *proxypool.Profile
	mux       *proxypool.Multiplexer
	statePath string

	mu     sync.Mutex
	state  []proxyNodeState
	cursor int
	dirty  bool
	// gateHoldUntil pauses candidate probes after a canary showed the gate
	// refusing the canonical profile from a verified exit: during a
	// request-scoped wave every probe would be charged to a healthy node.
	gateHoldUntil time.Time
	// wake nudges the fill and verify loops when the pool wants a node and
	// none is ready. Buffered so a signal left while no loop is listening
	// is not lost.
	wake chan struct{}
}

// newProxyManager loads the profile and prepares (but does not start) the
// multiplexer. A profile that cannot be read fails construction: the operator
// explicitly enabled the pool, so a broken path must be loud instead of
// silently dropping to address-only egress.
func newProxyManager(cfg config.ProxyPoolConfig, logger *slog.Logger) (*proxyManager, error) {
	profile, err := proxypool.LoadProfile(cfg.Profile)
	if err != nil {
		return nil, err
	}
	statePath := cfg.ResolveStateFile()
	mux := proxypool.NewMultiplexer(proxypool.MultiplexerConfig{
		Binary:    cfg.SingboxBinary,
		SocksPort: cfg.SocksPort,
		LogLevel:  cfg.LogLevel,
		StateDir:  filepath.Join(filepath.Dir(statePath), "multiplexer"),
	}, profile.Nodes, logger)
	manager := &proxyManager{
		cfg:       cfg,
		logger:    logger,
		profile:   profile,
		mux:       mux,
		statePath: statePath,
		state:     make([]proxyNodeState, len(profile.Nodes)),
		wake:      make(chan struct{}, 1),
	}
	manager.loadState()
	return manager, nil
}

func (m *proxyManager) socksAddr() string {
	if m == nil || m.mux == nil {
		return ""
	}
	return m.mux.SocksAddr()
}

// take claims the next verified, unused node for a lane. Callers that fail to
// build the lane must release it.
func (m *proxyManager) take() (proxypool.Node, bool) {
	if m == nil || len(m.state) == 0 {
		return proxypool.Node{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for offset := 0; offset < len(m.state); offset++ {
		index := (m.cursor + offset) % len(m.state)
		state := &m.state[index]
		if !state.verified || state.inUse || state.probing || state.coolingUntil.After(now) {
			continue
		}
		state.inUse = true
		m.cursor = (index + 1) % len(m.state)
		m.dirty = true
		return m.profile.Nodes[index], true
	}
	return proxypool.Node{}, false
}

// release returns a claimed node to the verified pool after a lane could not
// be built for it.
func (m *proxyManager) release(index int) {
	if m == nil || index < 0 || index >= len(m.state) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[index].inUse = false
	m.dirty = true
}

// requestVerify nudges the verify loop; it never blocks.
func (m *proxyManager) requestVerify() {
	if m == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// nextCandidate claims the next node for a probe: unverified nodes always,
// plus verified-but-unused nodes whose verdict is older than the re-verify
// window. The probing flag keeps concurrent workers off the same node.
func (m *proxyManager) nextCandidate() (proxypool.Node, bool) {
	if m == nil || len(m.state) == 0 {
		return proxypool.Node{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if now.Before(m.gateHoldUntil) {
		// A canary-confirmed gate rejection is in force: every probe would
		// be charged to a healthy node, so the sweep stays quiet until the
		// hold expires.
		return proxypool.Node{}, false
	}
	for offset := 0; offset < len(m.state); offset++ {
		index := (m.cursor + offset) % len(m.state)
		state := &m.state[index]
		if state.inUse || state.probing || state.coolingUntil.After(now) {
			continue
		}
		if state.verified && now.Sub(state.lastVerified) < proxyReverifyInterval {
			continue
		}
		state.probing = true
		m.cursor = (index + 1) % len(m.state)
		return m.profile.Nodes[index], true
	}
	return proxypool.Node{}, false
}

// completeVerify records the verdict of one probe.
func (m *proxyManager) completeVerify(index int, outcome proxyProbeOutcome) {
	if m == nil || index < 0 || index >= len(m.state) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := &m.state[index]
	state.probing = false
	now := time.Now()
	switch outcome {
	case proxyOutcomeVerified:
		state.verified = true
		state.failures = 0
		state.coolingUntil = time.Time{}
		state.lastVerified = now
		if m.logger != nil {
			m.logger.Info("proxy node verified", "component", "egress", "event", "proxy_node_verified", "node", index, "type", m.profile.Nodes[index].Type, "verified_total", m.verifiedLocked())
		}
		m.dirty = true
	case proxyOutcomeInconclusive:
		// No verdict: no state change, no cooldown.
		return
	default:
		state.failures++
		state.coolingUntil = now.Add(proxyCooldownFor(outcome, state.failures))
		// A failed verdict — including one on a stale re-verify — retires
		// the admission until the node passes again.
		state.verified = false
		if m.logger != nil {
			m.logger.Debug("proxy node probe failed", "component", "egress", "event", "proxy_node_probe_failed", "node", index, "outcome", outcome.String(), "failures", state.failures, "cooldown_s", int(time.Until(state.coolingUntil).Seconds()))
		}
		m.dirty = true
	}
}

// proxyCooldownFor maps a failure verdict and running failure count to the
// node's next probe window.
func proxyCooldownFor(outcome proxyProbeOutcome, failures int) time.Duration {
	shift := max(failures-1, 0)
	switch outcome {
	case proxyOutcomeDead:
		return min(proxyDeadCooldownBase*time.Duration(1<<min(shift, 4)), proxyDeadCooldownCap)
	case proxyOutcomeFreitier:
		return min(proxyFreitierCooldownBase*time.Duration(1<<min(shift, 2)), proxyFreitierCooldownCap)
	case proxyOutcomeRateLimited:
		return min(proxyRateLimitCooldownBase*time.Duration(1<<min(shift, 2)), proxyFreitierCooldownCap)
	default:
		return min(proxyRejectedCooldownBase*time.Duration(1<<min(shift, 2)), proxyFreitierCooldownCap)
	}
}

// availableVerified counts verified nodes that are not bound to a lane and
// are not cooling: the warm pool a park can draw from immediately.
func (m *proxyManager) availableVerified() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	count := 0
	for i := range m.state {
		state := &m.state[i]
		if state.verified && !state.inUse && !state.coolingUntil.After(now) {
			count++
		}
	}
	return count
}

func (m *proxyManager) verifiedLocked() int {
	count := 0
	for i := range m.state {
		if m.state[i].verified {
			count++
		}
	}
	return count
}

// staleVerified counts verified nodes that are unused and whose verdict is
// older than the re-verify window.
func (m *proxyManager) staleVerified() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	count := 0
	for i := range m.state {
		state := &m.state[i]
		if state.verified && !state.inUse && !state.probing && now.Sub(state.lastVerified) >= proxyReverifyInterval {
			count++
		}
	}
	return count
}

// proxyPoolStats is the point-in-time inventory the resource snapshot
// reports to operators.
type proxyPoolStats struct {
	Nodes     int
	Verified  int
	Available int
	Cooling   int
	Pending   int
}

// controlNodes returns up to limit verified nodes, ordered newest first.
func (m *proxyManager) controlNodes(limit int) []proxypool.Node {
	if m == nil || len(m.state) == 0 || limit <= 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	type cand struct {
		index int
		t     time.Time
	}
	var cands []cand
	for i := range m.state {
		if !m.state[i].verified {
			continue
		}
		cands = append(cands, cand{index: i, t: m.state[i].lastVerified})
	}
	sort.Slice(cands, func(i, j int) bool {
		return cands[i].t.After(cands[j].t)
	})
	if len(cands) > limit {
		cands = cands[:limit]
	}
	out := make([]proxypool.Node, len(cands))
	for i, c := range cands {
		out[i] = m.profile.Nodes[c.index]
	}
	return out
}

// controlNode returns the verified node a canary probe should test: the most
// recently verified node is the best available evidence of what the gate
// currently admits, and it is never a candidate the caller is about to
// judge (candidates are unverified or stale). The node is not claimed and
// carries no verdict of its own.
func (m *proxyManager) controlNode() (proxypool.Node, bool) {
	nodes := m.controlNodes(1)
	if len(nodes) == 0 {
		return proxypool.Node{}, false
	}
	return nodes[0], true
}

// holdSweep pauses candidate probes until now+d, never shortening a hold
// already in force. It is set when the canary proves the gate refuses the
// canonical request profile from a verified exit: recording failures during
// such a wave would cool healthy nodes for a decision they cannot change.
func (m *proxyManager) holdSweep(d time.Duration) {
	if m == nil || d <= 0 {
		return
	}
	m.mu.Lock()
	if until := time.Now().Add(d); until.After(m.gateHoldUntil) {
		m.gateHoldUntil = until
	}
	m.mu.Unlock()
}

// sweepHold reports whether a canary-detected gate rejection is currently
// pausing candidate sweeps, and until when.
func (m *proxyManager) sweepHold() (bool, time.Time) {
	if m == nil {
		return false, time.Time{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Now().Before(m.gateHoldUntil) {
		return true, m.gateHoldUntil
	}
	return false, time.Time{}
}

func (m *proxyManager) stats() proxyPoolStats {
	if m == nil {
		return proxyPoolStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	out := proxyPoolStats{Nodes: len(m.state)}
	for i := range m.state {
		state := &m.state[i]
		cooling := state.coolingUntil.After(now)
		if state.verified {
			out.Verified++
		}
		if state.verified && !state.inUse && !cooling {
			out.Available++
		}
		if cooling {
			out.Cooling++
		}
		if !state.verified && !state.inUse && !cooling {
			out.Pending++
		}
	}
	return out
}

// loadState restores verdicts persisted by a previous run. A missing file, a
// foreign fingerprint, or corrupt JSON is a cold start, never an error.
func (m *proxyManager) loadState() {
	if m == nil || m.statePath == "" {
		return
	}
	data, err := os.ReadFile(m.statePath)
	if err != nil {
		return
	}
	var doc proxyStateDoc
	if err := json.Unmarshal(data, &doc); err != nil || doc.Fingerprint != m.profile.Fingerprint {
		return
	}
	if doc.Version > proxyStateVersion {
		// A newer writer (a future binary) owns this file's shape.
		return
	}
	now := time.Now()
	for _, index := range doc.Verified {
		if index >= 0 && index < len(m.state) {
			m.state[index].verified = true
			m.state[index].lastVerified = now
		}
	}
	for raw, until := range doc.Cooling {
		index, err := strconv.Atoi(raw)
		if err != nil || index < 0 || index >= len(m.state) {
			continue
		}
		if when := time.Unix(0, until); when.After(now) {
			m.state[index].coolingUntil = when
		}
	}
	for raw, failures := range doc.Failures {
		index, err := strconv.Atoi(raw)
		if err != nil || index < 0 || index >= len(m.state) || failures <= 0 {
			continue
		}
		m.state[index].failures = failures
	}
}

// saveState persists current verdicts atomically and clears the dirty flag.
func (m *proxyManager) saveState() {
	if m == nil || m.statePath == "" {
		return
	}
	m.mu.Lock()
	if !m.dirty {
		m.mu.Unlock()
		return
	}
	doc := proxyStateDoc{
		Version:     proxyStateVersion,
		Fingerprint: m.profile.Fingerprint,
		Verified:    make([]int, 0, len(m.state)),
		Cooling:     make(map[string]int64),
		Failures:    make(map[string]int),
	}
	now := time.Now()
	for i := range m.state {
		state := &m.state[i]
		if state.verified {
			doc.Verified = append(doc.Verified, i)
		}
		if state.coolingUntil.After(now) {
			doc.Cooling[strconv.Itoa(i)] = state.coolingUntil.UnixNano()
		}
		if state.failures > 0 {
			doc.Failures[strconv.Itoa(i)] = state.failures
		}
	}
	m.dirty = false
	m.mu.Unlock()

	encoded, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(m.statePath), 0o755); err != nil {
		return
	}
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, encoded, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, m.statePath)
}

// stateSaver flushes verdicts periodically and once more at shutdown.
func (m *proxyManager) stateSaver(ctx context.Context) {
	if m == nil {
		return
	}
	ticker := time.NewTicker(proxyStateSaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.saveState()
			return
		case <-ticker.C:
			m.saveState()
		}
	}
}

// StartProxyPlugins launches the multiplexer supervisor and the pool's
// growth, verification, and persistence loops. The runtime calls it beside
// the egress health checks; everything stops when ctx ends.
func (g *Gateway) StartProxyPlugins(ctx context.Context) {
	manager := g.proxies
	if manager == nil {
		return
	}
	go manager.mux.Supervise(ctx)
	go manager.stateSaver(ctx)
	go g.proxyWarmSeededLoop(ctx)
	go g.proxyFillLoop(ctx)
	go g.proxyVerifyLoop(ctx)
}

// proxyWarmSeededLoop pre-dials the lanes seeded from persisted state once
// the multiplexer is up, so a restart does not pay TLS setup on first use.
// Warm misses are latency-only and never evict a lane.
func (g *Gateway) proxyWarmSeededLoop(ctx context.Context) {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		if g.proxies.mux.Ready() {
			for _, lane := range g.transports.snapshot() {
				if lane.proxy != nil {
					g.warmProxyLane(lane)
				}
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// proxyFillLoop grows the lane count toward the configured target as
// verification admits nodes. It runs on a timer and on wake nudges, so a
// fresh deployment populates within seconds of the first nodes passing.
func (g *Gateway) proxyFillLoop(ctx context.Context) {
	manager := g.proxies
	ticker := time.NewTicker(proxyFillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-manager.wake:
		}
		if ctx.Err() != nil {
			return
		}
		if !manager.mux.Ready() {
			continue
		}
		for g.transports.proxyLaneCount() < manager.cfg.Count {
			node, ok := manager.take()
			if !ok {
				// No verified node ready: let the verify loop know the pool
				// wants one, then wait for the next tick.
				manager.requestVerify()
				break
			}
			lane, err := g.transports.addProxyLane(node)
			if err != nil {
				// Pool cap reached or lane construction failed; the node
				// goes back so a later tick can retry.
				manager.release(node.Index)
				break
			}
			g.anonymous.addNode(lane)
			g.warmProxyLane(lane)
			if g.logger != nil {
				g.logger.Info("proxy egress lane admitted", "component", "egress", "event", "proxy_lane_admitted", "egress", config.RedactURL(lane.address), "node", node.Index, "type", node.Type, "lanes", g.transports.proxyLaneCount(), "target", manager.cfg.Count)
			}
		}
	}
}

// proxyVerifyNeeded reports whether the sweep should run: lanes below target
// means the pool wants nodes; below the warm reserve means parks will soon
// have nothing to swap in; a stale verdict means an admitted node needs
// revalidation. Once all are satisfied the sweep idles, so a healthy pool
// sends no gate probes at all.
func (g *Gateway) proxyVerifyNeeded(manager *proxyManager) bool {
	if g.transports.proxyLaneCount() < manager.cfg.Count {
		return true
	}
	if manager.availableVerified() < proxyWarmReserve {
		return true
	}
	return manager.staleVerified() > 0
}

// proxyVerifyLoop sweeps candidate nodes through the multiplexer and records
// verdicts. Concurrency is bounded by verify_concurrency; each tick launches
// at most one wave, so a failing sweep cannot flood the gate.
func (g *Gateway) proxyVerifyLoop(ctx context.Context) {
	manager := g.proxies
	workers := max(manager.cfg.VerifyConcurrency, 1)
	sem := make(chan struct{}, workers)
	ticker := time.NewTicker(proxyVerifyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-manager.wake:
		}
		if ctx.Err() != nil {
			return
		}
		if !manager.mux.Ready() || !g.proxyVerifyNeeded(manager) {
			continue
		}
	launch:
		for {
			node, ok := manager.nextCandidate()
			if !ok {
				break
			}
			select {
			case sem <- struct{}{}:
			default:
				// Every probe slot is busy; hand the candidate back and
				// finish this wave.
				manager.completeVerify(node.Index, proxyOutcomeInconclusive)
				break launch
			}
			go func(node proxypool.Node) {
				defer func() { <-sem }()
				outcome := g.probeProxyNode(ctx, node)
				if proxyGateFailure(outcome) {
					// A gate verdict may describe the request profile rather
					// than this exit (a global wave answers every exit with
					// the same 403, as the abbreviated-prompt incident did).
					// The canary probe distinguishes the two: when a freshly
					// verified exit is refused the same way, every exit would
					// have been refused, so the verdict is inconclusive and
					// sweeps pause until the wave passes.
					if rejected, decisive := g.proxyGateRejectsRequest(ctx); decisive && rejected {
						outcome = proxyOutcomeInconclusive
					}
				}
				manager.completeVerify(node.Index, outcome)
			}(node)
		}
	}
}

// proxyProbeTitlePrompt is the canonical OpenCode title-generator system
// prompt, verbatim from the production title payload. The free-tier gate
// admits bare tool-less requests only when they carry this full profile: a
// shortened prompt that still matches the gateway's own substring check
// answers 403 from every exit (verified against a known-passing node), so
// the probe must not abbreviate it.
const proxyProbeTitlePrompt = `You are a title generator. You output ONLY a thread title. Nothing else.

<task>
Generate a brief title that would help the user find this conversation later.

Follow all rules in <rules>
Use the <examples> so you know what a good title looks like.
Your output must be:
- A single line
- <=50 characters
- No explanations
</task>

<rules>
- you MUST use the same language as the user message you are summarizing
- Title must be grammatically correct and read naturally - no word salad
- Never include tool names in the title (e.g. "read tool", "bash tool", "edit tool")
- Focus on the main topic or question the user needs to retrieve
- Vary your phrasing - avoid repetitive patterns like always starting with "Analyzing"
- When a file is mentioned, focus on WHAT the user wants to do WITH the file, not just that they shared it
- Keep exact: technical terms, numbers, filenames, HTTP codes
- Remove: the, this, my, a, an
- Never assume tech stack
- Never use tools
- NEVER respond to questions, just generate a title for the conversation
- The title should NEVER include "summarizing" or "generating" when generating a title
- DO NOT SAY YOU CANNOT GENERATE A TITLE OR COMPLAIN ABOUT THE INPUT
- Always output something meaningful, even if the input is minimal.
- If the user message is short or conversational (e.g. "hello", "lol", "what's up", "hey"):
  -> create a title that reflects the user's tone or intent (such as Greeting, Quick check-in, Light chat, Intro message, etc.)
</rules>

<examples>
"debug 500 errors in production" -> Debugging production 500 errors
"refactor user service" -> Refactoring user service
"why is app.js failing" -> app.js failure investigation
"implement rate limiting" -> Rate limiting implementation
"how do I connect postgres to my API" -> Postgres API connection
"best practices for React hooks" -> React hooks best practices
"@src/credential.ts can you add refresh token support" -> Credential refresh token support
"@utils/parser.ts this is broken" -> Parser bug fix
"look at @config.json" -> Config review
"@App.tsx add dark mode toggle" -> Dark mode toggle in App
</examples>`

// proxyProbeUserPrompt is the companion user turn from the same production
// payload; the gate's title-profile match covers the pair.
const proxyProbeUserPrompt = "\"Reply with exactly: CAPTURE OK. Do not use tools.\""

// probeProxyNode tests one candidate through the multiplexer against the
// same gate production traffic faces. The probe carries the gateway's exact
// request shape (streaming title profile, anonymous fingerprint) so a 200
// here predicts a 200 on the request path; a 403 FreeTierError maps to the
// same park the pool applies to live lanes.
func (g *Gateway) probeProxyNode(ctx context.Context, node proxypool.Node) proxyProbeOutcome {
	model := g.probationModel()
	if model == "" {
		// The catalog has not loaded yet; probing against a guessed model
		// would produce verdicts that say nothing about the gate.
		return proxyOutcomeInconclusive
	}
	// The payload mirrors the production title request exactly (see
	// proxyProbeTitlePrompt): the gate rejects abbreviated title prompts
	// with 403 from every exit, which would poison every verdict.
	body, err := json.Marshal(map[string]any{
		"model": model,
		"messages": []any{
			map[string]any{"role": "user", "content": "ping"},
		},
		"max_tokens": 8,
		"stream":     true,
		"store":      false,
	})
	if err != nil {
		return proxyOutcomeInconclusive
	}
	body = shapeAnonymousBody(body, wire.Chat)
	ids := identity.RequestIDs{
		Session: identity.CanonicalSessionID("proxy-verify:" + node.User),
		Project: identity.ProjectID(""),
	}
	probeCtx, cancel := context.WithTimeout(ctx, proxyProbeTimeout)
	defer cancel()
	req, err := newUpstreamRequest(probeCtx, g.cfg.Upstream.Zen, wire.Chat, body, ids, anonymousZenKey)
	if err != nil {
		return proxyOutcomeInconclusive
	}
	dialer := proxypool.Dialer{
		Addr:     g.proxies.socksAddr(),
		User:     node.User,
		Password: proxypool.ProxyPassword,
		Timeout:  proxyProbeDialTimeout,
	}
	client := proxypool.Client(dialer)
	defer client.CloseIdleConnections()
	started := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(started)
	if err != nil {
		if ctx.Err() != nil {
			return proxyOutcomeInconclusive
		}
		g.logger.Debug("proxy candidate probe transport failed", "component", "egress", "event", "proxy_probe_transport_failed", "node", node.Index, "type", node.Type, "duration_ms", duration.Milliseconds(), "error", err)
		return proxyOutcomeDead
	}
	defer httpx.DrainAndClose(resp.Body)
	switch {
	case resp.StatusCode/100 == 2:
		return proxyOutcomeVerified
	case resp.StatusCode == http.StatusForbidden:
		if classifyForbidden(peekForbiddenSnippet(resp, 2048)) == forbiddenFreeTier {
			return proxyOutcomeFreitier
		}
		return proxyOutcomeRejected
	case resp.StatusCode == http.StatusTooManyRequests:
		return proxyOutcomeRateLimited
	default:
		return proxyOutcomeRejected
	}
}

// proxyGateFailure reports whether a probe verdict describes the gate's
// policy (FreeTier, rate limit, rejection) rather than the exit's transport.
// Policy verdicts are request-sensitive — a global wave answers them from
// every exit — so they are canary-checked before they are charged to a node;
// dead exits never are.
func proxyGateFailure(outcome proxyProbeOutcome) bool {
	switch outcome {
	case proxyOutcomeFreitier, proxyOutcomeRateLimited, proxyOutcomeRejected:
		return true
	default:
		return false
	}
}

// proxyGateRejectsRequest decides whether a burst of gate verdicts describes
// the request rather than the exits: it probes up to 3 recently verified
// nodes with the exact canonical request profile and reports whether the
// exits were refused too. Results are cached for proxyCanaryTTL so a wave
// costs one canary round per window instead of one per failing attempt, and
// a confirmed rejection holds candidate sweeps (see proxyManager.holdSweep).
// Dead control nodes are completed as dead so they don't stall future sweeps.
// The second return distinguishes a decisive verdict from "no verified exit
// to ask" or inconclusive canaries, where callers must keep the conservative
// (exit-scoped) accounting.
func (g *Gateway) proxyGateRejectsRequest(ctx context.Context) (bool, bool) {
	if g.proxies == nil {
		return false, false
	}
	g.proxyCanaryMu.Lock()
	if !g.proxyCanaryAt.IsZero() && time.Since(g.proxyCanaryAt) < proxyCanaryTTL {
		rejected := !g.proxyCanaryOK
		g.proxyCanaryMu.Unlock()
		return rejected, true
	}
	g.proxyCanaryMu.Unlock()
	nodes := g.proxies.controlNodes(3)
	if len(nodes) == 0 {
		return false, false
	}
	var anyVerified bool
	var allRejected = true
	var lastNode int
	for _, node := range nodes {
		lastNode = node.Index
		probeCtx, cancel := context.WithTimeout(ctx, proxyCanaryTimeout)
		outcome := g.probeProxyNode(probeCtx, node)
		cancel()
		switch outcome {
		case proxyOutcomeVerified:
			anyVerified = true
			allRejected = false
		case proxyOutcomeDead:
			g.proxies.completeVerify(node.Index, proxyOutcomeDead)
			allRejected = false
		case proxyOutcomeFreitier, proxyOutcomeRejected, proxyOutcomeRateLimited:
			// Gate policy refusal
		default:
			allRejected = false
		}
		if anyVerified {
			break
		}
	}
	if anyVerified {
		g.proxyCanaryMu.Lock()
		g.proxyCanaryAt, g.proxyCanaryOK = time.Now(), true
		g.proxyCanaryMu.Unlock()
		return false, true
	}
	if allRejected {
		g.proxyCanaryMu.Lock()
		g.proxyCanaryAt, g.proxyCanaryOK = time.Now(), false
		g.proxyCanaryMu.Unlock()
		g.proxies.holdSweep(proxyGateHold)
		if g.logger != nil {
			g.logger.Warn("gate rejected the canonical profile from verified exits; holding sweeps", "component", "egress", "event", "proxy_gate_hold", "node", lastNode, "hold_s", int(proxyGateHold.Seconds()))
		}
		return true, true
	}
	return false, false
}

// seedProxyLanes admits stored-verified nodes at construction so a restart
// resumes with warm lanes instead of waiting for the first sweep. The
// anonymous pool is built after seeding and inherits the lanes from the
// transport snapshot.
func (g *Gateway) seedProxyLanes() int {
	manager := g.proxies
	if manager == nil {
		return 0
	}
	seeded := 0
	for g.transports.proxyLaneCount() < manager.cfg.Count {
		node, ok := manager.take()
		if !ok {
			break
		}
		// The lane joins the anonymous rotation with the pool that is
		// constructed right after seeding (it snapshots the lane list).
		if _, err := g.transports.addProxyLane(node); err != nil {
			manager.release(node.Index)
			break
		}
		seeded++
	}
	return seeded
}

// warmProxyLane pre-dials one freshly admitted lane. Warm misses are
// latency-only: the lane stays marked healthy and real traffic owns health
// tracking, exactly like the startup warm of address lanes.
func (g *Gateway) warmProxyLane(lane *egressTransport) {
	if lane == nil || !lane.checking.CompareAndSwap(false, true) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), egressHealthCheckTimeout)
		defer cancel()
		result := g.transports.checkClaimedEgress(ctx, lane, egressHealthCheckURL, egressHealthCheckTimeout)
		if result.err == nil {
			if !result.wasHealthy {
				g.restoreEgress(result.egress)
			}
			return
		}
		if result.failed {
			result.egress.healthy.Store(true)
		}
	}()
}
