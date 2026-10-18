package config

import (
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"net"
	"path/filepath"
	"strings"
)

// EgressConfig selects the source addresses used for direct upstream
// connections. Every pool entry binds its own HTTP transport to one sampled
// IPv6 address, which rotates the visible egress across requests.
type EgressConfig struct {
	// EnableIPv6 samples PoolSize source addresses from Prefixes. When false
	// the gateway uses a single unbound direct connection.
	EnableIPv6 bool `json:"enable_ipv6"`
	// IPv4Only forces the direct lane to dial IPv4 (network "tcp4") instead
	// of letting the kernel race both families. The upstream free-tier gate
	// answers 403 for requests arriving over IPv6 while still accepting the
	// same request over IPv4, so a dual-stack direct lane would silently
	// prefer the blocked family. Requires enable_ipv6 to be false.
	IPv4Only bool `json:"ipv4_only,omitempty"`
	// Prefixes are sampled uniformly unless PrefixWeights says otherwise.
	// Relative order is preserved; duplicates are ignored.
	Prefixes []string `json:"prefixes,omitempty"`
	// PrefixWeights scales how often each prefix is drawn relative to its
	// siblings (a missing entry means 1). Keys are IPv6 CIDRs matched
	// canonically; entries naming no configured prefix are ignored, so a
	// primary prefix can be pre-weighted before it joins Prefixes. The
	// deployed composition weights the HE /48 at 24 against six weight-1
	// rented /64s, drawing the /48 ~80% of the pool while the rented lanes
	// stay as a reserve that carries traffic when the /48 is flagged.
	PrefixWeights map[string]int `json:"prefix_weights,omitempty"`
	// PoolSize is the number of source addresses held in rotation.
	PoolSize int `json:"pool_size"`
	// Proxies configures the optional sing-box multiplexed SOCKS5 exit
	// pool: verified profile nodes join the same rotation as source
	// addresses, one node per lane, with the same park/replenish behavior.
	Proxies ProxyPoolConfig `json:"proxies,omitempty"`
}

// ProxyPoolConfig configures the sing-box multiplexed proxy exit pool. Every
// profile node becomes one egress lane (its own exit IP); a verification
// loop continuously tests candidate nodes against the upstream gate and only
// admits nodes that pass, parking and replacing failures exactly like
// source-address lanes.
type ProxyPoolConfig struct {
	// Enabled turns the proxy pool on. When false every other field is
	// ignored and the gateway rotates source addresses only.
	Enabled bool `json:"enabled,omitempty"`
	// Profile is the path to a sing-box outbounds document, the "expanded"
	// profile shape written by the zen-proxy verification pipeline.
	Profile string `json:"profile,omitempty"`
	// Count is the number of proxy lanes held in rotation. Candidates are
	// verified continuously; the pool fills toward this count as nodes
	// pass the gate.
	Count int `json:"count,omitempty"`
	// SocksPort is the loopback SOCKS5 port the multiplexer listens on.
	SocksPort int `json:"socks_port,omitempty"`
	// SingboxBinary is the sing-box executable path.
	SingboxBinary string `json:"singbox_binary,omitempty"`
	// LogLevel is sing-box's own log level.
	LogLevel string `json:"log_level,omitempty"`
	// StateFile persists verified/cooling node state across restarts.
	// Defaults to "proxy-state.json" next to the profile.
	StateFile string `json:"state_file,omitempty"`
	// VerifyConcurrency bounds concurrent verification probes.
	VerifyConcurrency int `json:"verify_concurrency,omitempty"`
}

const (
	defaultEgressPrefix   = "2001:470:7331::/48"
	defaultEgressPoolSize = 100
	maxEgressPrefixes     = 8
	maxEgressPoolSize     = 128
	// maxEgressPrefixWeight caps a single prefix's draw weight; the bound
	// only keeps typos (e.g. a percentage slipped in as 8000) loud.
	maxEgressPrefixWeight = 256
	// Proxy pool bounds. The lane count shares maxPoolLanes with address
	// lanes, so keep it well below that cap.
	defaultProxyCount     = 64
	maxProxyCount         = 256
	defaultProxySocksPort = 20800
	defaultProxyLogLevel  = "warn"
	defaultProxyBinary    = "sing-box"
	maxVerifyConcurrency  = 32
	defaultVerifyWorkers  = 16
)

func defaultEgress() EgressConfig {
	return EgressConfig{EnableIPv6: true, Prefixes: []string{defaultEgressPrefix}, PoolSize: defaultEgressPoolSize}
}

func (e *EgressConfig) withDefaults() {
	if e.PoolSize == 0 {
		e.PoolSize = defaultEgressPoolSize
	}
	if e.EnableIPv6 && len(e.Prefixes) == 0 {
		e.Prefixes = []string{defaultEgressPrefix}
	}
	if e.Proxies.Enabled {
		if e.Proxies.Count == 0 {
			e.Proxies.Count = defaultProxyCount
		}
		if e.Proxies.SocksPort == 0 {
			e.Proxies.SocksPort = defaultProxySocksPort
		}
		if e.Proxies.SingboxBinary == "" {
			e.Proxies.SingboxBinary = defaultProxyBinary
		}
		if e.Proxies.LogLevel == "" {
			e.Proxies.LogLevel = defaultProxyLogLevel
		}
		if e.Proxies.VerifyConcurrency == 0 {
			e.Proxies.VerifyConcurrency = defaultVerifyWorkers
		}
	}
}

func (e EgressConfig) validate() error {
	if e.PoolSize < 1 || e.PoolSize > maxEgressPoolSize {
		return fmt.Errorf("egress.pool_size must be between 1 and %d", maxEgressPoolSize)
	}
	if e.IPv4Only && e.EnableIPv6 {
		return errors.New("egress.ipv4_only requires egress.enable_ipv6 to be false")
	}
	if err := e.Proxies.validate(); err != nil {
		return err
	}
	if !e.EnableIPv6 {
		return nil
	}
	if len(e.Prefixes) == 0 || len(e.Prefixes) > maxEgressPrefixes {
		return fmt.Errorf("egress.prefixes must contain between 1 and %d IPv6 CIDRs", maxEgressPrefixes)
	}
	for raw, weight := range e.PrefixWeights {
		if _, _, err := parseEgressPrefix(raw); err != nil {
			return fmt.Errorf("egress.prefix_weights: %w", err)
		}
		if weight < 1 || weight > maxEgressPrefixWeight {
			return fmt.Errorf("egress.prefix_weights[%q] must be between 1 and %d", raw, maxEgressPrefixWeight)
		}
	}
	capacity := new(big.Int)
	for _, raw := range e.Prefixes {
		_, prefixLen, err := parseEgressPrefix(raw)
		if err != nil {
			return err
		}
		// A /48 contributes its full host space to capacity: sampling prefers
		// distinct /64s (65535 usable, the zero /64 excluded) and only reuses
		// a /64 once the distinct subnets run out, so capacity is not capped
		// at the subnet count. Every other prefix reserves the all-zero
		// host; a /128 contributes its single address.
		room := new(big.Int).Lsh(big.NewInt(1), uint(128-prefixLen))
		if prefixLen != 48 && prefixLen != 128 {
			room.Sub(room, big.NewInt(1))
		}
		capacity.Add(capacity, room)
	}
	if capacity.Cmp(big.NewInt(int64(e.PoolSize))) < 0 {
		return fmt.Errorf("egress prefixes yield at most %s unique addresses, less than pool_size %d; widen the prefixes or lower egress.pool_size", capacity.String(), e.PoolSize)
	}
	return nil
}

// validate checks the proxy pool shape. It runs whether or not IPv6 lanes
// are enabled: proxy lanes are independent of source-address sampling, so a
// misconfigured pool must stay loud even in IPv4-only mode.
func (p ProxyPoolConfig) validate() error {
	if !p.Enabled {
		return nil
	}
	if strings.TrimSpace(p.Profile) == "" {
		return errors.New("egress.proxies.profile is required when the proxy pool is enabled")
	}
	if p.Count < 1 || p.Count > maxProxyCount {
		return fmt.Errorf("egress.proxies.count must be between 1 and %d", maxProxyCount)
	}
	if p.SocksPort < 1 || p.SocksPort > 65535 {
		return errors.New("egress.proxies.socks_port must be a valid TCP port")
	}
	if p.VerifyConcurrency < 1 || p.VerifyConcurrency > maxVerifyConcurrency {
		return fmt.Errorf("egress.proxies.verify_concurrency must be between 1 and %d", maxVerifyConcurrency)
	}
	return nil
}

// ResolveStateFile returns the proxy state file path: the configured
// StateFile when set, otherwise "proxy-state.json" next to the profile so
// state follows a moved profile without extra configuration.
func (p ProxyPoolConfig) ResolveStateFile() string {
	if trimmed := strings.TrimSpace(p.StateFile); trimmed != "" {
		return trimmed
	}
	return filepath.Join(filepath.Dir(p.Profile), "proxy-state.json")
}

func parseEgressPrefix(raw string) (*net.IPNet, int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, 0, errors.New("egress.prefixes must not contain an empty entry")
	}
	addr, network, err := net.ParseCIDR(trimmed)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid egress prefix %q: must be IPv6 CIDR notation", trimmed)
	}
	if addr.To4() != nil {
		return nil, 0, fmt.Errorf("invalid egress prefix %q: must be IPv6", trimmed)
	}
	ones, bits := network.Mask.Size()
	if bits != 128 || ones < 1 || ones > 128 {
		return nil, 0, fmt.Errorf("invalid egress prefix %q: must be an IPv6 CIDR", trimmed)
	}
	return network, ones, nil
}

// resolveEgress samples size unique source addresses across prefixes. It
// reports an actionable error when the prefixes cannot yield enough distinct
// addresses instead of returning a short pool. /48 draws land on distinct
// /64 subnets until the subnets run out (then reuse as fallback), so sibling
// lanes never share upstream per-/64 rate-limit fate. Prefixes narrower than
// a /64 (a /120 has only 256 addresses, a /128 exactly one) cap what the
// pool can hold, so callers validate size against the configured prefixes
// first. weights scales each prefix's share of draws (nil means uniform).
func resolveEgress(prefixes []string, weights map[string]int, size int) ([]string, error) {
	networks, lengths, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return nil, err
	}
	drawWeights, err := egressWeights(networks, weights)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, size)
	unique := make(map[string]struct{}, size)
	used64 := make(map[string]struct{}, size)
	for budget := size*32 + 64; len(out) < size && budget > 0; budget-- {
		addr, err := sampleEgressIP(networks, lengths, drawWeights)
		if err != nil {
			return nil, err
		}
		if _, dup := unique[addr]; dup {
			continue
		}
		// Keep /48 draws on distinct /64 subnets so sibling lanes never
		// share upstream per-/64 rate-limit fate. Reuse is allowed only as
		// a fallback once the pool wants more lanes than the 65535 usable
		// /64s — otherwise small pools could starve on unlucky redraws.
		if key, is48 := egressSubnetKey(addr, networks, lengths); is48 {
			if _, taken := used64[key]; taken && len(used64) < maxDistinct48Subnets(networks, lengths) {
				continue
			}
			used64[key] = struct{}{}
		}
		unique[addr] = struct{}{}
		out = append(out, addr)
	}
	if len(out) < size {
		return nil, fmt.Errorf("could only sample %d unique egress addresses from the configured prefixes; widen the prefixes or lower egress.pool_size", len(out))
	}
	return out, nil
}

// sampleEgressIP draws one address from the given networks, picking a
// prefix according to weights. A /48 keeps whole /64 subnets in rotation
// (the subnet index never draws zero); every other prefix draws uniform
// host bits with the all-zero host reserved, so the network address itself
// is never used as a source.
func sampleEgressIP(networks []*net.IPNet, lengths []int, weights []int) (string, error) {
	return sampleEgressIPExcept(networks, lengths, weights, nil)
}

// sampleEgressIPExcept draws like sampleEgressIP but skips addresses whose
// /64 (for /48 draws) is in exclude. It reports an error when every draw in
// its budget collides, so callers can decide whether to retry or reuse.
func sampleEgressIPExcept(networks []*net.IPNet, lengths []int, weights []int, exclude map[string]struct{}) (string, error) {
	for tries := 0; tries < 64; tries++ {
		addr, err := drawEgressIP(networks, lengths, weights)
		if err != nil {
			return "", err
		}
		if len(exclude) == 0 {
			return addr, nil
		}
		if key, is48 := egressSubnetKey(addr, networks, lengths); !is48 {
			return addr, nil
		} else if _, taken := exclude[key]; !taken {
			return addr, nil
		}
	}
	return "", errors.New("could not sample an unused /64 subnet; the configured prefixes are exhausted")
}

// drawEgressIP picks one network and draws one host address inside it. A
// weights slice aligned with networks biases the pick (rolls are uniform
// over the total weight); a nil or misaligned slice draws uniformly.
func drawEgressIP(networks []*net.IPNet, lengths []int, weights []int) (string, error) {
	pick := uint64(0)
	if len(weights) == len(networks) && len(weights) > 0 {
		total := 0
		for _, weight := range weights {
			total += weight
		}
		roll, err := cryptoBelow(uint64(total))
		if err != nil {
			return "", err
		}
		acc := uint64(0)
		for i, weight := range weights {
			acc += uint64(weight)
			if roll < acc {
				pick = uint64(i)
				break
			}
		}
	} else {
		roll, err := cryptoBelow(uint64(len(networks)))
		if err != nil {
			return "", err
		}
		pick = roll
	}
	base := new(big.Int).SetBytes(networks[pick].IP.To16())
	prefixLen := lengths[pick]
	if prefixLen == 128 {
		return ipv6String(base), nil
	}
	if prefixLen == 48 {
		sidRaw, err := rand.Int(rand.Reader, big.NewInt(65535))
		if err != nil {
			return "", fmt.Errorf("sample egress subnet: %w", err)
		}
		// 1..65535: the zero /64 stays out of rotation.
		sid := new(big.Int).Add(sidRaw, big.NewInt(1))
		iid, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 64))
		if err != nil {
			return "", fmt.Errorf("sample egress address: %w", err)
		}
		if iid.Sign() == 0 {
			iid = big.NewInt(1)
		}
		host := new(big.Int).Lsh(sid, 64)
		host.Or(host, iid)
		return ipv6String(new(big.Int).Or(base, host)), nil
	}
	hostMax := new(big.Int).Lsh(big.NewInt(1), uint(128-prefixLen))
	host, err := rand.Int(rand.Reader, hostMax)
	if err != nil {
		return "", fmt.Errorf("sample egress address: %w", err)
	}
	if host.Sign() == 0 {
		host = big.NewInt(1)
	}
	return ipv6String(new(big.Int).Or(base, host)), nil
}

// egressSubnetKey returns the /64 subnet key for a sampled address drawn
// from a /48 prefix, where upstream rate-limits apply per /64. The second
// return is false for narrower prefixes, whose draws already share one
// subnet by construction, and for unparsable literals (treated as
// non-/48 so sampling never blocks on them).
func egressSubnetKey(addr string, networks []*net.IPNet, lengths []int) (string, bool) {
	ip := net.ParseIP(addr)
	if ip == nil {
		return "", false
	}
	for i, network := range networks {
		if lengths[i] != 48 || !network.Contains(ip) {
			continue
		}
		masked := ip.Mask(net.CIDRMask(64, 128)).String()
		return masked + "/64", true
	}
	return "", false
}

// maxDistinct48Subnets counts the usable /64 subnets across /48 prefixes
// (65535 each: the zero /64 stays out of rotation). Distinct-/64 sampling
// reuses subnets only once the pool size exceeds this budget.
func maxDistinct48Subnets(networks []*net.IPNet, lengths []int) int {
	total := 0
	for i := range networks {
		if lengths[i] == 48 {
			total += 65535
		}
	}
	return total
}

// egressWeights resolves per-prefix sampling weights aligned with the parsed
// networks. Keys in weights are matched canonically, so any spelling of a
// configured CIDR applies; a missing entry means weight 1. Entries naming
// no parsed network are ignored, which lets a deployment pre-weight its
// primary prefix before adding it to Prefixes.
func egressWeights(networks []*net.IPNet, weights map[string]int) ([]int, error) {
	resolved := make([]int, len(networks))
	for i := range resolved {
		resolved[i] = 1
	}
	if len(weights) == 0 {
		return resolved, nil
	}
	canonical := make(map[string]int, len(weights))
	for raw, weight := range weights {
		network, _, err := parseEgressPrefix(raw)
		if err != nil {
			return nil, fmt.Errorf("egress.prefix_weights: %w", err)
		}
		if weight < 1 || weight > maxEgressPrefixWeight {
			return nil, fmt.Errorf("egress.prefix_weights[%q] must be between 1 and %d", raw, maxEgressPrefixWeight)
		}
		canonical[network.String()] = weight
	}
	for i, network := range networks {
		if weight, ok := canonical[network.String()]; ok {
			resolved[i] = weight
		}
	}
	return resolved, nil
}

// SampleFreshEgress draws count addresses from prefixes avoiding the given
// /64 subnet keys (as returned by SubnetKeyFor). It is the replenishment
// primitive: replacement lanes land on subnets no live lane already holds,
// so a flagged /64 never re-enters the pool through regrowth. It returns an
// error when the prefixes cannot yield count fresh subnets. weights scales
// each prefix's share of draws, exactly as in resolveEgress.
func SampleFreshEgress(prefixes []string, weights map[string]int, count int, exclude map[string]struct{}) ([]string, error) {
	networks, lengths, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return nil, err
	}
	drawWeights, err := egressWeights(networks, weights)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		addr, err := sampleEgressIPExcept(networks, lengths, drawWeights, exclude)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
		if key, is48 := egressSubnetKey(addr, networks, lengths); is48 {
			if exclude == nil {
				exclude = make(map[string]struct{})
			}
			exclude[key] = struct{}{}
		}
	}
	return out, nil
}

// SubnetKeyFor returns the /64 subnet key holding addr under any /48 in
// prefixes, or "" when addr sits under no configured /48. The gateway uses
// it to attribute lanes to subnets without reaching into sampling state.
func SubnetKeyFor(prefixes []string, addr string) string {
	networks, lengths, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return ""
	}
	if key, is48 := egressSubnetKey(addr, networks, lengths); is48 {
		return key
	}
	return ""
}

// PrefixKeyFor returns the configured CIDR from prefixes that contains addr,
// or "" when addr matches none (or is not an IP literal, such as "direct").
// The gateway uses it to attribute lane verdicts to the configured prefixes so
// an upstream block covering a whole prefix can be cooled as a unit.
func PrefixKeyFor(prefixes []string, addr string) string {
	ip := net.ParseIP(addr)
	if ip == nil {
		return ""
	}
	networks, _, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return ""
	}
	for _, network := range networks {
		if network.Contains(ip) {
			return network.String()
		}
	}
	return ""
}

// CanonicalizePrefix returns the canonical CIDR form of one configured prefix
// (parseEgressPrefixes masks host bits), or "" when raw is not a valid prefix.
// Prefix-health bookkeeping uses it so textually different spellings of the
// same prefix compare equal.
func CanonicalizePrefix(raw string) string {
	networks, _, err := parseEgressPrefixes([]string{raw})
	if err != nil || len(networks) == 0 {
		return ""
	}
	return networks[0].String()
}

// parseEgressPrefixes normalizes and validates a prefix list once, so
// replenishment does not re-parse per draw.
func parseEgressPrefixes(prefixes []string) ([]*net.IPNet, []int, error) {
	networks := make([]*net.IPNet, 0, len(prefixes))
	lengths := make([]int, 0, len(prefixes))
	seen := make(map[string]struct{}, len(prefixes))
	for _, raw := range prefixes {
		trimmed := strings.TrimSpace(raw)
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		network, prefixLen, err := parseEgressPrefix(trimmed)
		if err != nil {
			return nil, nil, err
		}
		networks = append(networks, network)
		lengths = append(lengths, prefixLen)
	}
	if len(networks) == 0 {
		return nil, nil, errors.New("egress.prefixes must contain at least one prefix")
	}
	return networks, lengths, nil
}

func cryptoBelow(max uint64) (uint64, error) {
	if max == 0 {
		return 0, errors.New("cannot draw from an empty range")
	}
	n, err := rand.Int(rand.Reader, new(big.Int).SetUint64(max))
	if err != nil {
		return 0, fmt.Errorf("sample egress prefix: %w", err)
	}
	return n.Uint64(), nil
}

// StableIndex maps an affinity key to a stable pool position. The pool
// cursors funnel session affinity through this single hash so key and
// anonymous selection cannot drift apart.
func StableIndex(affinity string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(affinity))
	return hash.Sum64()
}

// ipv6String renders a 128-bit integer as an IPv6 literal, left-padding the
// big-endian bytes that Int.Bytes surrenders when the high bits are zero.
func ipv6String(value *big.Int) string {
	raw := value.Bytes()
	addr := make([]byte, net.IPv6len)
	copy(addr[net.IPv6len-len(raw):], raw)
	return net.IP(addr).String()
}

func trimList(items *[]string) {
	out := (*items)[:0]
	for _, item := range *items {
		if value := strings.TrimSpace(item); value != "" {
			out = append(out, value)
		}
	}
	*items = out
}
