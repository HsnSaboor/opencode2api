package config

import (
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"net"
	"strings"
)

// EgressConfig selects the source addresses used for direct upstream
// connections. Every pool entry binds its own HTTP transport to one sampled
// IPv6 address, which rotates the visible egress across requests.
type EgressConfig struct {
	// EnableIPv6 samples PoolSize source addresses from Prefixes. When false
	// the gateway uses a single unbound direct connection.
	EnableIPv6 bool `json:"enable_ipv6"`
	// Prefixes are sampled uniformly. Relative order is preserved; duplicates
	// are ignored.
	Prefixes []string `json:"prefixes,omitempty"`
	// PoolSize is the number of source addresses held in rotation.
	PoolSize int `json:"pool_size"`
}

const (
	defaultEgressPrefix   = "2001:470:7331::/48"
	defaultEgressPoolSize = 100
	maxEgressPrefixes     = 8
	maxEgressPoolSize     = 128
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
}

func (e EgressConfig) validate() error {
	if e.PoolSize < 1 || e.PoolSize > maxEgressPoolSize {
		return fmt.Errorf("egress.pool_size must be between 1 and %d", maxEgressPoolSize)
	}
	if !e.EnableIPv6 {
		return nil
	}
	if len(e.Prefixes) == 0 || len(e.Prefixes) > maxEgressPrefixes {
		return fmt.Errorf("egress.prefixes must contain between 1 and %d IPv6 CIDRs", maxEgressPrefixes)
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
// first.
func resolveEgress(prefixes []string, size int) ([]string, error) {
	networks, lengths, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, size)
	unique := make(map[string]struct{}, size)
	used64 := make(map[string]struct{}, size)
	for budget := size*32 + 64; len(out) < size && budget > 0; budget-- {
		addr, err := sampleEgressIP(networks, lengths)
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

// sampleEgressIP draws one address uniformly from the given networks. A /48
// keeps whole /64 subnets in rotation (the subnet index never draws zero);
// every other prefix draws uniform host bits with the all-zero host
// reserved, so the network address itself is never used as a source.
func sampleEgressIP(networks []*net.IPNet, lengths []int) (string, error) {
	return sampleEgressIPExcept(networks, lengths, nil)
}

// sampleEgressIPExcept draws like sampleEgressIP but skips addresses whose
// /64 (for /48 draws) is in exclude. It reports an error when every draw in
// its budget collides, so callers can decide whether to retry or reuse.
func sampleEgressIPExcept(networks []*net.IPNet, lengths []int, exclude map[string]struct{}) (string, error) {
	for tries := 0; tries < 64; tries++ {
		addr, err := drawEgressIP(networks, lengths)
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

func drawEgressIP(networks []*net.IPNet, lengths []int) (string, error) {
	pick, err := cryptoBelow(uint64(len(networks)))
	if err != nil {
		return "", err
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

// SampleFreshEgress draws count addresses from prefixes avoiding the given
// /64 subnet keys (as returned by SubnetKeyFor). It is the replenishment
// primitive: replacement lanes land on subnets no live lane already holds,
// so a flagged /64 never re-enters the pool through regrowth. It returns an
// error when the prefixes cannot yield count fresh subnets.
func SampleFreshEgress(prefixes []string, count int, exclude map[string]struct{}) ([]string, error) {
	networks, lengths, err := parseEgressPrefixes(prefixes)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		addr, err := sampleEgressIPExcept(networks, lengths, exclude)
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
