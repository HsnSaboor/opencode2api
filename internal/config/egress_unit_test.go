package config

import (
	"net"
	"strings"
	"testing"
)

// IPv4-only mode is the escape hatch for upstream environments that reject
// IPv6 free-tier traffic: it must yield exactly one unbound tcp4 lane whose
// identity is the "direct4" sentinel, regardless of any configured prefixes.
func TestIPv4OnlyYieldsDirect4Lane(t *testing.T) {
	cfg := Config{Egress: EgressConfig{
		EnableIPv6: false,
		IPv4Only:   true,
		Prefixes:   []string{"2001:470:7331::/48"},
		PoolSize:   4,
	}}
	got := cfg.EgressAddresses()
	if len(got) != 1 || got[0] != "direct4" {
		t.Fatalf("EgressAddresses() = %v, want [direct4]", got)
	}
	if err := cfg.Egress.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

// ipv4_only contradicts IPv6 sampling: the pool cannot bind and ignore the
// prefixes at the same time, so the combination must be rejected loudly
// instead of silently dropping one of the two settings.
func TestIPv4OnlyRejectsIPv6Sampling(t *testing.T) {
	cfg := Config{Egress: EgressConfig{EnableIPv6: true, IPv4Only: true, Prefixes: []string{"2001:470:7331::/48"}, PoolSize: 4}}
	if err := cfg.Egress.validate(); err == nil {
		t.Fatal("validate accepted ipv4_only together with enable_ipv6")
	}
}

// Disabling IPv6 without IPv4Only keeps the original dual-stack direct lane:
// that mode is a deliberate "let the kernel choose" escape hatch and must not
// be upgraded to the family-forcing sentinel.
func TestDisabledIPv6KeepsDualStackDirect(t *testing.T) {
	cfg := Config{Egress: EgressConfig{EnableIPv6: false, PoolSize: 4}}
	got := cfg.EgressAddresses()
	if len(got) != 1 || got[0] != "direct" {
		t.Fatalf("EgressAddresses() = %v, want [direct]", got)
	}
	if err := cfg.Egress.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

// The shipped example must keep parsing as the field set evolves: it is the
// documented starting point for every deployment, comments included.
func TestExampleConfigLoads(t *testing.T) {
	cfg, err := Load("../../config.example.json")
	if err != nil {
		t.Fatalf("Load(config.example.json): %v", err)
	}
	if cfg.Egress.IPv4Only {
		t.Fatal("example config unexpectedly enables ipv4_only")
	}
	if !cfg.Egress.EnableIPv6 || len(cfg.Egress.Prefixes) == 0 {
		t.Fatalf("example config egress = %+v, want IPv6 rotation defaults", cfg.Egress)
	}
}

// PrefixWeights steer pool composition: the weighted primary takes roughly
// its share of lanes (24 of 30 total weight -> ~80%), keeps every /48 lane
// on a distinct /64, and the weight-1 reserve prefixes stay present so a
// flare on the primary degrades into the reserve instead of a dead pool.
func TestEgressWeightsSteerComposition(t *testing.T) {
	cfg := Config{Egress: EgressConfig{
		EnableIPv6: true,
		Prefixes: []string{
			"2001:470:7331::/48",
			"2a03:94e0:2496:ade6::/64",
			"2a12:bec0:2c1:ade6::/64",
			"2a05:1083:bef7:ade6::/64",
			"2a05:1083:bee3:ade6::/64",
			"2a05:dfc1:3c18:ade6::/64",
			"2a05:dfc1:3ccc:ade6::/64",
		},
		PrefixWeights: map[string]int{"2001:470:7331::/48": 24},
		PoolSize:      128,
	}}
	if err := cfg.Egress.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	addrs := cfg.EgressAddresses()
	if len(addrs) != 128 {
		t.Fatalf("pool size = %d, want 128", len(addrs))
	}
	primary, subnets := 0, map[string]struct{}{}
	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			t.Fatalf("non-IP lane %q in the pool", addr)
		}
		if strings.HasPrefix(addr, "2001:470:7331:") {
			primary++
			subnets[ip.Mask(net.CIDRMask(64, 128)).String()] = struct{}{}
		}
	}
	// Binomial(128, 24/30): mean ~102, sigma ~4.5; the bounds sit past
	// 3 sigma, so they stay stable across runs while still catching an
	// unweighted sampler (which would land ~18 lanes on the /48).
	if primary < 86 || primary > 118 {
		t.Fatalf("primary /48 lanes = %d of %d, want ~80%%", primary, len(addrs))
	}
	if len(subnets) != primary {
		t.Fatalf("primary lanes share /64s: %d lanes on %d subnets", primary, len(subnets))
	}
}

// Weighted replenishment keeps the primary share: fresh draws land
// predominantly on the heavily weighted /48.
func TestSampleFreshEgressWeights(t *testing.T) {
	prefixes := []string{"2001:470:7331::/48", "2a03:94e0:2496:ade6::/64"}
	addrs, err := SampleFreshEgress(prefixes, map[string]int{"2001:470:7331::/48": 24}, 40, nil)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	primary := 0
	for _, addr := range addrs {
		if strings.HasPrefix(addr, "2001:470:7331:") {
			primary++
		}
	}
	// 24/25 = 96% of 40 draws; assert far below the mean only to stay
	// robust against unlucky randomness.
	if primary < 32 {
		t.Fatalf("primary draws = %d of %d, want ~96%%", primary, len(addrs))
	}
}

// A weight entry may name a prefix that is not configured yet (a deployment
// pre-weights its primary before adding it), but malformed keys and sub-1
// weights must fail validation loudly.
func TestEgressWeightsValidation(t *testing.T) {
	base := EgressConfig{EnableIPv6: true, Prefixes: []string{"2001:470:7331::/48"}, PoolSize: 4}

	inert := base
	inert.PrefixWeights = map[string]int{"2001:db8::/48": 24}
	if err := inert.validate(); err != nil {
		t.Fatalf("inert weight entry rejected: %v", err)
	}

	badKey := base
	badKey.PrefixWeights = map[string]int{"not-cidr": 2}
	if err := badKey.validate(); err == nil {
		t.Fatal("validate accepted a malformed prefix_weights key")
	}

	badWeight := base
	badWeight.PrefixWeights = map[string]int{"2001:470:7331::/48": 0}
	if err := badWeight.validate(); err == nil {
		t.Fatal("validate accepted a zero prefix weight")
	}
}

// Clone must deep-copy PrefixWeights: a reloaded config may be mutated
// without reaching back into the caller's map.
func TestCloneCopiesPrefixWeights(t *testing.T) {
	cfg := Config{Egress: EgressConfig{PrefixWeights: map[string]int{"2001:470:7331::/48": 24}}}
	clone := Clone(cfg)
	clone.Egress.PrefixWeights["2001:470:7331::/48"] = 1
	if got := cfg.Egress.PrefixWeights["2001:470:7331::/48"]; got != 24 {
		t.Fatalf("Clone aliased PrefixWeights (original = %d, want 24)", got)
	}
}

// The proxy pool fills sensible defaults when enabled, resolves its state
// file next to the profile by default, and rejects impossible shapes loudly;
// a disabled pool stays inert no matter what the other fields hold.
func TestProxyPoolDefaultsAndValidation(t *testing.T) {
	cfg := EgressConfig{EnableIPv6: false, PoolSize: 4, Proxies: ProxyPoolConfig{Enabled: true, Profile: "/tmp/nodes.json"}}
	cfg.withDefaults()
	if cfg.Proxies.Count != defaultProxyCount || cfg.Proxies.SocksPort != defaultProxySocksPort ||
		cfg.Proxies.SingboxBinary != defaultProxyBinary || cfg.Proxies.LogLevel != defaultProxyLogLevel ||
		cfg.Proxies.VerifyConcurrency != defaultVerifyWorkers {
		t.Fatalf("defaults = %+v", cfg.Proxies)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate defaults: %v", err)
	}
	if got := cfg.Proxies.ResolveStateFile(); got != "/tmp/proxy-state.json" {
		t.Fatalf("ResolveStateFile = %q, want /tmp/proxy-state.json", got)
	}
	explicit := cfg.Proxies
	explicit.StateFile = "/var/lib/o2a/state.json"
	if got := explicit.ResolveStateFile(); got != "/var/lib/o2a/state.json" {
		t.Fatalf("ResolveStateFile = %q, want explicit path", got)
	}

	cases := []struct {
		name    string
		mutate  func(*ProxyPoolConfig)
		wantErr bool
	}{
		{"missing profile", func(p *ProxyPoolConfig) { p.Profile = "  " }, true},
		{"zero count", func(p *ProxyPoolConfig) { p.Count = 0 }, true},
		{"count over max", func(p *ProxyPoolConfig) { p.Count = maxProxyCount + 1 }, true},
		{"port out of range", func(p *ProxyPoolConfig) { p.SocksPort = 70000 }, true},
		{"zero workers", func(p *ProxyPoolConfig) { p.VerifyConcurrency = 0 }, true},
		{"workers over max", func(p *ProxyPoolConfig) { p.VerifyConcurrency = maxVerifyConcurrency + 1 }, true},
		{"valid", func(p *ProxyPoolConfig) {}, false},
	}
	for _, tc := range cases {
		candidate := cfg
		tc.mutate(&candidate.Proxies)
		if err := candidate.validate(); (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
		}
	}

	disabled := EgressConfig{EnableIPv6: false, PoolSize: 4, Proxies: ProxyPoolConfig{Count: 0, SocksPort: 70000}}
	if err := disabled.validate(); err != nil {
		t.Fatalf("disabled proxy pool must not validate fields: %v", err)
	}
}
