package config

import "testing"

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
