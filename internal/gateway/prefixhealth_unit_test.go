package gateway

import (
	"testing"
	"time"
)

func prefixTestPool(t *testing.T, prefixes []string) *transportPool {
	t.Helper()
	pool, err := newTransportPool([]string{"2001:db8:ffff::1"}, testPerf(), 0, prefixes)
	if err != nil {
		t.Fatalf("newTransportPool: %v", err)
	}
	return pool
}

// Two FreeTier verdicts from one prefix cool the whole prefix; a single
// verdict only strikes it. The healthy list then excludes the cooled prefix
// while sibling prefixes stay selectable.
func TestStrikePrefixCoolsWholePrefix(t *testing.T) {
	pool := prefixTestPool(t, []string{"2001:db8::/48", "2001:db8:1::/64"})

	prefix, strikes, cooling := pool.StrikePrefix("2001:db8:0:aaaa::1")
	if prefix != "2001:db8::/48" || strikes != 1 || cooling {
		t.Fatalf("first strike = (%q, %d, %v), want (/48, 1, false)", prefix, strikes, cooling)
	}
	prefix, strikes, cooling = pool.StrikePrefix("2001:db8:0:bbbb::1")
	if prefix != "2001:db8::/48" || strikes != prefixFreeTierStrikeLimit || !cooling {
		t.Fatalf("second strike = (%q, %d, %v), want (/48, %d, true)", prefix, strikes, cooling, prefixFreeTierStrikeLimit)
	}
	// Verdicts already inside the cooling window keep reporting cooling.
	if _, _, cooling := pool.StrikePrefix("2001:db8:0:cccc::1"); !cooling {
		t.Fatal("strike inside the cooldown window must report cooling")
	}

	if cooling := pool.CoolingPrefixes(); len(cooling) != 1 {
		t.Fatalf("cooling prefixes = %v, want exactly the /48", cooling)
	}
	healthy := pool.HealthyPrefixes([]string{"2001:db8::/48", "2001:db8:1::/64"})
	if len(healthy) != 1 || healthy[0] != "2001:db8:1::/64" {
		t.Fatalf("healthy prefixes = %v, want only the /64", healthy)
	}
}

// A success from a cooled prefix — the probation probe or a fallback lane —
// retires the cooldown and re-admits it.
func TestClearPrefixRestoresCooledPrefix(t *testing.T) {
	pool := prefixTestPool(t, []string{"2001:db8::/48"})
	pool.StrikePrefix("2001:db8:0:aaaa::1")
	pool.StrikePrefix("2001:db8:0:bbbb::1")

	pool.ClearPrefix("2001:db8:0:cccc::1")
	if cooling := pool.CoolingPrefixes(); len(cooling) != 0 {
		t.Fatalf("cooling prefixes after success = %v, want none", cooling)
	}
	healthy := pool.HealthyPrefixes([]string{"2001:db8::/48"})
	if len(healthy) != 1 || healthy[0] != "2001:db8::/48" {
		t.Fatalf("healthy prefixes = %v, want the /48", healthy)
	}
}

// With every prefix cooling, sampling falls back to the full list so a
// recovery probe can still reach the prefix; otherwise a total outage would
// never clear.
func TestHealthyPrefixesFallsBackWhenAllCooling(t *testing.T) {
	pool := prefixTestPool(t, []string{"2001:db8::/48", "2001:db8:1::/64"})
	pool.StrikePrefix("2001:db8:0:aaaa::1")
	pool.StrikePrefix("2001:db8:0:bbbb::1")
	pool.StrikePrefix("2001:db8:1::5")
	pool.StrikePrefix("2001:db8:1::6")

	all := []string{"2001:db8::/48", "2001:db8:1::/64"}
	healthy := pool.HealthyPrefixes(all)
	if len(healthy) != len(all) {
		t.Fatalf("healthy prefixes = %v, want the full fallback list", healthy)
	}
}

// reCoolPrefix restarts the window so a still-blocked prefix is not re-probed
// on every probation tick.
func TestReCoolPrefixRestartsWindow(t *testing.T) {
	pool := prefixTestPool(t, []string{"2001:db8::/48"})
	pool.StrikePrefix("2001:db8:0:aaaa::1")
	pool.StrikePrefix("2001:db8:0:bbbb::1")

	before := pool.CoolingPrefixes()["2001:db8::/48"]
	pool.reCoolPrefix("2001:db8::/48")
	after := pool.CoolingPrefixes()["2001:db8::/48"]
	if !after.After(before) {
		t.Fatalf("cooldown window not extended: before=%v after=%v", before, after)
	}
	if time.Until(after) > prefixFreeTierCooldown+time.Minute {
		t.Fatalf("cooldown window overshoots the configured duration: %v", time.Until(after))
	}
}

// Addresses outside every configured prefix (including the "direct" lane
// marker) are not attributable and must leave the state untouched.
func TestStrikePrefixIgnoresUnattributableAddresses(t *testing.T) {
	pool := prefixTestPool(t, []string{"2001:db8::/48"})
	for _, addr := range []string{"", "direct", "10.0.0.5", "2001:470:ffff::1"} {
		if prefix, _, _ := pool.StrikePrefix(addr); prefix != "" {
			t.Fatalf("StrikePrefix(%q) = %q, want no attribution", addr, prefix)
		}
	}
	if cooling := pool.CoolingPrefixes(); len(cooling) != 0 {
		t.Fatalf("unattributable verdicts changed prefix health: %v", cooling)
	}
}
