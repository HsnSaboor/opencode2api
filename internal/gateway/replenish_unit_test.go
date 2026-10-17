package gateway

import (
	"testing"
	"time"

	"opencode2api/internal/config"
)

func testPerf() config.PerformanceConfig {
	return config.PerformanceConfig{MaxIdleConns: 2048, MaxIdleConnsPerHost: 256, IdleConnTimeoutSeconds: 120, ConnectTimeoutSeconds: 5, FailureCooldownSeconds: 15}
}

func TestReplenishGrowsPoolOnFreshSubnet(t *testing.T) {
	prefixes := []string{"2001:470:735f::/48"}
	addrs, err := config.SampleFreshEgress(prefixes, 4, nil)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	pool, err := newTransportPool(addrs, testPerf(), 0, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	anon := newAnonymousPool(true, pool, time.Second, prefixes)
	parked := pool.snapshot()[0]
	subBefore := map[string]struct{}{}
	for _, e := range pool.snapshot() {
		subBefore[config.SubnetKeyFor(prefixes, e.address)] = struct{}{}
	}
	anon.ParkFreitier(parked)
	if got := len(pool.snapshot()); got != 5 {
		t.Fatalf("pool size after park = %d, want 5", got)
	}
	fresh := pool.snapshot()[4]
	if got := config.SubnetKeyFor(prefixes, fresh.address); got == "" {
		t.Fatal("fresh lane has no subnet key")
	} else if _, dup := subBefore[got]; dup {
		t.Fatalf("fresh lane reuses live subnet %s", got)
	}
	if !freitierParked(parked) {
		t.Fatal("parked lane must report parked")
	}
	if n := len(anon.snapshotNodes()); n != 5 {
		t.Fatalf("nodes = %d, want 5", n)
	}
}

func TestReplenishCapsAtMax(t *testing.T) {
	prefixes := []string{"2001:470:735f::/48"}
	addrs, err := config.SampleFreshEgress(prefixes, 2, nil)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	pool, err := newTransportPool(addrs, testPerf(), 0, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	pool.maxLanes = 3
	anon := newAnonymousPool(true, pool, time.Second, prefixes)
	anon.ParkFreitier(pool.snapshot()[0])
	if got := len(pool.snapshot()); got != 3 {
		t.Fatalf("pool = %d, want 3 (cap)", got)
	}
	// At cap the park stands alone: no growth, no error.
	anon.ParkFreitier(pool.snapshot()[0])
	if got := len(pool.snapshot()); got != 3 {
		t.Fatalf("pool = %d, want 3 (still capped)", got)
	}
}

func TestSelectableCountsSplit(t *testing.T) {
	prefixes := []string{"2001:470:735f::/48"}
	addrs, err := config.SampleFreshEgress(prefixes, 2, nil)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	pool, err := newTransportPool(addrs, testPerf(), 0, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	anon := newAnonymousPool(true, pool, time.Second, prefixes)
	total, healthy, parked, cooling := pool.selectableCounts(anon.snapshotNodes())
	if total != 2 || healthy != 2 || parked != 0 || cooling != 0 {
		t.Fatalf("got %d/%d/%d/%d, want 2/2/0/0", total, healthy, parked, cooling)
	}
	anon.ParkFreitier(pool.snapshot()[0])
	total, healthy, parked, _ = pool.selectableCounts(anon.snapshotNodes())
	if parked != 1 {
		t.Fatalf("parked = %d, want 1", parked)
	}
	_ = total
	_ = healthy
}
