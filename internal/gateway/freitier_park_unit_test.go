package gateway

import (
	"testing"
	"time"
)

func TestFreitierParkForEscalatesAndCaps(t *testing.T) {
	cases := []struct {
		parks int32
		want  time.Duration
	}{
		{0, freitierParkBase},
		{1, 3 * time.Minute},
		{2, 6 * time.Minute},
		{3, 12 * time.Minute},
		{4, 24 * time.Minute},
		{5, freitierCooldown},
		{50, freitierCooldown},
	}
	for _, tc := range cases {
		if got := freitierParkFor(tc.parks); got != tc.want {
			t.Fatalf("freitierParkFor(%d) = %v, want %v", tc.parks, got, tc.want)
		}
	}
}

func TestMarkSuccessResetsFreitierStreak(t *testing.T) {
	egress := &egressTransport{}
	egress.freitierParks.Store(3)
	pool := &anonymousPool{}
	pool.MarkSuccess(&anonymousNode{egress: egress})
	if got := egress.freitierParks.Load(); got != 0 {
		t.Fatalf("freitierParks after MarkSuccess = %d, want 0", got)
	}
}

func TestUnparkFreitierResetsStreakAndPark(t *testing.T) {
	egress := &egressTransport{}
	egress.freitierParks.Store(4)
	egress.freitierUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pool := &anonymousPool{}
	pool.UnparkFreitier(egress)
	if got := egress.freitierParks.Load(); got != 0 {
		t.Fatalf("freitierParks after UnparkFreitier = %d, want 0", got)
	}
	if got := egress.freitierUntil.Load(); got != 0 {
		t.Fatalf("freitierUntil after UnparkFreitier = %d, want 0", got)
	}
}
