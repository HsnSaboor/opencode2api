package gateway

import (
	"net/http"
	"testing"
	"time"
)

func TestCooldown429ForCapsAtThirtyMinutes(t *testing.T) {
	cases := []struct {
		prior uint32
		want  time.Duration
	}{
		{0, 5 * time.Minute},
		{1, 5 * time.Minute},
		{2, 15 * time.Minute},
		{3, 30 * time.Minute},
		{4, 30 * time.Minute},
		{5, 30 * time.Minute},
		{100, 30 * time.Minute},
	}
	for _, tc := range cases {
		if got := cooldown429For(tc.prior); got != tc.want {
			t.Errorf("cooldown429For(%d) = %v, want %v", tc.prior, got, tc.want)
		}
	}
}

func TestMarkSuccessClears429History(t *testing.T) {
	t.Run("anonymous", func(t *testing.T) {
		pool := &anonymousPool{}
		node := &anonymousNode{}
		node.total429s.Store(4)
		pool.MarkSuccess(node)
		if got := node.total429s.Load(); got != 0 {
			t.Fatalf("anonymous node total429s = %d after success, want 0", got)
		}
	})
	t.Run("key", func(t *testing.T) {
		pool := &nodePool{}
		node := &upstreamNode{}
		node.total429s.Store(4)
		pool.MarkSuccess(node)
		if got := node.total429s.Load(); got != 0 {
			t.Fatalf("key node total429s = %d after success, want 0", got)
		}
	})
}

func TestKey429FailureUsesCappedCooldown(t *testing.T) {
	pool := &nodePool{}
	node := &upstreamNode{}
	// Two prior 429s: the third offense would be 45m before the cap.
	node.total429s.Store(2)
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}}
	pool.MarkFailure(node, resp, nil)
	delay := time.Until(time.Unix(0, node.cooldownUntil.Load()))
	if delay < 29*time.Minute || delay > 30*time.Minute {
		t.Fatalf("cooldown = %v, want ~30m", delay)
	}
}
