package identity

import (
	"strconv"
	"testing"
	"time"
)

// The free-tier gate decodes the 12 hex digits of a session ID as
// ~(createdMs*4096 + counter) and rejects counters above 63. Minted sessions
// must therefore decode to a counter inside the accepted window while keeping
// the canonical shape and the client's exact encoder round-trip. The
// millisecond field is only 36 bits wide (48 minus the 12 counter bits), so
// the expected value is the encoder's input modulo 2^36.
func TestMintSessionIDDecodesToAcceptedCounter(t *testing.T) {
	now := time.Now()
	id := mintSessionID(now)
	if !canonicalSessionPattern.MatchString(id) {
		t.Fatalf("minted session %q does not match the canonical pattern", id)
	}
	raw, err := strconv.ParseUint(id[4:16], 16, 64)
	if err != nil {
		t.Fatalf("session hex part %q is not hex: %v", id[4:16], err)
	}
	value := (^raw) & ((1 << 48) - 1)
	if counter := value % 4096; counter != uint64(sessionCounter) {
		t.Fatalf("counter %d, want %d", counter, sessionCounter)
	}
	if counter := value % 4096; counter > uint64(sessionCounterMax) {
		t.Fatalf("counter %d exceeds the accepted limit %d", counter, sessionCounterMax)
	}
	const msWrap = int64(1) << 36
	want := (now.UnixMilli() - sessionBackdateMs) & (msWrap - 1)
	if got := int64(value >> 12); got != want {
		t.Fatalf("encoded millisecond field = %d, want %d", got, want)
	}
}

// A value already shaped like the official header (40 lowercase hex) or the
// official "global" default passes through; anything else hashes into a
// stable 40-hex identity.
func TestProjectIDReproducesOfficialShapes(t *testing.T) {
	official := "c6cc7785c41c67efda495ae7cbd5f1c580641653"
	if got := ProjectID(official); got != official {
		t.Fatalf("ProjectID(%q) = %q, want passthrough", official, got)
	}
	if got := ProjectID("global"); got != "global" {
		t.Fatalf("ProjectID(global) = %q, want passthrough", got)
	}
	synthetic := ProjectID("")
	if len(synthetic) != 40 || !isHex40(synthetic) {
		t.Fatalf("synthetic project %q is not 40-hex", synthetic)
	}
	if again := ProjectID(""); again != synthetic {
		t.Fatalf("synthetic project is not stable: %q vs %q", synthetic, again)
	}
	foreign := ProjectID("my-project-id")
	if len(foreign) != 40 || !isHex40(foreign) || foreign == synthetic {
		t.Fatalf("foreign project %q was not hashed distinctly", foreign)
	}
}
