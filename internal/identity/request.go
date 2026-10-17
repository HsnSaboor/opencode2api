// Package identity derives stable session identities and unique request IDs.
package identity

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"sync"
	"time"

	"opencode2api/internal/jsonutil"
)

type RequestIDs struct {
	Session       string
	Request       string
	Project       string
	ParentSession string
}

func DeriveRequestIDs(r *http.Request, body map[string]any) RequestIDs {
	signal := jsonutil.FirstString(
		r.Header.Get("x-opencode-session"),
		r.Header.Get("x-session-affinity"),
		r.Header.Get("X-Session-Id"),
		r.Header.Get("x-session-id"),
		r.Header.Get("conversation-id"),
		jsonutil.StringAt(body, "conversation_id"),
		jsonutil.StringAt(body, "metadata", "session_id"),
	)
	if signal == "" {
		// Using the first user turn keeps a multi-turn conversation stable as its
		// history grows while separating conversations with different beginnings.
		signal = conversationSeed(body)
	}
	if signal == "" {
		signal = jsonutil.StringAt(body, "previous_response_id")
	}
	if signal == "" || signal == `{}` {
		signal = RandomID("fallback", 16)
	}
	session := CanonicalSessionID(signal)
	projectSignal := jsonutil.FirstString(r.Header.Get("x-opencode-project"), jsonutil.StringAt(body, "metadata", "project_id"))
	parentSession := jsonutil.FirstString(
		r.Header.Get("x-parent-session-id"),
		jsonutil.StringAt(body, "metadata", "parent_session_id"),
	)
	return RequestIDs{
		Session:       session,
		Request:       RandomID("req", 16),
		Project:       ProjectID(projectSignal),
		ParentSession: parentSession,
	}
}

func conversationSeed(body map[string]any) string {
	if input, ok := body["input"].(string); ok && input != "" {
		return input
	}
	for _, field := range []string{"messages", "input"} {
		for _, raw := range jsonutil.SliceAt(body, field) {
			item, ok := raw.(map[string]any)
			if !ok || jsonutil.StringAt(item, "role") != "user" {
				continue
			}
			encoded, _ := json.Marshal(item["content"])
			if len(encoded) > 0 && string(encoded) != "null" {
				return string(encoded)
			}
		}
	}
	return ""
}

// defaultProjectSignal names the synthetic project the gateway attributes
// conversations to when the downstream client does not send one.
const defaultProjectSignal = "opencode2api"

// ProjectID reproduces the x-opencode-project value the official client
// sends: the 40-hex SHA-1 of "directory:<cwd>", or the literal "global"
// outside a project (packages/core/src/session/model-request.ts). Values that
// already carry that shape pass through untouched; everything else is hashed
// into a stable synthetic identity so provider-side session telemetry sees a
// plausible project instead of a gateway marker.
func ProjectID(signal string) string {
	if signal == "" {
		signal = defaultProjectSignal
	}
	if signal == "global" || isHex40(signal) {
		return signal
	}
	sum := sha1.Sum([]byte("directory:" + signal))
	return hex.EncodeToString(sum[:])
}

func isHex40(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// canonicalSessionPattern matches OpenCode's canonical session format:
// "ses_" + 12 lowercase hex timestamp characters + 14 Base62 characters.
// Since 2026-09-16 the Zen free tier (Authorization: Bearer public) rejects
// any other session shape with 403 FreeTierError.
var canonicalSessionPattern = regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$`)

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// OpenCode session IDs encode their creation time in the 12 hex digits after
// "ses_". The official client (SessionID.make) computes
//
//	value = ~(createdMs*4096 + counter) & 0xFFFFFFFFFFFF
//
// and the free-tier gate decodes the same shape: the low 12 bits are a
// per-millisecond counter and values above 63 are rejected with 403
// FreeTierError (verified empirically: counter 63 passes, 64 fails). The
// 36-bit millisecond field wraps every ~795 days and is not validated, so
// memoized sessions remain acceptable for their whole TTL. Random or hashed
// hex parts fail the decode outright.
const (
	sessionCounter    = int64(1)
	sessionCounterMax = int64(63)
	sessionBackdateMs = int64(30_000)
	sessionMemoTTL    = 7 * 24 * time.Hour
)

type sessionMemoEntry struct {
	id       string
	mintedAt time.Time
}

var (
	sessionMemoMu sync.RWMutex
	sessionMemo   = make(map[[32]byte]sessionMemoEntry)
)

// CanonicalSessionID returns signal unchanged when it already carries an
// official OpenCode session (preserving upstream prompt-cache affinity).
// Any other downstream identity (UUIDs, foreign client sessions, legacy
// gateway sessions, conversation seeds) is memoized into a freshly encoded
// canonical session so the same conversation keeps a stable session while
// its timestamp stays inside the free-tier acceptance window.
func CanonicalSessionID(signal string) string {
	if canonicalSessionPattern.MatchString(signal) {
		return signal
	}
	key := sha256.Sum256([]byte("ses\x00" + signal))
	now := time.Now()

	sessionMemoMu.RLock()
	entry, ok := sessionMemo[key]
	sessionMemoMu.RUnlock()
	if ok && now.Sub(entry.mintedAt) < sessionMemoTTL {
		return entry.id
	}

	minted := mintSessionID(now)
	sessionMemoMu.Lock()
	if len(sessionMemo) >= 1<<16 {
		sessionMemo = make(map[[32]byte]sessionMemoEntry)
	}
	sessionMemo[key] = sessionMemoEntry{id: minted, mintedAt: now}
	sessionMemoMu.Unlock()
	return minted
}

// mintSessionID encodes the current time the way the official client does
// (backdated slightly to absorb clock skew) and appends a random Base62 tail.
// The counter is 1: the first session a real client mints in any millisecond
// carries that value, and every counter up to 63 is accepted by the gate.
func mintSessionID(now time.Time) string {
	createdMs := now.UnixMilli() - sessionBackdateMs
	value := (^(createdMs*4096 + sessionCounter)) & ((1 << 48) - 1)
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	randomPart := base62Fixed(new(big.Int).SetBytes(buf), 14)
	return fmt.Sprintf("ses_%012x%s", value, randomPart)
}

func base62Fixed(n *big.Int, width int) string {
	base := big.NewInt(62)
	out := make([]byte, width)
	remainder := new(big.Int)
	for i := width - 1; i >= 0; i-- {
		n.DivMod(n, base, remainder)
		out[i] = base62Alphabet[remainder.Int64()]
	}
	return string(out)
}

func RandomID(prefix string, size int) string {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(buf)
}
