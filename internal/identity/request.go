// Package identity derives stable session identities and unique request IDs.
package identity

import (
	"crypto/rand"
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
	if projectSignal == "" {
		projectSignal = "opencode2api:default-project"
	}
	parentSession := jsonutil.FirstString(
		r.Header.Get("x-parent-session-id"),
		jsonutil.StringAt(body, "metadata", "parent_session_id"),
	)
	return RequestIDs{
		Session:       session,
		Request:       RandomID("req", 16),
		Project:       StableID("prj", projectSignal),
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

func StableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

// canonicalSessionPattern matches OpenCode's canonical session format:
// "ses_" + 12 lowercase hex timestamp characters + 14 Base62 characters.
// Since 2026-09-16 the Zen free tier (Authorization: Bearer public) rejects
// any other session shape with 403 FreeTierError.
var canonicalSessionPattern = regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$`)

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// sessionEpochMs is the fixed countdown epoch of OpenCode's session ID
// encoder (verified against 400+ real client sessions):
//
//	value = (sessionEpochMs - createdMs) * 4096 - 2  (mod 2^48)
//
// The 12 hex digits are a 1/4096-ms countdown; the Zen free-tier gate decodes
// them and rejects sessions that do not map to a plausible creation time, so
// hashed or random hex parts fail with 403 FreeTierError.
const (
	sessionEpochMs    = int64(1855425872000)
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

// mintSessionID encodes now (backdated slightly to absorb clock skew) with
// OpenCode's session countdown and appends a random Base62 tail.
func mintSessionID(now time.Time) string {
	createdMs := now.UnixMilli() - sessionBackdateMs
	value := (sessionEpochMs-createdMs)*4096 - 2
	value &= (1 << 48) - 1
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
