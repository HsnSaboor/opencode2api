package config

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
)

func MaskValue(value string) string {
	runes := []rune(value)
	if len(runes) <= 5 {
		return "••••"
	}
	return "••••" + string(runes[len(runes)-5:])
}

// RedactURL strips credentials from a URL's userinfo for safe logging.
// Inputs without a "://" scheme (bare egress values such as "direct", IPv6
// literals, or host:port) carry no userinfo and never survive url.Parse, so
// they pass through as-is instead of collapsing to "<invalid>".
func RedactURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	if u.User != nil {
		u.User = url.User("***")
	}
	return u.String()
}

type SecretRedactor struct {
	values atomic.Value
}

func NewSecretRedactor() *SecretRedactor {
	r := &SecretRedactor{}
	r.values.Store([]string(nil))
	return r
}

func (r *SecretRedactor) Replace(cfg Config) {
	values := make([]string, 0, len(cfg.ServerKeys)+len(cfg.ZenKeys)+len(cfg.GoKeys)+2)
	values = append(values, cfg.ServerKeys...)
	for _, tier := range cfg.Tiers() {
		values = append(values, cfg.TierKeys(tier)...)
	}
	if cfg.WebUI.Password != "" {
		values = append(values, cfg.WebUI.Password)
	}
	for _, tier := range cfg.Tiers() {
		if parsed, err := url.Parse(cfg.TierUpstream(tier)); err == nil && parsed.User != nil {
			values = append(values, parsed.String(), parsed.User.Username())
			if password, ok := parsed.User.Password(); ok {
				values = append(values, password)
			}
		}
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	r.values.Store(values)
}

func (r *SecretRedactor) String(value string) string {
	for _, secret := range r.values.Load().([]string) {
		if len(secret) >= 4 {
			value = strings.ReplaceAll(value, secret, "***")
		}
	}
	return value
}

func Fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:10]
}

// keyDisplayID is intentionally separate from secretFingerprint. The latter
// is an internal stable identifier used by session/config bookkeeping; this
// value is safe for logs and the operator UI and shows only the key suffix.
func KeyDisplayID(value string) string {
	runes := []rune(value)
	if len(runes) <= 5 {
		return string(runes)
	}
	return string(runes[len(runes)-5:])
}
