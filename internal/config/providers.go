package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ProviderConfig declares one OpenAI-compatible upstream served through its
// own rotating pool of API keys. The provider's name doubles as its routing
// tier: models discovered from the provider's /v1/models endpoint are served
// through this tier exactly like Zen or Go models, and a request resolves to
// the provider tier whenever the model is advertised there.
//
// Providers speak the plain OpenAI-compatible wire shape: chat completions at
// /v1/chat/completions with "Authorization: Bearer <key>". They do not share
// the OpenCode inference contract (vendor-scoped protocol paths, the free-tier
// gate and its request shaping), so provider traffic never enters the
// anonymous lane and never charges OpenCode egress verdicts.
type ProviderConfig struct {
	// Name is the unique tier name for this provider (lowercase letters,
	// digits, and hyphens; "zen" and "go" are reserved).
	Name string `json:"name"`
	// BaseURL is the provider's API root. Protocol paths are appended to it:
	// chat completions resolves to <base_url>/v1/chat/completions.
	BaseURL string `json:"base_url"`
	// APIKeys is the rotating key pool for this provider. Every key serves
	// every model the provider advertises.
	APIKeys []string `json:"api_keys"`
	// ModelSuffix is appended to every discovered model ID before it enters
	// the catalog. Providers whose free or tiered routes use a name suffix
	// (tokenharbor serves "mimo-v2.5:free") declare it here once so clients
	// address the exact ID the upstream accepts. Empty keeps IDs as listed.
	ModelSuffix string `json:"model_suffix,omitempty"`
}

// Tiers returns every configured routing tier in canonical order: the
// built-in Zen and Go channels first, then the declared providers in
// configuration order. It is the one place the rest of the service asks what
// tiers exist, so adding a provider needs no changes anywhere else.
func (cfg Config) Tiers() []Tier {
	tiers := make([]Tier, 0, 2+len(cfg.Providers))
	tiers = append(tiers, TierZen, TierGo)
	for _, provider := range cfg.Providers {
		tiers = append(tiers, Tier(provider.Name))
	}
	return tiers
}

// HasTier reports whether tier names a configured routing tier.
func (cfg Config) HasTier(tier Tier) bool {
	switch tier {
	case TierZen, TierGo:
		return true
	}
	_, ok := cfg.provider(string(tier))
	return ok
}

// TierKeys returns the key pool configured for one tier, or nil for an
// unknown tier.
func (cfg Config) TierKeys(tier Tier) []string {
	switch tier {
	case TierZen:
		return cfg.ZenKeys
	case TierGo:
		return cfg.GoKeys
	}
	if provider, ok := cfg.provider(string(tier)); ok {
		return provider.APIKeys
	}
	return nil
}

// TierUpstream returns the base URL of one tier's upstream, or an empty
// string for an unknown tier.
func (cfg Config) TierUpstream(tier Tier) string {
	switch tier {
	case TierZen:
		return cfg.Upstream.Zen
	case TierGo:
		return cfg.Upstream.Go
	}
	if provider, ok := cfg.provider(string(tier)); ok {
		return provider.BaseURL
	}
	return ""
}

// TierModelSuffix returns the suffix appended to one tier's discovered model
// IDs, or an empty string when the tier's IDs enter the catalog as listed.
func (cfg Config) TierModelSuffix(tier Tier) string {
	if provider, ok := cfg.provider(string(tier)); ok {
		return provider.ModelSuffix
	}
	return ""
}

func (cfg Config) provider(name string) (ProviderConfig, bool) {
	for _, provider := range cfg.Providers {
		if provider.Name == name {
			return provider, true
		}
	}
	return ProviderConfig{}, false
}

// normalizeProviders trims and validates the provider list. Names must be
// unique, lowercase, reserved-word-free tier identifiers so a provider can
// never shadow Zen or Go routing.
func normalizeProviders(cfg *Config) error {
	seen := make(map[string]bool, len(cfg.Providers))
	normalized := make([]ProviderConfig, 0, len(cfg.Providers))
	for _, provider := range cfg.Providers {
		provider.Name = strings.TrimSpace(provider.Name)
		if err := validateProviderName(provider.Name); err != nil {
			return err
		}
		if seen[provider.Name] {
			return fmt.Errorf("provider %q is declared more than once", provider.Name)
		}
		seen[provider.Name] = true
		provider.BaseURL = strings.TrimSpace(provider.BaseURL)
		parsed, err := url.Parse(provider.BaseURL)
		if err != nil || parsed.Host == "" || (strings.ToLower(parsed.Scheme) != "http" && strings.ToLower(parsed.Scheme) != "https") {
			return fmt.Errorf("provider %q base_url must be an http or https URL", provider.Name)
		}
		provider.ModelSuffix = strings.TrimSpace(provider.ModelSuffix)
		trimList(&provider.APIKeys)
		if len(provider.APIKeys) == 0 {
			return fmt.Errorf("provider %q must contain at least one API key", provider.Name)
		}
		normalized = append(normalized, provider)
	}
	cfg.Providers = normalized
	return nil
}

func validateProviderName(name string) error {
	if name == "" {
		return errors.New("every provider must have a name")
	}
	if name == string(TierZen) || name == string(TierGo) {
		return fmt.Errorf("provider name %q is reserved for the built-in tier", name)
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			continue
		}
		return fmt.Errorf("provider name %q must contain only lowercase letters, digits, and hyphens", name)
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
		return fmt.Errorf("provider name %q must not start or end with a hyphen", name)
	}
	return nil
}
