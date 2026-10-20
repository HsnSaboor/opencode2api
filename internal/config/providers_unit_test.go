package config

import (
	"strings"
	"testing"
)

// providersOnlyConfig is the smallest valid configuration whose upstream keys
// come exclusively from provider declarations: Normalize must accept it so an
// operator can run without Zen or Go keys.
func providersOnlyConfig() Config {
	return Config{
		Listen:     "127.0.0.1:8080",
		ServerKeys: []string{"local-key"},
		Prefer:     TierGo,
		Upstream: UpstreamConfig{
			Zen: "https://opencode.ai/inference",
			Go:  "https://opencode.ai/inference/go",
		},
		Providers: []ProviderConfig{{
			Name:        "  tokenharbor ",
			BaseURL:     " https://tokenharbor.ai ",
			ModelSuffix: " :free ",
			APIKeys:     []string{" th-key-1 ", "", "  "},
		}},
		Retry:  RetryConfig{MaxAttempts: 1, TimeoutSeconds: 1},
		Models: ModelsConfig{RefreshSeconds: 1},
		Performance: PerformanceConfig{
			MaxIdleConns: 1, MaxIdleConnsPerHost: 1,
			IdleConnTimeoutSeconds: 1, ConnectTimeoutSeconds: 1, FailureCooldownSeconds: 1,
		},
		Logging: LoggingConfig{Level: "info", RingSize: 100},
	}
}

func TestNormalizeAcceptsProviderOnlyUpstream(t *testing.T) {
	normalized, err := Normalize("", providersOnlyConfig())
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	provider := normalized.Providers[0]
	if provider.Name != "tokenharbor" || provider.BaseURL != "https://tokenharbor.ai" {
		t.Fatalf("provider not trimmed: %+v", provider)
	}
	if len(provider.APIKeys) != 1 || provider.APIKeys[0] != "th-key-1" {
		t.Fatalf("provider keys not trimmed: %v", provider.APIKeys)
	}
	tiers := normalized.Tiers()
	want := []Tier{TierZen, TierGo, Tier("tokenharbor")}
	if len(tiers) != len(want) {
		t.Fatalf("Tiers() = %v, want %v", tiers, want)
	}
	for i := range want {
		if tiers[i] != want[i] {
			t.Fatalf("Tiers() = %v, want %v", tiers, want)
		}
	}
	if !normalized.HasTier(Tier("tokenharbor")) || normalized.HasTier(Tier("unknown")) {
		t.Fatal("HasTier disagrees with the declared providers")
	}
	if keys := normalized.TierKeys(Tier("tokenharbor")); len(keys) != 1 || keys[0] != "th-key-1" {
		t.Fatalf("TierKeys(provider) = %v", keys)
	}
	if upstream := normalized.TierUpstream(Tier("tokenharbor")); upstream != "https://tokenharbor.ai" {
		t.Fatalf("TierUpstream(provider) = %q", upstream)
	}
	if suffix := normalized.TierModelSuffix(Tier("tokenharbor")); suffix != ":free" {
		t.Fatalf("TierModelSuffix(provider) = %q", suffix)
	}
	if normalized.TierModelSuffix(TierZen) != "" || normalized.TierModelSuffix(Tier("unknown")) != "" {
		t.Fatal("built-in and unknown tiers must not carry a model suffix")
	}
	if normalized.TierKeys(Tier("unknown")) != nil || normalized.TierUpstream(Tier("unknown")) != "" {
		t.Fatal("unknown tiers must not resolve keys or upstreams")
	}
}

func TestProviderValidation(t *testing.T) {
	cases := []struct {
		name     string
		provider ProviderConfig
		want     string
	}{
		{"empty name", ProviderConfig{Name: " ", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "must have a name"},
		{"reserved zen", ProviderConfig{Name: "zen", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "reserved"},
		{"reserved go", ProviderConfig{Name: "go", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "reserved"},
		{"uppercase", ProviderConfig{Name: "TokenHarbor", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "lowercase"},
		{"underscore", ProviderConfig{Name: "token_harbor", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "lowercase"},
		{"leading hyphen", ProviderConfig{Name: "-vx", BaseURL: "https://x.example", APIKeys: []string{"k"}}, "hyphen"},
		{"bad scheme", ProviderConfig{Name: "vx", BaseURL: "ftp://x.example", APIKeys: []string{"k"}}, "http"},
		{"missing host", ProviderConfig{Name: "vx", BaseURL: "/v1", APIKeys: []string{"k"}}, "http"},
		{"no keys", ProviderConfig{Name: "vx", BaseURL: "https://x.example", APIKeys: []string{" ", ""}}, "at least one API key"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{Providers: []ProviderConfig{test.provider}}
			err := normalizeProviders(&cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("normalizeProviders = %v, want error containing %q", err, test.want)
			}
		})
	}
	duplicate := Config{Providers: []ProviderConfig{
		{Name: "vx", BaseURL: "https://a.example", APIKeys: []string{"k1"}},
		{Name: "vx", BaseURL: "https://b.example", APIKeys: []string{"k2"}},
	}}
	if err := normalizeProviders(&duplicate); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("normalizeProviders(duplicate) = %v", err)
	}
}

func TestPreferMayNameAProvider(t *testing.T) {
	cfg := providersOnlyConfig()
	cfg.Prefer = Tier("tokenharbor")
	if _, err := Normalize("", cfg); err != nil {
		t.Fatalf("Normalize(prefer=provider): %v", err)
	}
	cfg.Prefer = Tier("unconfigured")
	if _, err := Normalize("", cfg); err == nil || !strings.Contains(err.Error(), "prefer") {
		t.Fatalf("Normalize(prefer=unknown) = %v", err)
	}
}

func TestCloneCopiesProviderKeys(t *testing.T) {
	cfg := providersOnlyConfig()
	clone := Clone(cfg)
	clone.Providers[0].APIKeys[0] = "mutated"
	if cfg.Providers[0].APIKeys[0] == "mutated" {
		t.Fatal("Clone aliased provider API keys")
	}
}

func TestRedactorCoversProviderSecrets(t *testing.T) {
	cfg := Config{Providers: []ProviderConfig{{
		Name:    "ph",
		BaseURL: "https://provider-user:provider-pass@ph.example",
		APIKeys: []string{"provider-key-1"},
	}}}
	redactor := NewSecretRedactor()
	redactor.Replace(cfg)
	if got := redactor.String("bearer provider-key-1"); strings.Contains(got, "provider-key-1") {
		t.Fatalf("redactor leaked a provider key: %q", got)
	}
	if got := redactor.String("upstream https://provider-user:provider-pass@ph.example/v1"); strings.Contains(got, "provider-user") || strings.Contains(got, "provider-pass") {
		t.Fatalf("redactor leaked provider URL credentials: %q", got)
	}
}
