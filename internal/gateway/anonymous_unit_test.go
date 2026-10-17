package gateway

import (
	"encoding/json"
	"testing"

	wire "opencode2api/internal/protocol"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("shaped body is not JSON: %v (%s)", err, body)
	}
	return payload
}

// A declared client tool profile is completed with the official lowercase
// names the free-tier gate recognizes (other clients capitalize or alias
// them), while the client's own entries stay untouched.
func TestShapeAnonymousBodyCompletesDeclaredToolProfile(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"read"}}]}`)
	shaped := shapeAnonymousBody(original, wire.Chat, false)
	payload := decodeBody(t, shaped)
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) != len(anonymousCoreTools) {
		t.Fatalf("completed profile = %v, want %d tools", payload["tools"], len(anonymousCoreTools))
	}
	first := tools[0].(map[string]any)["function"].(map[string]any)["name"]
	if first != "read" {
		t.Fatalf("declared tool reordered or renamed: %v", first)
	}
	present := map[string]bool{}
	for _, item := range tools {
		present[item.(map[string]any)["function"].(map[string]any)["name"].(string)] = true
	}
	for _, name := range anonymousCoreTools {
		if !present[name] {
			t.Fatalf("completed profile missing %q: %v", name, payload["tools"])
		}
	}
}

func TestShapeAnonymousBodyDoesNotInventTools(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	shaped := shapeAnonymousBody(original, wire.Chat, false)
	if payload := decodeBody(t, shaped); payload["tools"] != nil {
		t.Fatalf("tools invented for a client request: %v", payload["tools"])
	}
}

func TestShapeAnonymousBodyForcesStreamingAndUsage(t *testing.T) {
	original := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat, false))
	if payload["stream"] != true {
		t.Fatalf("stream not forced: %v", payload["stream"])
	}
	options, ok := payload["stream_options"].(map[string]any)
	if !ok || options["include_usage"] != true {
		t.Fatalf("chat usage not enabled: %v", payload["stream_options"])
	}
}

func TestShapeAnonymousBodyLeavesSystemOneVerbatim(t *testing.T) {
	original := []byte(`{"model":"m","state":"1 + 1 = 2"}`)
	if shaped := shapeAnonymousBody(original, wire.SystemOne, true); string(shaped) != string(original) {
		t.Fatalf("System One payload rewritten: %s", shaped)
	}
}

// Probe traffic is synthetic and must present the agent shape the gate
// accepts on fabricated bodies.
func TestShapeAnonymousBodyStampsProbeTools(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"Reply with OK."}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat, true))
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) != len(anonymousCoreTools) {
		t.Fatalf("probe tools = %v, want %d", payload["tools"], len(anonymousCoreTools))
	}
	present := map[string]bool{}
	for _, item := range tools {
		entry := item.(map[string]any)["function"].(map[string]any)
		present[entry["name"].(string)] = true
	}
	for _, name := range anonymousCoreTools {
		if !present[name] {
			t.Fatalf("probe tools missing %q: %v", name, payload["tools"])
		}
	}
}

func TestEnsureAnonymousToolsAppendsOnlyMissing(t *testing.T) {
	payload := map[string]any{"tools": []any{map[string]any{
		"type":     "function",
		"function": map[string]any{"name": "read"},
	}}}
	if !ensureAnonymousTools(payload, wire.Chat, false) {
		t.Fatal("missing core tools were not appended")
	}
	tools := payload["tools"].([]any)
	if len(tools) != 1+len(anonymousCoreTools)-1 {
		t.Fatalf("tool count = %d, want %d", len(tools), len(anonymousCoreTools))
	}
	seen := map[string]int{}
	for _, item := range tools {
		name := item.(map[string]any)["function"].(map[string]any)["name"].(string)
		seen[name]++
	}
	for name, count := range seen {
		if count != 1 {
			t.Fatalf("tool %q declared %d times", name, count)
		}
	}
	if ensureAnonymousTools(payload, wire.Chat, false) {
		t.Fatal("second pass must be a no-op")
	}
}

// Client traffic is never handed tools it did not declare; only synthetic
// probe payloads may have the set fabricated for them.
func TestEnsureAnonymousToolsNeverFabricatesForClients(t *testing.T) {
	payload := map[string]any{}
	if ensureAnonymousTools(payload, wire.Chat, false) {
		t.Fatal("tools fabricated without the fabricate flag")
	}
	if _, exists := payload["tools"]; exists {
		t.Fatalf("tools key invented: %v", payload["tools"])
	}
	if !ensureAnonymousTools(payload, wire.Chat, true) {
		t.Fatal("probe payload was not stamped")
	}
}

func TestAnonymousToolForAnthropicUsesInputSchema(t *testing.T) {
	payload := map[string]any{}
	ensureAnonymousTools(payload, wire.Anthropic, true)
	tools := payload["tools"].([]any)
	entry := tools[0].(map[string]any)
	if entry["input_schema"] == nil {
		t.Fatalf("anthropic tool missing input_schema: %v", entry)
	}
}
