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

func shapedToolNames(t *testing.T, payload map[string]any) []string {
	t.Helper()
	tools, ok := payload["tools"].([]any)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(tools))
	for _, item := range tools {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if function, ok := entry["function"].(map[string]any); ok {
			if name, ok := function["name"].(string); ok {
				names = append(names, name)
			}
			continue
		}
		if name, ok := entry["name"].(string); ok {
			names = append(names, name)
		}
	}
	return names
}

// A tool-less agent request must be stamped with the single tool the
// free-tier gate requires; without it the upstream answers 403 FreeTierError.
func TestShapeAnonymousBodyStampsAgentProfile(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat))
	names := shapedToolNames(t, payload)
	if len(names) != 1 || names[0] != "read" {
		t.Fatalf("stamped tools = %v, want [read]", names)
	}
	tools := payload["tools"].([]any)
	entry := tools[0].(map[string]any)
	if entry["type"] != "function" {
		t.Fatalf("chat tool entry lacks function type: %v", entry)
	}
}

// Declared tool walls are preserved: an existing read entry (with the rest of
// the client's own tools) must never be reordered, renamed, or completed with
// tools the caller did not declare.
func TestShapeAnonymousBodyPreservesDeclaredTools(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"grep"}},{"type":"function","function":{"name":"read"}}]}`)
	shaped := shapeAnonymousBody(original, wire.Chat)
	payload := decodeBody(t, shaped)
	names := shapedToolNames(t, payload)
	if len(names) != 2 || names[0] != "grep" || names[1] != "read" {
		t.Fatalf("declared profile rewritten: %v", names)
	}
}

// Foreign harnesses ship tool walls without a "read" entry; the gate rejects
// them, so a single read declaration is appended and the declared tools stay
// untouched.
func TestShapeAnonymousBodyAppendsMissingRead(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"bash"}}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat))
	names := shapedToolNames(t, payload)
	if len(names) != 2 || names[0] != "bash" || names[1] != "read" {
		t.Fatalf("appended profile = %v, want [bash read]", names)
	}
}

// The title generator's no-tools profile is one of the two shapes the gate
// admits, and stamping tools onto it would turn it into a rejected body.
func TestShapeAnonymousBodyLeavesTitleProfileToolFree(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"system","content":"You are a title generator. You output ONLY a thread title. Nothing else.\n\n<task>\nGenerate a brief title.\n"},{"role":"user","content":"hi"}]}`)
	shaped := shapeAnonymousBody(original, wire.Chat)
	payload := decodeBody(t, shaped)
	if tools, exists := payload["tools"]; exists {
		t.Fatalf("tools stamped onto the title profile: %v", tools)
	}
}

// A title-profile body that already carries tools is left exactly as it is:
// the official client never emits that shape, and rewriting another caller's
// tools would change the response semantics.
func TestShapeAnonymousBodyLeavesTitleProfileToolsUntouched(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"system","content":"You are a title generator."},{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"bash"}}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat))
	names := shapedToolNames(t, payload)
	if len(names) != 1 || names[0] != "bash" {
		t.Fatalf("title-profile tools rewritten: %v", names)
	}
}

// The marker only counts inside system/developer prompts. A user message that
// happens to mention the phrase must not suppress the agent stamp.
func TestShapeAnonymousBodyIgnoresMarkerInUserMessages(t *testing.T) {
	original := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"write a title generator"}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat))
	if names := shapedToolNames(t, payload); len(names) != 1 || names[0] != "read" {
		t.Fatalf("stamped tools = %v, want [read]", names)
	}
}

func TestShapeAnonymousBodyForcesStreamingAndUsage(t *testing.T) {
	original := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	payload := decodeBody(t, shapeAnonymousBody(original, wire.Chat))
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
	if shaped := shapeAnonymousBody(original, wire.SystemOne); string(shaped) != string(original) {
		t.Fatalf("System One payload rewritten: %s", shaped)
	}
}

// Anthropic-shaped bodies use a top-level system prompt and input_schema
// tools; both the title detection and the stamped tool follow that shape.
func TestShapeAnonymousBodyBridgesAnthropicProfile(t *testing.T) {
	agent := []byte(`{"model":"m","stream":true,"system":"You are a harness.","messages":[{"role":"user","content":"hi"}]}`)
	payload := decodeBody(t, shapeAnonymousBody(agent, wire.Anthropic))
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("anthropic tools = %v", payload["tools"])
	}
	entry := tools[0].(map[string]any)
	if entry["name"] != "read" || entry["input_schema"] == nil {
		t.Fatalf("anthropic read tool malformed: %v", entry)
	}

	title := []byte(`{"model":"m","stream":true,"system":"You are a title generator.","messages":[{"role":"user","content":"hi"}]}`)
	payload = decodeBody(t, shapeAnonymousBody(title, wire.Anthropic))
	if tools, exists := payload["tools"]; exists {
		t.Fatalf("tools stamped onto the anthropic title profile: %v", tools)
	}
}

func TestEnsureAnonymousToolsNoOpWhenReadDeclared(t *testing.T) {
	payload := map[string]any{"tools": []any{map[string]any{
		"type":     "function",
		"function": map[string]any{"name": "read"},
	}}}
	if ensureAnonymousTools(payload, wire.Chat) {
		t.Fatal("read already declared; shaping must be a no-op")
	}
	tools := payload["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tool list mutated: %v", tools)
	}
}
