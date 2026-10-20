package gateway

import (
	"testing"

	wire "opencode2api/internal/protocol"
)

func toolNames(tools []any) map[string]bool {
	names := make(map[string]bool, len(tools))
	for _, item := range tools {
		if tool, ok := item.(map[string]any); ok {
			if name, ok := tool["name"].(string); ok && name != "" {
				names[name] = true
			}
		}
	}
	return names
}

func hasToolType(tools []any, want string) bool {
	for _, item := range tools {
		if tool, ok := item.(map[string]any); ok {
			if toolType, ok := tool["type"].(string); ok && toolType == want {
				return true
			}
		}
	}
	return false
}

func codexToolInput() map[string]any {
	return map[string]any{"tools": []any{
		map[string]any{"type": "custom", "name": "exec", "description": "Run JS", "format": map[string]any{"type": "text"}},
		map[string]any{"type": "function", "name": "wait"},
		map[string]any{"type": "web_search", "external_web_access": false, "search_content_types": []any{"text", "image"}},
		map[string]any{"type": "namespace", "name": "multi_agent_v1"},
	}}
}

func TestSanitizeToolsForUpstreamRewritesCodexDeclarations(t *testing.T) {
	tools, changed := sanitizeToolsForUpstream(codexToolInput(), wire.Responses)
	if !changed {
		t.Fatal("expected declarations to change")
	}
	names := toolNames(tools)
	for _, want := range []string{"wait", "exec_command", "write_stdin"} {
		if !names[want] {
			t.Fatalf("missing %q in %v", want, names)
		}
	}
	if !hasToolType(tools, "web_search") {
		t.Fatal("web_search declaration must survive for Responses targets")
	}
	for _, unwanted := range []string{"exec", "multi_agent_v1"} {
		if names[unwanted] {
			t.Fatalf("%q should have been dropped", unwanted)
		}
	}
	for _, item := range tools {
		tool := item.(map[string]any)
		if _, present := tool["search_content_types"]; present {
			t.Fatal("search_content_types must not reach the upstream")
		}
	}
}

func TestSanitizeToolsForUpstreamLeavesInputUntouched(t *testing.T) {
	input := codexToolInput()
	if _, changed := sanitizeToolsForUpstream(input, wire.Responses); !changed {
		t.Fatal("expected declarations to change")
	}
	names := toolNames(input["tools"].([]any))
	if !names["exec"] || names["exec_command"] {
		t.Fatalf("input payload must not be modified: %v", names)
	}
}

func TestSanitizeToolsForUpstreamDropsWebSearchForChat(t *testing.T) {
	tools, changed := sanitizeToolsForUpstream(codexToolInput(), wire.Chat)
	if !changed {
		t.Fatal("expected declarations to change")
	}
	names := toolNames(tools)
	if names["web_search"] || names["exec"] {
		t.Fatalf("chat target must not carry web_search or custom tools: %v", names)
	}
	if !names["exec_command"] || !names["write_stdin"] {
		t.Fatalf("unified exec tools missing for chat target: %v", names)
	}
}

func TestSanitizeToolsForUpstreamKeepsPlainFunctions(t *testing.T) {
	input := map[string]any{"tools": []any{
		map[string]any{"type": "function", "name": "read"},
		map[string]any{"type": "web_search_preview", "external_web_access": false},
	}}
	if tools, changed := sanitizeToolsForUpstream(input, wire.Responses); changed || tools != nil {
		t.Fatalf("plain declarations must pass through: changed=%v tools=%v", changed, tools)
	}
}

func TestSanitizeToolsForUpstreamAvoidsDuplicateUnifiedExec(t *testing.T) {
	input := map[string]any{"tools": []any{
		map[string]any{"type": "custom", "name": "exec"},
		map[string]any{"type": "function", "name": "write_stdin"},
	}}
	tools, changed := sanitizeToolsForUpstream(input, wire.Responses)
	if !changed {
		t.Fatal("expected declarations to change")
	}
	counts := map[string]int{}
	for _, item := range tools {
		if name, ok := item.(map[string]any)["name"].(string); ok {
			counts[name]++
		}
	}
	if counts["exec_command"] != 1 || counts["write_stdin"] != 1 {
		t.Fatalf("unified exec declarations must be unique: %v", counts)
	}
}

func TestUnifiedExecToolDeclsParse(t *testing.T) {
	decls := unifiedExecTools()
	if len(decls) != 2 {
		t.Fatalf("declarations = %d, want 2", len(decls))
	}
	names := toolNames(decls)
	if !names["exec_command"] || !names["write_stdin"] {
		t.Fatalf("unexpected declaration names: %v", names)
	}
}
