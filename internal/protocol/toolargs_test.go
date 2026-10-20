package protocol

import (
	"testing"

	"opencode2api/internal/jsonutil"
)

func TestRepairChatToolCallArguments(t *testing.T) {
	body := map[string]any{
		"model": "deepseek-v4.1-flash-free",
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "assistant", "tool_calls": []any{
				map[string]any{"id": "a", "type": "function", "function": map[string]any{"name": "read", "arguments": `{"file":"main.go"}`}},
				map[string]any{"id": "b", "type": "function", "function": map[string]any{"name": "bash", "arguments": `{"cmd":"ls`}},
				map[string]any{"id": "c", "type": "function", "function": map[string]any{"name": "grep"}},
				map[string]any{"id": "d", "type": "function", "function": map[string]any{"name": "edit", "arguments": map[string]any{"path": "x"}}},
				map[string]any{"id": "e", "type": "function", "function": map[string]any{"name": "read", "arguments": []any{1}}},
			}},
			map[string]any{"role": "tool", "tool_call_id": "a", "content": "ok"},
		},
	}
	if repaired := RepairChatToolCallArguments(body); repaired != 4 {
		t.Fatalf("repaired = %d, want 4", repaired)
	}
	cases := []struct {
		id   string
		want string
	}{
		{id: "a", want: `{"file":"main.go"}`},
		{id: "b", want: `{}`},
		{id: "c", want: `{}`},
		{id: "d", want: `{"path":"x"}`},
		{id: "e", want: `{}`},
	}
	for _, tc := range cases {
		arguments, ok := callArguments(t, body, tc.id).(string)
		if !ok {
			t.Fatalf("tool call %q arguments = %#v, want a string", tc.id, callArguments(t, body, tc.id))
		}
		if arguments != tc.want {
			t.Fatalf("tool call %q arguments = %q, want %q", tc.id, arguments, tc.want)
		}
		if !isJSONObjectString(arguments) {
			t.Fatalf("tool call %q arguments %q does not parse to a JSON object", tc.id, arguments)
		}
	}
	// Repairs are idempotent: a repaired history must not be rewritten again.
	if repaired := RepairChatToolCallArguments(body); repaired != 0 {
		t.Fatalf("second repair = %d, want 0", repaired)
	}
}

// Only assistant messages carry tool calls; anything else is left untouched so
// the repair never masks a different wire problem.
func TestRepairChatToolCallArgumentsIgnoresOtherRoles(t *testing.T) {
	body := map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "tool_calls": []any{
				map[string]any{"id": "a", "type": "function", "function": map[string]any{"name": "read", "arguments": `{"x":`}},
			}},
		},
	}
	if repaired := RepairChatToolCallArguments(body); repaired != 0 {
		t.Fatalf("repaired = %d, want 0", repaired)
	}
	message := jsonutil.SliceAt(body, "messages")[0].(map[string]any)
	call := jsonutil.SliceAt(message, "tool_calls")[0].(map[string]any)
	if got := jsonutil.StringAt(jsonutil.MapAt(call, "function"), "arguments"); got != `{"x":` {
		t.Fatalf("user tool call arguments = %q, want the original fragment", got)
	}
}

// A malformed history with no messages at all must not panic.
func TestRepairChatToolCallArgumentsEmptyBody(t *testing.T) {
	if repaired := RepairChatToolCallArguments(map[string]any{}); repaired != 0 {
		t.Fatalf("repaired = %d, want 0", repaired)
	}
	if repaired := RepairChatToolCallArguments(nil); repaired != 0 {
		t.Fatalf("repaired = %d, want 0", repaired)
	}
}

func TestIsJSONObjectString(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{value: `{}`, want: true},
		{value: ` {"a":1} `, want: true},
		{value: `[]`, want: false},
		{value: `"x"`, want: false},
		{value: `null`, want: false},
		{value: `{"a":`, want: false},
		{value: ``, want: false},
	}
	for _, tc := range cases {
		if got := isJSONObjectString(tc.value); got != tc.want {
			t.Fatalf("isJSONObjectString(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func callArguments(t *testing.T, body map[string]any, id string) any {
	t.Helper()
	for _, raw := range jsonutil.SliceAt(body, "messages") {
		message, _ := raw.(map[string]any)
		for _, rawCall := range jsonutil.SliceAt(message, "tool_calls") {
			call, _ := rawCall.(map[string]any)
			if jsonutil.StringAt(call, "id") == id {
				return jsonutil.MapAt(call, "function")["arguments"]
			}
		}
	}
	t.Fatalf("tool call %q not found", id)
	return nil
}
