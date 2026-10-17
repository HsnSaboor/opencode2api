package protocol

import (
	"encoding/json"
	"testing"
)

// A tool result that carries an image must not serialize the image into the
// Chat tool message: the picture rides in a follow-up user message instead.
func TestChatToolResultSplitsImagesIntoUserMessage(t *testing.T) {
	data := "data:image/png;base64,AAAABBBB"
	request := bridgeRequest{
		Model: "m",
		Messages: []bridgeMessage{
			{Role: "assistant", Blocks: []bridgeBlock{{Kind: "tool_call", ID: "call_1", Name: "view_image", ArgumentsJSON: `{"path":"/tmp/x.png"}`}}},
			{Role: "user", Blocks: []bridgeBlock{{Kind: "tool_result", CallID: "call_1", Result: []any{
				map[string]any{"type": "input_image", "image_url": data},
				map[string]any{"type": "input_text", "text": "image attached"},
			}}}},
		},
	}
	encoded, err := encodeChatRequest(request)
	if err != nil {
		t.Fatalf("encodeChatRequest: %v", err)
	}
	messages := encoded["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("want 3 messages (assistant, tool, user attachment), got %d: %s", len(messages), mustJSON(t, messages))
	}
	tool := messages[1].(map[string]any)
	if tool["role"] != "tool" || tool["tool_call_id"] != "call_1" {
		t.Fatalf("tool message malformed: %s", mustJSON(t, tool))
	}
	if content, _ := tool["content"].(string); content != "image attached" {
		t.Fatalf("tool content = %q, want text only", content)
	}
	user := messages[2].(map[string]any)
	if user["role"] != "user" {
		t.Fatalf("attachment message role = %v", user["role"])
	}
	parts := user["content"].([]any)
	if len(parts) != 1 {
		t.Fatalf("attachment parts = %s", mustJSON(t, parts))
	}
	part := parts[0].(map[string]any)
	img, _ := part["image_url"].(map[string]any)
	if part["type"] != "image_url" || img["url"] != data {
		t.Fatalf("attachment part = %s", mustJSON(t, part))
	}
}

// An image-only tool result still produces a non-empty tool message content so
// text-only upstreams accept the transcript.
func TestChatToolResultImageOnlyUsesPlaceholder(t *testing.T) {
	request := bridgeRequest{
		Model: "m",
		Messages: []bridgeMessage{
			{Role: "assistant", Blocks: []bridgeBlock{{Kind: "tool_call", ID: "call_1", Name: "view_image", ArgumentsJSON: "{}"}}},
			{Role: "user", Blocks: []bridgeBlock{{Kind: "tool_result", CallID: "call_1", Result: []any{
				map[string]any{"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
			}}}},
		},
	}
	encoded, err := encodeChatRequest(request)
	if err != nil {
		t.Fatalf("encodeChatRequest: %v", err)
	}
	messages := encoded["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("want 3 messages, got %d", len(messages))
	}
	tool := messages[1].(map[string]any)
	if content, _ := tool["content"].(string); content == "" {
		t.Fatalf("image-only tool result must keep a non-empty content")
	}
}

// Text-only tool results keep their historical behavior: one tool message,
// no extra user message, and no JSON re-encoding.
func TestChatToolResultKeepsTextOnlyResults(t *testing.T) {
	request := bridgeRequest{
		Model: "m",
		Messages: []bridgeMessage{
			{Role: "assistant", Blocks: []bridgeBlock{{Kind: "tool_call", ID: "call_1", Name: "exec", ArgumentsJSON: "{}"}}},
			{Role: "user", Blocks: []bridgeBlock{{Kind: "tool_result", CallID: "call_1", Result: []any{
				map[string]any{"type": "text", "text": "line one\n"},
				map[string]any{"type": "text", "text": "line two"},
			}}}},
		},
	}
	encoded, err := encodeChatRequest(request)
	if err != nil {
		t.Fatalf("encodeChatRequest: %v", err)
	}
	messages := encoded["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("want 2 messages, got %d: %s", len(messages), mustJSON(t, messages))
	}
	tool := messages[1].(map[string]any)
	if content, _ := tool["content"].(string); content != "line one\nline two" {
		t.Fatalf("tool content = %q", content)
	}
}

// Anthropic-shaped image blocks inside a tool result take the same path.
func TestChatToolResultAcceptsAnthropicImageShape(t *testing.T) {
	request := bridgeRequest{
		Model: "m",
		Messages: []bridgeMessage{
			{Role: "assistant", Blocks: []bridgeBlock{{Kind: "tool_call", ID: "call_1", Name: "Read", ArgumentsJSON: "{}"}}},
			{Role: "user", Blocks: []bridgeBlock{{Kind: "tool_result", CallID: "call_1", Result: []any{
				map[string]any{"type": "text", "text": "screenshot"},
				map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "AAAA"}},
			}}}},
		},
	}
	encoded, err := encodeChatRequest(request)
	if err != nil {
		t.Fatalf("encodeChatRequest: %v", err)
	}
	messages := encoded["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("want 3 messages, got %d: %s", len(messages), mustJSON(t, messages))
	}
	part := messages[2].(map[string]any)["content"].([]any)[0].(map[string]any)
	img, _ := part["image_url"].(map[string]any)
	if img["url"] != "data:image/png;base64,AAAA" {
		t.Fatalf("attachment part = %s", mustJSON(t, part))
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
