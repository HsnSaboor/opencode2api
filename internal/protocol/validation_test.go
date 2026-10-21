package protocol

import "testing"

func TestHasAssistantContent(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		want     bool
	}{
		{
			name:     "chat text",
			protocol: Chat,
			body:     `{"choices":[{"message":{"role":"assistant","content":"hello"}}]}`,
			want:     true,
		},
		{
			name:     "chat reasoning only",
			protocol: Chat,
			body:     `{"choices":[{"message":{"reasoning_content":"thinking"}}]}`,
			want:     true,
		},
		{
			name:     "chat tool call only",
			protocol: Chat,
			body:     `{"choices":[{"message":{"tool_calls":[{"id":"call_1","function":{"name":"read","arguments":"{}"}}]}}]}`,
			want:     true,
		},
		{
			name:     "chat empty choices",
			protocol: Chat,
			body:     `{"choices":[]}`,
		},
		{
			name:     "chat whitespace content",
			protocol: Chat,
			body:     `{"choices":[{"message":{"content":"   \n"}}]}`,
		},
		{
			name:     "chat error envelope",
			protocol: Chat,
			body:     `{"error":{"message":"quota exhausted"}}`,
		},
		{
			name:     "chat empty body",
			protocol: Chat,
			body:     ``,
		},
		{
			name:     "responses output text",
			protocol: Responses,
			body:     `{"output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`,
			want:     true,
		},
		{
			name:     "responses function call",
			protocol: Responses,
			body:     `{"output":[{"type":"function_call","name":"read","arguments":"{}"}]}`,
			want:     true,
		},
		{
			name:     "responses empty output",
			protocol: Responses,
			body:     `{"output":[]}`,
		},
		{
			name:     "anthropic text block",
			protocol: Anthropic,
			body:     `{"content":[{"type":"text","text":"hello"}]}`,
			want:     true,
		},
		{
			name:     "anthropic tool use",
			protocol: Anthropic,
			body:     `{"content":[{"type":"tool_use","id":"toolu_1","name":"read","input":{}}]}`,
			want:     true,
		},
		{
			name:     "anthropic empty content",
			protocol: Anthropic,
			body:     `{"content":[]}`,
		},
		{
			name:     "anthropic whitespace text",
			protocol: Anthropic,
			body:     `{"content":[{"type":"text","text":" "}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasAssistantContent(tt.protocol, []byte(tt.body)); got != tt.want {
				t.Fatalf("HasAssistantContent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAssistantDelta(t *testing.T) {
	tests := []struct {
		name      string
		protocol  Protocol
		eventName string
		data      string
		want      bool
	}{
		{
			name:     "chat role only",
			protocol: Chat,
			data:     `{"choices":[{"delta":{"role":"assistant"}}]}`,
		},
		{
			name:     "chat content",
			protocol: Chat,
			data:     `{"choices":[{"delta":{"content":"hi"}}]}`,
			want:     true,
		},
		{
			name:     "chat reasoning",
			protocol: Chat,
			data:     `{"choices":[{"delta":{"reasoning_content":"hmm"}}]}`,
			want:     true,
		},
		{
			name:     "chat tool call",
			protocol: Chat,
			data:     `{"choices":[{"delta":{"tool_calls":[{"id":"call_1","function":{"name":"read"}}]}}]}`,
			want:     true,
		},
		{
			name:      "responses text delta",
			protocol:  Responses,
			eventName: "response.output_text.delta",
			data:      `{"type":"response.output_text.delta","delta":"hi"}`,
			want:      true,
		},
		{
			name:      "responses lifecycle",
			protocol:  Responses,
			eventName: "response.created",
			data:      `{"type":"response.created","response":{"id":"resp_1"}}`,
		},
		{
			name:      "anthropic role-only message start",
			protocol:  Anthropic,
			eventName: "message_start",
			data:      `{"type":"message_start","message":{"content":[]}}`,
		},
		{
			name:      "anthropic text delta",
			protocol:  Anthropic,
			eventName: "content_block_delta",
			data:      `{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`,
			want:      true,
		},
		{
			name:      "anthropic tool start",
			protocol:  Anthropic,
			eventName: "content_block_start",
			data:      `{"type":"content_block_start","content_block":{"type":"tool_use","id":"toolu_1","name":"read","input":{}}}`,
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAssistantDelta(tt.protocol, tt.eventName, tt.data); got != tt.want {
				t.Fatalf("IsAssistantDelta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamEventError(t *testing.T) {
	if message, ok := StreamEventError(Chat, "", `{"error":{"message":"boom"}}`); !ok || message != "boom" {
		t.Fatalf("StreamEventError() = %q, %v; want boom, true", message, ok)
	}
	if _, ok := StreamEventError(Chat, "", `{"choices":[{"delta":{"content":"hi"}}]}`); ok {
		t.Fatal("StreamEventError() reported an error for a content delta")
	}
}
