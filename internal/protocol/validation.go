package protocol

import (
	"encoding/json"
	"strings"

	"opencode2api/internal/jsonutil"
)

// HasAssistantContent reports whether body is a parseable response document
// carrying at least one real assistant payload. A 2xx is only usable when this
// returns true: error envelopes, empty bodies, empty choice lists, and
// whitespace-only text all fail the check. System One payloads are opaque and
// are never routed through this validator.
func HasAssistantContent(protocol Protocol, body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	if documentError(payload) != "" {
		return false
	}
	switch protocol {
	case Chat:
		return chatDocumentReal(payload)
	case Responses:
		return responsesDocumentReal(payload)
	case Anthropic:
		return anthropicDocumentReal(payload)
	default:
		return false
	}
}

// IsAssistantDelta reports whether one SSE data payload carries a real
// assistant delta: visible content, reasoning, or a tool call. Role-only,
// lifecycle, usage, and empty deltas return false.
func IsAssistantDelta(protocol Protocol, eventName, data string) bool {
	if data == "" || data == "[DONE]" {
		return false
	}
	var payload map[string]any
	if json.Unmarshal([]byte(data), &payload) != nil {
		return false
	}
	if documentError(payload) != "" {
		return false
	}
	switch protocol {
	case Chat:
		return chatDeltaReal(payload)
	case Responses:
		return responsesDeltaReal(payload, eventName)
	case Anthropic:
		return anthropicDeltaReal(payload, eventName)
	default:
		return false
	}
}

// StreamEventError extracts an upstream error carried by one SSE event. The
// caller uses it to abandon an attempt before any content arrives instead of
// treating the error frame as a normal lifecycle event.
func StreamEventError(protocol Protocol, eventName, data string) (string, bool) {
	if data == "" || data == "[DONE]" {
		return "", false
	}
	var payload map[string]any
	if json.Unmarshal([]byte(data), &payload) != nil {
		return "", false
	}
	if eventName == "error" || jsonutil.StringAt(payload, "type") == "error" {
		if message := validationErrorMessage(payload); message != "" {
			return message, true
		}
		return "upstream stream error", true
	}
	if message := documentError(payload); message != "" {
		return message, true
	}
	if strings.HasSuffix(jsonutil.StringAt(payload, "type"), ".failed") {
		if message := jsonutil.StringAt(payload, "response", "error", "message"); message != "" {
			return message, true
		}
		return "upstream response failed", true
	}
	return "", false
}

func documentError(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if message := jsonutil.StringAt(payload, "error", "message"); message != "" {
		return message
	}
	if message, ok := payload["error"].(string); ok && strings.TrimSpace(message) != "" {
		return message
	}
	if payload["error"] != nil && jsonutil.StringAt(payload, "type") == "error" {
		return "upstream response failed"
	}
	return ""
}

func validationErrorMessage(payload map[string]any) string {
	if message := jsonutil.StringAt(payload, "error", "message"); message != "" {
		return message
	}
	if message, ok := payload["error"].(string); ok && strings.TrimSpace(message) != "" {
		return message
	}
	if message := jsonutil.StringAt(payload, "message"); message != "" {
		return message
	}
	return ""
}

func nonEmptyText(value any) bool {
	text, ok := value.(string)
	return ok && strings.TrimSpace(text) != ""
}

func textPartsReal(value any) bool {
	parts, ok := value.([]any)
	if !ok {
		return false
	}
	for _, part := range parts {
		if nonEmptyText(part) {
			return true
		}
		block, _ := part.(map[string]any)
		if block == nil {
			continue
		}
		if nonEmptyText(block["text"]) || nonEmptyText(block["content"]) || nonEmptyText(block["refusal"]) {
			return true
		}
	}
	return false
}

func toolCallsReal(value any) bool {
	if item, ok := value.(map[string]any); ok {
		return toolCallReal(item)
	}
	calls, ok := value.([]any)
	if !ok || len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		item, _ := call.(map[string]any)
		if toolCallReal(item) {
			return true
		}
	}
	return false
}

func toolCallReal(item map[string]any) bool {
	if item == nil {
		return false
	}
	function, _ := item["function"].(map[string]any)
	if nonEmptyText(item["id"]) || nonEmptyText(item["call_id"]) || nonEmptyText(item["name"]) ||
		nonEmptyText(item["arguments"]) || nonEmptyText(item["input"]) {
		return true
	}
	return function != nil && (nonEmptyText(function["name"]) || nonEmptyText(function["arguments"]))
}

func chatMessageReal(message map[string]any) bool {
	if message == nil {
		return false
	}
	if nonEmptyText(message["content"]) || textPartsReal(message["content"]) {
		return true
	}
	if nonEmptyText(message["reasoning_content"]) || nonEmptyText(message["reasoning"]) {
		return true
	}
	return toolCallsReal(message["tool_calls"]) || toolCallsReal(message["function_call"])
}

func chatDocumentReal(payload map[string]any) bool {
	choices := jsonutil.SliceAt(payload, "choices")
	for _, choice := range choices {
		item, _ := choice.(map[string]any)
		if item == nil {
			continue
		}
		if chatMessageReal(jsonutil.MapAt(item, "message")) || nonEmptyText(item["text"]) {
			return true
		}
	}
	return false
}

func chatDeltaReal(payload map[string]any) bool {
	choices := jsonutil.SliceAt(payload, "choices")
	for _, choice := range choices {
		item, _ := choice.(map[string]any)
		if item == nil {
			continue
		}
		if chatMessageReal(jsonutil.MapAt(item, "delta")) || chatMessageReal(jsonutil.MapAt(item, "message")) || nonEmptyText(item["text"]) {
			return true
		}
	}
	return false
}

func responsesItemReal(item map[string]any) bool {
	if item == nil {
		return false
	}
	if nonEmptyText(item["text"]) || nonEmptyText(item["content"]) || textPartsReal(item["content"]) {
		return true
	}
	switch jsonutil.StringAt(item, "type") {
	case "function_call", "custom_tool_call", "local_shell_call":
		if nonEmptyText(item["name"]) || nonEmptyText(item["arguments"]) || nonEmptyText(item["input"]) ||
			nonEmptyText(item["call_id"]) || nonEmptyText(item["id"]) {
			return true
		}
	case "reasoning":
		for _, summary := range jsonutil.SliceAt(item, "summary") {
			block, _ := summary.(map[string]any)
			if nonEmptyText(jsonutil.AnyAt(block, "text")) {
				return true
			}
		}
		if nonEmptyText(item["encrypted_content"]) {
			return true
		}
	}
	return false
}

func responsesDocumentReal(payload map[string]any) bool {
	if nonEmptyText(payload["output_text"]) {
		return true
	}
	output := jsonutil.SliceAt(payload, "output")
	for _, item := range output {
		block, _ := item.(map[string]any)
		if responsesItemReal(block) {
			return true
		}
	}
	return false
}

func responsesDeltaReal(payload map[string]any, eventName string) bool {
	eventType := jsonutil.StringAt(payload, "type")
	if eventType == "" {
		eventType = eventName
	}
	switch eventType {
	case "response.output_text.delta", "response.refusal.delta",
		"response.reasoning_summary_text.delta", "response.reasoning_text.delta",
		"response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		return nonEmptyText(payload["delta"])
	case "response.output_text.done", "response.refusal.done",
		"response.reasoning_summary_text.done", "response.reasoning_text.done":
		return nonEmptyText(payload["text"])
	case "response.output_item.added", "response.output_item.done":
		return responsesItemReal(jsonutil.MapAt(payload, "item"))
	case "response.content_part.added", "response.content_part.done":
		return textPartsReal([]any{jsonutil.MapAt(payload, "part")})
	case "response.completed", "response.incomplete":
		return responsesDocumentReal(jsonutil.MapAt(payload, "response"))
	default:
		return false
	}
}

func anthropicBlockReal(block map[string]any) bool {
	if block == nil {
		return false
	}
	switch jsonutil.StringAt(block, "type") {
	case "text":
		return nonEmptyText(block["text"])
	case "thinking":
		return nonEmptyText(block["thinking"])
	case "redacted_thinking":
		return nonEmptyText(block["data"])
	case "tool_use", "server_tool_use":
		return nonEmptyText(block["id"]) || nonEmptyText(block["name"]) || block["input"] != nil
	default:
		return nonEmptyText(block["text"]) || nonEmptyText(block["thinking"])
	}
}

func anthropicContentReal(value any) bool {
	if nonEmptyText(value) {
		return true
	}
	blocks, ok := value.([]any)
	if !ok {
		return false
	}
	for _, block := range blocks {
		item, _ := block.(map[string]any)
		if anthropicBlockReal(item) {
			return true
		}
	}
	return false
}

func anthropicDocumentReal(payload map[string]any) bool {
	return anthropicContentReal(payload["content"])
}

func anthropicDeltaReal(payload map[string]any, eventName string) bool {
	eventType := jsonutil.StringAt(payload, "type")
	if eventType == "" {
		eventType = eventName
	}
	switch eventType {
	case "content_block_delta":
		delta := jsonutil.MapAt(payload, "delta")
		switch jsonutil.StringAt(delta, "type") {
		case "text_delta":
			return nonEmptyText(delta["text"])
		case "thinking_delta":
			return nonEmptyText(delta["thinking"])
		case "input_json_delta":
			return nonEmptyText(delta["partial_json"])
		default:
			return nonEmptyText(delta["text"]) || nonEmptyText(delta["thinking"]) || nonEmptyText(delta["partial_json"])
		}
	case "content_block_start":
		return anthropicBlockReal(jsonutil.MapAt(payload, "content_block"))
	case "message_start":
		return anthropicContentReal(jsonutil.AnyAt(payload, "message", "content"))
	default:
		return false
	}
}
