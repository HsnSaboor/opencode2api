package protocol

import (
	"encoding/json"
	"strings"

	"opencode2api/internal/jsonutil"
)

// RepairChatToolCallArguments rewrites assistant tool call arguments in a
// Chat-shaped request body so that every call carries a string that parses to
// a JSON object, which is the shape strict upstreams validate.
//
// Client histories can acquire malformed arguments when a stream is cut while
// a tool call is being generated: the partial JSON fragment is persisted and
// replayed on every later request, and an upstream that validates the field
// rejects the whole conversation ("Assistant tool call function.arguments must
// be a JSON object"). Repairing the value degrades that one call to empty
// arguments instead of failing every request that carries the history. It
// returns the number of calls that were rewritten.
func RepairChatToolCallArguments(body map[string]any) int {
	repaired := 0
	for _, raw := range jsonutil.SliceAt(body, "messages") {
		message, ok := raw.(map[string]any)
		if !ok || jsonutil.StringAt(message, "role") != "assistant" {
			continue
		}
		for _, rawCall := range jsonutil.SliceAt(message, "tool_calls") {
			call, ok := rawCall.(map[string]any)
			if !ok {
				continue
			}
			function := jsonutil.MapAt(call, "function")
			if function == nil {
				continue
			}
			normalized, changed := normalizedToolArguments(function["arguments"])
			if !changed {
				continue
			}
			function["arguments"] = normalized
			repaired++
		}
	}
	return repaired
}

// normalizedToolArguments maps one arguments value onto the wire shape strict
// upstreams expect: a string whose content is a JSON object. A valid object
// string is left alone; an object value is serialized; anything else —
// truncated fragments, empty strings, null, numbers, arrays — collapses to an
// empty object. The second result reports whether a rewrite is needed.
func normalizedToolArguments(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		if isJSONObjectString(typed) {
			return typed, false
		}
		return "{}", true
	case map[string]any:
		data, err := json.Marshal(typed)
		if err != nil {
			return "{}", true
		}
		return string(data), true
	default:
		return "{}", true
	}
}

// isJSONObjectString reports whether the value is a string that parses to a
// JSON object.
func isJSONObjectString(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	var parsed any
	if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return false
	}
	_, ok := parsed.(map[string]any)
	return ok
}
