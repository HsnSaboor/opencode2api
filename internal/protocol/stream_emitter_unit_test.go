package protocol

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// A chat stream that fails mid-flight must still end in a shape an OpenAI
// client can parse: a terminal chunk followed by [DONE]. A bare {"error":...}
// data line is not a chat.completion.chunk and strict clients reject it as an
// invalid stream event, aborting the whole session step mid-turn.
func TestChatStreamFailureEndsWithTerminalChunk(t *testing.T) {
	recorder := httptest.NewRecorder()
	emitter := newBridgeStreamEmitter(recorder, recorder, Chat, "test-model")
	if err := emitter.Emit(bridgeStreamEvent{
		Kind:      "error",
		Error:     "upstream stream failed",
		ErrorType: "upstream_error",
	}); err == nil {
		t.Fatal("Emit returned nil for an error event")
	}
	body := recorder.Body.String()
	if strings.Contains(body, `"error":{`) {
		t.Fatalf("chat stream carried a bare error frame: %s", body)
	}
	if !strings.Contains(body, "[gateway] upstream stream failed mid-response: upstream stream failed") {
		t.Fatalf("chat stream did not report the failure as content: %s", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) {
		t.Fatalf("chat stream missing terminal chunk: %s", body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("chat stream missing [DONE] terminator: %q", body)
	}
}

// No OpenAI chat delta carries a reasoning signature, so the chat target must
// not invent one.
func TestChatCarriesNoReasoningSignature(t *testing.T) {
	recorder := httptest.NewRecorder()
	emitter := newBridgeStreamEmitter(recorder, recorder, Chat, "test-model")
	if err := emitter.emitReasoningSignature("sig-1"); err != nil {
		t.Fatalf("emitReasoningSignature: %v", err)
	}
	if body := recorder.Body.String(); strings.Contains(body, "reasoning_signature") {
		t.Fatalf("chat stream carried a reasoning signature: %s", body)
	}
}
