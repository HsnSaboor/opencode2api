package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"opencode2api/internal/httpx"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

const maxUpstreamResponseBody = 64 << 20

var (
	// errUpstreamNoAssistantContent marks a 2xx that carried no usable
	// assistant payload. It is retryable inside the request: another key or
	// lane may answer with real content, but the client must never receive
	// the empty 2xx itself.
	errUpstreamNoAssistantContent = errors.New("upstream returned 2xx without assistant content")
	// errUpstreamInvalidContent marks a 2xx whose body could not be used at
	// all (non-streaming reply to a streaming request, truncated body, or an
	// SSE stream that could not be collapsed).
	errUpstreamInvalidContent = errors.New("upstream returned unusable content")
	// errUpstreamUnavailable marks an exhausted pool with nothing to try, as
	// opposed to a transport or upstream failure.
	errUpstreamUnavailable = errors.New("no usable upstream capacity")
)

func eventStreamResponse(resp *http.Response) bool {
	return resp != nil && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// validateUpstreamSuccess checks one 2xx before any retry loop accepts it.
// For a streaming client it only verifies that the response is an event
// stream: doInferenceAttempt already waited for the first real delta without
// committing downstream headers. For a non-streaming client it reads the
// whole body, collapses a forced SSE response into the single JSON document
// the client asked for, and rejects anything without a real assistant payload.
// The body is left readable for the caller on success.
func validateUpstreamSuccess(resp *http.Response, route models.Route, stream bool) (string, error) {
	if resp == nil || resp.StatusCode/100 != 2 || route.Protocol == wire.SystemOne {
		return "", nil
	}
	if stream {
		if !eventStreamResponse(resp) {
			httpx.DrainAndClose(resp.Body)
			return "non_streaming_2xx", fmt.Errorf("%w: streaming request answered with %q", errUpstreamInvalidContent, resp.Header.Get("Content-Type"))
		}
		return "", nil
	}
	if resp.Body == nil {
		return "empty_body", errUpstreamNoAssistantContent
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBody))
	httpx.DrainAndClose(resp.Body)
	if err != nil {
		return "truncated_body", fmt.Errorf("%w: %v", errUpstreamInvalidContent, err)
	}
	if eventStreamResponse(resp) {
		collapsed, collapseErr := wire.CollapseStream(bytes.NewReader(body), route.Protocol, route.ID)
		if collapseErr != nil {
			return "collapse_failed", fmt.Errorf("%w: %v", errUpstreamInvalidContent, collapseErr)
		}
		body = collapsed
	}
	if !wire.HasAssistantContent(route.Protocol, body) {
		return "empty_content", errUpstreamNoAssistantContent
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	if resp.Header == nil {
		resp.Header = http.Header{}
	}
	resp.Header.Set("Content-Type", "application/json")
	return "", nil
}
