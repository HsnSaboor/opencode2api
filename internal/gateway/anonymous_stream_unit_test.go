package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// errReader yields prefix bytes and then fails with err, standing in for a
// proxy exit that truncates an event stream mid-flight.
type errReader struct {
	prefix []byte
	err    error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.prefix) == 0 {
		return 0, r.err
	}
	n := copy(p, r.prefix)
	r.prefix = r.prefix[n:]
	return n, nil
}

func (r *errReader) Close() error { return nil }

func sseResponse(body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       body,
	}
}

func TestAnonymousAttemptTimeout(t *testing.T) {
	tests := []struct {
		configured int
		want       time.Duration
	}{
		{0, anonymousFirstEventTimeout},
		{-1, anonymousFirstEventTimeout},
		{45, 45 * time.Second},
		{1, time.Second},
	}
	for _, tt := range tests {
		if got := anonymousAttemptTimeout(tt.configured); got != tt.want {
			t.Errorf("anonymousAttemptTimeout(%d) = %s, want %s", tt.configured, got, tt.want)
		}
	}
}

func TestAnonymousEventStream(t *testing.T) {
	tests := []struct {
		name        string
		resp        *http.Response
		contentType string
		want        bool
	}{
		{name: "nil response", resp: nil, want: false},
		{name: "sse", resp: &http.Response{StatusCode: 200}, contentType: "text/event-stream", want: true},
		{name: "sse with charset", resp: &http.Response{StatusCode: 200}, contentType: "text/event-stream; charset=utf-8", want: true},
		{name: "plain json", resp: &http.Response{StatusCode: 200}, contentType: "application/json", want: false},
		{name: "missing content type", resp: &http.Response{StatusCode: 200}, want: false},
		{name: "html error page", resp: &http.Response{StatusCode: 200}, contentType: "text/html", want: false},
	}
	for _, tt := range tests {
		if tt.contentType != "" {
			tt.resp.Header = http.Header{"Content-Type": []string{tt.contentType}}
		}
		if got := anonymousEventStream(tt.resp); got != tt.want {
			t.Errorf("%s: anonymousEventStream() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestBufferAnonymousBody(t *testing.T) {
	// Non-2xx responses are left alone.
	if reason, err := bufferAnonymousBody(&http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("x"))}, false); reason != "" || err != nil {
		t.Errorf("non-2xx: got (%q, %v), want (\"\", nil)", reason, err)
	}
	if reason, err := bufferAnonymousBody(nil, false); reason != "" || err != nil {
		t.Errorf("nil response: got (%q, %v), want (\"\", nil)", reason, err)
	}

	// A 2xx without an event stream cannot be relayed at all.
	jsonResp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}
	if reason, err := bufferAnonymousBody(jsonResp, false); reason != "non_streaming_2xx" || err != errAnonymousInvalidResponse {
		t.Errorf("json 2xx: got (%q, %v), want (non_streaming_2xx, %v)", reason, err, errAnonymousInvalidResponse)
	}

	// A streaming client gets the body untouched.
	streamResp := sseResponse(io.NopCloser(strings.NewReader("data: one\n\n")))
	if reason, err := bufferAnonymousBody(streamResp, true); reason != "" || err != nil {
		t.Errorf("streaming client: got (%q, %v), want (\"\", nil)", reason, err)
	}
	got, err := io.ReadAll(streamResp.Body)
	if err != nil || string(got) != "data: one\n\n" {
		t.Errorf("streaming client body = %q, %v; want it left readable", got, err)
	}

	// A non-streaming client needs the whole body; a complete one is buffered.
	bufferedResp := sseResponse(io.NopCloser(strings.NewReader("data: one\n\ndata: two\n\n")))
	if reason, err := bufferAnonymousBody(bufferedResp, false); reason != "" || err != nil {
		t.Fatalf("complete body: got (%q, %v), want (\"\", nil)", reason, err)
	}
	got, err = io.ReadAll(bufferedResp.Body)
	if err != nil || string(got) != "data: one\n\ndata: two\n\n" {
		t.Errorf("buffered body = %q, %v; want the original bytes", got, err)
	}

	// A truncated body is a lane failure, never a success.
	truncated := sseResponse(&errReader{prefix: []byte("data: partial"), err: io.ErrUnexpectedEOF})
	reason, err := bufferAnonymousBody(truncated, false)
	if reason != "truncated_body" || err != io.ErrUnexpectedEOF {
		t.Errorf("truncated body: got (%q, %v), want (truncated_body, %v)", reason, err, io.ErrUnexpectedEOF)
	}
}
