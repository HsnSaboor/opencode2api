package gateway

import (
	"net/http"
	"testing"
	"time"
)

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
