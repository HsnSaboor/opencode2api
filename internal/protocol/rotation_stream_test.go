package protocol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rotationReady(w http.ResponseWriter) func() error {
	return func() error {
		w.Header().Set("Content-Type", "text/event-stream")
		return nil
	}
}

func TestRotationStreamDoesNotCommitRoleOnlyStream(t *testing.T) {
	rec := httptest.NewRecorder()
	stream := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n"
	_, _, committed, err := RotationStream(context.Background(), rec, strings.NewReader(stream), Chat, Chat, "alias", rotationReady(rec))
	if committed {
		t.Fatal("committed = true, want false for a role-only stream")
	}
	if err == nil {
		t.Fatal("err = nil, want an empty-content failure")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("downstream body = %q, want nothing before a real delta", rec.Body.String())
	}
}

func TestRotationStreamCommitsRealContent(t *testing.T) {
	rec := httptest.NewRecorder()
	stream := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"real\"}}]}\n\ndata: [DONE]\n\n"
	_, _, committed, err := RotationStream(context.Background(), rec, strings.NewReader(stream), Chat, Chat, "alias", rotationReady(rec))
	if err != nil {
		t.Fatalf("RotationStream: %v", err)
	}
	if !committed {
		t.Fatal("committed = false, want true after a content delta")
	}
	if !strings.Contains(rec.Body.String(), "real") {
		t.Fatalf("downstream body = %q, want the content delta", rec.Body.String())
	}
}
