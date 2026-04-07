package infocenter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestHandler(m *TopicManager, timeout time.Duration) *Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(m, timeout, 1 << 20, logger)
}

func TestHandlePublishReturns204(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 30 * time.Second)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
	w := httptest.NewRecorder()

	h.HandlePublish(w, req, "news")

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestHandlePublishBodyTooLarge(t *testing.T) {
	m := NewTopicManager(16)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(m, 30 * time.Second, 4, logger)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
	w := httptest.NewRecorder()

	h.HandlePublish(w, req, "news")

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestHandlePublishReadErrorReturns500(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 30 * time.Second)

	req := httptest.NewRequest(http.MethodPost, "/", errReader{err: errors.New("disk error")})
	w := httptest.NewRecorder()

	h.HandlePublish(w, req, "news")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleSubscribeSSEHeaders(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 50 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.HandleSubscribe(w, req, "news")

	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("expected no-cache, got %q", cc)
	}
}

func TestHandleSubscribeReceivesMessage(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.HandleSubscribe(w, req, "news")
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if m.subscriberCount("news") > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	m.Publish("news", "hello")
	cancel()
	<-done

	body := w.Body.String()
	if !strings.Contains(body, "event: msg") {
		t.Fatalf("expected SSE msg event, got: %q", body)
	}
	if !strings.Contains(body, "data: hello") {
		t.Fatalf("expected data 'hello', got: %q", body)
	}
}

func TestHandleSubscribeTimeoutEvent(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 50 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.HandleSubscribe(w, req, "news")

	body := w.Body.String()
	if !strings.Contains(body, "event: timeout") {
		t.Fatalf("expected timeout event, got: %q", body)
	}
}

func TestHandleSubscribeClientDisconnectCleansUpTopic(t *testing.T) {
	m := NewTopicManager(16)
	h := newTestHandler(m, 30 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.HandleSubscribe(w, req, "news")
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if m.subscriberCount("news") > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-done

	if m.topicExists("news") {
		t.Fatal("topic should be cleaned up after last subscriber disconnects")
	}
}
