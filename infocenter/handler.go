package infocenter

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Handler struct {
	manager      *TopicManager
	sseTimeout   time.Duration
	maxBodyBytes int64
	logger       *slog.Logger
}

func NewHandler(m *TopicManager, sseTimeout time.Duration, maxBodyBytes int64, logger *slog.Logger) *Handler {
	return &Handler{
		manager:      m,
		sseTimeout:   sseTimeout,
		maxBodyBytes: maxBodyBytes,
		logger:       logger,
	}
}

func (h *Handler) HandlePublish(w http.ResponseWriter, r *http.Request, topicName string) {
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		h.logger.Error("failed to read body", "topic", topicName, "err", err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	h.manager.Publish(topicName, string(body))

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) HandleSubscribe(w http.ResponseWriter, r *http.Request, topicName string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	flusher, ok := w.(http.Flusher)
	if !ok {
		h.logger.Error("streaming unsupported")
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	flusher.Flush()

	sub := h.manager.Subscribe(topicName)
	defer h.manager.Unsubscribe(topicName, sub)

	defer func() {
		if n := sub.Dropped(); n > 0 {
			h.logger.Warn("slow consumer dropped messages", "topic", topicName, "dropped", n)
		}
	}()

	connectionStart := time.Now()

	timeout := time.NewTimer(h.sseTimeout)

	defer timeout.Stop()

	for {
		select {
		case msg := <-sub.ch:
			lines := strings.Split(msg.Data, "\n")

			fmt.Fprintf(w, "id: %d\nevent: msg\n", msg.ID)

			for _, line := range lines {
				fmt.Fprintf(w, "data: %s\n", line)
			}

			fmt.Fprint(w, "\n")
			flusher.Flush()
		case <-timeout.C:
			elapsed := time.Since(connectionStart).Round(time.Second)
			fmt.Fprintf(w, "event: timeout\ndata: %s\n\n", elapsed)
			flusher.Flush()
			return
		case <-r.Context().Done():
			return
		}
	}

}
