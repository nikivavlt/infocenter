package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nikivavlt/infocenter/infocenter"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := loadConfig()
	mux := http.NewServeMux()

	manager := infocenter.NewTopicManager(cfg.ChannelBufferSize)
	handler := infocenter.NewHandler(manager, cfg.SSETimeout, cfg.MaxBodyBytes, logger)

	mux.HandleFunc("/infocenter/", func(w http.ResponseWriter, r *http.Request) {
		topicName := strings.TrimPrefix(r.URL.Path, "/infocenter/")

		if topicName == "" {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodPost:
			handler.HandlePublish(w, r, topicName)
		case http.MethodGet:
			handler.HandleSubscribe(w, r, topicName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // NOTE: Disabled - will kill SSE connections
		IdleTimeout:  120 * time.Second,
		BaseContext:  func (net.Listener) context.Context { return baseCtx },
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("server starting", "port", cfg.Port)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-stop
	cancelBase()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "err", err)
	} else {
		slog.Info("server stopped")
	}
}
