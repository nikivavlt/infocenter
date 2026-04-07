package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              string
	SSETimeout        time.Duration
	MaxBodyBytes      int64
	ShutdownTimeout   time.Duration
	ChannelBufferSize int
}

func loadConfig() Config {
	return Config{
		Port:              envString("PORT", "8080"),
		SSETimeout:        envDuration("SSE_TIMEOUT", 30 * time.Second),
		MaxBodyBytes:      envInt64("MAX_BODY_BYTES", 1 << 20),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", 5 * time.Second),
		ChannelBufferSize: envInt("CHANNEL_BUFFER", 16),
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
