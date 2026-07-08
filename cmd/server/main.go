package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/worker"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	id := resolveWorkerID()
	q := queue.New(pool)
	go worker.Run(ctx, q, id, resolvePollInterval())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("Orchestrator listening on :%s (worker %s)", port, id)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "yggdrasil-orchestrator",
	})
}

// resolveWorkerID identifies this Orchestrator replica for the queue's
// locked_by column — distinct IDs are what let multiple replicas (ADR 003)
// claim jobs concurrently without stepping on each other.
func resolveWorkerID() string {
	if id := os.Getenv("WORKER_ID"); id != "" {
		return id
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "orchestrator"
	}
	return hostname + "-" + randomSuffix()
}

func randomSuffix() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	for i := range b {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return string(b)
}

func resolvePollInterval() time.Duration {
	raw := os.Getenv("QUEUE_POLL_INTERVAL")
	if raw == "" {
		return 0 // worker.Run applies its own default
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("invalid QUEUE_POLL_INTERVAL %q, ignoring: %v", raw, err)
		return 0
	}
	return d
}
