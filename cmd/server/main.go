package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/messages"
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

	apiInternalURL := os.Getenv("API_INTERNAL_URL")
	if apiInternalURL == "" {
		log.Fatal("API_INTERNAL_URL is required")
	}
	internalAPIToken := os.Getenv("INTERNAL_API_TOKEN")
	if internalAPIToken == "" {
		log.Fatal("INTERNAL_API_TOKEN is required")
	}
	apiClient := apiclient.New(apiInternalURL, internalAPIToken)

	id := resolveWorkerID()
	q := queue.New(pool)
	msgs := messages.New(pool)
	go worker.Run(ctx, q, worker.Config{
		WorkerID:          id,
		PollInterval:      resolvePollInterval(),
		MaxConcurrentJobs: resolveMaxConcurrentJobs(),
		Images:            resolveAgentImages(),
		PlaceholderImage:  os.Getenv("JOB_PLACEHOLDER_IMAGE"),
		PlaceholderScript: os.Getenv("JOB_PLACEHOLDER_SCRIPT"),
		RuntimeClassName:  resolveRuntimeClassName(),
		APIClient:         apiClient,
		Clusters:          worker.NewAPIClusterProvider(apiClient),
		Messages:          msgs,
		AppsDomain:        resolveWithDefault("APPS_BASE_DOMAIN", "yggdrasil.local"),
		IngressClassName:  resolveWithDefault("INGRESS_CLASS_NAME", "traefik"),
		CertIssuerName:    resolveWithDefault("CERT_ISSUER_NAME", "selfsigned-issuer"),

		MaxConcurrentPreviews: resolveMaxConcurrentPreviews(),
		PreviewTTL:            resolvePreviewTTL(),
		PreviewSweepInterval:  resolvePreviewSweepInterval(),
		RecordingMaxBytes:     resolveRecordingMaxBytes(),
	})

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

// resolveMaxConcurrentJobs reads how many jobs this replica may run at once
// (ADR 006 item 1) — unset or invalid falls back to worker.Run's own
// default rather than failing startup.
func resolveMaxConcurrentJobs() int {
	raw := os.Getenv("MAX_CONCURRENT_JOBS")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		log.Printf("invalid MAX_CONCURRENT_JOBS %q, ignoring: %v", raw, err)
		return 0
	}
	return n
}

// resolveMaxConcurrentPreviews reads the per-project cap on simultaneous
// ephemeral preview deployments (ADR 003 §17's "initial default: 3").
//
// The three cases are deliberately distinct: unset keeps the ADR's default of
// 3, an explicit 0 disables previews without disabling any other job kind, and
// a negative value is treated as "off" too. Running with previews off is the
// supported way to deploy an Orchestrator against a database whose migrations
// predate the previews table, since the cap is what makes the claim query
// reference it.
func resolveMaxConcurrentPreviews() int {
	raw := os.Getenv("MAX_CONCURRENT_PREVIEWS")
	if raw == "" {
		return 0 // worker.Config applies the ADR 003 §17 default
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid MAX_CONCURRENT_PREVIEWS %q, ignoring: %v", raw, err)
		return 0
	}
	return n
}

// resolvePreviewTTL bounds how long a preview may outlive its job when nothing
// else knows the job is gone; resolvePreviewSweepInterval is how often the
// orphan sweep runs. Zero means "use the worker's default".
func resolvePreviewTTL() time.Duration {
	return resolveDuration("PREVIEW_TTL")
}

// resolveRecordingMaxBytes bounds the screen recording this replica will
// collect from a finished test_run pod (ADR 029).
//
// Three cases, and the middle one is what makes this different from
// resolveMaxConcurrentPreviews' "0 means take the default": unset keeps ADR
// 029's default; an explicit 0 or negative **disables** collection (the
// supported way to run without the feature, and what a deployment whose API
// predates the recordings table should set); and an unparseable value falls
// back to the default rather than to "off", because a typo in an env var must
// not silently drop every artifact.
func resolveRecordingMaxBytes() int64 {
	raw := os.Getenv("RECORDING_MAX_BYTES")
	if raw == "" {
		return worker.DefaultRecordingMaxBytes
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Printf("invalid RECORDING_MAX_BYTES %q, using the default: %v", raw, err)
		return worker.DefaultRecordingMaxBytes
	}
	return n
}

func resolvePreviewSweepInterval() time.Duration {
	return resolveDuration("PREVIEW_SWEEP_INTERVAL")
}

func resolveDuration(envVar string) time.Duration {
	raw := os.Getenv(envVar)
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Printf("invalid %s %q, ignoring: %v", envVar, raw, err)
		return 0
	}
	return d
}

// resolveAgentImages reads the per-job-kind agent-images image references
// (ADR 004) — a kind whose env var is unset is simply absent from the map,
// so worker.resolveAgentImage falls back to the placeholder dev stand-in for
// that kind rather than failing startup. `deploy` has no entry: it never
// runs Pi, it applies a Helm chart (ADR 003 §13).
func resolveAgentImages() map[queue.JobKind]string {
	images := map[queue.JobKind]string{}
	if v := os.Getenv("SPEC_GRILL_IMAGE"); v != "" {
		images[queue.KindSpecGrill] = v
	}
	if v := os.Getenv("FEATURE_BUILD_IMAGE"); v != "" {
		images[queue.KindFeatureBuild] = v
	}
	if v := os.Getenv("TEST_RUN_IMAGE"); v != "" {
		images[queue.KindTestRun] = v
	}
	// ADR 015 items 10 & 13 (Track B5/B6): the two new job kinds. script_test_run
	// is a non-Pi plain container (no skill/tools), so its image is a
	// lightweight test runner; agentic_review is a Pi RPC kind like
	// spec_grill/feature_build, so it needs its own agent image + skill.
	if v := os.Getenv("SCRIPT_TEST_RUN_IMAGE"); v != "" {
		images[queue.KindScriptTestRun] = v
	}
	if v := os.Getenv("AGENTIC_REVIEW_IMAGE"); v != "" {
		images[queue.KindAgenticReview] = v
	}
	if v := os.Getenv("DESIGN_GRILL_IMAGE"); v != "" {
		images[queue.KindDesignGrill] = v
	}
	return images
}

// resolveRuntimeClassName is nil unless the target cluster has a sandboxed
// runtime (gVisor/Kata, ADR 003 §6) installed and configured via
// JOB_RUNTIME_CLASS — not every cluster (e.g. a local k3d dev cluster) has
// one available.
func resolveRuntimeClassName() *string {
	if name := os.Getenv("JOB_RUNTIME_CLASS"); name != "" {
		return &name
	}
	return nil
}

// resolveWithDefault reads an env var, falling back to a default when unset
// — used for the ingress class/domain/cert-issuer values that are meant to
// differ between a local k3d dev cluster and a self-hosted/managed one
// (config, not code).
func resolveWithDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
