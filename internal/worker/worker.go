// Package worker runs the Orchestrator's job-claim poll loop.
package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultPollInterval     = 2 * time.Second
	defaultPlaceholderImage = "busybox:1.36"
	// The Phase 2 (ADR 003) stand-in for the Pi agent — pi.dev's RPC/SDK
	// surface is still an open TODO (docs/concepts/pi-agent.md), so this
	// placeholder container just proves the "claim -> run in k8s -> report
	// result" mechanics work, independently of Pi.
	defaultPlaceholderScript = "echo job $JOB_ID kind=$JOB_KIND; exit 0"
)

// Config configures a worker's poll loop.
type Config struct {
	WorkerID     string
	PollInterval time.Duration

	// PlaceholderImage/PlaceholderScript stand in for a real Pi agent run
	// until pi.dev's RPC surface is defined (see docs/concepts/pi-agent.md).
	PlaceholderImage  string
	PlaceholderScript string

	// RuntimeClassName is left nil unless the target cluster has a sandboxed
	// runtime (gVisor/Kata, ADR 003 §6) installed — not available on every
	// cluster (e.g. a local k3d dev cluster).
	RuntimeClassName *string
}

// Run polls the queue on an interval, claiming at most one job per tick, and
// blocks until ctx is cancelled.
func Run(ctx context.Context, q *queue.Queue, clientset kubernetes.Interface, cfg Config) {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if cfg.PlaceholderImage == "" {
		cfg.PlaceholderImage = defaultPlaceholderImage
	}
	if cfg.PlaceholderScript == "" {
		cfg.PlaceholderScript = defaultPlaceholderScript
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processOne(ctx, q, clientset, cfg)
		}
	}
}

func processOne(ctx context.Context, q *queue.Queue, clientset kubernetes.Interface, cfg Config) {
	job, err := q.Claim(ctx, cfg.WorkerID)
	if err != nil {
		log.Printf("worker %s: claim failed: %v", cfg.WorkerID, err)
		return
	}
	if job == nil {
		return
	}

	log.Printf("worker %s: claimed job %s (kind=%s project=%s)", cfg.WorkerID, job.ID, job.Kind, job.ProjectID)

	if err := runInCluster(ctx, clientset, job, cfg); err != nil {
		log.Printf("worker %s: job %s failed: %v", cfg.WorkerID, job.ID, err)
		if failErr := q.Fail(ctx, job.ID, err); failErr != nil {
			log.Printf("worker %s: failed to record failure for job %s: %v", cfg.WorkerID, job.ID, failErr)
		}
		return
	}

	log.Printf("worker %s: job %s completed", cfg.WorkerID, job.ID)
	if err := q.Complete(ctx, job.ID); err != nil {
		log.Printf("worker %s: failed to complete job %s: %v", cfg.WorkerID, job.ID, err)
	}
}

// runInCluster executes a claimed job as a real Kubernetes Job in the
// project's namespace (ADR 003).
func runInCluster(ctx context.Context, clientset kubernetes.Interface, job *queue.Job, cfg Config) error {
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, job.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to provision namespace: %w", err)
	}

	return k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "job-" + job.ID,
		Image:     cfg.PlaceholderImage,
		Command:   []string{"sh", "-c", cfg.PlaceholderScript},
		Env: map[string]string{
			"JOB_ID":     job.ID,
			"JOB_KIND":   string(job.Kind),
			"PROJECT_ID": job.ProjectID,
		},
		RuntimeClassName: cfg.RuntimeClassName,
	})
}
