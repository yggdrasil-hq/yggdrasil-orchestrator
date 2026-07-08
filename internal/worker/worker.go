// Package worker runs the Orchestrator's job-claim poll loop.
package worker

import (
	"context"
	"log"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

const defaultPollInterval = 2 * time.Second

// Run polls the queue on an interval, claiming at most one job per tick, and
// blocks until ctx is cancelled.
//
// Phase 1 (ADR 003) stub: "processing" a claimed job is just logging it and
// marking it complete — real Kubernetes/Pi execution replaces this in a
// later phase.
func Run(ctx context.Context, q *queue.Queue, workerID string, pollInterval time.Duration) {
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processOne(ctx, q, workerID)
		}
	}
}

func processOne(ctx context.Context, q *queue.Queue, workerID string) {
	job, err := q.Claim(ctx, workerID)
	if err != nil {
		log.Printf("worker %s: claim failed: %v", workerID, err)
		return
	}
	if job == nil {
		return
	}

	log.Printf("worker %s: claimed job %s (kind=%s project=%s)", workerID, job.ID, job.Kind, job.ProjectID)

	if err := q.Complete(ctx, job.ID); err != nil {
		log.Printf("worker %s: failed to complete job %s: %v", workerID, job.ID, err)
	}
}
