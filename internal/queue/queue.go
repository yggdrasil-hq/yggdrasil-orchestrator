// Package queue implements the ADR-003 job queue: the Orchestrator claims
// pending rows directly from the API's `jobs` table using `FOR UPDATE SKIP
// LOCKED`, rather than talking to a dedicated broker. This is what makes
// running multiple Orchestrator replicas safe with no extra coordination.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobKind string

const (
	KindSpecGrill     JobKind = "spec_grill"
	KindFeatureBuild  JobKind = "feature_build"
	KindTestRun       JobKind = "test_run"
	KindDeploy        JobKind = "deploy"
	KindScriptTestRun JobKind = "script_test_run"
	KindAgenticReview JobKind = "agentic_review"
	KindDesignGrill   JobKind = "design_grill"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

// Job mirrors the shape of a row in the API's `jobs` table.
type Job struct {
	ID        string
	ProjectID string
	Kind      JobKind
	FeatureID *string
	TestID    *string
	TestGroup *string
	Ref       *string
	Trigger   *string
	Status    JobStatus
	CreatedAt time.Time
	StartedAt *time.Time
}

type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Claim atomically picks the oldest pending job and marks it running under
// this worker's lock. Returns (nil, nil) if there is no pending job — that is
// the normal "nothing to do" case, not an error.
func (q *Queue) Claim(ctx context.Context, workerID string) (*Job, error) {
	row := q.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'running',
		    started_at = now(),
		    locked_at = now(),
		    locked_by = $1
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'pending'
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, project_id, kind, feature_id, test_id, test_group, ref, trigger_source, status, created_at, started_at
	`, workerID)

	var j Job
	err := row.Scan(
		&j.ID, &j.ProjectID, &j.Kind, &j.FeatureID, &j.TestID, &j.TestGroup, &j.Ref, &j.Trigger,
		&j.Status, &j.CreatedAt, &j.StartedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// Complete marks a claimed job as successfully finished. Guarded to only
// transition from 'running': a job the API already marked 'cancelled'
// (WatchCancellation, below) must not be silently flipped back to
// 'completed' by a session that was already mid-teardown when the
// cancellation landed.
func (q *Queue) Complete(ctx context.Context, id string) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs SET status = 'completed', completed_at = now()
		WHERE id = $1 AND status = 'running'
	`, id)
	return err
}

// Fail marks a claimed job as failed and records the cause. Guarded to only
// transition from 'running', for the same reason as Complete — a cancelled
// job's own session unwinding (via ctx cancellation) surfaces as an error
// here too, and must not overwrite the 'cancelled' status the API already
// recorded.
func (q *Queue) Fail(ctx context.Context, id string, cause error) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    completed_at = now(),
		    attempts = attempts + 1,
		    last_error = $2
		WHERE id = $1 AND status = 'running'
	`, id, cause.Error())
	return err
}

// WatchCancellation blocks until jobID's job is cancelled (status set to
// 'cancelled' by the API's cancel endpoint) or ctx ends, whichever happens
// first. nil means cancelled; ctx.Err() means ctx ended first — the
// ordinary "the run finished on its own" case, not a failure. Held for a
// running spec_grill job's entire session (driveSpecGrillSession), not just
// while waiting on a human reply, so a cancellation lands even mid-turn.
func (q *Queue) WatchCancellation(ctx context.Context, jobID string) error {
	conn, err := q.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire a connection to listen for cancellations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN job_cancellations"); err != nil {
		return fmt.Errorf("failed to listen for job cancellations: %w", err)
	}

	for {
		cancelled, err := q.isCancelled(ctx, conn, jobID)
		if err != nil {
			return err
		}
		if cancelled {
			return nil
		}

		// Check (above) then wait, not wait then check — see
		// messages.Store.WaitForReply's identical comment: a cancellation
		// that lands between this call starting and the LISTEN above taking
		// effect would otherwise be missed entirely.
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return fmt.Errorf("failed waiting for a job cancellation notification: %w", err)
		}
	}
}

func (q *Queue) isCancelled(ctx context.Context, conn *pgxpool.Conn, jobID string) (bool, error) {
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		return false, fmt.Errorf("failed to check job %s status: %w", jobID, err)
	}
	return status == string(StatusCancelled), nil
}
