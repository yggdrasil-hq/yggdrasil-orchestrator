// Package queue implements the ADR-003 job queue: the Orchestrator claims
// pending rows directly from the API's `jobs` table using `FOR UPDATE SKIP
// LOCKED`, rather than talking to a dedicated broker. This is what makes
// running multiple Orchestrator replicas safe with no extra coordination.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobKind string

const (
	KindSpecGrill    JobKind = "spec_grill"
	KindFeatureBuild JobKind = "feature_build"
	KindTestRun      JobKind = "test_run"
	KindDeploy       JobKind = "deploy"
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
		RETURNING id, project_id, kind, feature_id, test_id, status, created_at, started_at
	`, workerID)

	var j Job
	err := row.Scan(
		&j.ID, &j.ProjectID, &j.Kind, &j.FeatureID, &j.TestID,
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

// Complete marks a claimed job as successfully finished.
func (q *Queue) Complete(ctx context.Context, id string) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs SET status = 'completed', completed_at = now() WHERE id = $1
	`, id)
	return err
}

// Fail marks a claimed job as failed and records the cause.
func (q *Queue) Fail(ctx context.Context, id string, cause error) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    completed_at = now(),
		    attempts = attempts + 1,
		    last_error = $2
		WHERE id = $1
	`, id, cause.Error())
	return err
}
