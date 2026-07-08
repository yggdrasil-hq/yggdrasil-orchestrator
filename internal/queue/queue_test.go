package queue_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// setupTestQueue connects to a real Postgres (skipping the test if none is
// configured) and creates a throwaway `jobs` fixture table scoped to just the
// columns the queue package touches. This intentionally does not depend on
// the API repo's migrations — it's a self-contained fixture for the
// Orchestrator's own test suite.
func setupTestQueue(t *testing.T) (*queue.Queue, *pgxpool.Pool) {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed queue integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS jobs;
		CREATE TABLE jobs (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			project_id UUID NOT NULL,
			kind VARCHAR(32) NOT NULL,
			feature_id UUID,
			test_id UUID,
			status VARCHAR(32) NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			locked_at TIMESTAMPTZ,
			locked_by TEXT,
			attempts INT NOT NULL DEFAULT 0,
			last_error TEXT
		);
	`)
	if err != nil {
		t.Fatalf("failed to set up jobs fixture table: %v", err)
	}

	return queue.New(pool), pool
}

func insertJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (project_id, kind, status)
		VALUES (gen_random_uuid(), $1, 'pending')
		RETURNING id
	`, kind).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert fixture job: %v", err)
	}
	return id
}

func TestClaim_ReturnsNilWhenEmpty(t *testing.T) {
	q, _ := setupTestQueue(t)
	ctx := context.Background()

	job, err := q.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job != nil {
		t.Fatalf("expected no job, got %+v", job)
	}
}

func TestClaim_MarksJobRunning(t *testing.T) {
	q, pool := setupTestQueue(t)
	ctx := context.Background()

	id := insertJob(t, ctx, pool, "spec_grill")

	job, err := q.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if job.ID != id {
		t.Fatalf("expected job %s, got %s", id, job.ID)
	}
	if job.Status != queue.StatusRunning {
		t.Fatalf("expected status running, got %s", job.Status)
	}
	if job.StartedAt == nil {
		t.Fatal("expected started_at to be set")
	}

	// A second claim must not see the same job again — it's no longer pending.
	again, err := q.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if again != nil {
		t.Fatalf("expected no further pending job, got %+v", again)
	}
}

// TestClaim_SkipLockedPreventsDoubleClaim is the core guarantee ADR 003 relies
// on for running multiple Orchestrator replicas safely: concurrent claimers
// must never see the same row twice.
func TestClaim_SkipLockedPreventsDoubleClaim(t *testing.T) {
	q, pool := setupTestQueue(t)
	ctx := context.Background()

	const jobCount = 10
	for i := 0; i < jobCount; i++ {
		insertJob(t, ctx, pool, "feature_build")
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]int{}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		workerID := "worker-" + string(rune('a'+w))
		go func(workerID string) {
			defer wg.Done()
			for {
				job, err := q.Claim(ctx, workerID)
				if err != nil {
					t.Errorf("worker %s: claim failed: %v", workerID, err)
					return
				}
				if job == nil {
					return
				}
				mu.Lock()
				claimed[job.ID]++
				mu.Unlock()
			}
		}(workerID)
	}
	wg.Wait()

	if len(claimed) != jobCount {
		t.Fatalf("expected %d distinct jobs claimed, got %d", jobCount, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Fatalf("job %s claimed %d times, expected exactly 1", id, count)
		}
	}
}

func TestComplete(t *testing.T) {
	q, pool := setupTestQueue(t)
	ctx := context.Background()

	insertJob(t, ctx, pool, "test_run")
	claimed, err := q.Claim(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim fixture job: %v", err)
	}

	if err := q.Complete(ctx, claimed.ID); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, claimed.ID).Scan(&status); err != nil {
		t.Fatalf("failed to read back status: %v", err)
	}
	if status != string(queue.StatusCompleted) {
		t.Fatalf("expected completed, got %s", status)
	}
}

func TestFail(t *testing.T) {
	q, pool := setupTestQueue(t)
	ctx := context.Background()

	insertJob(t, ctx, pool, "test_run")
	claimed, err := q.Claim(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim fixture job: %v", err)
	}

	if err := q.Fail(ctx, claimed.ID, errors.New("boom")); err != nil {
		t.Fatalf("fail failed: %v", err)
	}

	var status string
	var attempts int
	var lastError string
	err = pool.QueryRow(ctx,
		`SELECT status, attempts, last_error FROM jobs WHERE id = $1`, claimed.ID,
	).Scan(&status, &attempts, &lastError)
	if err != nil {
		t.Fatalf("failed to read back failure state: %v", err)
	}
	if status != string(queue.StatusFailed) {
		t.Fatalf("expected failed, got %s", status)
	}
	if attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", attempts)
	}
	if lastError != "boom" {
		t.Fatalf("expected last_error 'boom', got %q", lastError)
	}
}
