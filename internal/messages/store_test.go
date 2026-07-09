package messages_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/messages"
)

// setupTestStore connects to a real Postgres (skipping the test if none is
// configured) and creates a throwaway `job_messages` fixture table scoped
// to just the columns the messages package touches — self-contained, not
// dependent on the API repo's migrations, same pattern as internal/queue's
// own tests.
func setupTestStore(t *testing.T) (*messages.Store, *pgxpool.Pool) {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed messages integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)

	// job_id is TEXT here (the real schema has it as UUID, FK'd to jobs(id))
	// — this fixture only exercises the messages package's query logic, not
	// exact schema fidelity, so plain "job-1"-style test IDs read easier
	// than real UUIDs, same trade-off internal/queue's own fixture makes.
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS job_messages;
		CREATE TABLE job_messages (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			job_id TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			delivered_at TIMESTAMPTZ
		);
	`)
	if err != nil {
		t.Fatalf("failed to set up job_messages fixture table: %v", err)
	}

	return messages.New(pool), pool
}

// insertReply mirrors exactly what the API's JobMessageRepository.create
// does: insert the row, then notify — the two statements a real reply
// submission performs.
func insertReply(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID, content string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO job_messages (job_id, content) VALUES ($1, $2)`, jobID, content); err != nil {
		t.Fatalf("failed to insert fixture reply: %v", err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_notify('job_replies', $1)`, jobID); err != nil {
		t.Fatalf("failed to notify fixture reply: %v", err)
	}
}

func TestWaitForReply_ReturnsAlreadyPendingReply(t *testing.T) {
	store, pool := setupTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	insertReply(t, ctx, pool, "job-1", "use OAuth")

	content, err := store.WaitForReply(ctx, "job-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content != "use OAuth" {
		t.Fatalf("expected content %q, got %q", "use OAuth", content)
	}

	var deliveredAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT delivered_at FROM job_messages WHERE job_id = $1`, "job-1").Scan(&deliveredAt); err != nil {
		t.Fatalf("failed to read back delivered_at: %v", err)
	}
	if deliveredAt == nil {
		t.Fatal("expected delivered_at to be set once claimed")
	}
}

func TestWaitForReply_UnblocksWhenReplyArrivesLater(t *testing.T) {
	store, pool := setupTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resultCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		content, err := store.WaitForReply(ctx, "job-2")
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- content
	}()

	// Give WaitForReply time to actually start LISTENing before the reply
	// is inserted — proves this isn't just succeeding because the row
	// happened to exist before the call started (that's the previous test).
	time.Sleep(200 * time.Millisecond)
	insertReply(t, ctx, pool, "job-2", "use email/password")

	select {
	case content := <-resultCh:
		if content != "use email/password" {
			t.Fatalf("expected content %q, got %q", "use email/password", content)
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for WaitForReply to unblock")
	}
}

func TestWaitForReply_IgnoresOtherJobsReplies(t *testing.T) {
	store, pool := setupTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resultCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		content, err := store.WaitForReply(ctx, "job-3")
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- content
	}()

	time.Sleep(200 * time.Millisecond)
	insertReply(t, ctx, pool, "job-other", "irrelevant reply")

	select {
	case content := <-resultCh:
		t.Fatalf("expected WaitForReply(job-3) not to resolve on another job's reply, got %q", content)
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Still blocked, as expected — now satisfy it for real.
	}

	insertReply(t, ctx, pool, "job-3", "the actual reply")

	select {
	case content := <-resultCh:
		if content != "the actual reply" {
			t.Fatalf("expected content %q, got %q", "the actual reply", content)
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for WaitForReply to unblock on the right job's reply")
	}
}

func TestWaitForReply_RespectsContextCancellation(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := store.WaitForReply(ctx, "job-never-replied")
	if err == nil {
		t.Fatal("expected an error when the context is cancelled before a reply arrives")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("expected WaitForReply to return promptly on cancellation, took %v", elapsed)
	}
}
