// Package messages delivers a human's reply to a running spec_grill job's
// ask_user question back into its RPC session (ADR 006 items 9-10) — the
// inbound half of the mid-run reply flow (the outbound half,
// posting the question itself, is apiclient.PostJobEvent). Reuses the same
// Postgres database the job queue already lives in (ADR 003), via
// LISTEN/NOTIFY on 'job_replies' rather than polling.
package messages

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store waits for replies queued in the API's job_messages table.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// WaitForReply blocks until a pending (undelivered) reply for jobID
// exists — claiming it (marking it delivered) and returning its content —
// or ctx is cancelled. A spec_grill session calls this once per ask_user
// turn, not once for the whole run, so the connection this holds (LISTEN
// is connection-scoped, so a dedicated one is acquired for the duration)
// is only held as long as a human takes to answer one question, not the
// whole job.
func (s *Store) WaitForReply(ctx context.Context, jobID string) (string, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to acquire a connection to listen for replies: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN job_replies"); err != nil {
		return "", fmt.Errorf("failed to listen for job replies: %w", err)
	}

	for {
		content, ok, err := claimPendingReply(ctx, conn, jobID)
		if err != nil {
			return "", err
		}
		if ok {
			return content, nil
		}

		// Check (above) then wait, not wait then check: a reply inserted
		// between this call starting and the LISTEN above taking effect
		// would otherwise notify before we were listening for it, and be
		// missed entirely. The claim query already covers that race on
		// every iteration (including the first) — WaitForNotification just
		// blocks until *some* reply is queued (for this job or another)
		// before re-checking; it doesn't need to filter by payload itself.
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return "", fmt.Errorf("failed waiting for a job reply notification: %w", err)
		}
	}
}

// claimPendingReply atomically claims (marks delivered) and returns the
// oldest undelivered reply for jobID, if any. ok is false (not an error)
// when there's nothing pending yet. FOR UPDATE SKIP LOCKED mirrors the job
// queue's own claim pattern (internal/queue) — not needed for
// concurrent-claimer safety here (only one goroutine ever waits on a given
// job's replies), but keeps the delivered_at update atomic with the read.
func claimPendingReply(ctx context.Context, conn *pgxpool.Conn, jobID string) (content string, ok bool, err error) {
	row := conn.QueryRow(ctx, `
		UPDATE job_messages
		SET delivered_at = now()
		WHERE id = (
			SELECT id FROM job_messages
			WHERE job_id = $1 AND delivered_at IS NULL
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING content
	`, jobID)

	if err := row.Scan(&content); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to claim pending reply for job %s: %w", jobID, err)
	}
	return content, true, nil
}
