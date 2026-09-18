package capabilities

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// The schema under test is the API's, from
// `api/src/db/migrations/050_job_kind_capabilities.sql` — mirrored here as a
// throwaway fixture rather than imported, the same way `queue_test.go` fixtures
// the `jobs` table it touches. If that migration's shape changes, this file
// changes with it; the coupling is real either way, and mirroring it means the
// tests fail loudly rather than silently exercising a table nobody has.
//
// These are **real-database** tests on purpose. The whole feature is "write rows
// another service reads"; a fake pool would assert that this process called its
// own SQL, which is the class of test that let #43 (a repository method that threw
// on every call) pass 880 green tests in the API repo.
const fixtureSchema = `
DROP TABLE IF EXISTS job_kind_capabilities;
CREATE TABLE job_kind_capabilities (
  job_kind VARCHAR(32) PRIMARY KEY,
  runnable BOOLEAN NOT NULL,
  reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

func setupTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed capabilities integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, fixtureSchema); err != nil {
		t.Fatalf("failed to create the capabilities fixture table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS job_kind_capabilities")
	})

	return pool
}

// imagesWithEverything is a fully-configured install. Built from
// ImageBackedKinds so a seventh kind does not have to be added here too — that
// would make this test agree with a wrong list.
func imagesWithEverything() map[queue.JobKind]string {
	images := map[queue.JobKind]string{}
	for _, kind := range ImageBackedKinds() {
		images[kind] = "ghcr.io/example/" + string(kind) + ":latest"
	}
	return images
}

func TestRunnability_EverythingConfiguredIsRunnable(t *testing.T) {
	runnable := Runnability(imagesWithEverything())

	if len(runnable) != len(ImageBackedKinds()) {
		t.Fatalf("expected a claim for every image-backed kind, got %d", len(runnable))
	}
	for kind, canRun := range runnable {
		if !canRun {
			t.Fatalf("expected %s to be runnable with an image configured", kind)
		}
	}
}

func TestRunnability_MissingImageIsNotRunnable(t *testing.T) {
	images := imagesWithEverything()
	delete(images, queue.KindScriptTestRun)

	runnable := Runnability(images)

	if runnable[queue.KindScriptTestRun] {
		t.Fatal("expected script_test_run to be unrunnable with no image")
	}
	// The others are unaffected: this is per-kind, not an installation-wide flag.
	if !runnable[queue.KindTestRun] {
		t.Fatal("expected test_run to stay runnable")
	}
}

// An install with nothing configured is the state #44/#53/#63 all describe, and
// the one where every claim flips.
func TestRunnability_NothingConfiguredIsAllUnrunnable(t *testing.T) {
	runnable := Runnability(map[queue.JobKind]string{})

	for kind, canRun := range runnable {
		if canRun {
			t.Fatalf("expected %s to be unrunnable with no images configured", kind)
		}
	}
}

// A whitespace-only env var is a broken config either way, and the honest thing is
// to *agree with* `resolveAgentImages` rather than invent a second rule: it keeps
// any value that is not the empty string, so such a value reaches a job and fails
// there. Treating it as absent here would make the two disagree — this process
// would claim it can run a kind it would dispatch with a nonsense image.
func TestRunnability_WhitespaceIsConfiguredConsistentWithResolveAgentImages(t *testing.T) {
	runnable := Runnability(map[queue.JobKind]string{queue.KindSpecGrill: "   "})

	if !runnable[queue.KindSpecGrill] {
		t.Fatal(
			"expected a non-empty string to count as configured, matching resolveAgentImages' `v != \"\"`; " +
				"changing this needs changing both sides",
		)
	}
}

func TestImageBackedKinds_CoversEveryKindTheWorkerGatesAnImageOn(t *testing.T) {
	// The six are exactly the kinds `resolveAgentImages` reads an env var for,
	// and `runInCluster` treats as image-driven. `deploy` and `rollback` run no
	// image, so a claim about them would not be derived from configuration.
	got := make([]string, 0, len(ImageBackedKinds()))
	for _, kind := range ImageBackedKinds() {
		got = append(got, string(kind))
	}
	sort.Strings(got)

	want := []string{
		"agentic_review",
		"design_grill",
		"feature_build",
		"script_test_run",
		"spec_grill",
		"test_run",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// The row the whole feature exists to write: an install with no
// SCRIPT_TEST_RUN_IMAGE must say so, because that is the single fact the API's
// Testing gate reads.
func TestReport_WritesUnrunnableScriptTestRun(t *testing.T) {
	pool := setupTestPool(t)
	ctx := context.Background()

	images := imagesWithEverything()
	delete(images, queue.KindScriptTestRun)

	result, err := New(pool, images, time.Minute).Report(ctx)
	if err != nil {
		t.Fatalf("expected the report to succeed, got: %v", err)
	}
	if result.Reported != len(ImageBackedKinds()) {
		t.Fatalf("expected %d rows, got %d", len(ImageBackedKinds()), result.Reported)
	}
	if strings.Join(result.Unrunnable, ",") != "script_test_run" {
		t.Fatalf("expected only script_test_run to be unrunnable, got %v", result.Unrunnable)
	}

	// Read it back the way the API reads it — the negative rows inside the trust
	// window — rather than trusting the result struct.
	var unrunnable []string
	rows, err := pool.Query(ctx, `
		SELECT job_kind FROM job_kind_capabilities
		 WHERE runnable = FALSE AND reported_at >= NOW() - INTERVAL '15 minutes'
		 ORDER BY job_kind
	`)
	if err != nil {
		t.Fatalf("failed to read back capabilities: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		unrunnable = append(unrunnable, kind)
	}
	if strings.Join(unrunnable, ",") != "script_test_run" {
		t.Fatalf("expected the API to see exactly script_test_run, got %v", unrunnable)
	}
}

func TestReport_IsIdempotentAndRefreshesReportedAt(t *testing.T) {
	pool := setupTestPool(t)
	ctx := context.Background()
	publisher := New(pool, imagesWithEverything(), time.Minute)

	if _, err := publisher.Report(ctx); err != nil {
		t.Fatalf("first report failed: %v", err)
	}

	// Age the claim past the reader's window, which is exactly what happens if
	// this process stops reporting.
	if _, err := pool.Exec(ctx,
		"UPDATE job_kind_capabilities SET reported_at = NOW() - INTERVAL '20 minutes'",
	); err != nil {
		t.Fatalf("failed to age the rows: %v", err)
	}

	var before int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM job_kind_capabilities
		 WHERE reported_at >= NOW() - INTERVAL '15 minutes'
	`).Scan(&before); err != nil {
		t.Fatalf("failed to count fresh rows: %v", err)
	}
	if before != 0 {
		t.Fatalf("expected the aged claim to be stale, got %d fresh rows", before)
	}

	if _, err := publisher.Report(ctx); err != nil {
		t.Fatalf("second report failed: %v", err)
	}

	// Still one row per kind (upsert, not insert), and fresh again — this is what
	// stops the feature silently expiring fifteen minutes after startup.
	var total, fresh int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_kind_capabilities").Scan(&total); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM job_kind_capabilities
		 WHERE reported_at >= NOW() - INTERVAL '15 minutes'
	`).Scan(&fresh); err != nil {
		t.Fatalf("failed to count fresh rows: %v", err)
	}
	if total != len(ImageBackedKinds()) {
		t.Fatalf("expected %d rows after re-reporting, got %d", len(ImageBackedKinds()), total)
	}
	if fresh != len(ImageBackedKinds()) {
		t.Fatalf("expected every row refreshed, got %d of %d", fresh, total)
	}
}

// A kind that gains its image must flip back to runnable: the whole point of the
// freshness window is that an operator fixing a config is seen.
func TestReport_FlipsWhenTheConfigurationChanges(t *testing.T) {
	pool := setupTestPool(t)
	ctx := context.Background()

	without := imagesWithEverything()
	delete(without, queue.KindScriptTestRun)
	if _, err := New(pool, without, time.Minute).Report(ctx); err != nil {
		t.Fatalf("first report failed: %v", err)
	}

	if _, err := New(pool, imagesWithEverything(), time.Minute).Report(ctx); err != nil {
		t.Fatalf("second report failed: %v", err)
	}

	var unrunnable int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM job_kind_capabilities WHERE runnable = FALSE",
	).Scan(&unrunnable); err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if unrunnable != 0 {
		t.Fatalf("expected every kind runnable after the image was added, got %d unrunnable", unrunnable)
	}
}

// The migration is the API's and the two services can start in either order, so
// this is an ordinary transient condition — not a failure, and not something to
// log a stack trace about every interval.
func TestReport_MissingTableIsReportedAsSuchAndNotAsFailure(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed capabilities integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(pool.Close)

	// Deliberately absent: no fixture created.
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS job_kind_capabilities"); err != nil {
		t.Fatalf("failed to drop the table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS job_kind_capabilities")
	})

	_, err = New(pool, imagesWithEverything(), time.Minute).Report(ctx)
	if !IsTableMissing(err) {
		t.Fatalf("expected the missing-table condition, got %v", err)
	}
}

func TestNew_ClampsAnUndersizedInterval(t *testing.T) {
	publisher := New(nil, nil, time.Millisecond)

	if publisher.interval != undersizedReportInterval {
		t.Fatalf("expected the floor %s, got %s", undersizedReportInterval, publisher.interval)
	}
}

func TestNew_DefaultIntervalLeavesRoomInsideTheReadersTrustWindow(t *testing.T) {
	// The API ignores a claim older than 15 minutes. If this default ever rises
	// to or above that, the feature starts expiring between passes and stops
	// working in a way that looks like it works — so the relationship is asserted
	// rather than left in a comment.
	const apiTrustWindow = 15 * time.Minute

	if New(nil, nil, 0).interval >= apiTrustWindow {
		t.Fatalf(
			"the report interval (%s) must stay well below the API's trust window (%s), or a live claim lapses between passes",
			DefaultReportInterval, apiTrustWindow,
		)
	}
}

// Run must publish immediately rather than waiting a whole interval, and must
// stop when its context does.
func TestRun_ReportsImmediatelyThenStopsOnContextCancel(t *testing.T) {
	pool := setupTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())

	images := imagesWithEverything()
	delete(images, queue.KindScriptTestRun)
	publisher := New(pool, images, time.Hour)

	var logged []string
	// printf-style, matching log.Printf — so the format has to be applied here.
	// Capturing the raw format string instead is what made my first version of
	// this assertion fail: it searched for "script_test_run" in
	// "...cannot run: %v", which is the shape of test that verifies nothing.
	publisher.logf = func(format string, args ...any) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		publisher.Run(ctx)
	}()

	// The immediate report should land without waiting for the hour-long tick.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := pool.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM job_kind_capabilities",
		).Scan(&count); err == nil && count > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM job_kind_capabilities",
	).Scan(&count); err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != len(ImageBackedKinds()) {
		t.Fatalf("expected an immediate report of %d rows, got %d", len(ImageBackedKinds()), count)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if len(logged) == 0 {
		t.Fatal("expected the first report to be logged")
	}
	if !strings.Contains(strings.Join(logged, "\n"), "script_test_run") {
		t.Fatalf("expected the log to name what cannot run, got %v", logged)
	}
}
