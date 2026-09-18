// Package capabilities publishes which job kinds this installation can actually
// run, so the API can stop dispatching work nothing here can carry out (#63).
//
// **The problem.** `submit_build_result` dispatches a `script_test_run` probe for
// `unit` and `integration` on *every* feature, because the API deliberately never
// reads a repository — ADR 015 item 10 makes the script's *presence* the group's
// toggle, and only the image can check that. On an install with no
// `SCRIPT_TEST_RUN_IMAGE` both probes are therefore dispatched and neither can
// run: two wasted job rows per feature, and every feature fails at Testing
// because nothing was able to verify it (#44, #53). The API cannot avoid that,
// because the images live in *this* process's environment and nothing carried
// that fact across.
//
// **Why the shared database and not an HTTP endpoint.** The API never calls the
// Orchestrator today — there is no `ORCHESTRATOR_URL` reader anywhere in its
// `src/` — while the queue in Postgres is already the one channel both services
// share (ADR 003 §18: that table is how a job reaches this process at all). An
// HTTP capabilities endpoint would add a second, differently-authenticated
// service-to-service path for one boolean per job kind, and it would make the
// API's *dispatch* depend on the Orchestrator being reachable rather than merely
// having reported. A row in the shared database carries the same information with
// neither problem, and it is issue #63's own second option ("or a flag on the
// queue"). This process already writes to that database directly — `queue` and
// `messages` both take a pool and issue SQL — so this follows the existing
// precedent rather than introducing a new kind of dependency.
//
// **Absence means "capable", deliberately.** The API treats a kind with no row,
// or a row whose claim has gone stale, as runnable, so nothing about dispatch
// changes until this process publishes. That is what makes the seam safe to
// deploy in either order, and it errs toward *dispatching*: a probe that cannot
// run is visible and recoverable, whereas one that was silently never dispatched
// loses the check without saying so.
package capabilities

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// DefaultReportInterval is how often the claim is re-asserted.
//
// **It must stay comfortably below the API's trust window.** The reader
// (`api/src/jobs/capabilities.ts`) ignores any row whose `reported_at` is older
// than `CAPABILITY_TRUST_MS`, which is 15 minutes, and falls back to treating the
// kind as runnable. So a single report at startup is not enough: fifteen minutes
// later every row would silently expire and this feature would stop working with
// nothing saying so. Five minutes gives three refreshes inside that window, which
// tolerates a slow or failed pass without ever letting a live claim lapse.
//
// The two constants are coupled across repos and cannot be checked by either
// side's tests. If the API's window is ever shortened below this interval, this
// feature degrades to "sometimes applies" — which is the failure mode to avoid,
// because it looks like it works.
const DefaultReportInterval = 5 * time.Minute

// undersizedReportInterval is a floor, in the same spirit as the API's own
// lower bounds on background intervals: a misconfigured env var must not turn
// the reporter into a busy loop against the shared database.
const undersizedReportInterval = 30 * time.Second

// ImageBackedKinds are the job kinds whose runnability is decided by whether an
// agent image is configured for them — the kinds `cmd/server`'s
// `resolveAgentImages` reads an env var for.
//
// **`deploy` and `rollback` are deliberately absent.** Neither runs an image at
// all (they apply a Helm chart from this process, ADR 003 §13), so a row for them
// could not be derived from the image configuration — it would have to be a
// hard-coded `true`, and a hard-coded `true` in a table otherwise derived from
// configuration is the kind of row that goes quietly wrong when the rule changes.
// Leaving them out is the honest statement: this process has nothing to say about
// whether it can deploy, because that does not depend on an image.
//
// Kept in step with `resolveAgentImages` by a test in `cmd/server`, which asserts
// that setting every image env var yields exactly these kinds. That coupling is
// the only thing preventing a seventh kind from being added to one list and not
// the other, and the drift would be silent: a kind missing here simply gets no
// row, which the API reads as capable.
func ImageBackedKinds() []queue.JobKind {
	return []queue.JobKind{
		queue.KindSpecGrill,
		queue.KindFeatureBuild,
		queue.KindTestRun,
		queue.KindScriptTestRun,
		queue.KindAgenticReview,
		queue.KindDesignGrill,
	}
}

// Runnability derives the claim for every image-backed kind from the configured
// images. Pure, so what this process is about to assert can be asserted in a test
// without a database.
//
// **What `false` means, precisely, because it is not one thing.** It means "no
// real image is configured for this kind", and what follows from that differs:
//
//   - for `script_test_run` the Orchestrator *refuses* the job outright
//     (`runInCluster` returns "no image configured"), which is the case this
//     feature exists for;
//   - for the five model-consuming kinds the job still runs, falling back to
//     `PlaceholderImage`/`PlaceholderScript` — a container that echoes its
//     environment and exits 0 without doing the work. So the job would *complete*
//     while producing nothing.
//
// A consumer must not read `false` as "the Orchestrator will error", because for
// five of the six it will not. It should read it as "this installation cannot
// actually carry out a job of this kind", which is the question #63 asks and the
// reason the shared default is worth having.
func Runnability(images map[queue.JobKind]string) map[queue.JobKind]bool {
	runnable := make(map[queue.JobKind]bool, len(ImageBackedKinds()))
	for _, kind := range ImageBackedKinds() {
		runnable[kind] = images[kind] != ""
	}
	return runnable
}

// Result is what one report pass asserted, kept so a caller can log it and so a
// change between passes is observable.
type Result struct {
	// Reported is how many kinds were written.
	Reported int
	// Unrunnable is the sorted subset this process claimed it cannot run.
	Unrunnable []string
}

// Publisher writes this process's capability claim to the shared database.
type Publisher struct {
	pool     *pgxpool.Pool
	images   map[queue.JobKind]string
	interval time.Duration
	// logf is a field so tests can capture output; production always uses
	// log.Printf. Nothing outside this package needs to change it.
	logf func(string, ...any)
}

// New builds a Publisher for the given images. An interval <= 0 takes the
// default, and one below the floor is raised to it rather than rejected, so a
// typo in an env var degrades the reporter instead of disabling it.
func New(pool *pgxpool.Pool, images map[queue.JobKind]string, interval time.Duration) *Publisher {
	if interval <= 0 {
		interval = DefaultReportInterval
	}
	if interval < undersizedReportInterval {
		interval = undersizedReportInterval
	}
	return &Publisher{
		pool:     pool,
		images:   images,
		interval: interval,
		logf:     log.Printf,
	}
}

// Report writes one claim for every image-backed kind, in a single statement.
//
// **One statement rather than one per kind, deliberately.** Six separate
// upserts could half-apply if this process died between them, leaving some rows
// freshly written and others stale — a partial claim, which is worse than either
// an old one or a new one because a reader cannot tell it is partial. A single
// INSERT ... SELECT ... ON CONFLICT is one transaction, so the table only ever
// shows a complete pass, and it also stamps every row with the same
// `reported_at` so the reader's freshness check sees one coherent claim.
//
// A missing table is not an error. The API owns the migration that creates it
// (`050_job_kind_capabilities.sql`), and the two services can be started in
// either order — so an Orchestrator that comes up first must not fail, or log a
// stack trace every interval, for a table that is about to exist. It is reported
// once and then silently retried; the next pass after the migration succeeds
// normally.
func (p *Publisher) Report(ctx context.Context) (Result, error) {
	runnable := Runnability(p.images)

	kinds := make([]string, 0, len(runnable))
	flags := make([]bool, 0, len(runnable))
	for _, kind := range ImageBackedKinds() {
		kinds = append(kinds, string(kind))
		flags = append(flags, runnable[kind])
	}

	_, err := p.pool.Exec(ctx, `
		INSERT INTO job_kind_capabilities (job_kind, runnable, reported_at)
		SELECT kind, runnable, NOW()
		  FROM unnest($1::text[], $2::boolean[]) AS t(kind, runnable)
		ON CONFLICT (job_kind) DO UPDATE
		SET runnable = EXCLUDED.runnable,
		    reported_at = NOW()
	`, kinds, flags)
	if err != nil {
		if isUndefinedTable(err) {
			return Result{}, errTableMissing
		}
		return Result{}, fmt.Errorf("failed to publish job-kind capabilities: %w", err)
	}

	unrunnable := make([]string, 0, len(runnable))
	for kind, canRun := range runnable {
		if !canRun {
			unrunnable = append(unrunnable, string(kind))
		}
	}
	sort.Strings(unrunnable)

	return Result{Reported: len(kinds), Unrunnable: unrunnable}, nil
}

// errTableMissing marks the one condition that is expected transiently and must
// not be treated as a failure.
var errTableMissing = errors.New("job_kind_capabilities does not exist yet")

// IsTableMissing reports whether err is the "migration has not run yet"
// condition, so a caller can choose to stay quiet about it.
func IsTableMissing(err error) bool {
	return errors.Is(err, errTableMissing)
}

// isUndefinedTable matches Postgres's undefined_table. Checked by SQLSTATE rather
// than by matching the message, which is localised and version-dependent — and
// narrowly, so a different error that happens to mention a missing relation (a
// permissions failure naming it, say) is not swallowed as "expected".
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// Run publishes once immediately and then re-asserts on a timer until ctx ends.
//
// **Why a timer at all, when the configuration cannot change while this process
// runs.** A row is a claim about a *running* installation, and the API stops
// trusting it after 15 minutes — so re-asserting is not about detecting a config
// change (this process reads its env once, at startup, and cannot observe one),
// it is about keeping a live claim from expiring. A single startup write would
// work for the first fifteen minutes and then quietly revert the API to its
// pre-#63 behaviour, which is the worst of both: it would look implemented and
// stop working.
//
// Failures are logged and retried on the next tick rather than being fatal. A
// capability report is advisory — the API's safe default without it is to keep
// dispatching — so it must never be able to take this process down or stop it
// running jobs.
//
// **Known limitation: the table holds one claim per kind, not one per worker.**
// Every replica of a deployment shares that deployment's environment, so they
// agree and last-writer-wins is harmless. Replicas configured *differently* would
// overwrite each other's claim every interval and the API would see it flap. That
// is not solvable from this side — the row is keyed by kind, and per-worker rows
// would be the API's schema to decide — so it is recorded here and in the issue
// rather than guarded against.
func (p *Publisher) Run(ctx context.Context) {
	var lastUnrunnable []string
	first := true

	report := func() {
		result, err := p.Report(ctx)
		if err != nil {
			if IsTableMissing(err) {
				// Logged once; a permanent condition would otherwise repeat every
				// interval forever and drown out things worth reading.
				if first {
					p.logf(
						"capabilities: job_kind_capabilities does not exist yet; " +
							"the API's migration creates it and this will retry. " +
							"Until it does, the API treats every kind as runnable",
					)
				}
				return
			}
			p.logf("capabilities: failed to publish job-kind capabilities: %v", err)
			return
		}

		// Logged on the first success and whenever the claim changes, not on
		// every pass: a line every five minutes for the life of the deployment is
		// noise, but "this install just stopped being able to run script_test_run"
		// is the kind of thing an operator wants in the log.
		if first || !equalStrings(lastUnrunnable, result.Unrunnable) {
			if len(result.Unrunnable) == 0 {
				p.logf("capabilities: reported %d job kind(s), all runnable", result.Reported)
			} else {
				p.logf(
					"capabilities: reported %d job kind(s); cannot run: %v",
					result.Reported, result.Unrunnable,
				)
			}
		}
		lastUnrunnable = result.Unrunnable
		first = false
	}

	report()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
