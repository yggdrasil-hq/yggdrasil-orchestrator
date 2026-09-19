package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
Issue #82: a grill question nobody answers must not wait forever.

These cases are split deliberately. The helper-level ones pin the *decisions* that
are easy to get wrong and cheap to assert (which error a timeout is, versus which
it is not; whether the bound applies per wait or per run). The end-to-end one then
proves the wiring — that a stalled session actually fails with a legible reason
rather than hanging — because a helper that returns the right error is worth
nothing if nothing calls it.
*/

// neverReplies mimics a question nobody will ever answer: it blocks until the
// context ends and reports the context's own error, exactly as a real
// `messages.Store` does when its ctx expires.
type neverReplies struct{}

func (neverReplies) WaitForReply(ctx context.Context, _ string) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func TestReplyTimeout_FailsAQuestionNobodyAnswers(t *testing.T) {
	start := time.Now()
	_, err := waitForReplyOnce(
		context.Background(), neverReplies{}, "job-1", "Which database should the API use?", 20*time.Millisecond,
	)
	elapsed := time.Since(start)

	var timeoutErr replyTimeoutErr
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected a replyTimeoutErr, got %T (%v)", err, err)
	}
	// It must actually have waited the bound rather than failing immediately —
	// otherwise the error would be right for the wrong reason, and the wait would
	// be pointless.
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected the wait to last at least the bound, got %s", elapsed)
	}
	if timeoutErr.waited != 20*time.Millisecond {
		t.Fatalf("expected the bound recorded on the error, got %s", timeoutErr.waited)
	}

	/*
	 * The message is the user-facing half, and this is where "legible" is
	 * asserted rather than assumed. It lands in `jobs.last_error` and is what an
	 * operator sees on a failed run, so it has to say that nobody answered, how
	 * long it waited, that the question survived, and *which* question — the four
	 * facts that make the failure actionable rather than mysterious.
	 */
	msg := err.Error()
	for _, want := range []string{"20ms", "no reply", "transcript", "Which database should the API use?"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected the failure message to mention %q, got: %s", want, msg)
		}
	}
}

func TestReplyTimeout_PrefersARealReplyOverTheBound(t *testing.T) {
	reply, err := waitForReplyOnce(
		context.Background(), fixedReplyWaiter{reply: "use-oauth"}, "job-1", "Which auth model?", time.Hour,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if reply != "use-oauth" {
		t.Fatalf("expected the reply, got %q", reply)
	}
}

/*
The discrimination that matters most, and the one a naive implementation gets
wrong. `context.WithTimeout` derives from the run context, so **both** a fired
deadline and a parent cancellation surface as a context error — and reporting a
cancellation as "nobody answered within 24h" would tell the operator the opposite
of what happened.

A cancellation already has its own meaning end to end (it becomes
`EventRunCancelled` and a job status of `cancelled`, not `failed`), so a timeout
must be *only* a timeout.

What this covers: the ordinary shape, where the parent is cancelled and the child
inherits `Canceled` — which is not our deadline, so the distinction is made on the
`DeadlineExceeded` test alone. What it does **not** cover: the same-instant race
where the deadline fires as a cancel lands and the child records
`DeadlineExceeded` while the parent is already done. That second case is the
`ctx.Err() == nil` clause in `waitForReplyOnce`, which is defended by reasoning
rather than by an assertion here — see its comment, which says so.
*/
func TestReplyTimeout_DoesNotMistakeACancellationForATimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := waitForReplyOnce(ctx, neverReplies{}, "job-1", "Which auth model?", time.Hour)

	var timeoutErr replyTimeoutErr
	if errors.As(err, &timeoutErr) {
		t.Fatalf("a cancelled run must not be reported as a timeout, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancellation to come through, got %T (%v)", err, err)
	}
}

/*
The design claim: the bound is **per wait, not per run**.

A grill with several questions is the normal case rather than an edge one, and a
whole-run bound would kill a productive interview partway through — twelve
questions answered within twenty minutes each is three hours of legitimate work
and exactly what the grill exists for. Bounding each wait instead measures the
thing that actually went wrong (*this* question, unanswered), so a long
conversation never accumulates toward a limit while an abandoned run is still
caught on the question it abandoned.

This is asserted rather than left to the comment because the failure it guards
against is invisible in any single-wait test: three waits of 30ms each total 90ms
against a 50ms bound, so an implementation that started one deadline for the whole
run would fail on the second or third wait and this asserts that it does not.
*/
func TestReplyTimeout_AppliesToEachWaitRatherThanTheWholeRun(t *testing.T) {
	const (
		each = 30 * time.Millisecond
		// A bound longer than one wait and shorter than three, which is the only
		// window that distinguishes the two designs.
		bound = 50 * time.Millisecond
		waits = 3
	)

	waiter := &slowReplyWaiter{delay: each}
	start := time.Now()
	for i := 0; i < waits; i++ {
		if _, err := waitForReplyOnce(context.Background(), waiter, "job-1", "Which auth model?", bound); err != nil {
			t.Fatalf("wait %d of %d failed, so the bound is being applied to the run rather than to the wait: %v", i+1, waits, err)
		}
	}
	total := time.Since(start)

	if total < 3*each {
		t.Fatalf("expected %d waits of %s to take at least %s, got %s", waits, each, 3*each, total)
	}
	// And the total genuinely exceeded the bound, or the test would pass for the
	// wrong reason by never having exercised the distinction.
	if total <= bound {
		t.Fatalf("the test did not actually exceed the bound (%s vs %s), so it proves nothing", total, bound)
	}
}

// slowReplyWaiter answers after `delay`, standing in for a human who takes a
// while but does answer.
type slowReplyWaiter struct{ delay time.Duration }

func (s *slowReplyWaiter) WaitForReply(ctx context.Context, _ string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(s.delay):
		return "use-oauth", nil
	}
}

// A question is model-authored prose, so the excerpt has to survive whatever it
// contains: multi-line text, and characters outside ASCII.
func TestReplyExcerpt_CollapsesWhitespaceAndBoundsLength(t *testing.T) {
	// Newlines are meaningful in the transcript and noise in a one-line
	// `last_error`, so they collapse to single spaces.
	if got := excerptOf("  Which\n  database\n\nshould we use?  ", 200); got != "Which database should we use?" {
		t.Fatalf("expected collapsed whitespace, got %q", got)
	}

	// Bounded, and marked as bounded so a reader knows text was withheld rather
	// than the question being oddly short. Asserted in **runes**, because that is
	// the contract: the marker is counted inside the budget, so a caller bounding
	// a message can rely on the bound rather than discovering the marker pushed it
	// over.
	long := strings.Repeat("word ", 100)
	got := excerptOf(long, 40)
	if n := len([]rune(got)); n > 40 {
		t.Fatalf("expected at most 40 runes, got %d: %q", n, got)
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("expected a truncation marker, got %q", got)
	}
	// A degenerate budget must not panic or produce something longer than asked.
	if tiny := excerptOf(long, 1); len([]rune(tiny)) > 1 {
		t.Fatalf("expected a 1-rune budget to yield at most the marker, got %q", tiny)
	}

	// A multi-byte question must not be cut mid-rune, which would put a
	// replacement character in an operator-facing message. Emoji and CJK are both
	// realistic — the questions are model-authored.
	for _, q := range []string{strings.Repeat("中", 100), strings.Repeat("🎉", 100)} {
		got := excerptOf(q, 20)
		if strings.ContainsRune(got, '\uFFFD') {
			t.Fatalf("excerpt cut a rune in half: %q", got)
		}
		if n := len([]rune(got)); n > 20 {
			t.Fatalf("expected at most 20 runes for a multi-byte question, got %d: %q", n, got)
		}
	}

	// Nothing in, nothing out — so the caller omits the quote rather than
	// printing an empty pair of quotes.
	if got := excerptOf("   \n  ", 40); got != "" {
		t.Fatalf("expected an empty excerpt for blank text, got %q", got)
	}
}

func TestConfigReplyTimeout_DefaultsWhenUnset(t *testing.T) {
	// The same shape as `previewTTL`: zero or negative means "take the default".
	// There is deliberately no value that disables the bound, because an unbounded
	// wait is the bug — see `Config.ReplyTimeout`.
	for _, configured := range []time.Duration{0, -1 * time.Second} {
		cfg := Config{ReplyTimeout: configured}
		if got := cfg.replyTimeout(); got != defaultReplyTimeout {
			t.Fatalf("expected the default for %s, got %s", configured, got)
		}
	}

	cfg := Config{ReplyTimeout: 90 * time.Minute}
	if got := cfg.replyTimeout(); got != 90*time.Minute {
		t.Fatalf("expected the configured value to win, got %s", got)
	}

	// The default itself, pinned so a change to it is a visible decision rather
	// than a silent shift in how long an abandoned run holds a pod.
	if defaultReplyTimeout != 24*time.Hour {
		t.Fatalf("expected a 24h default, got %s", defaultReplyTimeout)
	}
}

/*
Issue #96: the API mirrors this bound, so the pairing is pinned from this side too.

`api/src/config.test.ts` declares this repo's default locally and asserts its own
constant equals it — which catches a change to the **API's** copy. It cannot catch a
change to **this** default, because that suite cannot read this repo. So the
mirror-image assertion lives here, following the precedent #63 set for
`capabilities.DefaultReportInterval` against the API's trust window: whichever repo
owns one side of a cross-repo pair declares the other's value locally, because
neither suite can see the other repo.

**What this catches, and what it does not.** It catches a *shipped default or
variable name* drifting on either side — change one and not the other, and one
repo's suite goes red naming the other repo, which is the property that makes a
duplicate safe rather than merely documented. It cannot catch an operator setting
`GRILL_REPLY_TIMEOUT` on this side only: the API cannot read this process's
environment, which is why its read exposes `timeoutSource` so a client can hedge a
countdown that may be an assumption. Both halves are stated here rather than left
for a reader to assume the assertions prove more — the same honesty the API's own
module comment applies from the other direction.
*/
func TestDefaultReplyTimeout_MatchesTheAPIsMirror(t *testing.T) {
	// The API's copy, transcribed from `api/src/config.ts`'s
	// `DEFAULT_GRILL_REPLY_TIMEOUT_MS` — and in the **API's own unit**, so a
	// mismatch reports as a number to compare rather than as two durations a reader
	// has to convert in their head.
	const apiDefaultReplyTimeoutMs = 24 * 60 * 60 * 1000

	if got := defaultReplyTimeout.Milliseconds(); got != apiDefaultReplyTimeoutMs {
		t.Fatalf(
			"this default (%s) must equal the API's mirror (%d ms, declared in api/src/config.ts): "+
				"the API renders a wait countdown from its copy and cannot detect a change to this one",
			defaultReplyTimeout, apiDefaultReplyTimeoutMs,
		)
	}
}

// The other half of the pairing, and the one a rename would break silently: a
// different name here would leave a value that is still settable in one `.env` and
// quietly ignored in the other, which is exactly the "duplicate that cannot be kept
// in sync" the copy exists to avoid.
func TestReplyTimeoutEnv_MatchesTheAPIsExpectation(t *testing.T) {
	// The API's own constant, transcribed from `api/src/config.ts`'s
	// `GRILL_REPLY_TIMEOUT_ENV` (which asserts its name on that side).
	const apiReplyTimeoutEnv = "GRILL_REPLY_TIMEOUT"

	if ReplyTimeoutEnv != apiReplyTimeoutEnv {
		t.Fatalf(
			"expected the variable name the API reads (%q), got %q — the two files would no longer be settable from one value",
			apiReplyTimeoutEnv, ReplyTimeoutEnv,
		)
	}
}

/*
The end-to-end half: a session whose question goes unanswered must **return**, and
must fail the run with a legible reason.

Without this the helper tests above would pass while nothing called the helper —
which is precisely the #38/#59/#73/#88 shape this burn-down keeps finding: a
correct piece of code that is inert because it is not wired to anything.
*/
func TestDriveAgentSession_FailsAnUnansweredQuestionRatherThanHanging(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Asks one question, then blocks forever — the shape of a run nobody answers.
	script := `read line; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var mu sync.Mutex
	var received []rpc.CuratedEvent
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- driveAgentSession(
			ctx, clientset.Interface, restConfig,
			neverReplies{}, neverCancels{},
			namespace, podName, "job-1", "New feature: dark mode",
			// Short, so the bound is exercised within the test rather than after
			// twenty-four hours.
			150*time.Millisecond,
			func(ev rpc.CuratedEvent) {
				mu.Lock()
				received = append(received, ev)
				mu.Unlock()
			},
			noStats, discardUsage, nil,
		)
	}()

	select {
	case err := <-sessionDone:
		if err == nil {
			t.Fatal("expected the session to fail rather than hang, got nil")
		}
		var timeoutErr replyTimeoutErr
		if !errors.As(err, &timeoutErr) {
			t.Fatalf("expected a replyTimeoutErr, got %T (%v)", err, err)
		}
	case <-time.After(30 * time.Second):
		// The failure this whole change exists to prevent: an unanswered question
		// holding the session — and therefore the pod and the concurrency slot —
		// open for as long as the process lives.
		t.Fatal("driveAgentSession never returned, so an unanswered question still hangs the run")
	}

	mu.Lock()
	defer mu.Unlock()

	// The question itself must have been relayed before the wait began — it is
	// what makes the failure explainable, and what a retry reads in the
	// transcript.
	if len(received) == 0 || received[0].Type != rpc.EventAskUser {
		t.Fatalf("expected the question to be relayed first, got %+v", received)
	}

	// And the run must fail with the reason, not merely stop: `EventRunFailed` is
	// what carries the message into `jobs.last_error` and marks the job failed
	// rather than cancelled (ADR 012's retry semantics hang off that distinction).
	var failure *rpc.CuratedEvent
	for i := range received {
		if received[i].Type == rpc.EventRunFailed {
			failure = &received[i]
		}
	}
	if failure == nil {
		t.Fatalf("expected an EventRunFailed naming the timeout, got %+v", received)
	}
	for _, want := range []string{"150ms", "no reply", "Which auth model?"} {
		if !strings.Contains(failure.Message, want) {
			t.Fatalf("expected the failure to mention %q, got: %s", want, failure.Message)
		}
	}
	// A timeout is not a cancellation, and the two reach the API as different job
	// statuses — so nothing here may look like one.
	for _, ev := range received {
		if ev.Type == rpc.EventRunCancelled {
			t.Fatalf("a timeout must not be reported as a cancellation, got %+v", received)
		}
	}
}
