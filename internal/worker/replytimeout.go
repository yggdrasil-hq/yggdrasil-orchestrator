package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

/*
Issue #82: a grill question nobody answers used to wait forever.

`ask_user` ends the agent's **turn** without ending the **run** (it is deliberately
absent from `CuratedEvent.Terminal`), after which `driveAgentSession` blocks on
`msgs.WaitForReply`. Nothing bounded that wait: `runCtx` is cancelled only by a
human's cancel request or by process shutdown, so a `spec_grill` that asked a
question nobody answered held its pod and its concurrency slot **indefinitely**.

Two shapes of that, and the second is why this is a correctness fix rather than a
housekeeping one:

1. **A run nobody is watching.** A job dispatched while the operator is away keeps
   a pod alive for as long as the Orchestrator runs. ADR 030's per-project quota
   counts pods, so a handful of these consume a project's whole allocation with
   nothing happening.
2. **A run nobody *can* answer.** #38's API half now rejects `ask_user` on a job
   kind with no chat surface, so the new tool path fails loudly rather than
   stalling — but that is a backstop on the *question*, not a guarantee about the
   *reply*: any future kind that gains the tool without gaining a surface reaches
   this wait with no one able to enter anything.

There is also a quieter reason: the pod holds the project's GitHub token and the
model key (ADR 004). An indefinitely idle pod holding credentials is a standing
surface, not only a stranded resource.
*/

// ReplyTimeoutEnv is the environment variable that overrides defaultReplyTimeout.
//
// **Exported because the name is half of a cross-repo contract.** The API mirrors
// this bound (`api/src/config.ts`, `DEFAULT_GRILL_REPLY_TIMEOUT_MS`) because it
// renders a "waiting 18h" countdown from it, and it reads **the same variable
// name** on purpose: one value an operator writes must be pasteable into both
// `.env` files. The API asserts the name it expects (`GRILL_REPLY_TIMEOUT_ENV`);
// `replytimeout_test.go` asserts this one matches it, so a rename here cannot
// silently leave the value settable in one file and ignored in the other.
const ReplyTimeoutEnv = "GRILL_REPLY_TIMEOUT"

// defaultReplyTimeout bounds one unanswered question.
//
// **24h, and the number is the point rather than an implementation detail.** The
// grill is human-gated *by design*, so waiting a long time is correct and the
// bound exists to catch "nobody is coming", not "the human is slow". Twenty-four
// hours spans a full working day plus an overnight gap, which is the longest a
// legitimate interview plausibly idles before answering is the less likely
// explanation — the same reasoning issue #82 proposed ("24h is defensible for a
// human-gated interview and 1h far too aggressive").
//
// Deliberately generous rather than tight. A bound that fires on a slow but real
// human is worse than the stall it prevents: it discards a run that was going to
// finish, and it would teach operators to distrust the timeout.
const defaultReplyTimeout = 24 * time.Hour

// replyExcerptLimit bounds the question text quoted into the failure message.
//
// The message lands in `jobs.last_error` and is read by a person, so it wants
// enough to recognise which question went unanswered and not a pasted paragraph.
// The full text is not lost — it is on the `job_events` row and in the transcript
// — which is what makes quoting only an excerpt honest rather than lossy.
const replyExcerptLimit = 160

// replyTimeout is the configured bound, or the default when unset.
//
// See `Config.ReplyTimeout` for why no value disables it.
func (c Config) replyTimeout() time.Duration {
	if c.ReplyTimeout <= 0 {
		return defaultReplyTimeout
	}
	return c.ReplyTimeout
}

// waitForReplyOnce waits for jobID's reply to the question just asked, bounded by
// replyTimeout, and returns a timeout error rather than waiting forever.
//
// **The bound is per wait, not per run, and that is the substance of the fix.**
// A conversation with several questions is the normal case for a grill, and
// bounding the whole run would kill a productive interview partway through — a
// twelve-question interview that answered each within twenty minutes would be
// terminated for taking three hours in total, which is exactly what the grill is
// for. Bounding each wait measures what actually went wrong instead: *this*
// question, unanswered. A run making progress is one where every wait ends, so a
// long conversation never accumulates toward a limit, and an abandoned run is
// caught on the question it abandoned.
//
// A reply that is already pending is returned immediately by `claimPendingReply`
// before any waiting happens, so this adds no latency to the ordinary path and
// cannot fail a question that was in fact answered.
//
// The deadline is applied here rather than inside `messages.Store` on purpose:
// how long is too long is policy, which belongs with `Config`; `Store` is the
// transport and stays free of it. The same reason `cancelWatcher` and
// `replyWaiter` are narrow interfaces rather than the concrete types.
func waitForReplyOnce(
	ctx context.Context,
	msgs replyWaiter,
	jobID string,
	question string,
	replyTimeout time.Duration,
) (string, error) {
	waitCtx, cancelWait := context.WithTimeout(ctx, replyTimeout)
	defer cancelWait()

	reply, err := msgs.WaitForReply(waitCtx, jobID)
	if err == nil {
		return reply, nil
	}

	/*
	 * Distinguish *our* deadline from the run being torn down, because the two
	 * want opposite messages: one is "nobody answered", the other is "someone
	 * asked to stop" or a process shutdown, and `reportSessionError` already
	 * maps the latter correctly (a cancellation becomes EventRunCancelled and a
	 * job status of `cancelled`, not `failed`).
	 *
	 * The `DeadlineExceeded` test is the load-bearing one, and it **is** guarded by
	 * `TestReplyTimeout_DoesNotMistakeACancellationForATimeout`: a parent
	 * cancellation arrives as `Canceled`, which is not our deadline, so the
	 * ordinary case is reported correctly on that assertion alone.
	 *
	 * `ctx.Err() == nil` covers a **same-instant race** — the deadline firing in
	 * the moment a cancel lands, where the child can record `DeadlineExceeded`
	 * while the parent is already done. Without it, that run would be reported as
	 * "nobody answered" when a human had just cancelled it.
	 *
	 * **That clause is not claimed as test-covered.** Removing it fails no test
	 * here, because Go's context records whichever error came first and a test
	 * cannot fix that ordering without racing it — so it is defended on the
	 * reasoning above rather than on an assertion. Said plainly because "this is
	 * guarded" and "this is reasoned" are different claims, and a comment that
	 * conflates them is how an unguarded branch comes to be trusted.
	 */
	if errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		return "", replyTimeoutErr{waited: replyTimeout, question: question}
	}

	return "", err
}

// replyTimeoutErr is the failure a question that went unanswered produces.
//
// Its own type rather than a formatted `errors.New` so a caller can recognise the
// case without matching on message text — which matters because the message is
// user-facing prose and would otherwise be load-bearing for control flow.
type replyTimeoutErr struct {
	waited   time.Duration
	question string
}

func (e replyTimeoutErr) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b,
		"no reply to this grill question within %s — the run was failed so its pod and "+
			"concurrency slot are released rather than held indefinitely. The question is "+
			"recorded in the transcript, so the feature can be retried once someone can answer it.",
		e.waited,
	)

	// The question is quoted so an operator scanning a list of failed jobs can
	// tell *which* one stalled without opening the transcript. Omitted rather
	// than printed as an empty pair of quotes when the tool sent none.
	if excerpt := excerptOf(e.question, replyExcerptLimit); excerpt != "" {
		fmt.Fprintf(&b, " Unanswered: %q", excerpt)
	}

	return b.String()
}

// excerptOf collapses whitespace and bounds length, for quoting model-authored
// text into a message a person reads.
//
// **`limit` is a rune count, and the result is never longer than it** — the
// ellipsis is counted inside the budget rather than added on top of it, so a
// caller bounding a message can rely on the bound instead of discovering that the
// marker pushed it over. Working in runes rather than bytes is what keeps a
// multi-byte character from being cut in half into a replacement character in an
// operator-facing string; the questions are model-authored and routinely contain
// punctuation outside ASCII.
//
// Whitespace is collapsed because a question may be a multi-line paragraph: the
// newlines are meaningful in the transcript and noise in a single-line
// `last_error`.
func excerptOf(s string, limit int) string {
	collapsed := strings.Join(strings.Fields(s), " ")
	if limit <= 0 {
		return ""
	}

	runes := []rune(collapsed)
	if len(runes) <= limit {
		return collapsed
	}

	// One rune of the budget goes to the marker, so `limit: 1` is just the marker.
	if limit == 1 {
		return "…"
	}
	// Trailing space is dropped so the marker does not read as "word …" when the
	// cut happened to land on a word boundary.
	return strings.TrimRight(string(runes[:limit-1]), " ") + "…"
}
