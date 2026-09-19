package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// usageFetchTimeout bounds the extra turn spent asking Pi for its session
// accounting. This runs after the job's real work is already finished, so it
// must never be able to hold a worker slot open indefinitely: a Pi process that
// accepted the command but never answered must not turn a completed job into a
// stuck one (defaultMaxConcurrentJobs is small — ADR 006 item 1).
const usageFetchTimeout = 20 * time.Second

// forkMessagesGrace bounds how long the terminal turn waits for
// `get_fork_messages` *after* the two required answers have already arrived.
//
// **Why a separate, shorter grace rather than treating it as a third required
// answer.** The fork points are ADR 032 item 2's input and worth having, but they
// are not worth what awaiting them unconditionally would cost: this turn is the
// only chance to capture the session file (item 1) and the run's accounting
// (ADR 023), and a Pi that answers those two but not this one would otherwise
// burn the full usageFetchTimeout and then report the *required* pair as a
// failure — degrading session collection for every run to gain fork points for
// some. So the required pair still decides the turn's outcome, and this bounds
// only the extra wait.
//
// Two seconds is generous for one command whose answer Pi computes in-process
// from the session tree it already holds — unlike anything that calls a model —
// and the timer is only armed once both required answers are in, so it can never
// extend the turn beyond `usageFetchTimeout`.
const forkMessagesGrace = 2 * time.Second

// sessionReport is what a live session tells us about itself at the end of a
// run: its own accounting (ADR 023) and where its session file lives (ADR 032
// item 1).
//
// **Why one type rather than two fetches.** Both facts are answers to the same
// question — "what does this session say about itself now that it has stopped" —
// and both must be read in the *same* terminal turn, because Pi's stdin is
// per-attach (see k8s.Attach's doc comment) and the pod is deleted as soon as the
// session function returns. Two separate fetches would mean two attaches, two
// timeouts, and a second chance for the pod to be gone between them, for no gain.
//
// The fields are separate rather than one merged payload so that a partial
// answer is expressible: Pi can report accounting without a session file
// (`--no-session`), and the ADR 032 item 5 distinction between "no session was
// written" and "the session could not be read" depends on being able to say so.
type sessionReport struct {
	Stats rpc.SessionStats
	// Session is zero when Pi reported no session file — a session created
	// in-memory, or a run that never got far enough to write one. collectSession
	// treats an empty FilePath as `not_collected` rather than as a failure.
	Session rpc.SessionFile
	// Fork is item 2's answer: which previous user messages this session can be
	// forked from, with Pi's own entry ids.
	//
	// **Zero-with-Asked=false is the ordinary case on a Pi that did not answer it**,
	// and that is a different fact from `Asked=true` with no points — the same
	// distinction Session.Asked carries for the session file, and for the same
	// reason (ADR 032 item 5, applied to a second question). It is carried as
	// `rpc.ForkMessages` rather than as a bare slice so the distinction survives
	// this function rather than having to be re-invented by its caller.
	Fork rpc.ForkMessages
	// stateAnswered distinguishes "Pi answered get_state and reported no file"
	// from "Pi has not answered yet". The two are different facts and only the
	// first is a result: without this the loop below could return on a session
	// that legitimately has no file before its own answer had arrived, and the
	// empty FilePath would be indistinguishable from a missing response.
	stateAnswered bool
}

// sessionStatsFetcher asks a live session for that report (ADR 023, ADR 032
// item 1). A function type rather than a direct call, for exactly the reason
// replyWaiter and cancelWatcher are interfaces (specgrill.go): the real
// implementation needs a live attached pod speaking Pi's RPC, so tests that
// attach to a stand-in pod substitute a fake and keep exercising session
// control instead of blocking on an endpoint their pod cannot answer.
//
// The timeout below is the real implementation's own bound, not this type's —
// a substitute is free to answer immediately.
type sessionStatsFetcher func(ctx context.Context, rpcClient *rpc.Client, namespace, podName string) (sessionReport, error)

// fetchSessionStats asks the job's still-running Pi session for its own
// token/cost accounting (ADR 023) and its session file path (ADR 032 item 1)
// over the same RPC session driving the run.
//
// It opens one more k8s.Attach turn, exactly like runTurn, because Pi's stdin
// is per-attach (see k8s.Attach's doc comment): there is no way to ask a
// question outside a turn. This is deliberately the *last* thing that happens
// to the pod — runAgentRPCJob's deferred DeleteJob runs as soon as the session
// function returns, so this is the final moment the questions can be answered at
// all.
//
// **All three commands go out in this one turn**, and the answers are collected by
// matching each response's own `command` field rather than by position. That is
// the whole reason this is one function: a second turn for the session file would
// need a second attach and a second timeout, and would give the pod another
// chance to be gone in between — while `get_state` costs one more line on a
// stream that is already open.
//
// **They are matched by name because they are genuinely not ordered.** A real Pi
// 0.84.4 was sent `fork`, then `get_state`, then `get_fork_messages` on one turn
// and answered in the order get_state, get_fork_messages, fork: a command that
// touches the session tree answers later than ones that read it, so arrival order
// is not send order. Matching by name is also what makes an interleaved unrelated
// response harmless, and it is the same reason ParseSessionStats and
// ParseSessionFile each check their own `command` field.
//
// **A missing session file is not an error.** A session created in-memory
// legitimately has none, and ADR 032 item 5 needs that case to reach the caller as
// `not_collected` rather than as a failure. So an empty `sessionFile` returns a
// zero SessionFile with a nil error, and only a stream that ends before *any*
// answer arrives is an error — because then nothing is known at all.
//
// **A missing fork-message answer is not an error either, and is not awaited as
// one.** It is the third command on this turn and the least important of the
// three: the session file is item 1's artifact and the accounting is ADR 023's, so
// a Pi that answers those two and ignores this one must still have them reported.
// The answer is therefore taken if it arrives within `forkMessagesGrace` of the
// required pair, and otherwise left with `Asked=false` — which is the honest
// answer, since nothing then established whether there are fork points.
//
// Every failure path is therefore reported to the caller rather than retried:
// there is nothing left to retry against.
func fetchSessionStats(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	rpcClient *rpc.Client,
	namespace, podName string,
) (sessionReport, error) {
	statsCtx, cancel := context.WithTimeout(ctx, usageFetchTimeout)
	defer cancel()

	// Same reason runTurn discards stale events first: the turn that just
	// ended can still have its own trailing agent_end/agent_settled
	// bookkeeping sitting in the shared Events buffer, and this loop must not
	// read that as its own answer.
	rpcClient.DrainStaleEvents()

	stdin, err := rpcClient.BeginTurn()
	if err != nil {
		return sessionReport{}, fmt.Errorf("failed to begin usage turn: %w", err)
	}

	attachCtx, cancelAttach := context.WithCancel(statsCtx)
	defer cancelAttach()

	attachErr := make(chan error, 1)
	go func() {
		attachErr <- k8s.Attach(attachCtx, clientset, restConfig, namespace, podName, k8s.RunContainerName, stdin, rpcClient, rpcClient)
	}()

	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandGetSessionStats}); err != nil {
		return sessionReport{}, fmt.Errorf("failed to request session stats: %w", err)
	}
	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandGetState}); err != nil {
		return sessionReport{}, fmt.Errorf("failed to request session state: %w", err)
	}
	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandGetForkMessages}); err != nil {
		return sessionReport{}, fmt.Errorf("failed to request fork messages: %w", err)
	}

	// Two answers are *required* — the stats (ADR 023's older contract) and the
	// state (what ADR 032 item 1 exists for) — and the fork messages are awaited
	// best-effort behind a short grace, so a Pi that answers the first two cannot
	// lose them to a third command it ignored (see forkMessagesGrace).
	//
	// The loop returns only once the required pair is in; the grace only decides
	// whether it waits a little longer for the third. A partial report is a result,
	// not an error: the required facts are what make it usable.
	var report sessionReport
	var gotStats bool
	// armed is the grace timer for the fork-messages answer, started once the
	// required pair has arrived and never before — so it cannot extend the turn
	// while something required is still outstanding.
	var grace *time.Timer
	var graceC <-chan time.Time
	defer func() {
		if grace != nil {
			grace.Stop()
		}
	}()

	// finish tears the turn down and returns the report as it stands, which is the
	// one exit path for "the required pair arrived" — whether the fork answer came,
	// ran out of grace, or the caller stopped waiting.
	finish := func() (sessionReport, error) {
		if err := closeUsageTurn(statsCtx, rpcClient, attachErr, namespace, podName); err != nil {
			// The readings were read successfully, but the attach did not
			// tear down cleanly. Report them anyway and let the caller log the
			// teardown problem: discarding a real reading because of a late
			// attach hiccup would lose data for no benefit — the pod is
			// deleted next regardless.
			log.Printf("worker: usage turn for pod %s/%s did not close cleanly: %v", namespace, podName, err)
		}
		return report, nil
	}

	for {
		select {
		case ev, ok := <-rpcClient.Events():
			if !ok {
				return sessionReport{}, fmt.Errorf("RPC event stream for pod %s/%s ended before the session-stats response arrived", namespace, podName)
			}
			if stats, matched := rpc.ParseSessionStats(ev); matched {
				report.Stats = stats
				gotStats = true
			} else if session, matched := rpc.ParseSessionFile(ev); matched {
				report.Session = session
				report.stateAnswered = true
			} else if fork, matched := rpc.ParseForkMessages(ev); matched {
				report.Fork = fork
				// Answered: return immediately rather than waiting out the grace.
				if gotStats && report.stateAnswered {
					return finish()
				}
			} else {
				// Every other line on this stream is agent traffic this call
				// has no interest in.
				continue
			}
			if !gotStats || !report.stateAnswered {
				continue
			}
			// The required pair is in. The fork answer may already have arrived — a
			// real Pi answers in a different order from the one commands are sent,
			// so it often does — in which case there is nothing left to wait for.
			if report.Fork.Asked {
				return finish()
			}
			// Otherwise give it a bounded moment to arrive. `continue` rather than
			// falling through, so a duplicate of an already-read answer cannot
			// return early while the fork answer is still outstanding.
			if grace == nil {
				grace = time.NewTimer(forkMessagesGrace)
				graceC = grace.C
			}
			continue

		case <-graceC:
			// The fork answer never came. `Asked` stays false, which is how the
			// API tells "Pi said there are none" from "nobody found out"
			// (ADR 032 item 5) — it must not be inferred from an empty list.
			log.Printf("worker: no fork-message answer for pod %s/%s within %s; the session and its accounting are unaffected", namespace, podName, forkMessagesGrace)
			return finish()

		case err := <-attachErr:
			if err == nil {
				err = fmt.Errorf("attach stream to pod %s/%s ended unexpectedly while reading session stats", namespace, podName)
			}
			return sessionReport{}, err

		case <-statsCtx.Done():
			return sessionReport{}, statsCtx.Err()
		}
	}
}

// closeUsageTurn ends the usage turn's stdin pipe and waits for its Attach call
// to return — the same three-way race closeOutTurn handles for a normal turn,
// kept separate because this turn's result is a stats payload rather than a
// curated event to hand back.
func closeUsageTurn(
	ctx context.Context,
	rpcClient *rpc.Client,
	attachErr <-chan error,
	namespace, podName string,
) error {
	if err := rpcClient.EndTurn(); err != nil {
		return fmt.Errorf("failed to end usage turn: %w", err)
	}
	select {
	case endErr := <-attachErr:
		if endErr != nil {
			return fmt.Errorf("attach ended with an error while closing out the usage turn: %w", endErr)
		}
	case <-time.After(endTurnGrace):
		return fmt.Errorf("attach call for pod %s/%s did not end within %s of ending the usage turn", namespace, podName, endTurnGrace)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// usageReportFrom builds the API payload for one finished job: Pi's own
// accounting, the wall-clock time the agent actually spent working, and the
// model id the pod ran with.
//
// The model id is read from the pod env the Orchestrator itself built, so it
// records what actually served the run rather than what the API's config
// resolution currently says should have — if the two ever disagree (a
// configuration change mid-run), the pod is the ground truth. Only MODEL_ID is
// ever read; nothing else from that env map reaches the API.
//
// Extracted from the closure that calls it so the mapping is unit-testable
// without a cluster.
func usageReportFrom(stats rpc.SessionStats, turnDuration time.Duration, env map[string]string) apiclient.JobUsage {
	durationMs := turnDuration.Milliseconds()
	return apiclient.JobUsage{
		ModelID:          env["MODEL_ID"],
		InputTokens:      stats.InputTokens,
		OutputTokens:     stats.OutputTokens,
		CacheReadTokens:  stats.CacheReadTokens,
		CacheWriteTokens: stats.CacheWriteTokens,
		TotalTokens:      stats.TotalTokens,
		CostUSD:          stats.CostUSD,
		DurationMs:       &durationMs,
	}
}
