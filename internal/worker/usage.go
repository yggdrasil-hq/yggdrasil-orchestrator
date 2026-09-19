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
// **Both commands go out in this one turn**, and the two answers are collected by
// matching each response's own `command` field rather than by position. That is
// the whole reason this is one function: a second turn for the session file would
// need a second attach and a second timeout, and would give the pod another
// chance to be gone in between — while `get_state` costs one more line on a
// stream that is already open. (Pi answers commands in the order they arrive, so
// the two are not ordered by construction here; they are matched by name, which
// is also what makes an interleaved unrelated response harmless.)
//
// **A missing session file is not an error.** A session created in-memory
// legitimately has none, and ADR 032 item 5 needs that case to reach the caller as
// `not_collected` rather than as a failure. So an empty `sessionFile` returns a
// zero SessionFile with a nil error, and only a stream that ends before *any*
// answer arrives is an error — because then nothing is known at all.
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

	// Both answers are awaited, but only the stats answer is *required*: it is
	// the older contract (ADR 023) and a session that cannot report its accounting
	// is a genuine failure to read. The state answer is awaited too, because it is
	// what ADR 032 item 1 exists for, and returning before it arrives would make
	// the session file depend on which response happened to win the race.
	var report sessionReport
	var gotStats bool
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
				// The loop keeps going until *both* have been seen, so the order Pi
				// answers in does not change the result.
			} else {
				// Every other line on this stream is agent traffic this call
				// has no interest in.
				continue
			}
			if !gotStats || !report.stateAnswered {
				continue
			}
			if err := closeUsageTurn(statsCtx, rpcClient, attachErr, namespace, podName); err != nil {
				// The readings were read successfully, but the attach did not
				// tear down cleanly. Report them anyway and let the caller log the
				// teardown problem: discarding a real reading because of a late
				// attach hiccup would lose data for no benefit — the pod is
				// deleted next regardless.
				log.Printf("worker: usage turn for pod %s/%s did not close cleanly: %v", namespace, podName, err)
			}
			return report, nil

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
