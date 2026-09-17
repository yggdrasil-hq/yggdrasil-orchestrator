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

// sessionStatsFetcher asks a live session for its own token/cost accounting
// (ADR 023). A function type rather than a direct call, for exactly the reason
// replyWaiter and cancelWatcher are interfaces (specgrill.go): the real
// implementation needs a live attached pod speaking Pi's RPC, so tests that
// attach to a stand-in pod substitute a fake and keep exercising session
// control instead of blocking on an endpoint their pod cannot answer.
//
// The timeout above is the real implementation's own bound, not this type's —
// a substitute is free to answer immediately.
type sessionStatsFetcher func(ctx context.Context, rpcClient *rpc.Client, namespace, podName string) (rpc.SessionStats, error)

// fetchSessionStats asks the job's still-running Pi session for its own
// token/cost accounting (ADR 023) over the same RPC session driving the run.
//
// It opens one more k8s.Attach turn, exactly like runTurn, because Pi's stdin
// is per-attach (see k8s.Attach's doc comment): there is no way to ask a
// question outside a turn. This is deliberately the *last* thing that happens
// to the pod — runAgentRPCJob's deferred DeleteJob runs as soon as the session
// function returns, so this is the final moment the question can be answered at
// all. Every failure path is therefore reported to the caller rather than
// retried: there is nothing left to retry against.
func fetchSessionStats(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	rpcClient *rpc.Client,
	namespace, podName string,
) (rpc.SessionStats, error) {
	statsCtx, cancel := context.WithTimeout(ctx, usageFetchTimeout)
	defer cancel()

	// Same reason runTurn discards stale events first: the turn that just
	// ended can still have its own trailing agent_end/agent_settled
	// bookkeeping sitting in the shared Events buffer, and this loop must not
	// read that as its own answer.
	rpcClient.DrainStaleEvents()

	stdin, err := rpcClient.BeginTurn()
	if err != nil {
		return rpc.SessionStats{}, fmt.Errorf("failed to begin usage turn: %w", err)
	}

	attachCtx, cancelAttach := context.WithCancel(statsCtx)
	defer cancelAttach()

	attachErr := make(chan error, 1)
	go func() {
		attachErr <- k8s.Attach(attachCtx, clientset, restConfig, namespace, podName, "run", stdin, rpcClient, rpcClient)
	}()

	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandGetSessionStats}); err != nil {
		return rpc.SessionStats{}, fmt.Errorf("failed to request session stats: %w", err)
	}

	for {
		select {
		case ev, ok := <-rpcClient.Events():
			if !ok {
				return rpc.SessionStats{}, fmt.Errorf("RPC event stream for pod %s/%s ended before the session-stats response arrived", namespace, podName)
			}
			stats, matched := rpc.ParseSessionStats(ev)
			if !matched {
				// Every other line on this stream is agent traffic this call
				// has no interest in.
				continue
			}
			if err := closeUsageTurn(statsCtx, rpcClient, attachErr, namespace, podName); err != nil {
				// The numbers were read successfully, but the attach did not
				// tear down cleanly. Report the stats anyway and let the
				// caller log the teardown problem: discarding a real reading
				// because of a late attach hiccup would lose data for no
				// benefit — the pod is deleted next regardless.
				log.Printf("worker: usage turn for pod %s/%s did not close cleanly: %v", namespace, podName, err)
			}
			return stats, nil

		case err := <-attachErr:
			if err == nil {
				err = fmt.Errorf("attach stream to pod %s/%s ended unexpectedly while reading session stats", namespace, podName)
			}
			return rpc.SessionStats{}, err

		case <-statsCtx.Done():
			return rpc.SessionStats{}, statsCtx.Err()
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
