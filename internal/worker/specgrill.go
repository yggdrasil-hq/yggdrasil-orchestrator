package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// endTurnGrace bounds how long runTurn waits for an Attach call to return
// after EndTurn closes its stdin pipe. Attach reliably returns almost
// immediately once stdin reaches EOF (verified against a real pod); this
// is a safety net against a hang, not an expected wait.
const endTurnGrace = 10 * time.Second

// errJobCancelled is returned by driveSpecGrillSession (and propagates
// unwrapped through runSpecGrillJob/runInCluster) when a human asked to
// stop the run, so runClaimedJob's log line can say "cancelled" instead of
// "failed", and so callers can tell the two apart via errors.Is. The DB
// outcome doesn't depend on this: queue.Queue.Fail's own guard (only
// transitions a 'running' row) is what actually stops the job's already-
// 'cancelled' status from being clobbered, regardless of this sentinel.
var errJobCancelled = errors.New("job cancelled")

// replyWaiter is the subset of *messages.Store (ADR 006 items 9-10)
// driveSpecGrillSession needs — an interface so tests can substitute a
// fake reply source without a real Postgres connection, decoupling
// "does the session correctly pause/resume around ask_user" (testable
// against a real attached pod with a fake replyWaiter) from "does
// messages.Store really deliver via LISTEN/NOTIFY" (internal/messages'
// own tests, against a real Postgres).
type replyWaiter interface {
	WaitForReply(ctx context.Context, jobID string) (string, error)
}

// cancelWatcher is the subset of *queue.Queue driveSpecGrillSession needs
// to learn a human asked to stop this job (the cancel/abort follow-up
// tracked in ADR 006's "Follow-ups" section) — an interface for the same
// reason as replyWaiter: tests can substitute a fake without a real
// Postgres connection.
type cancelWatcher interface {
	WatchCancellation(ctx context.Context, jobID string) error
}

// runSpecGrillJob runs a spec_grill job by attaching to its pod and driving
// Pi's RPC session directly (ADR 006 items 2-4, 7, 11), instead of the
// placeholder-compatible blocking k8s.RunJob every other job kind still
// uses (runAgentJob, worker.go). Only reached once a real image is
// configured for spec_grill (cfg.Images — see runInCluster's routing).
//
// Kubernetes Job status is not the completion signal here: Pi's RPC
// process never exits on its own, so driveSpecGrillSession itself decides
// when the run is over — from the event stream, not from Job.Status — and
// this function explicitly deletes the Job once it does (or once the
// session ends any other way), rather than waiting for Kubernetes to
// report success.
func runSpecGrillJob(ctx context.Context, q *queue.Queue, clientset kubernetes.Interface, job *queue.Job, namespace string, cfg Config) error {
	env, spec, err := buildAgentEnv(ctx, cfg, job)
	if err != nil {
		return err
	}

	image, command := resolveAgentImage(cfg, job.Kind)
	name := "job-" + job.ID

	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace:        namespace,
		Name:             name,
		Image:            image,
		Command:          command,
		Env:              env,
		RuntimeClassName: cfg.RuntimeClassName,
		Stdin:            true,
	}); err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	defer func() {
		// A background context: cleanup must still run even when ctx
		// itself is what ended the session (worker shutdown, deadline) —
		// an already-cancelled ctx here would make DeleteJob a no-op and
		// leak the pod.
		if err := k8s.DeleteJob(context.Background(), clientset, namespace, name); err != nil {
			log.Printf("worker: failed to delete spec_grill job %s: %v", job.ID, err)
		}
	}()

	podName, err := k8s.WaitForJobPod(ctx, clientset, namespace, name)
	if err != nil {
		return fmt.Errorf("pod never became attachable: %w", err)
	}

	initialPrompt := buildInitialPrompt(spec)

	return driveSpecGrillSession(ctx, clientset, cfg.RESTConfig, cfg.Messages, q, namespace, podName, job.ID, initialPrompt, func(ev rpc.CuratedEvent) {
		if err := cfg.APIClient.PostJobEvent(ctx, job.ID, ev); err != nil {
			// A failed relay is a visibility gap, not a job failure: the
			// job's actual outcome (an ADR submitted, or not) is decided by
			// the event itself, independent of whether this side-channel
			// post to the API succeeded.
			log.Printf("worker: failed to relay event %s for job %s: %v", ev.Type, job.ID, err)
		}
	})
}

// buildInitialPrompt states the job's constraints explicitly instead of
// leaving the agent to (re)discover them — verified against a real stuck
// run: a bare "New feature: <title>" prompt relied entirely on the model's
// own initiative to think to read a skill file before doing anything else,
// which isn't guaranteed, and gave no indication of where (or whether)
// repos were already on disk. This lists each repo's actual local path so
// the agent doesn't need to guess or re-derive it, mirroring entrypoint.sh's
// own clone layout (primary at /workspace, sub-repos at
// /workspace/<repo-name>) exactly.
//
// It also branches on spec.FeatureType (ADR 008 items 1-2) to name the
// exact skill file governing this run, rather than leaving the model to
// infer from Title which of the two spec_grill skills applies — Title for
// a project_init feature is the fixed, non-descriptive string "Project
// initialization" and carries no signal the container could use on its own.
func buildInitialPrompt(spec apiclient.FeatureSpec) string {
	var b strings.Builder
	if spec.FeatureType == "project_init" {
		b.WriteString("This is a project_init job — the very first spec_grill run for this ")
		b.WriteString("project. Your goal is to bootstrap/adapt the repo(s) below for Yggdrasil: ")
		b.WriteString("interview the user about what the project does, its tech stack, and how its ")
		b.WriteString("repos relate; check the target repo(s) against the structure standard; and ")
		b.WriteString("submit an ADR describing what to scaffold or restructure. Read ")
		b.WriteString("/root/.pi/agent/skills/project-init/SKILL.md first — it governs this entire ")
		b.WriteString("run, and is the only skill that applies here (not feature-grill).\n\n")
	} else {
		fmt.Fprintf(&b, "New feature: %s\n\n", spec.Title)
		b.WriteString("This is a normal feature's spec_grill job. Read ")
		b.WriteString("/root/.pi/agent/skills/feature-grill/SKILL.md first — it governs this entire ")
		b.WriteString("run, and is the only skill that applies here (not project-init).\n\n")
	}
	b.WriteString("You have read-only access to the repo(s) below — already cloned to the paths ")
	b.WriteString("listed, nothing left to fetch. Your only job is to explore, interview the user, ")
	b.WriteString("and call submit_adr; you cannot and must not modify these repos in any way.")
	b.WriteString("\n\nRepos:\n")
	for _, repo := range spec.Repos {
		dir := repoLocalDir(repo)
		role := "sub-repo"
		if repo.IsPrimary {
			role = "primary"
		}
		fmt.Fprintf(&b, "- %s (%s): %s\n", dir, role, repo.CloneURL)
	}
	return b.String()
}

// repoLocalDir mirrors entrypoint.sh's clone layout: the primary repo lands
// directly in /workspace, every sub-repo in /workspace/<repo-name> (the
// clone URL's path segment with any .git suffix stripped). Keep these two
// in sync if the layout ever changes.
func repoLocalDir(repo apiclient.FeatureSpecRepo) string {
	if repo.IsPrimary {
		return "/workspace"
	}
	name := strings.TrimSuffix(repo.CloneURL, ".git")
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		name = name[idx+1:]
	}
	return "/workspace/" + name
}

// driveSpecGrillSession drives a spec_grill job's Pi RPC session turn by
// turn: send initialPrompt, read the response, and — for as long as the
// yggdrasil-contract extension's ask_user tool keeps firing (a non-terminal
// signal; see CuratedEvent.Terminal) — wait for the human's reply (ADR 006
// items 9-10, via msgs) and send it as the next turn's prompt, until
// submit_adr (or a hard failure) ends the run (ADR 006 item 11).
//
// Each turn is its own k8s.Attach call (runTurn), not one continuous attach
// for the whole session — see k8s.Attach's doc comment for why: client-go's
// remotecommand doesn't reliably deliver a second stdin write within one
// attach call, verified against a real pod. Every curated event (ADR 006
// item 7) is passed to handle as soon as its turn ends.
//
// A background goroutine watches cancels.WatchCancellation(jobID) for the
// whole session (not just while waiting on a reply) and cancels runCtx the
// moment it fires, unblocking whichever of runTurn/msgs.WaitForReply is
// currently in progress — mid-turn cancellation reaches runTurn's own
// ctx.Done() case, which best-effort sends Pi an `abort` command before
// returning. cancelled (checked after each error) distinguishes that
// deliberate stop from any other ctx-cancellation-shaped error (Orchestrator
// shutdown, a real attach failure), so the right curated event/return value
// goes out.
func driveSpecGrillSession(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	msgs replyWaiter,
	cancels cancelWatcher,
	namespace, podName, jobID, initialPrompt string,
	handle func(rpc.CuratedEvent),
) error {
	rpcClient := rpc.NewClient()
	defer rpcClient.Close()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var cancelled atomic.Bool
	go func() {
		if err := cancels.WatchCancellation(runCtx, jobID); err == nil {
			cancelled.Store(true)
			cancelRun()
		}
	}()

	prompt := initialPrompt
	for {
		curated, err := runTurn(runCtx, clientset, restConfig, rpcClient, namespace, podName, prompt)
		if err != nil {
			return reportSessionError(handle, cancelled.Load(), err)
		}
		handle(curated)
		if curated.Terminal() {
			if curated.Type == rpc.EventRunFailed {
				// Unlike reportSessionError's EventRunFailed (a dead
				// stream/ctx cancellation), this one comes straight from
				// rpc.Translate reading a genuine agent_end error while the
				// session was otherwise healthy — an error return here is
				// what makes runClaimedJob call q.Fail instead of
				// q.Complete.
				return fmt.Errorf("agent run ended with an error: %s", curated.Message)
			}
			return nil
		}

		reply, err := msgs.WaitForReply(runCtx, jobID)
		if err != nil {
			return reportSessionError(handle, cancelled.Load(), err)
		}
		prompt = reply
	}
}

// reportSessionError translates a runTurn/WaitForReply error into the right
// curated event and return value: a deliberate cancellation gets
// EventRunCancelled and errJobCancelled (so runClaimedJob logs "cancelled",
// not "failed"); anything else gets EventRunFailed and the original error.
func reportSessionError(handle func(rpc.CuratedEvent), cancelled bool, err error) error {
	if cancelled {
		handle(rpc.CuratedEvent{Type: rpc.EventRunCancelled, Message: "job cancelled"})
		return errJobCancelled
	}
	handle(rpc.CuratedEvent{Type: rpc.EventRunFailed, Message: err.Error()})
	return err
}

// runTurn attaches once (one k8s.Attach call = one turn), sends prompt as
// Pi's next RPC prompt, and reads events until the yggdrasil-contract
// extension's tool-call-based signal (ask_user or submit_adr) says the
// turn is over — neither Pi's own agent_end event nor a tool result's own
// terminate:true flag mean that (ask_user sets terminate:true too; only
// the tool's identity, via rpc.Translate, distinguishes "end the turn" from
// "end the run") — then ends the turn (letting the attach call return
// without restarting the container process) and returns that curated
// event for the caller to act on.
func runTurn(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	rpcClient *rpc.Client,
	namespace, podName, prompt string,
) (rpc.CuratedEvent, error) {
	stdin, err := rpcClient.BeginTurn()
	if err != nil {
		return rpc.CuratedEvent{}, fmt.Errorf("failed to begin turn: %w", err)
	}

	// A turn-scoped context: cancelling it (on every return path, via the
	// deferred call below) unblocks k8s.Attach's goroutine if this function
	// returns before EndTurn cleanly closed it out (e.g. ctx cancelled,
	// Send failed).
	attachCtx, cancelAttach := context.WithCancel(ctx)
	defer cancelAttach()

	attachErr := make(chan error, 1)
	go func() {
		attachErr <- k8s.Attach(attachCtx, clientset, restConfig, namespace, podName, "run", stdin, rpcClient, rpcClient)
	}()

	if err := rpcClient.Send(rpc.Command{Type: "prompt", Message: prompt}); err != nil {
		return rpc.CuratedEvent{}, fmt.Errorf("failed to send prompt: %w", err)
	}

	for {
		select {
		case ev, ok := <-rpcClient.Events():
			if !ok {
				return rpc.CuratedEvent{}, fmt.Errorf("RPC event stream for pod %s/%s ended before a turn-ending event was seen", namespace, podName)
			}
			curated, matched := rpc.Translate(ev)
			if !matched {
				continue
			}

			if err := rpcClient.EndTurn(); err != nil {
				return rpc.CuratedEvent{}, fmt.Errorf("failed to end turn: %w", err)
			}
			select {
			case endErr := <-attachErr:
				if endErr != nil {
					return rpc.CuratedEvent{}, fmt.Errorf("attach ended with an error while closing out the turn: %w", endErr)
				}
			case <-time.After(endTurnGrace):
				return rpc.CuratedEvent{}, fmt.Errorf("attach call for pod %s/%s did not end within %s of EndTurn", namespace, podName, endTurnGrace)
			case <-ctx.Done():
				return rpc.CuratedEvent{}, ctx.Err()
			}
			return curated, nil

		case err := <-attachErr:
			if err == nil {
				err = fmt.Errorf("attach stream to pod %s/%s ended unexpectedly", namespace, podName)
			}
			return rpc.CuratedEvent{}, err

		case <-ctx.Done():
			// Best-effort: give Pi a chance to end its current operation
			// cleanly (the cancel/abort follow-up from ADR 006) before the
			// pod is deleted — k8s.DeleteJob (runSpecGrillJob) is what
			// actually guarantees termination, so a failed send here isn't
			// fatal, just a missed courtesy. Safe to call Send here (unlike
			// the nested ctx.Done() above): EndTurn hasn't run on this path,
			// so the turn's stdin pipe is still open.
			_ = rpcClient.Send(rpc.Command{Type: "abort"})
			return rpc.CuratedEvent{}, ctx.Err()
		}
	}
}
