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
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
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

// errJobCancelled is returned by driveAgentSession (and propagates
// unwrapped through runAgentRPCJob/runInCluster) when a human asked to
// stop the run, so runClaimedJob's log line can say "cancelled" instead of
// "failed", and so callers can tell the two apart via errors.Is. The DB
// outcome doesn't depend on this: queue.Queue.Fail's own guard (only
// transitions a 'running' row) is what actually stops the job's already-
// 'cancelled' status from being clobbered, regardless of this sentinel.
var errJobCancelled = errors.New("job cancelled")

// replyWaiter is the subset of *messages.Store (ADR 006 items 9-10)
// driveAgentSession needs — an interface so tests can substitute a
// fake reply source without a real Postgres connection, decoupling
// "does the session correctly pause/resume around ask_user" (testable
// against a real attached pod with a fake replyWaiter) from "does
// messages.Store really deliver via LISTEN/NOTIFY" (internal/messages'
// own tests, against a real Postgres).
type replyWaiter interface {
	WaitForReply(ctx context.Context, jobID string) (string, error)
}

// cancelWatcher is the subset of *queue.Queue driveAgentSession needs
// to learn a human asked to stop this job (the cancel/abort follow-up
// tracked in ADR 006's "Follow-ups" section) — an interface for the same
// reason as replyWaiter: tests can substitute a fake without a real
// Postgres connection.
type cancelWatcher interface {
	WatchCancellation(ctx context.Context, jobID string) error
}

// runAgentRPCJob runs a spec_grill, feature_build, test_run, or design_grill job by attaching to its
// pod and driving Pi's RPC session directly (ADR 006 items 2-4, 7, 11;
// widened to feature_build by ADR 010), instead of the placeholder-
// compatible blocking k8s.RunJob every other job kind still uses
// (runAgentJob, worker.go). Only reached once a real image is configured
// for the job's kind (cfg.Images — see runInCluster's routing).
//
// Kubernetes Job status is not the completion signal here: Pi's RPC
// process never exits on its own, so driveAgentSession itself decides
// when the run is over — from the event stream, not from Job.Status — and
// this function explicitly deletes the Job once it does (or once the
// session ends any other way), rather than waiting for Kubernetes to
// report success.
func runAgentRPCJob(ctx context.Context, q *queue.Queue, client *k8s.Client, job *queue.Job, namespace string, cfg Config) error {
	handle := func(ev rpc.CuratedEvent) {
		if err := cfg.APIClient.PostJobEvent(ctx, job.ID, ev); err != nil {
			// A failed relay is a visibility gap, not a job failure: the
			// job's actual outcome (an ADR submitted, a PR opened, or not)
			// is decided by the event itself, independent of whether this
			// side-channel post to the API succeeded.
			log.Printf("worker: failed to relay event %s for job %s: %v", ev.Type, job.ID, err)
		}
	}

	env, spec, err := buildAgentEnv(ctx, cfg, job)
	if err != nil {
		// Same gap ADR 011 item 3 closed for WaitForJobPod, one call site
		// earlier: without this, a buildAgentEnv failure (fetching the
		// feature spec, minting the installation token, ...) leaves the
		// feature stuck in 'queued' forever — runClaimedJob's q.Fail only
		// touches the jobs row, and no event would otherwise ever be
		// posted for this job.
		handle(rpc.CuratedEvent{
			Type:    rpc.EventRunFailed,
			Message: fmt.Sprintf("failed to prepare job environment: %v", err),
		})
		return err
	}

	var cleanupPreview func()
	if job.Kind == queue.KindTestRun {
		cleanupPreview, err = createTestPreview(ctx, client, job, cfg)
		if err != nil {
			handle(rpc.CuratedEvent{
				Type:    rpc.EventRunFailed,
				Message: fmt.Sprintf("failed to create test preview: %v", err),
			})
			return err
		}
		defer cleanupPreview()
	}

	image, command := resolveAgentImage(cfg, job.Kind)
	name := "job-" + job.ID

	if err := k8s.CreateJob(ctx, client.Interface, k8s.JobSpec{
		Namespace:        namespace,
		Name:             name,
		Image:            image,
		Command:          command,
		Env:              env,
		RuntimeClassName: cfg.RuntimeClassName,
		Stdin:            true,
		Resources:        k8s.AgentResources(),
	}); err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	defer func() {
		// A background context: cleanup must still run even when ctx
		// itself is what ended the session (worker shutdown, deadline) —
		// an already-cancelled ctx here would make DeleteJob a no-op and
		// leak the pod.
		if err := k8s.DeleteJob(context.Background(), client.Interface, namespace, name); err != nil {
			log.Printf("worker: failed to delete job %s (kind=%s): %v", job.ID, job.Kind, err)
		}
	}()

	podName, err := k8s.WaitForJobPod(ctx, client.Interface, namespace, name)
	if err != nil {
		// ADR 011 item 3: without this, a pod that never becomes attachable
		// leaves the feature stuck in 'queued' forever — runClaimedJob's
		// q.Fail only touches the jobs row, and no event would otherwise
		// ever be posted for this job.
		handle(rpc.CuratedEvent{
			Type:    rpc.EventRunFailed,
			Message: fmt.Sprintf("pod never became attachable: %v", err),
		})
		return fmt.Errorf("pod never became attachable: %w", err)
	}
	// ADR 011 item 2: synthesized the moment the pod is confirmed up, not
	// decoded from Pi — feature_build has no non-terminal curated event to
	// hang this off of, so it can't wait for the first turn to complete.
	handle(rpc.CuratedEvent{Type: rpc.EventRunStarted})

	initialPrompt := buildInitialPrompt(job.Kind, spec)

	return driveAgentSession(ctx, client.Interface, client.Config, cfg.Messages, q, namespace, podName, job.ID, initialPrompt, handle)
}

func createTestPreview(
	ctx context.Context,
	client *k8s.Client,
	job *queue.Job,
	cfg Config,
) (func(), error) {
	chrt, err := resolveChart(ctx, cfg, job.ProjectID)
	if err != nil {
		return nil, err
	}
	releaseName := "preview-" + job.ID
	namespace := k8s.ProjectNamespace(job.ProjectID)
	helmCfg, err := helm.NewConfiguration(client.Config, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize preview helm: %w", err)
	}
	if err := helm.Deploy(ctx, helmCfg, namespace, releaseName, chrt, nil); err != nil {
		return nil, fmt.Errorf("failed to deploy test preview: %w", err)
	}
	slug, err := cfg.APIClient.FetchProjectMetadata(ctx, job.ProjectID)
	if err != nil {
		_ = helm.Uninstall(context.Background(), helmCfg, releaseName)
		return nil, fmt.Errorf("failed to fetch project slug for test preview: %w", err)
	}
	host := fmt.Sprintf("%s-test-run-%s.preview.%s", slug, job.ID, cfg.AppsDomain)
	ingressName := "preview-" + job.ID
	if err := k8s.EnsureNamedIngress(
		ctx,
		client.Interface,
		namespace,
		ingressName,
		host,
		releaseName,
		80,
		cfg.IngressClassName,
		ingressName+"-tls",
		cfg.CertIssuerName,
	); err != nil {
		_ = helm.Uninstall(context.Background(), helmCfg, releaseName)
		return nil, fmt.Errorf("failed to expose test preview: %w", err)
	}
	return func() {
		cleanupCtx := context.Background()
		if err := k8s.DeleteIngress(cleanupCtx, client.Interface, namespace, ingressName); err != nil {
			log.Printf("worker: failed to delete test preview ingress %s: %v", ingressName, err)
		}
		if err := helm.Uninstall(cleanupCtx, helmCfg, releaseName); err != nil {
			log.Printf("worker: failed to uninstall test preview %s: %v", releaseName, err)
		}
	}, nil
}

// buildInitialPrompt picks the prompt shape for the job's kind (ADR 010
// item 6): feature_build and test_run get their dedicated skills, while
// spec_grill gets the fuller grill prompt.
func buildInitialPrompt(kind queue.JobKind, spec apiclient.FeatureSpec) string {
	if kind == queue.KindFeatureBuild {
		return buildFeatureBuildPrompt(spec)
	}
	if kind == queue.KindTestRun {
		return buildTestRunPrompt(spec)
	}
	if kind == queue.KindAgenticReview {
		return buildAgenticReviewPrompt(spec)
	}
	if kind == queue.KindDesignGrill {
		return buildDesignGrillPrompt(spec)
	}
	return buildSpecGrillPrompt(spec)
}

func buildAgenticReviewPrompt(spec apiclient.FeatureSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review the implementation of feature %s against its approved ADR.\n\n", spec.Title)
	b.WriteString("Read /root/.pi/agent/skills/review/SKILL.md first. ")
	b.WriteString("The approved ADR is at /workspace/.yggdrasil/adr.md. ")
	b.WriteString("Review the feature branch diff against main and inspect any ")
	b.WriteString("Testing reports under /workspace/.yggdrasil/. ")
	b.WriteString("Submit exactly one internal verdict with submit_review.")
	return b.String()
}

func buildDesignGrillPrompt(spec apiclient.FeatureSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Design session: %s\n\n", spec.DesignName)
	b.WriteString("Read /root/.pi/agent/skills/design-grill/SKILL.md first. ")
	b.WriteString("Create and refine a live, self-contained HTML mockup from the design brief below. ")
	b.WriteString("The design folder is already checked out on the branch prepared for this session.\n\n")
	fmt.Fprintf(&b, "Design brief:\n%s", spec.DesignDescription)
	return b.String()
}

func buildTestRunPrompt(spec apiclient.FeatureSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Run the test suite for feature %s.\n\n", spec.Title)
	b.WriteString("Read /root/.pi/agent/skills/run-tests/SKILL.md first. ")
	b.WriteString("The test specification is available at /workspace/.yggdrasil/test-spec.md. ")
	b.WriteString("The application preview is available at $PREVIEW_URL. ")
	b.WriteString("Run every section in order, report each step, and submit exactly one final report.")
	return b.String()
}

// buildFeatureBuildPrompt is deliberately short: unlike spec_grill (which
// has to spell out each repo's local path itself, since the agent has no
// other way to learn the clone layout), feature_build/skills/implement/
// SKILL.md already documents its own assumptions in full — repos cloned,
// feature branch checked out, approved ADR written to
// /workspace/.yggdrasil/adr.md (ADR 010 item 3) — so the Orchestrator
// doesn't need to restate any of it here, just point at the skill.
func buildFeatureBuildPrompt(spec apiclient.FeatureSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Implement this feature: %s\n\n", spec.Title)
	b.WriteString("Read /root/.pi/agent/skills/implement/SKILL.md first — it governs this ")
	b.WriteString("entire run and already states what's been set up for you before you started.")
	return b.String()
}

// buildSpecGrillPrompt states the job's constraints explicitly instead of
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
func buildSpecGrillPrompt(spec apiclient.FeatureSpec) string {
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
	appendSpecContext(&b, spec.SpecContext)
	return b.String()
}

func appendSpecContext(b *strings.Builder, context *apiclient.SpecGrillContext) {
	if context == nil {
		return
	}
	b.WriteString("\n\nThis is a continuation of an earlier specification run. Preserve useful decisions from the context below, revisit anything the kickback makes invalid, and do not make the user repeat settled answers.\n")
	if context.PreviousAdrMarkdown != "" {
		b.WriteString("\nPreviously approved ADR:\n---\n")
		b.WriteString(context.PreviousAdrMarkdown)
		b.WriteString("\n---\n")
	}
	if context.GrillTranscriptSummary != "" {
		b.WriteString("\nPrevious grill transcript summary:\n---\n")
		b.WriteString(context.GrillTranscriptSummary)
		b.WriteString("\n---\n")
	}
	if context.KickbackReason != "" {
		b.WriteString("\nImplementation kickback reason:\n---\n")
		b.WriteString(context.KickbackReason)
		b.WriteString("\n---\n")
	}
	if len(context.RequestedActionItems) > 0 {
		b.WriteString("\nRequested Action Items:\n")
		for _, item := range context.RequestedActionItems {
			fmt.Fprintf(b, "- %s: %s\n", item.Type, item.Description)
		}
	}
	for _, design := range context.DesignSnapshots {
		fmt.Fprintf(b, "\nFinalized design snapshot from session %s:\n", design.SessionID)
		for path, content := range design.Snapshot {
			fmt.Fprintf(b, "\nFile: %s\n```html\n%s\n```\n", path, content)
		}
	}
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

// driveAgentSession drives a spec_grill or feature_build job's Pi RPC
// session turn by turn: send initialPrompt, read the response, and — for as
// long as the yggdrasil-contract extension's ask_user tool keeps firing (a
// non-terminal signal; see CuratedEvent.Terminal) — wait for the human's
// reply (ADR 006 items 9-10, via msgs) and send it as the next turn's
// prompt, until submit_adr/submit_build_result (or a hard failure) ends the
// run (ADR 006 item 11; ADR 010 item 8). feature_build's implement skill
// has no ask_user tool registered, so for that kind this loop's reply-wait
// branch is simply never reached — the first turn's curated event is
// always terminal (submit_build_result), with no special-casing needed here
// to skip it.
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
func driveAgentSession(
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
		curated, err := runTurn(runCtx, clientset, restConfig, rpcClient, namespace, podName, prompt, handle)
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
			if curated.Type == rpc.EventSubmitBuildResult && curated.Status == "failure" {
				// Mirrors the EventRunFailed branch above: submit_build_result
				// is always terminal (ADR 010 item 7), but only a "failure"
				// status should make runClaimedJob call q.Fail instead of
				// q.Complete — a "success" status falls through to the
				// unconditional `return nil` below, same as submit_adr.
				return fmt.Errorf("feature_build run reported failure: %s", curated.Summary)
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
// extension's tool-call-based signal (ask_user/submit_adr for spec_grill,
// submit_build_result for feature_build) says the turn is over — neither
// Pi's own agent_end event nor a tool result's own
// terminate:true flag mean that (ask_user sets terminate:true too; only
// the tool's identity, via rpc.Translate, distinguishes "end the turn" from
// "end the run") — then ends the turn (letting the attach call return
// without restarting the container process) and returns that curated
// event for the caller to act on. handle is invoked directly, inline, for
// any EventAgentText seen along the way (rpc.Translate's one non-terminal
// match) — the only curated event type this loop forwards without also
// ending the turn on it; see EventAgentText's doc comment for why.
func runTurn(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	rpcClient *rpc.Client,
	namespace, podName, prompt string,
	handle func(rpc.CuratedEvent),
) (rpc.CuratedEvent, error) {
	// Discards whatever the *previous* turn's own trailing agent_end/
	// agent_settled bookkeeping left sitting in rpcClient's shared Events
	// buffer (see DrainStaleEvents' doc comment) before this turn reads
	// anything or sends its own prompt — otherwise this turn's read loop
	// below could pick up that stale data first and misreport itself as
	// having ended before Pi ever saw prompt.
	rpcClient.DrainStaleEvents()

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

	// Remembers the most recent low-level agent run's stop reason (one of
	// Pi's own values: "stop", "length", "toolUse", "error", "aborted") so
	// the agent_settled branch below can say why, if it ends up firing.
	var lastStopReason string

	for {
		select {
		case ev, ok := <-rpcClient.Events():
			if !ok {
				return rpc.CuratedEvent{}, fmt.Errorf("RPC event stream for pod %s/%s ended before a turn-ending event was seen", namespace, podName)
			}
			if reason, ok := rpc.LastMessageStopReason(ev); ok {
				lastStopReason = reason
			}

			// Pi's session-level "fully settled" signal (no automatic
			// retry, compaction retry, or queued follow-up remains — Pi
			// RPC docs) is not itself a contract-tool-driven signal, so
			// rpc.Translate never matches it. Left unhandled, a turn that
			// settles without ever calling a contract tool (ask_user/
			// submit_adr/submit_build_result) — most commonly the model
			// hitting its own output length limit mid-response
			// (lastStopReason "length") before it could call one —
			// produces no further events ever, and this loop would block
			// on Events() forever: verified against a real stuck
			// feature_build job (pod healthy and idle, job stuck
			// 'running' indefinitely, no PR ever opened). Treated as a
			// definitive failure, same as a request-level error, rather
			// than left to hang.
			if ev.Type == "agent_settled" {
				curated := rpc.CuratedEvent{
					Type:    rpc.EventRunFailed,
					Message: agentSettledFailureMessage(lastStopReason),
				}
				return closeOutTurn(ctx, rpcClient, attachErr, namespace, podName, curated)
			}

			curated, matched := rpc.Translate(ev)
			if !matched {
				continue
			}
			if curated.Type == rpc.EventAgentText ||
				curated.Type == rpc.EventReportTestStep ||
				curated.Type == rpc.EventUpdateDesignPreview {
				// Forwarded live, not turn-ending: the model's own prose
				// alongside (or instead of) a contract tool call in this
				// same turn. Test steps have the same property: they are
				// progress records, not a request for another prompt.
				// Keep reading — the real terminating event (a tool call,
				// agent_settled's failure branch above, or the attach stream
				// ending) is what actually ends the turn.
				handle(curated)
				continue
			}
			return closeOutTurn(ctx, rpcClient, attachErr, namespace, podName, curated)

		case err := <-attachErr:
			if err == nil {
				err = fmt.Errorf("attach stream to pod %s/%s ended unexpectedly", namespace, podName)
				if reason := k8s.PodContainerTerminationReason(ctx, clientset, namespace, podName); reason != "" {
					err = fmt.Errorf("%w (%s)", err, reason)
				}
			}
			return rpc.CuratedEvent{}, err

		case <-ctx.Done():
			// Best-effort: give Pi a chance to end its current operation
			// cleanly (the cancel/abort follow-up from ADR 006) before the
			// pod is deleted — k8s.DeleteJob (runAgentRPCJob) is what
			// actually guarantees termination, so a failed send here isn't
			// fatal, just a missed courtesy. Safe to call Send here (unlike
			// the nested ctx.Done() above): EndTurn hasn't run on this path,
			// so the turn's stdin pipe is still open.
			_ = rpcClient.Send(rpc.Command{Type: "abort"})
			return rpc.CuratedEvent{}, ctx.Err()
		}
	}
}

// closeOutTurn ends the current turn's stdin pipe and waits for the Attach
// call to return, the shared tail end of runTurn's loop for every event
// that ends a turn (a matched curated event, or the synthesized
// agent_settled failure) — factored out so the wait's three-way race
// (attach error / grace timeout / ctx cancellation) is defined once.
func closeOutTurn(
	ctx context.Context,
	rpcClient *rpc.Client,
	attachErr <-chan error,
	namespace, podName string,
	curated rpc.CuratedEvent,
) (rpc.CuratedEvent, error) {
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
}

// agentSettledFailureMessage builds the run_failed message for a turn that
// settled (Pi's session-level "nothing more is coming" signal) without ever
// calling a contract tool — lastStopReason is the most recent low-level
// agent run's stop reason seen beforehand (see rpc.LastMessageStopReason),
// "" if none was ever observed this turn.
func agentSettledFailureMessage(lastStopReason string) string {
	switch lastStopReason {
	case "":
		return "the agent's session ended without submitting a result"
	case "length":
		return "the agent's response was cut off by the model's own output length limit before it submitted a result (stopReason: length)"
	default:
		return fmt.Sprintf("the agent's session ended without submitting a result (stopReason: %s)", lastStopReason)
	}
}
