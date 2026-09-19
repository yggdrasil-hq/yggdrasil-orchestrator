package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ForkStages are the places a fork attempt can stop, and they are a closed set
// because each one is a different thing for an operator to do about it
// (ADR 032 item 5: "a refused or unavailable fork fails honestly, never
// silently").
//
// **Why stages rather than one error.** An agent that cannot load the session file
// has a delivery problem; an agent whose entry id Pi rejected has a *stale fork
// point* — the session was collected, the point was offered, and the branch no
// longer exists (a compaction, or a point from a session that has since been
// superseded). Those need different words on screen and different follow-ups, and
// folding them into "the fork failed" leaves a user with no next step. It is the
// same argument `unavailable` vs `not_collected` carries for capture (item 5),
// applied to the dispatch side.
type ForkStage string

const (
	// ForkStageWrite — the stored session never reached the pod.
	ForkStageWrite ForkStage = "write"
	// ForkStageSwitch — the file arrived but Pi would not load it, or loaded
	// nothing from it. Both are one stage because the user's next step is the same
	// (the artifact is unusable) even though the causes differ, and the reason
	// string names which.
	ForkStageSwitch ForkStage = "switch"
	// ForkStageFork — the session loaded and the entry id was rejected, or an
	// extension declined the fork.
	ForkStageFork ForkStage = "fork"
)

// forkRefusal is a fork attempt that stopped, with the stage it stopped at and a
// reason written to be shown to a person.
//
// A separate type from error so the caller cannot accidentally treat a stated
// refusal as an unexpected fault: this is a *reported outcome*, which is why the
// caller relays it as an event and then fails the job deliberately rather than
// propagating it as an error and letting retry policy guess.
type forkRefusal struct {
	Stage  ForkStage
	Reason string
}

func (f *forkRefusal) Error() string {
	return fmt.Sprintf("fork stopped at %s: %s", f.Stage, f.Reason)
}

// forkSetup is a fork that succeeded: what the first turn should send, and where
// the new session lives.
type forkSetup struct {
	// Prompt is the fork point's own text, which Pi returns from `fork`.
	//
	// **It is re-sent rather than replaced with the usual grill seed**, and that is
	// the semantics of "resume from here": a fork's context ends *before* the fork
	// point, so re-sending that message is what makes the agent answer it again —
	// inside a branch that leaves the original conversation intact (ADR 032 item
	// 3's table). The stored session already carries the rest of the context, which
	// is the difference between this and ADR 024's reconstruction.
	Prompt string
	// Session is the post-fork `get_state`: the file the *next* collection must
	// store. Reading it here rather than at collection time is what makes "the
	// artifact chain is the job chain" true rather than aspirational — this job's
	// session is the forked file, not the one it restored.
	Session rpc.SessionFile
}

// forkRestoreDir is where the restored session is written inside the pod.
//
// **`/tmp`, and deliberately not a directory of our own.** The write is an argv
// slice (`tee`, see k8s.WritePodFile), so nothing creates a parent directory for
// it — and `mkdir -p` would mean either a second exec or a shell, both of which the
// write's contract rules out. `/tmp` exists in every container and is the one path
// that needs no setup. That it is ephemeral is a *feature* here: the restored file
// is an input to one run, and the pod is deleted when the run ends (ADR 006), so a
// durable location would imply a lifetime this artifact does not have.
const forkRestoreDir = "/tmp"

// forkRestorePath names the restored session file after the job it came from, so
// a pod left behind by a crash says which artifact it was holding.
func forkRestorePath(sourceJobID string) string {
	return fmt.Sprintf("%s/yggdrasil-session-%s.jsonl", forkRestoreDir, sourceJobID)
}

// forkTurnTimeout bounds the whole fork preamble.
//
// It is a separate, generous bound rather than the turn timeout because this
// happens once, before any agent work, and covers four round trips plus a pod
// exec. A Pi that accepted `switch_session` and never answered must not hold a
// worker slot (defaultMaxConcurrentJobs is small — ADR 006 item 1), but the bound
// should not be so tight that a legitimately slow load of a multi-megabyte session
// reads as a refusal.
const forkTurnTimeout = 60 * time.Second

// forkIntoSession performs ADR 032 item 3's preamble: put the stored session
// where Pi can read it, load it, **verify it loaded**, fork it at the chosen
// entry, and read back where the fork lives.
//
// **The verification is a step, not a check, and that is the whole design.** A real
// Pi 0.84.4 answers `{"success":true,"cancelled":false}` for a session path that
// does not exist — no error, no file, and a `get_state` with no `sessionFile` — so
// `switch_session`'s own flag cannot distinguish "the session loaded" from "the
// fork is about to run on an empty conversation". Only a follow-up `get_state` can,
// and without it those two outcomes are the same observable, which is exactly the
// silent failure item 5 forbids.
//
// **Every command is sent and awaited one at a time.** `fetchSessionStats` sends a
// batch and matches the answers by name, because a real Pi answers a batch in a
// different order from the one commands were sent (a command touching the session
// tree answers after the ones that read it). Serialising here is the simpler and
// stronger fix for the same hazard: with only one command outstanding, the next
// matching response *is* that command's answer, and the ordering question cannot
// arise. It costs three extra round trips on a path that runs once per fork.
//
// The turn is opened **after** the write, so the exec and the RPC stream cannot
// interleave — the file must exist before `switch_session` names it, and doing the
// write first makes that ordering structural rather than a matter of timing.
func forkIntoSession(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	rpcClient *rpc.Client,
	namespace, podName, sourceJobID, entryID string,
	sessionBytes []byte,
	maxBytes int64,
) (forkSetup, *forkRefusal, error) {
	sessionPath := forkRestorePath(sourceJobID)

	// Step 1: get the bytes into the pod. A failure here is the `write` stage —
	// the artifact never arrived, so nothing downstream can be diagnosed from Pi's
	// behaviour.
	if err := k8s.WritePodFile(
		ctx, clientset, restConfig, namespace, podName, k8s.RunContainerName,
		sessionPath, sessionBytes, maxBytes,
	); err != nil {
		return forkSetup{}, &forkRefusal{
			Stage:  ForkStageWrite,
			Reason: fmt.Sprintf("the stored session could not be placed in the run's container: %v", err),
		}, nil
	}

	forkCtx, cancel := context.WithTimeout(ctx, forkTurnTimeout)
	defer cancel()

	// Same reason runTurn and fetchSessionStats discard first: the previous turn's
	// trailing bookkeeping can still be sitting in the shared Events buffer, and
	// this loop must not read it as its own answer. In practice
	// DrainStaleEvents here has nothing to discard — this runs before the first
	// turn — but relying on that would make the function correct only in its
	// current call position.
	rpcClient.DrainStaleEvents()

	stdin, err := rpcClient.BeginTurn()
	if err != nil {
		return forkSetup{}, nil, fmt.Errorf("failed to begin the fork turn: %w", err)
	}

	attachCtx, cancelAttach := context.WithCancel(forkCtx)
	defer cancelAttach()

	attachErr := make(chan error, 1)
	go func() {
		attachErr <- k8s.Attach(
			attachCtx, clientset, restConfig, namespace, podName,
			k8s.RunContainerName, stdin, rpcClient, rpcClient,
		)
	}()

	// closeTurn is the one exit path, so the attach is ended exactly once however
	// the sequence stops. A teardown problem is logged rather than returned in
	// place of the real result: the reading that matters has already been taken,
	// and the pod is deleted next regardless.
	closeTurn := func() {
		if endErr := closeForkTurn(forkCtx, rpcClient, attachErr, namespace, podName); endErr != nil {
			fmt.Printf("worker: fork turn for pod %s/%s did not close cleanly: %v\n", namespace, podName, endErr)
		}
	}

	// Step 2: ask Pi to load it.
	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandSwitchSession, SessionPath: sessionPath}); err != nil {
		closeTurn()
		return forkSetup{}, nil, fmt.Errorf("failed to send switch_session: %w", err)
	}
	switched, err := awaitResponse(forkCtx, rpcClient, attachErr, func(ev rpc.Event) bool {
		_, ok := rpc.ParseSwitchSession(ev)
		return ok
	})
	if err != nil {
		closeTurn()
		return forkSetup{}, nil, err
	}
	switchResult, _ := rpc.ParseSwitchSession(switched)
	if switchResult.Cancelled {
		closeTurn()
		return forkSetup{}, &forkRefusal{
			Stage:  ForkStageSwitch,
			Reason: "an extension declined to load the session, so nothing was forked",
		}, nil
	}
	if !switchResult.Success {
		closeTurn()
		return forkSetup{}, &forkRefusal{
			Stage:  ForkStageSwitch,
			Reason: "the run's session process refused to load the stored session",
		}, nil
	}

	// Step 3: verify it actually loaded. See the doc comment — `success` above
	// cannot be trusted for this.
	restored, err := readState(forkCtx, rpcClient, attachErr)
	if err != nil {
		closeTurn()
		return forkSetup{}, nil, err
	}
	if refusal := verifySwitch(restored, sessionPath); refusal != nil {
		closeTurn()
		return forkSetup{}, refusal, nil
	}

	// Step 4: branch at the chosen point.
	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandFork, EntryID: entryID}); err != nil {
		closeTurn()
		return forkSetup{}, nil, fmt.Errorf("failed to send fork: %w", err)
	}
	forked, err := awaitResponse(forkCtx, rpcClient, attachErr, func(ev rpc.Event) bool {
		_, ok := rpc.ParseFork(ev)
		return ok
	})
	if err != nil {
		closeTurn()
		return forkSetup{}, nil, err
	}
	forkResult, _ := rpc.ParseFork(forked)
	if forkResult.Cancelled {
		closeTurn()
		return forkSetup{}, &forkRefusal{
			Stage:  ForkStageFork,
			Reason: "an extension declined the fork, so the conversation was left as it was",
		}, nil
	}
	if !forkResult.Success {
		closeTurn()
		return forkSetup{}, &forkRefusal{
			Stage:  ForkStageFork,
			Reason: forkRejectionReason(entryID, forkResult.Error),
		}, nil
	}

	// Step 5: where the fork lives. This is the file the *next* collection stores,
	// so a failure to read it is not cosmetic — without it this job would report
	// the restored session as its own artifact, and the chain would fork from a
	// point that moves backwards on every resume.
	forked2, err := readState(forkCtx, rpcClient, attachErr)
	if err != nil {
		closeTurn()
		return forkSetup{}, nil, err
	}

	closeTurn()
	return forkSetup{Prompt: forkResult.Text, Session: forked2}, nil, nil
}

// verifySwitch decides whether a `get_state` after `switch_session` shows the
// session actually loaded, returning nil when it did.
//
// **Two conditions, because each alone is satisfiable by the failure it is meant
// to catch.** A path match alone passes for a file that exists but is empty — and
// `switch_session` pointed at a *missing* file reports no `sessionFile` at all, so
// a path match alone would also pass if Pi echoed back what it was asked for. A
// non-zero message count alone passes for a session that loaded but is not the one
// that was named. Together they say "the conversation at this path is present",
// which is the claim the fork depends on.
//
// The path comparison is on the *restored* path rather than on the predecessor's
// original path, because Pi re-homes a loaded session: the file it reports after a
// switch is the one it was given, not the one the artifact was named after. (The
// fork that follows creates a third file, whose header names this one as its
// parent — so the chain is three files deep, and only the last is this job's
// artifact.)
func verifySwitch(state rpc.SessionFile, wantPath string) *forkRefusal {
	if !state.Asked {
		return &forkRefusal{
			Stage:  ForkStageSwitch,
			Reason: "the run's session process did not answer the state question, so the session could not be confirmed as loaded",
		}
	}
	if state.FilePath != wantPath {
		return &forkRefusal{
			Stage: ForkStageSwitch,
			Reason: fmt.Sprintf(
				"the stored session was not loaded: asked for %s and the session process reports %s",
				wantPath, describeSessionPath(state.FilePath),
			),
		}
	}
	if state.MessageCount == 0 {
		return &forkRefusal{
			Stage:  ForkStageSwitch,
			Reason: "the stored session loaded but holds no messages, so there was nothing to resume from",
		}
	}
	return nil
}

// describeSessionPath names an absent path in words, so a refusal reads as a
// sentence rather than as a dangling colon.
func describeSessionPath(path string) string {
	if path == "" {
		return "no session at all"
	}
	return path
}

// forkRejectionReason turns Pi's own error text into something a user can act on,
// keeping Pi's wording when there is any.
//
// Pi's answer for an unknown entry id is `"Invalid entry ID for forking"`, which is
// accurate and unhelpful on its own: the user is looking at a list of fork points,
// so the useful sentence names the id as one this session no longer offers — which
// happens when the point was captured from a session that has since been
// superseded, or when compaction removed the branch.
func forkRejectionReason(entryID, piError string) string {
	base := fmt.Sprintf("this session no longer offers the message %s as a resume point", entryID)
	if strings.TrimSpace(piError) == "" {
		return base
	}
	return fmt.Sprintf("%s (%s)", base, piError)
}

// readState sends `get_state` and returns its parsed answer.
func readState(
	ctx context.Context,
	rpcClient *rpc.Client,
	attachErr <-chan error,
) (rpc.SessionFile, error) {
	if err := rpcClient.Send(rpc.Command{Type: rpc.CommandGetState}); err != nil {
		return rpc.SessionFile{}, fmt.Errorf("failed to send get_state: %w", err)
	}
	ev, err := awaitResponse(ctx, rpcClient, attachErr, func(ev rpc.Event) bool {
		_, ok := rpc.ParseSessionFile(ev)
		return ok
	})
	if err != nil {
		return rpc.SessionFile{}, err
	}
	state, _ := rpc.ParseSessionFile(ev)
	return state, nil
}

// awaitResponse reads the shared event stream until match accepts a line, and is
// how this file gets one command's answer without depending on arrival order.
//
// Lines that do not match are dropped rather than buffered: the only traffic on
// this handle before the first prompt is responses to the commands sent here, and
// anything else (a start-up banner, a stray event) is not this function's to
// interpret. The attach failing or the context ending is an *error* rather than a
// refusal, because neither says anything about the fork — the fork may well have
// been possible, and reporting it as refused would be a claim nothing established.
func awaitResponse(
	ctx context.Context,
	rpcClient *rpc.Client,
	attachErr <-chan error,
	match func(rpc.Event) bool,
) (rpc.Event, error) {
	for {
		select {
		case ev, ok := <-rpcClient.Events():
			if !ok {
				return rpc.Event{}, fmt.Errorf("the session's RPC stream ended before its answer arrived")
			}
			if match(ev) {
				return ev, nil
			}
		case err := <-attachErr:
			if err == nil {
				err = fmt.Errorf("the attach stream to the run's container ended unexpectedly")
			}
			return rpc.Event{}, fmt.Errorf("the session's RPC stream ended before its answer arrived: %w", err)
		case <-ctx.Done():
			return rpc.Event{}, fmt.Errorf("the session's RPC stream ended before its answer arrived: %w", ctx.Err())
		}
	}
}

// closeForkTurn ends the fork turn's stdin pipe and waits for its Attach call to
// return — the same three-way race closeOutTurn and closeUsageTurn handle, kept
// separate for the same reason they are: this turn's result is a fork, not a
// curated event.
func closeForkTurn(
	ctx context.Context,
	rpcClient *rpc.Client,
	attachErr <-chan error,
	namespace, podName string,
) error {
	if err := rpcClient.EndTurn(); err != nil {
		return fmt.Errorf("failed to end the fork turn: %w", err)
	}
	select {
	case endErr := <-attachErr:
		if endErr != nil {
			return fmt.Errorf("attach ended with an error while closing out the fork turn: %w", endErr)
		}
	case <-time.After(endTurnGrace):
		return fmt.Errorf("attach call for pod %s/%s did not end within %s of ending the fork turn", namespace, podName, endTurnGrace)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// setupForkForJob is the Orchestrator-side half of ADR 032 item 3: fetch the
// stored session, put it into the pod, and branch it.
//
// **Why the fetch happens here and not in the pod is the decision, not a detail.**
// ADR 032 item 3 chose write-into-pod over a pod-side pull because a pod clones a
// user's repository and runs their build code, so handing it an API authorisation
// — even one scoped to a single session — would add an outbound channel and a third
// piece of standing access to the least-trusted process in the system, with a reach
// that includes other runs' conversations. Fetching here uses the client this
// process already holds and leaves the pod's reach exactly what it was.
//
// The two failing returns are deliberately different types. A *refusal* is a
// reported outcome — it has a stage and a sentence written for a person, and it is
// relayed as an event before the job is failed. An *error* is a fault this function
// could not characterise. Collapsing them would lose the distinction the feature
// exists to preserve (ADR 032 item 5).
func setupForkForJob(
	ctx context.Context,
	client *k8s.Client,
	cfg Config,
	namespace, podName string,
	specContext *apiclient.SpecGrillContext,
) (string, *forkRefusal, error) {
	sessionBytes, status, err := cfg.APIClient.FetchJobSession(ctx, specContext.ForkFromJobID)
	if err != nil {
		// The bytes never reached the pod, so this is the `write` stage — and the
		// status is in the sentence because "we never collected one", "retention
		// reclaimed it" and "that job does not exist" are three different things for
		// an operator to do about it. The API's own wording is carried through.
		return "", &forkRefusal{
			Stage: ForkStageWrite,
			Reason: fmt.Sprintf(
				"the session to resume from could not be retrieved: %s (%v)",
				describeSessionFetch(status), err,
			),
		}, nil
	}

	// The bound is the length of what was fetched, plus one. The API has already
	// refused anything over its own cap, so re-imposing a second, fixed cap here
	// would make a deployment that raised `SESSION_MAX_BYTES` fail for a reason
	// nothing states — while still keeping k8s.WritePodFile's "empty is refused,
	// oversized is a sentinel" contract intact.
	maxBytes := int64(len(sessionBytes)) + 1

	rpcClient := rpc.NewClient()
	defer rpcClient.Close()

	setup, refusal, err := forkIntoSession(
		ctx, client.Interface, client.Config, rpcClient,
		namespace, podName, specContext.ForkFromJobID, specContext.ForkEntryID,
		sessionBytes, maxBytes,
	)
	if err != nil || refusal != nil {
		return "", refusal, err
	}
	return setup.Prompt, nil, nil
}

// describeSessionFetch turns the API's status for a missing session into the
// distinction a user needs.
//
// The route answers 404 for "nothing was ever stored", "the stored outcome was a
// failure" and "the live object is gone", and 410 for "retention reclaimed it" —
// and only the last is a *finished* artifact rather than a broken one. Naming them
// apart is the same split the content route's own comment makes when it refuses to
// answer 404 for an expired session.
func describeSessionFetch(status int) string {
	switch status {
	case 410:
		return "the stored session was reclaimed after its retention window"
	case 404:
		return "no usable session is stored for that run"
	case 0:
		return "the API could not be reached"
	default:
		return fmt.Sprintf("the API could not supply it (status %d)", status)
	}
}
