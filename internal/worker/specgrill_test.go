package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

const testStandInImage = "busybox:1.36"

// startAttachablePod creates a Job running script (a busybox `sh -c`
// script) with stdin attached, waits for its pod to become attachable, and
// returns everything driveAgentSession needs. script stands in for
// Pi + the yggdrasil-contract extension without needing a real agent-images
// image: it reads whatever driveAgentSession sends as the initial
// prompt and reacts however the test wants.
func startAttachablePod(t *testing.T, ctx context.Context, script string) (namespace, podName, jobName string) {
	t.Helper()
	clientset := testClient(t)

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset.Interface, "test-"+rand.String(8))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.Interface.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	jobName = "test-specgrill-" + rand.String(6)
	if err := k8s.CreateJob(ctx, clientset.Interface, k8s.JobSpec{
		Namespace: namespace,
		Name:      jobName,
		Image:     testStandInImage,
		Command:   []string{"sh", "-c", script},
		Stdin:     true,
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	t.Cleanup(func() {
		_ = k8s.DeleteJob(context.Background(), clientset.Interface, namespace, jobName)
	})

	podName, err = k8s.WaitForJobPod(ctx, clientset.Interface, namespace, jobName)
	if err != nil {
		t.Fatalf("pod never became attachable: %v", err)
	}
	return namespace, podName, jobName
}

// blockingReplyWaiter simulates "no human has replied yet": it never
// resolves until ctx is cancelled. Used to prove ask_user doesn't end a
// session without needing a real Postgres-backed messages.Store.
type blockingReplyWaiter struct{}

func (blockingReplyWaiter) WaitForReply(ctx context.Context, _ string) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

// fixedReplyWaiter simulates a reply that's already sitting there waiting
// to be claimed: it resolves immediately with a canned value.
type fixedReplyWaiter struct{ reply string }

func (f fixedReplyWaiter) WaitForReply(context.Context, string) (string, error) {
	return f.reply, nil
}

// neverCancels simulates "nobody asked to cancel this job": it blocks until
// ctx is done, mirroring blockingReplyWaiter's role for WaitForReply.
type neverCancels struct{}

func (neverCancels) WatchCancellation(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

// delayedCancel simulates a human's cancel request landing after a short
// delay — long enough to be sure the session is genuinely mid-turn or
// mid-wait (not racing the very first Send) when it fires.
type delayedCancel struct{ after time.Duration }

func (d delayedCancel) WatchCancellation(ctx context.Context, _ string) error {
	select {
	case <-time.After(d.after):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Proves driveAgentSession correctly identifies submit_adr as the
// run-ending signal (ADR 006 item 11) — not agent_end, and not the tool
// result's own terminate:true (ask_user sets that too) — and hands the
// curated event to its caller before returning.
func TestDriveSpecGrillSession_SubmitADREndsSessionAndIsCurated(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"# Test ADR"},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err != nil {
		t.Fatalf("expected the session to end cleanly on submit_adr, got: %v", err)
	}

	if len(received) != 1 {
		t.Fatalf("expected exactly one curated event, got %d: %+v", len(received), received)
	}
	if received[0].Type != rpc.EventSubmitADR {
		t.Fatalf("expected EventSubmitADR, got %q", received[0].Type)
	}
	if received[0].Markdown != "# Test ADR" {
		t.Fatalf("expected the ADR markdown to be carried through, got %q", received[0].Markdown)
	}
}

// Proves driveAgentSession treats a successful submit_build_result
// (feature_build's terminating tool, ADR 010 items 7-8) the same way
// submit_adr ends a spec_grill run: session ends cleanly (nil error), one
// curated event, PRUrl/Summary carried through.
func TestDriveAgentSession_SubmitBuildResultSuccessEndsSessionCleanly(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"tool_execution_end","toolName":"submit_build_result","result":{"details":{"kind":"submit_build_result","status":"success","prUrl":"https://github.com/acme/web/pull/42","summary":"Added dark mode."},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "Implement this feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err != nil {
		t.Fatalf("expected the session to end cleanly on a successful submit_build_result, got: %v", err)
	}

	if len(received) != 1 || received[0].Type != rpc.EventSubmitBuildResult {
		t.Fatalf("expected exactly one submit_build_result event, got %+v", received)
	}
	if received[0].PRUrl != "https://github.com/acme/web/pull/42" {
		t.Fatalf("expected the PR URL to be carried through, got %q", received[0].PRUrl)
	}
}

// Proves a failed submit_build_result (the agent concluded the feature
// couldn't be completed) still ends the session — Terminal() is true either
// way (ADR 010 item 7) — but returns a non-nil error, so runClaimedJob
// calls q.Fail instead of q.Complete, unlike the success case above.
func TestDriveAgentSession_SubmitBuildResultFailureEndsSessionAsError(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"tool_execution_end","toolName":"submit_build_result","result":{"details":{"kind":"submit_build_result","status":"failure","summary":"ADR referenced a package that does not exist."},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "Implement this feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err == nil {
		t.Fatal("expected a non-nil error when submit_build_result reports failure")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected the error to carry the summary, got: %v", err)
	}
	if len(received) != 1 || received[0].Type != rpc.EventSubmitBuildResult || received[0].Status != "failure" {
		t.Fatalf("expected exactly one failed submit_build_result event, got %+v", received)
	}
}

// Proves ask_user does NOT end the run despite its own tool result also
// setting terminate:true — the session should keep waiting for a human
// reply rather than treating this as completion. Uses blockingReplyWaiter
// (no reply ever arrives) so the only way this test passes is if
// driveAgentSession is still running after a few seconds — a real
// completion would return almost immediately.
func TestDriveSpecGrillSession_AskUserIsNotTerminal(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var mu sync.Mutex
	var received []rpc.CuratedEvent
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
			mu.Lock()
			received = append(received, ev)
			mu.Unlock()
		})
	}()

	select {
	case err := <-sessionDone:
		t.Fatalf("expected driveAgentSession to still be waiting on a reply, but it returned: %v", err)
	case <-time.After(3 * time.Second):
		// Still running, as expected — the assertions below are what
		// actually prove why.
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0].Type != rpc.EventAskUser {
		t.Fatalf("expected exactly one ask_user event, got %+v", received)
	}
	if received[0].Question != "Which auth model?" {
		t.Fatalf("expected the question to be carried through, got %q", received[0].Question)
	}
}

// Proves a reply (fixedReplyWaiter stands in for a human's answer already
// queued in job_messages, ADR 006 items 9-10) actually gets sent back to
// Pi as the next prompt, resuming the session — not just parked forever.
// The stand-in script only emits submit_adr after reading a *second* line
// from stdin, so this only passes if driveAgentSession genuinely wrote
// the reply back to the pod.
func TestDriveSpecGrillSession_ReplyResumesSessionAndReachesSubmitADR(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// line2 is itself a JSONL prompt command (e.g. {"type":"prompt","message":"use-oauth"}),
	// not a bare string — driveAgentSession sends replies the same way it sends the
	// initial prompt. Extract just the message field before embedding it in this script's
	// own JSON output; echoing line2 raw would nest unescaped quotes and produce invalid JSON.
	script := `read line1; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; read line2; reply=$(echo "$line2" | sed -n 's/.*"message":"\([^"]*\)".*/\1/p'); echo "{\"type\":\"tool_execution_end\",\"toolName\":\"submit_adr\",\"result\":{\"details\":{\"kind\":\"submit_adr\",\"markdown\":\"reply was: $reply\"},\"terminate\":true}}"; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, fixedReplyWaiter{reply: "use-oauth"}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err != nil {
		t.Fatalf("expected the session to end cleanly on submit_adr, got: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected ask_user then submit_adr, got %d events: %+v", len(received), received)
	}
	if received[0].Type != rpc.EventAskUser {
		t.Fatalf("expected the first event to be ask_user, got %q", received[0].Type)
	}
	if received[1].Type != rpc.EventSubmitADR {
		t.Fatalf("expected the second event to be submit_adr, got %q", received[1].Type)
	}
	if !strings.Contains(received[1].Markdown, "use-oauth") {
		t.Fatalf("expected the ADR markdown to reflect the delivered reply, got %q", received[1].Markdown)
	}
}

// Proves a turn's own trailing bookkeeping (Pi keeps emitting agent_end
// then agent_settled for a few moments after the contract tool call that
// already ended the turn from Yggdrasil's point of view — a normal part of
// Pi's protocol, not something the stand-in script invents) doesn't leak
// into the *next* turn and get misread as that turn's own outcome. Traced
// from two real production failures of this exact shape: run_failed fired
// within milliseconds of a human's reply being sent — too fast for any
// model round trip — immediately after a session that had already
// completed one or more ask_user/reply cycles successfully. Before the
// DrainStaleEvents fix, this script reliably reproduced it: the second
// runTurn call would read the leftover agent_settled first and fail before
// line2 (the reply) was ever sent to the pod.
func TestDriveSpecGrillSession_TrailingEventsFromPriorTurnDontFailTheNextOne(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// After the ask_user tool call, immediately (no sleep) also emits the
	// agent_end + agent_settled pair Pi's own protocol produces once a turn
	// is fully idle — before ever reading line2. If these leak into the
	// next runTurn call, it'll see agent_settled first and fail instead of
	// reaching submit_adr.
	script := `read line1; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; echo '{"type":"agent_end","messages":[{"stopReason":"stop"}]}'; echo '{"type":"agent_settled"}'; read line2; echo '{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"reached submit_adr"}}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, fixedReplyWaiter{reply: "use-oauth"}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err != nil {
		t.Fatalf("expected the session to reach submit_adr despite the prior turn's trailing events, got: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected ask_user then submit_adr, got %d events: %+v", len(received), received)
	}
	if received[0].Type != rpc.EventAskUser {
		t.Fatalf("expected the first event to be ask_user, got %q", received[0].Type)
	}
	if received[1].Type != rpc.EventSubmitADR {
		t.Fatalf("expected the second event to be submit_adr, not a run_failed synthesized from the prior turn's leftovers, got %q (message: %q)", received[1].Type, received[1].Message)
	}
}

// Proves an assistant message_end (Pi's own plain text, e.g. the model
// thinking out loud alongside a tool call in the same turn) is relayed live
// via handle as a non-terminal EventAgentText, without ending the turn on
// it — the session must still proceed to submit_adr afterward. The one
// exception to "runTurn returns the instant Translate matches something."
func TestDriveSpecGrillSession_AgentTextIsForwardedLiveNotTurnEnding(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Thinking about the ADR structure before I submit it."}],"timestamp":1}}'; echo '{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"# Test ADR"},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	if err != nil {
		t.Fatalf("expected the session to still reach submit_adr despite the intervening agent_text, got: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected agent_text then submit_adr, got %d events: %+v", len(received), received)
	}
	if received[0].Type != rpc.EventAgentText {
		t.Fatalf("expected the first event to be agent_text, got %q", received[0].Type)
	}
	if received[0].Message != "Thinking about the ADR structure before I submit it." {
		t.Fatalf("expected the agent_text message to be carried through, got %q", received[0].Message)
	}
	if received[1].Type != rpc.EventSubmitADR {
		t.Fatalf("expected the second event to be submit_adr, got %q", received[1].Type)
	}
}

// Proves an unexpected end to the attach stream (the container exiting
// without ever producing a terminating contract event) surfaces as a
// synthesized run_failed curated event and a non-nil error — not a silent
// success.
func TestDriveSpecGrillSession_AttachFailureSurfacesAsRunFailed(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Blocks on read (so the pod is reliably caught Running by
	// WaitForJobPod) until the initial prompt arrives, then exits non-zero
	// without ever emitting a contract event.
	script := `read line; exit 1`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})

	if err == nil {
		t.Fatal("expected an error when the attach stream ends without a terminating event")
	}
	if len(received) != 1 || received[0].Type != rpc.EventRunFailed {
		t.Fatalf("expected exactly one synthesized run_failed event, got %+v", received)
	}
	if received[0].Message == "" {
		t.Fatal("expected the run_failed event to carry a non-empty message")
	}
}

// Proves the gap this fix closes: a buildAgentEnv failure (fetching the
// feature spec, minting the installation token, ...) — which happens before
// any Kubernetes Job is even created, one call site earlier than the
// WaitForJobPod failure ADR 011 item 3 already covers above — still posts a
// synthesized run_failed event. Without it, runClaimedJob's q.Fail only
// touches the jobs row and the feature is stuck in 'queued' forever with no
// event ever posted (this exact failure mode reached production: the API's
// /spec endpoint 502ed and the affected feature never left 'queued').
func TestRunAgentRPCJob_BuildAgentEnvFailureSurfacesAsRunFailed(t *testing.T) {
	var mu sync.Mutex
	var received []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/secrets"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{}})
		case strings.Contains(r.URL.Path, "/events"):
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			received = append(received, body)
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		default:
			// Stands in for the real production failure: the /spec fetch
			// itself erroring out (a 502, here a 500 — either way
			// FetchFeatureSpec returns a non-nil error).
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	featureID := "feat-broken"
	job := &queue.Job{ID: "job-buildenv-fail", ProjectID: "proj-1", Kind: queue.KindFeatureBuild, FeatureID: &featureID}
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	// clientset and q are never touched on this early-return path — nil is
	// safe here and keeps the test from depending on a reachable cluster.
	err := runAgentRPCJob(context.Background(), nil, nil, job, "test-namespace", cfg)
	if err == nil {
		t.Fatal("expected an error when buildAgentEnv fails")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected exactly one posted job event, got %d: %+v", len(received), received)
	}
	if received[0]["type"] != "run_failed" {
		t.Fatalf("expected a run_failed event, got %+v", received[0])
	}
	if msg, _ := received[0]["message"].(string); msg == "" {
		t.Fatal("expected the run_failed event to carry a non-empty message")
	}
}

// Proves a mid-turn cancellation (a human asked to stop the job — the
// cancel/abort follow-up from ADR 006) unblocks runTurn via ctx
// cancellation and ends the session with a synthesized run_cancelled
// curated event and errJobCancelled — not run_failed — even though the
// pod's script never produces a terminating contract event of its own.
func TestDriveSpecGrillSession_CancellationMidTurnEndsSessionAsCancelled(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Never responds to the initial prompt, so runTurn is genuinely still
	// attached and waiting on events when cancellation fires.
	script := `read line; sleep 30`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	start := time.Now()
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, delayedCancel{after: 500 * time.Millisecond}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})
	elapsed := time.Since(start)

	if !errors.Is(err, errJobCancelled) {
		t.Fatalf("expected errJobCancelled, got: %v", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("expected cancellation to end the session promptly, took %s", elapsed)
	}
	if len(received) != 1 || received[0].Type != rpc.EventRunCancelled {
		t.Fatalf("expected exactly one run_cancelled event, got %+v", received)
	}
}

// Proves cancellation also interrupts a session that's waiting *between*
// turns (msgs.WaitForReply, after ask_user) — not just mid-turn.
func TestDriveSpecGrillSession_CancellationWhileAwaitingReplyEndsSessionAsCancelled(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	script := `read line; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveAgentSession(ctx, clientset.Interface, restConfig, blockingReplyWaiter{}, delayedCancel{after: 500 * time.Millisecond}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
		received = append(received, ev)
	})

	if !errors.Is(err, errJobCancelled) {
		t.Fatalf("expected errJobCancelled, got: %v", err)
	}
	if len(received) != 2 {
		t.Fatalf("expected ask_user then run_cancelled, got %d events: %+v", len(received), received)
	}
	if received[0].Type != rpc.EventAskUser {
		t.Fatalf("expected the first event to be ask_user, got %q", received[0].Type)
	}
	if received[1].Type != rpc.EventRunCancelled {
		t.Fatalf("expected the second event to be run_cancelled, got %q", received[1].Type)
	}
}

// TestBuildInitialPrompt_NamesTheSkillMatchingFeatureType verifies ADR 008
// items 1-2: the prompt must name project-init's SKILL.md for a
// project_init feature and feature-grill's for everything else, since
// Title alone ("Project initialization") carries no signal the container
// could use to tell the two cases apart on its own.
func TestBuildInitialPrompt_NamesTheSkillMatchingFeatureType(t *testing.T) {
	projectInit := buildInitialPrompt(queue.KindSpecGrill, apiclient.FeatureSpec{
		Title:       "Project initialization",
		FeatureType: "project_init",
		Repos:       []apiclient.FeatureSpecRepo{{CloneURL: "https://github.com/acme/web.git", IsPrimary: true}},
	})
	if !strings.Contains(projectInit, "project-init/SKILL.md") {
		t.Fatalf("expected project_init prompt to name project-init/SKILL.md, got: %s", projectInit)
	}
	if strings.Contains(projectInit, "feature-grill/SKILL.md") {
		t.Fatalf("project_init prompt must not also name feature-grill/SKILL.md, got: %s", projectInit)
	}

	normal := buildInitialPrompt(queue.KindSpecGrill, apiclient.FeatureSpec{
		Title:       "Add dark mode",
		FeatureType: "normal",
		Repos:       []apiclient.FeatureSpecRepo{{CloneURL: "https://github.com/acme/web.git", IsPrimary: true}},
	})
	if !strings.Contains(normal, "feature-grill/SKILL.md") {
		t.Fatalf("expected normal-feature prompt to name feature-grill/SKILL.md, got: %s", normal)
	}
	if strings.Contains(normal, "project-init/SKILL.md") {
		t.Fatalf("normal-feature prompt must not also name project-init/SKILL.md, got: %s", normal)
	}
	if !strings.Contains(normal, "Add dark mode") {
		t.Fatalf("expected normal-feature prompt to include the feature title, got: %s", normal)
	}
}

// TestBuildInitialPrompt_FeatureBuildPointsAtImplementSkill verifies ADR
// 010 item 6: a feature_build job gets the short prompt naming the
// implement skill, not spec_grill's fuller repo-listing form — the skill
// file itself, not the Orchestrator, documents the repo/branch/ADR-file
// assumptions for this job kind.
func TestBuildInitialPrompt_FeatureBuildPointsAtImplementSkill(t *testing.T) {
	prompt := buildInitialPrompt(queue.KindFeatureBuild, apiclient.FeatureSpec{
		Title: "Add dark mode",
		Repos: []apiclient.FeatureSpecRepo{{CloneURL: "https://github.com/acme/web.git", IsPrimary: true}},
	})
	if !strings.Contains(prompt, "implement/SKILL.md") {
		t.Fatalf("expected feature_build prompt to name implement/SKILL.md, got: %s", prompt)
	}
	if !strings.Contains(prompt, "Add dark mode") {
		t.Fatalf("expected feature_build prompt to include the feature title, got: %s", prompt)
	}
	if strings.Contains(prompt, "feature-grill/SKILL.md") || strings.Contains(prompt, "project-init/SKILL.md") {
		t.Fatalf("feature_build prompt must not name a spec_grill skill, got: %s", prompt)
	}
}

func TestBuildInitialPrompt_DesignGrillPointsAtDesignSkill(t *testing.T) {
	prompt := buildInitialPrompt(queue.KindDesignGrill, apiclient.FeatureSpec{
		DesignName:        "Checkout flow",
		DesignDescription: "A responsive checkout mockup.",
	})
	if !strings.Contains(prompt, "design-grill/SKILL.md") {
		t.Fatalf("expected design_grill prompt to name design-grill/SKILL.md, got: %s", prompt)
	}
	if !strings.Contains(prompt, "Checkout flow") || !strings.Contains(prompt, "responsive checkout") {
		t.Fatalf("expected design metadata in prompt, got: %s", prompt)
	}
}

func TestBuildInitialPrompt_SpecGrillIncludesKickbackContext(t *testing.T) {
	prompt := buildInitialPrompt(queue.KindSpecGrill, apiclient.FeatureSpec{
		Title:       "Add dark mode",
		FeatureType: "normal",
		SpecContext: &apiclient.SpecGrillContext{
			PreviousAdrMarkdown:    "# Previous ADR",
			GrillTranscriptSummary: "User chose CSS variables.",
			KickbackReason:         "secret_request: Need DARK_MODE_KEY",
			RequestedActionItems: []rpc.RequestedActionItem{
				{Type: "secret_request", Description: "Need DARK_MODE_KEY"},
			},
			DesignSnapshots: []apiclient.DesignSnapshotContext{{
				SessionID: "design-1",
				Snapshot:  map[string]string{"designs/dark-mode/page.html": "<h1>Dark mode</h1>"},
			}},
		},
	})

	for _, want := range []string{
		"# Previous ADR",
		"User chose CSS variables.",
		"Need DARK_MODE_KEY",
		"designs/dark-mode/page.html",
		"<h1>Dark mode</h1>",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("expected prompt to include %q, got: %s", want, prompt)
		}
	}
}
