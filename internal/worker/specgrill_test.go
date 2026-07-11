package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

const testStandInImage = "busybox:1.36"

// startAttachablePod creates a Job running script (a busybox `sh -c`
// script) with stdin attached, waits for its pod to become attachable, and
// returns everything driveSpecGrillSession needs. script stands in for
// Pi + the yggdrasil-contract extension without needing a real agent-images
// image: it reads whatever driveSpecGrillSession sends as the initial
// prompt and reacts however the test wants.
func startAttachablePod(t *testing.T, ctx context.Context, script string) (namespace, podName, jobName string) {
	t.Helper()
	clientset := testClient(t)

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, "test-"+rand.String(8))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	jobName = "test-specgrill-" + rand.String(6)
	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      jobName,
		Image:     testStandInImage,
		Command:   []string{"sh", "-c", script},
		Stdin:     true,
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	t.Cleanup(func() {
		_ = k8s.DeleteJob(context.Background(), clientset, namespace, jobName)
	})

	podName, err = k8s.WaitForJobPod(ctx, clientset, namespace, jobName)
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

// Proves driveSpecGrillSession correctly identifies submit_adr as the
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
	err = driveSpecGrillSession(ctx, clientset, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
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

// Proves ask_user does NOT end the run despite its own tool result also
// setting terminate:true — the session should keep waiting for a human
// reply rather than treating this as completion. Uses blockingReplyWaiter
// (no reply ever arrives) so the only way this test passes is if
// driveSpecGrillSession is still running after a few seconds — a real
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
		sessionDone <- driveSpecGrillSession(ctx, clientset, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
			mu.Lock()
			received = append(received, ev)
			mu.Unlock()
		})
	}()

	select {
	case err := <-sessionDone:
		t.Fatalf("expected driveSpecGrillSession to still be waiting on a reply, but it returned: %v", err)
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
// from stdin, so this only passes if driveSpecGrillSession genuinely wrote
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
	// not a bare string — driveSpecGrillSession sends replies the same way it sends the
	// initial prompt. Extract just the message field before embedding it in this script's
	// own JSON output; echoing line2 raw would nest unescaped quotes and produce invalid JSON.
	script := `read line1; echo '{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}'; read line2; reply=$(echo "$line2" | sed -n 's/.*"message":"\([^"]*\)".*/\1/p'); echo "{\"type\":\"tool_execution_end\",\"toolName\":\"submit_adr\",\"result\":{\"details\":{\"kind\":\"submit_adr\",\"markdown\":\"reply was: $reply\"},\"terminate\":true}}"; cat`
	namespace, podName, _ := startAttachablePod(t, ctx, script)

	var received []rpc.CuratedEvent
	err = driveSpecGrillSession(ctx, clientset, restConfig, fixedReplyWaiter{reply: "use-oauth"}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
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
	err = driveSpecGrillSession(ctx, clientset, restConfig, blockingReplyWaiter{}, neverCancels{}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
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
	err = driveSpecGrillSession(ctx, clientset, restConfig, blockingReplyWaiter{}, delayedCancel{after: 500 * time.Millisecond}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
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
	err = driveSpecGrillSession(ctx, clientset, restConfig, blockingReplyWaiter{}, delayedCancel{after: 500 * time.Millisecond}, namespace, podName, "job-1", "New feature: dark mode", func(ev rpc.CuratedEvent) {
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
	projectInit := buildInitialPrompt(apiclient.FeatureSpec{
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

	normal := buildInitialPrompt(apiclient.FeatureSpec{
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
