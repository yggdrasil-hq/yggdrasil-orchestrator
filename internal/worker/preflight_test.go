package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
Issues #29 and #44: the two ways a job kind fails before any of its own code
runs. Both are *setup* problems, and both used to surface as something else — a
context deadline (#29) or a failed feature on every feature (#44).

The decisions themselves are unit-tested where they live
(`internal/k8s/imagepull_test.go`). These tests cover the worker's wiring: which
image each case resolves to, what the preflight logs, and what the skip path
actually posts.
*/

// fakeCluster is a k8s.Client backed by a fake clientset, plus the namespace's
// `default` service account when one is wanted.
func fakeCluster(t *testing.T, imagePullReferences ...string) *k8s.Client {
	t.Helper()
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: preflightTestNamespace},
	}
	for _, ref := range imagePullReferences {
		sa.ImagePullSecrets = append(sa.ImagePullSecrets, corev1.LocalObjectReference{Name: ref})
	}
	return &k8s.Client{
		Interface: fake.NewSimpleClientset(sa),
		Config:    &rest.Config{Host: "https://127.0.0.1:1"},
	}
}

const preflightTestNamespace = "proj-preflight-test"

// captureLog redirects the standard logger for the duration of a test, so what
// the worker *tells the operator* is the thing under assertion. The log line is
// the preflight's entire output — it deliberately does not fail a job — so a
// test that only inspected the return value would be testing nothing.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	original := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(original) })
	return &buf
}

// --- #29: the preflight decision, per case ----------------------------------

// fakeRegistry is a stand-in for GHCR's token endpoint. The real endpoint answers
// 200-with-a-token for a public package and 401 for a private one, which is the
// distinction the preflight is built on — see `registry_test.go` in the k8s
// package for the decision itself. This file checks the worker's wiring: which
// image each case resolves to, and what the operator is told.
func fakeRegistry(t *testing.T, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`{"token":"anonymous-token"}`))
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// Case one: the image is missing from the config entirely. An agent kind then
// runs the placeholder, which is a public docker.io image — so there is nothing
// to warn about, and warning would be noise on every local-dev job. Note the
// registry is *private* here: the placeholder must not even be probed.
func TestPreflightImagePull_SaysNothingAboutThePlaceholderDefault(t *testing.T) {
	logged := captureLog(t)
	registry := fakeRegistry(t, http.StatusUnauthorized)
	cfg := Config{
		Images:           map[queue.JobKind]string{},
		PlaceholderImage: "busybox:1.36",
		PullPreflight:    &k8s.ImagePullPreflight{TokenEndpoint: registry.URL},
	}
	job := &queue.Job{ID: "job-1", ProjectID: "proj-1", Kind: queue.KindSpecGrill}

	preflightImagePull(context.Background(), fakeCluster(t), job, preflightTestNamespace, cfg)

	if strings.Contains(logged.String(), "WARNING") {
		t.Fatalf("expected no warning for a public placeholder image, got:\n%s", logged.String())
	}
}

// Case two: an image is configured and the registry refuses an anonymous pull for
// it, and nothing in the namespace can authenticate. This is a fresh install with
// a private package — the case the issue is about.
func TestPreflightImagePull_WarnsWhenThePackageNeedsACredential(t *testing.T) {
	logged := captureLog(t)
	registry := fakeRegistry(t, http.StatusUnauthorized)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindTestRun: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/test_run:latest",
		},
		PullPreflight: &k8s.ImagePullPreflight{TokenEndpoint: registry.URL},
	}
	job := &queue.Job{ID: "job-2", ProjectID: "proj-1", Kind: queue.KindTestRun}

	preflightImagePull(context.Background(), fakeCluster(t), job, preflightTestNamespace, cfg)

	output := logged.String()
	for _, want := range []string{"WARNING", "ghcr.io", "JOB_IMAGE_PULL_SECRET", "setup error"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected the warning to mention %q, got:\n%s", want, output)
		}
	}
}

// Case three: the same private package, with a credential available. Silence — a
// warning here would be a false positive on a correctly configured install.
func TestPreflightImagePull_IsSilentWhenTheNamespaceCanAuthenticate(t *testing.T) {
	logged := captureLog(t)
	registry := fakeRegistry(t, http.StatusUnauthorized)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindTestRun: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/test_run:latest",
		},
		PullPreflight: &k8s.ImagePullPreflight{TokenEndpoint: registry.URL},
	}
	job := &queue.Job{ID: "job-3", ProjectID: "proj-1", Kind: queue.KindTestRun}

	// The namespace's default service account references a credential, which is
	// the route that needs no Orchestrator configuration at all.
	preflightImagePull(context.Background(), fakeCluster(t, "ghcr-pull"), job, preflightTestNamespace, cfg)

	if strings.Contains(logged.String(), "WARNING") {
		t.Fatalf("expected no warning when the cluster can authenticate, got:\n%s", logged.String())
	}
}

// Case four: the package is public (five of the six published ones are), so no
// credential is needed and there is nothing to report even though the namespace
// has none. This is the case a host-based heuristic would have got wrong.
func TestPreflightImagePull_IsSilentForAPublicPackage(t *testing.T) {
	logged := captureLog(t)
	registry := fakeRegistry(t, http.StatusOK)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindSpecGrill: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/spec_grill:latest",
		},
		PullPreflight: &k8s.ImagePullPreflight{TokenEndpoint: registry.URL},
	}
	job := &queue.Job{ID: "job-5", ProjectID: "proj-1", Kind: queue.KindSpecGrill}

	preflightImagePull(context.Background(), fakeCluster(t), job, preflightTestNamespace, cfg)

	if strings.Contains(logged.String(), "WARNING") {
		t.Fatalf("expected no warning for an anonymously pullable package, got:\n%s", logged.String())
	}
}

// The configured secret is taken as sufficient: whether it exists is the kubelet's
// answer to give, and reporting a second, less reliable version of the same
// problem would be worse than saying nothing.
func TestPreflightImagePull_TrustsAConfiguredPullSecret(t *testing.T) {
	logged := captureLog(t)
	registry := fakeRegistry(t, http.StatusUnauthorized)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindTestRun: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/test_run:latest",
		},
		ImagePullSecret: "ghcr-pull",
		PullPreflight:   &k8s.ImagePullPreflight{TokenEndpoint: registry.URL},
	}
	job := &queue.Job{ID: "job-4", ProjectID: "proj-1", Kind: queue.KindTestRun}

	// No service account in the fake at all: the configured name is enough.
	preflightImagePull(context.Background(), fakeCluster(t), job, preflightTestNamespace, cfg)

	if strings.Contains(logged.String(), "WARNING") {
		t.Fatalf("expected no warning when JOB_IMAGE_PULL_SECRET is set, got:\n%s", logged.String())
	}
}

// --- #44: a script_test_run group this installation cannot run --------------
// eventRecorder is a fake API that records the job events posted to it.
type eventRecorder struct {
	server *httptest.Server
	events []map[string]any
}

func newEventRecorder(t *testing.T) *eventRecorder {
	t.Helper()
	recorder := &eventRecorder{}
	recorder.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode posted event: %v", err)
		}
		recorder.events = append(recorder.events, body)
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(recorder.server.Close)
	return recorder
}

func (r *eventRecorder) config() Config {
	return Config{APIClient: apiclient.New(r.server.URL, "test-token")}
}

func scriptTestJob(group string) *queue.Job {
	return &queue.Job{
		ID:        "job-script-1",
		ProjectID: "proj-1",
		Kind:      queue.KindScriptTestRun,
		TestGroup: &group,
	}
}

// The behaviour the issue asks for: a group whose image is not configured is
// reported as skipped — the same outcome a missing `test-unit.sh` produces —
// rather than failing the job, so one unset variable cannot fail every feature
// in the installation.
func TestSkipWhenScriptImageUnconfigured_ReportsTheGroupAsSkipped(t *testing.T) {
	recorder := newEventRecorder(t)
	cfg := Config{
		Images:    map[queue.JobKind]string{},
		APIClient: recorder.config().APIClient,
	}
	captureLog(t)

	handled, err := skipWhenScriptImageUnconfigured(context.Background(), scriptTestJob("unit"), cfg)
	if !handled {
		t.Fatal("expected the job to be handled (not run)")
	}
	if err != nil {
		t.Fatalf("expected no error — the group was recorded as skipped: %v", err)
	}

	if len(recorder.events) != 1 {
		t.Fatalf("expected exactly one event, got %d: %#v", len(recorder.events), recorder.events)
	}
	event := recorder.events[0]
	if event["type"] != string(rpc.EventSubmitTestReport) {
		t.Fatalf("expected a %s event, got %v", rpc.EventSubmitTestReport, event["type"])
	}
	// Zero failures is what keeps the group from being read as a code failure —
	// nothing about the project's code was learned either way.
	if failed := event["failed"]; failed != float64(0) {
		t.Fatalf("expected failed=0, got %v", failed)
	}
	if skipped := event["skipped"]; skipped != float64(1) {
		t.Fatalf("expected skipped=1 (one group was skipped), got %v", skipped)
	}
	if total := event["total"]; total != float64(1) {
		t.Fatalf("expected total=1 (the schema requires total >= passed+failed+skipped), got %v", total)
	}

	// Issue #53: the *reason* travels as a value, not only as prose. The API's gate
	// has to decide differently for the two causes of a skip — an install that
	// cannot run a group means nothing was verified, whereas a repository with no
	// script has disabled the group by choice — and inferring that from the
	// summary's words would fail silently in the direction of advancing if the
	// sentence were ever reworded. So this is the field the gate acts on, and the
	// summary below is what a human reads.
	if reason, _ := event["skipReason"].(string); reason != "runner_unavailable" {
		t.Fatalf("expected skipReason=runner_unavailable, got %#v", event["skipReason"])
	}

	// The summary carries the same cause for a person looking at the run, and
	// names the variable to fix.
	summary, _ := event["summary"].(string)
	for _, want := range []string{"unit", "SCRIPT_TEST_RUN_IMAGE", "No verification was performed"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("expected the summary to mention %q, got %q", want, summary)
		}
	}
	// The api client tags `failingTests` omitempty, so a skipped group sends no
	// list at all — which the API's schema accepts (`optional`) and which is
	// accurate: there are no failing assertions to name.
	if failures, present := event["failingTests"]; present {
		t.Fatalf("expected no failingTests for a skipped group, got %#v", failures)
	}
}

// The other value of the enum belongs to the runner that actually looked and
// found no script (agent-images' script_test_run entrypoint). This path only runs
// when the image for the kind is missing outright, so nothing ever looked — and
// sending `no_script` here would tell the gate to advance over a group the
// install could not run, which is the exact bug #53 exists to fix.
func TestSkipWhenScriptImageUnconfigured_DoesNotClaimTheProjectHasNoScript(t *testing.T) {
	recorder := newEventRecorder(t)
	cfg := Config{
		Images:    map[queue.JobKind]string{},
		APIClient: recorder.config().APIClient,
	}
	captureLog(t)

	if _, err := skipWhenScriptImageUnconfigured(context.Background(), scriptTestJob("unit"), cfg); err != nil {
		t.Fatalf("expected no error: %v", err)
	}

	if len(recorder.events) != 1 {
		t.Fatalf("expected one event, got %d", len(recorder.events))
	}
	if reason, _ := recorder.events[0]["skipReason"].(string); reason == "no_script" {
		t.Fatal("an unconfigured image must not report no_script — that would tell the gate to advance over unverified work")
	}
}

// A `script_test_run` this installation *can* run must be left alone: the whole
// point of the skip is that it only applies to the case that cannot work.
func TestSkipWhenScriptImageUnconfigured_LeavesAConfiguredGroupAlone(t *testing.T) {
	recorder := newEventRecorder(t)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindScriptTestRun: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/script_test_run:latest",
		},
		APIClient: recorder.config().APIClient,
	}

	handled, err := skipWhenScriptImageUnconfigured(context.Background(), scriptTestJob("unit"), cfg)
	if handled {
		t.Fatal("expected a configured group to run normally")
	}
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
	if len(recorder.events) != 0 {
		t.Fatalf("expected no events, got %#v", recorder.events)
	}
}

func TestSkipWhenScriptImageUnconfigured_IgnoresOtherJobKinds(t *testing.T) {
	recorder := newEventRecorder(t)
	cfg := Config{
		Images:    map[queue.JobKind]string{},
		APIClient: recorder.config().APIClient,
	}

	for _, kind := range []queue.JobKind{
		queue.KindSpecGrill,
		queue.KindFeatureBuild,
		queue.KindTestRun,
		queue.KindDeploy,
		queue.KindAgenticReview,
		queue.KindDesignGrill,
	} {
		job := &queue.Job{ID: "job-x", ProjectID: "proj-1", Kind: kind}
		handled, err := skipWhenScriptImageUnconfigured(context.Background(), job, cfg)
		if handled || err != nil {
			t.Fatalf("%s: expected the job to proceed normally, got handled=%v err=%v", kind, handled, err)
		}
	}
	if len(recorder.events) != 0 {
		t.Fatalf("expected no events, got %#v", recorder.events)
	}
}

// A report that cannot be posted is the one case that still fails: an empty
// group with no record of why would be read as a pass, so the honest outcome is
// a failed job rather than a silent one.
func TestSkipWhenScriptImageUnconfigured_FailsWhenTheReportCannotBePosted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	captureLog(t)

	cfg := Config{
		Images:    map[queue.JobKind]string{},
		APIClient: apiclient.New(server.URL, "test-token"),
	}

	handled, err := skipWhenScriptImageUnconfigured(context.Background(), scriptTestJob("integration"), cfg)
	if !handled {
		t.Fatal("expected the job to be handled")
	}
	if err == nil {
		t.Fatal("expected an error when the skip report could not be posted")
	}
}

// The two setup gaps are different, and this is the distinction the operator is
// sent to fix: no image configured is not the same problem as no credential for
// a configured image.
func TestSkipWhenScriptImageUnconfigured_DoesNotFireForAPullFailure(t *testing.T) {
	recorder := newEventRecorder(t)
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindScriptTestRun: "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/script_test_run:latest",
		},
		APIClient: recorder.config().APIClient,
	}

	// The image *is* configured, so this is #29's case: it must fail with the
	// setup error naming the credential, not be skipped. Skipping here would hide
	// a real mistake from an operator who explicitly asked for these tests.
	handled, err := skipWhenScriptImageUnconfigured(context.Background(), scriptTestJob("integration"), cfg)
	if handled || err != nil {
		t.Fatalf("expected the pull-failure case to be left to the preflight, got handled=%v err=%v", handled, err)
	}
}
