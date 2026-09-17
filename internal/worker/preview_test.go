package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// fakeClusterProvider records which projects a pass asked about and answers
// with whatever the test wants — the same interface-substitution the worker
// already uses for replyWaiter/cancelWatcher, so preview behaviour is testable
// without a real cluster.
type fakeClusterProvider struct {
	mu       sync.Mutex
	resolved []string
	client   *k8s.Client
	err      error
}

func (f *fakeClusterProvider) Resolve(_ context.Context, job *queue.Job) (*k8s.Client, error) {
	f.mu.Lock()
	f.resolved = append(f.resolved, job.ProjectID)
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

func (f *fakeClusterProvider) projects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.resolved...)
}

// unreachableClient is a k8s.Client whose REST config points at a closed port,
// so anything that genuinely talks to a cluster fails fast instead of hanging
// or (worse) appearing to succeed.
func unreachableClient() *k8s.Client {
	return &k8s.Client{
		Interface: fake.NewSimpleClientset(),
		Config:    &rest.Config{Host: "https://127.0.0.1:1"},
	}
}

// previewAPI is a fake API recording the preview calls the worker makes.
type previewAPI struct {
	server     *httptest.Server
	stale      []apiclient.StalePreview
	staleQuery url.Values
	mu         sync.Mutex
	teardowns  []string
	registra   []string
}

func newPreviewAPI(stale []apiclient.StalePreview) *previewAPI {
	api := &previewAPI{stale: stale}
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/internal/previews/stale":
			api.mu.Lock()
			api.staleQuery = r.URL.Query()
			api.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"previews": api.stale})
		case len(r.URL.Path) > len("/preview/teardown") &&
			r.URL.Path[len(r.URL.Path)-len("/preview/teardown"):] == "/preview/teardown":
			api.mu.Lock()
			api.teardowns = append(api.teardowns, r.URL.Path)
			api.mu.Unlock()
			_, _ = w.Write([]byte(`{"preview":null}`))
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	return api
}

func (a *previewAPI) close() { a.server.Close() }

func (a *previewAPI) client() *apiclient.Client {
	return apiclient.New(a.server.URL, "test-token")
}

func (a *previewAPI) teardownCalls() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.teardowns...)
}

// --- configuration ---------------------------------------------------------

func TestPreviewSettings_Defaults(t *testing.T) {
	var cfg Config

	// Unset keeps ADR 003 §17's documented default rather than disabling the
	// feature, which is what a zero value would otherwise mean.
	if got := cfg.maxConcurrentPreviews(); got != 3 {
		t.Fatalf("expected the ADR 003 §17 default cap of 3, got %d", got)
	}
	if got := cfg.previewTTL(); got != 2*time.Hour {
		t.Fatalf("expected a 2h default TTL, got %s", got)
	}
	if got := cfg.previewSweepInterval(); got != 15*time.Minute {
		t.Fatalf("expected a 15m default sweep interval, got %s", got)
	}

	configured := Config{
		MaxConcurrentPreviews: 5,
		PreviewTTL:            time.Hour,
		PreviewSweepInterval:  time.Minute,
	}
	if got := configured.maxConcurrentPreviews(); got != 5 {
		t.Fatalf("expected the configured cap, got %d", got)
	}
	if got := configured.previewTTL(); got != time.Hour {
		t.Fatalf("expected the configured TTL, got %s", got)
	}
	if got := configured.previewSweepInterval(); got != time.Minute {
		t.Fatalf("expected the configured interval, got %s", got)
	}
}

// A negative cap is the supported "previews off" setting: it disables previews
// without disabling any other job kind, and it keeps the claim query free of
// the previews table so an Orchestrator can run against a database whose
// migrations predate it.
func TestPreviewSettings_NegativeCapDisablesPreviews(t *testing.T) {
	cfg := Config{MaxConcurrentPreviews: -1}

	if got := cfg.maxConcurrentPreviews(); got != 0 {
		t.Fatalf("expected a negative cap to resolve to 0 (disabled), got %d", got)
	}
	if handle := startJobPreview(context.Background(), nil, &queue.Job{
		ID: "job-1", ProjectID: "proj-1", Kind: queue.KindTestRun,
	}, "proj-ns", cfg); handle != nil {
		t.Fatal("expected no preview when previews are disabled")
	}
}

// --- eligibility gating ----------------------------------------------------

// Only ADR 003 §10's three kinds get a preview. A deploy/rollback manages the
// always-on release, and the rest produce no deployable artifact — none may
// occupy one of the project's three preview slots.
func TestStartJobPreview_OnlyPreviewEligibleKinds(t *testing.T) {
	cfg := Config{}

	notEligible := []queue.JobKind{
		queue.KindDeploy,
		queue.KindRollback,
		queue.KindAgenticReview,
		queue.KindDesignGrill,
		queue.KindScriptTestRun,
	}
	for _, kind := range notEligible {
		// A nil client and nil API client prove the gate short-circuits before
		// anything is touched: if it did not, this would nil-panic.
		handle := startJobPreview(context.Background(), nil, &queue.Job{
			ID: "job-1", ProjectID: "proj-1", Kind: kind,
		}, "proj-ns", cfg)
		if handle != nil {
			t.Fatalf("expected %s to get no preview", kind)
		}
	}
}

// --- the orphan sweep ------------------------------------------------------

func TestSweepPreviews_FetchesTheWorkListAndReportsNothingItDidNotRemove(t *testing.T) {
	api := newPreviewAPI([]apiclient.StalePreview{
		{JobID: "job-a", ProjectID: "proj-1", Host: "a.preview.local"},
		{JobID: "job-b", ProjectID: "proj-2", Host: "b.preview.local"},
	})
	defer api.close()

	clusters := &fakeClusterProvider{err: context.DeadlineExceeded}

	SweepPreviews(context.Background(), Config{
		APIClient: api.client(),
		Clusters:  clusters,
	})

	// Every stale preview is attempted.
	if got := len(clusters.projects()); got != 2 {
		t.Fatalf("expected both projects to be resolved, got %v", clusters.projects())
	}

	// Fail-closed invariant: a preview whose cluster could not be reached is
	// NOT reported as torn down. Reporting it would free the project's §17
	// slot while the cluster resource is still running — the leak this sweep
	// exists to prevent, now invisible.
	if calls := api.teardownCalls(); len(calls) != 0 {
		t.Fatalf("expected no teardown reports when nothing was removed, got %v", calls)
	}
}

// A project that cannot be reached must not stop the pass — otherwise one bad
// org kubeconfig would block cleanup for every other project.
func TestSweepPreviews_ContinuesPastAFailingProject(t *testing.T) {
	api := newPreviewAPI([]apiclient.StalePreview{
		{JobID: "job-a", ProjectID: "proj-1", Host: "a.preview.local"},
		{JobID: "job-b", ProjectID: "proj-2", Host: "b.preview.local"},
		{JobID: "job-c", ProjectID: "proj-3", Host: "c.preview.local"},
	})
	defer api.close()

	clusters := &fakeClusterProvider{err: context.DeadlineExceeded}
	SweepPreviews(context.Background(), Config{APIClient: api.client(), Clusters: clusters})

	if got := clusters.projects(); len(got) != 3 {
		t.Fatalf("expected the pass to attempt all three projects, got %v", got)
	}
}

// A teardown that fails at the cluster (here: the release cannot be uninstalled
// because the cluster is unreachable) must also stay out of the registry, for
// the same fail-closed reason.
func TestSweepPreviews_DoesNotReportATeardownThatFailed(t *testing.T) {
	api := newPreviewAPI([]apiclient.StalePreview{
		{JobID: "job-a", ProjectID: "proj-1", Host: "a.preview.local"},
	})
	defer api.close()

	clusters := &fakeClusterProvider{client: unreachableClient()}

	SweepPreviews(context.Background(), Config{APIClient: api.client(), Clusters: clusters})

	if calls := api.teardownCalls(); len(calls) != 0 {
		t.Fatalf("expected no teardown report after a failed removal, got %v", calls)
	}
}

func TestSweepPreviews_NoWorkMeansNoClusterCalls(t *testing.T) {
	api := newPreviewAPI(nil)
	defer api.close()

	clusters := &fakeClusterProvider{client: unreachableClient()}

	SweepPreviews(context.Background(), Config{APIClient: api.client(), Clusters: clusters})

	if got := clusters.projects(); len(got) != 0 {
		t.Fatalf("expected no cluster resolution with an empty work list, got %v", got)
	}
}

// The sweep passes its configured TTL through, because that value is what caps
// how long a hard-crashed job's preview can survive — a wrong value here is a
// silent leak or a preview that vanishes too early.
func TestSweepPreviews_PassesTheConfiguredTTL(t *testing.T) {
	api := newPreviewAPI(nil)
	defer api.close()

	SweepPreviews(context.Background(), Config{
		APIClient:  api.client(),
		Clusters:   &fakeClusterProvider{},
		PreviewTTL: 90 * time.Minute,
	})

	api.mu.Lock()
	query := api.staleQuery
	api.mu.Unlock()
	if query == nil {
		t.Fatal("expected the sweep to fetch a work list")
	}
	if got := query.Get("ttlSeconds"); got != "5400" {
		t.Fatalf("expected ttlSeconds=5400 (90m), got %q", got)
	}
}

// Disabled previews mean no sweep at all: no work list fetched, no cluster
// contacted. Nothing should be tearing down previews in an install that does
// not create them.
func TestSweepPreviews_DisabledPreviewsDoNothing(t *testing.T) {
	api := newPreviewAPI([]apiclient.StalePreview{
		{JobID: "job-a", ProjectID: "proj-1", Host: "a.preview.local"},
	})
	defer api.close()

	clusters := &fakeClusterProvider{}
	SweepPreviews(context.Background(), Config{
		APIClient:             api.client(),
		Clusters:              clusters,
		MaxConcurrentPreviews: -1,
	})

	if got := clusters.projects(); len(got) != 0 {
		t.Fatalf("expected no cluster resolution when previews are disabled, got %v", got)
	}
	api.mu.Lock()
	query := api.staleQuery
	api.mu.Unlock()
	if query != nil {
		t.Fatal("expected no work-list fetch when previews are disabled")
	}
}

// A sweep whose work list cannot be fetched must not panic or invent work.
func TestSweepPreviews_APIFailureIsSurvivable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	clusters := &fakeClusterProvider{}
	SweepPreviews(context.Background(), Config{
		APIClient: apiclient.New(server.URL, "test-token"),
		Clusters:  clusters,
	})

	if got := clusters.projects(); len(got) != 0 {
		t.Fatalf("expected no cluster resolution after an API failure, got %v", got)
	}
}
