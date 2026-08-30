package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testClient connects to whatever cluster KUBECONFIG (or in-cluster config)
// points at, skipping the test if none is reachable — same pattern used by
// internal/k8s and internal/helm. Returns a *k8s.Client so it can be passed
// straight to the per-org client-consuming funcs under test.
func testClient(t *testing.T) *k8s.Client {
	t.Helper()
	clientset, err := k8s.NewClient()
	if err != nil {
		t.Skipf("no Kubernetes config available; skipping: %v", err)
	}
	if _, err := clientset.Discovery().ServerVersion(); err != nil {
		t.Skipf("Kubernetes cluster unreachable; skipping: %v", err)
	}
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}
	return &k8s.Client{Interface: clientset, Config: restConfig}
}

// Proves runDeploy's chart-resolution glue actually prefers a scaffolded
// project chart when the internal endpoint has one, and correctly falls
// back to the embedded placeholder on a 404 — both without needing a real
// GitHub repo (per the Phase 3c plan's verification note).
func TestResolveChart_PrefersScaffoldedChartWhenFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"files": map[string]string{
				"Chart.yaml": "apiVersion: v2\nname: scaffolded-test-chart\nversion: 0.1.0\n",
				"values.yaml": `replicaCount: 1
image:
  repository: nginxdemos/hello
  tag: latest
`,
				"templates/deployment.yaml": `apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ .Release.Name }}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {{ .Release.Name }}
    spec:
      containers:
        - name: app
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
`,
			},
		})
	}))
	defer server.Close()

	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
	chrt, err := resolveChart(context.Background(), cfg, "proj-123")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if chrt.Metadata.Name != "scaffolded-test-chart" {
		t.Fatalf("expected the scaffolded chart to be used, got chart name %q", chrt.Metadata.Name)
	}
}

func TestResolveChart_FallsBackToPlaceholderWhenNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
	chrt, err := resolveChart(context.Background(), cfg, "proj-123")
	if err != nil {
		t.Fatalf("expected no error (404 should fall back, not fail), got: %v", err)
	}
	if chrt.Metadata.Name != "placeholder" {
		t.Fatalf("expected fallback to the embedded placeholder chart, got chart name %q", chrt.Metadata.Name)
	}
}

// A deploy shouldn't succeed silently without an Ingress just because the
// slug-metadata fetch failed — proves runDeploy surfaces that error rather
// than swallowing it after a successful helm.Deploy.
func TestRunDeploy_SurfacesSlugFetchError(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/secrets"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{}})
		case strings.HasSuffix(r.URL.Path, "/chart"):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/slug"):
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	projectID := "test-" + time.Now().Format("150405")
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset.Interface, projectID)
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.Interface.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := Config{
		APIClient:        apiclient.New(server.URL, "test-token"),
		AppsDomain:       "yggdrasil.local",
		IngressClassName: "traefik",
		CertIssuerName:   "selfsigned-issuer",
	}

	err = runDeploy(ctx, clientset, projectID, namespace, cfg)
	if err == nil {
		t.Fatal("expected runDeploy to return an error when the slug fetch fails, got nil")
	}
}

// Proves a job kind with a configured agent-images image (ADR 004) runs
// with no Command override, so the image's own ENTRYPOINT (pi --mode rpc)
// takes over instead of the placeholder shell script.
func TestResolveAgentImage_UsesConfiguredImageWithNoCommandOverride(t *testing.T) {
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindSpecGrill: "registry.example.com/yggdrasil-agent-spec-grill:v1",
		},
		PlaceholderImage:  "busybox:1.36",
		PlaceholderScript: "echo job $JOB_ID kind=$JOB_KIND; exit 0",
	}

	image, command := resolveAgentImage(cfg, queue.KindSpecGrill)
	if image != "registry.example.com/yggdrasil-agent-spec-grill:v1" {
		t.Fatalf("expected the configured spec_grill image, got %q", image)
	}
	if command != nil {
		t.Fatalf("expected no command override for a real agent-images image, got %v", command)
	}
}

// Proves the same per-kind lookup (ADR 004) resolves feature_build's own
// configured image (FEATURE_BUILD_IMAGE) independently of spec_grill's —
// ADR 010 item 4 widens runInCluster's routing to key off this same map for
// both kinds, so a config with only one of the two images set must not
// accidentally resolve the other kind's image.
func TestResolveAgentImage_UsesConfiguredImagePerKindIndependently(t *testing.T) {
	cfg := Config{
		Images: map[queue.JobKind]string{
			queue.KindSpecGrill:    "registry.example.com/yggdrasil-agent-spec-grill:v1",
			queue.KindFeatureBuild: "registry.example.com/yggdrasil-agent-feature-build:v1",
		},
		PlaceholderImage:  "busybox:1.36",
		PlaceholderScript: "echo job $JOB_ID kind=$JOB_KIND; exit 0",
	}

	image, command := resolveAgentImage(cfg, queue.KindFeatureBuild)
	if image != "registry.example.com/yggdrasil-agent-feature-build:v1" {
		t.Fatalf("expected the configured feature_build image, got %q", image)
	}
	if command != nil {
		t.Fatalf("expected no command override for a real agent-images image, got %v", command)
	}
}

// Proves a job kind absent from cfg.Images falls back to the placeholder
// dev stand-in and its shell-script Command, rather than failing or running
// with an empty image.
func TestResolveAgentImage_FallsBackToPlaceholderWhenKindNotConfigured(t *testing.T) {
	cfg := Config{
		Images:            map[queue.JobKind]string{queue.KindSpecGrill: "registry.example.com/spec-grill:v1"},
		PlaceholderImage:  "busybox:1.36",
		PlaceholderScript: "echo job $JOB_ID kind=$JOB_KIND; exit 0",
	}

	image, command := resolveAgentImage(cfg, queue.KindFeatureBuild)
	if image != "busybox:1.36" {
		t.Fatalf("expected fallback to the placeholder image, got %q", image)
	}
	if len(command) != 3 || command[0] != "sh" || command[1] != "-c" {
		t.Fatalf("expected the placeholder shell command, got %v", command)
	}
}

// Proves only the three model config keys (ADR 004) are ever copied into a
// job pod's env — an unrelated project secret must not leak into the
// container just because it happened to be in project_secrets.
func TestFilterModelEnv_OnlyCopiesModelKeys(t *testing.T) {
	secrets := map[string]string{
		"MODEL_BASE_URL": "https://api.example.com/v1",
		"MODEL_API_KEY":  "sk-test",
		"MODEL_ID":       "gpt-test",
		"DATABASE_URL":   "postgres://should-not-leak",
	}

	env := filterModelEnv(secrets)

	if len(env) != 3 {
		t.Fatalf("expected exactly 3 keys, got %v", env)
	}
	if env["MODEL_BASE_URL"] != "https://api.example.com/v1" || env["MODEL_API_KEY"] != "sk-test" || env["MODEL_ID"] != "gpt-test" {
		t.Fatalf("expected model keys to be copied verbatim, got %v", env)
	}
	if _, leaked := env["DATABASE_URL"]; leaked {
		t.Fatal("expected DATABASE_URL not to be copied into job-pod env")
	}
}

// Proves a project with no model config set (e.g. hasn't configured it yet)
// doesn't error or inject empty-string env vars — job pods should be able
// to fall back to Pi's own default model resolution.
func TestFilterModelEnv_MissingKeysAreOmittedNotEmptyString(t *testing.T) {
	env := filterModelEnv(map[string]string{})
	if len(env) != 0 {
		t.Fatalf("expected no keys when the project has no model config, got %v", env)
	}
}

// Proves the concurrency limiter (ADR 006 item 1) actually caps how many
// slots can be held at once, rather than just being a no-op wrapper.
func TestLimiter_TryAcquireRespectsCapacity(t *testing.T) {
	l := newLimiter(2)

	if !l.tryAcquire() {
		t.Fatal("expected the first acquire (of 2) to succeed")
	}
	if !l.tryAcquire() {
		t.Fatal("expected the second acquire (of 2) to succeed")
	}
	if l.tryAcquire() {
		t.Fatal("expected a third acquire to fail once the limiter is saturated")
	}

	l.release()
	if !l.tryAcquire() {
		t.Fatal("expected an acquire to succeed again after a release freed a slot")
	}
}

// Proves a non-positive capacity doesn't produce a limiter that blocks
// every acquire forever (a nil/zero-buffer channel would) — it should fall
// back to a capacity of 1, matching Run's "at least sequential" behavior.
func TestLimiter_NonPositiveCapacityFallsBackToOne(t *testing.T) {
	l := newLimiter(0)
	if !l.tryAcquire() {
		t.Fatal("expected a zero-capacity limiter to still allow one acquire (falls back to 1)")
	}
	if l.tryAcquire() {
		t.Fatal("expected a second acquire to fail with the capacity-1 fallback")
	}
}

// Proves agentRepoEnv (ADR 006 item 5, widened by ADR 010 item 2) fetches
// the feature spec and shapes it into the job-pod env vars entrypoint.sh
// will eventually consume, and that it passes the job's own kind through
// to the API as the query param FetchFeatureSpec expects.
func TestAgentRepoEnv_FetchesAndBuildsTargetReposAndToken(t *testing.T) {
	var gotPath, gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Add dark mode",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
				{"cloneUrl": "https://github.com/acme/worker.git", "isPrimary": false},
			},
			"githubToken": "ghs_test-token",
		})
	}))
	defer server.Close()

	featureID := "feat-456"
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindSpecGrill, FeatureID: &featureID}
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	env, spec, err := agentRepoEnv(context.Background(), cfg, job)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotPath != "/internal/projects/proj-123/features/feat-456/spec" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/features/feat-456/spec", gotPath)
	}
	if gotQuery != "jobId=job-1&kind=spec_grill" {
		t.Fatalf("expected query %q, got %q", "jobId=job-1&kind=spec_grill", gotQuery)
	}
	if env["GITHUB_TOKEN"] != "ghs_test-token" {
		t.Fatalf("expected GITHUB_TOKEN %q, got %q", "ghs_test-token", env["GITHUB_TOKEN"])
	}
	if spec.Title != "Add dark mode" {
		t.Fatalf("expected the fetched spec's title to be returned too, got %q", spec.Title)
	}
	if _, ok := env["ADR_MARKDOWN"]; ok {
		t.Fatalf("expected no ADR_MARKDOWN for a spec_grill job, got %v", env)
	}
	if _, ok := env["FEATURE_BRANCH"]; ok {
		t.Fatalf("expected no FEATURE_BRANCH for a spec_grill job, got %v", env)
	}

	var repos []map[string]any
	if err := json.Unmarshal([]byte(env["TARGET_REPOS"]), &repos); err != nil {
		t.Fatalf("expected TARGET_REPOS to be valid JSON, got %q: %v", env["TARGET_REPOS"], err)
	}
	if len(repos) != 2 || repos[0]["cloneUrl"] != "https://github.com/acme/web.git" || repos[0]["isPrimary"] != true {
		t.Fatalf("unexpected TARGET_REPOS contents: %v", repos)
	}
}

// Proves a feature_build job's kind reaches the API too, and that
// ADR_MARKDOWN/FEATURE_BRANCH (ADR 010 items 1-3) get set from the
// response's adrMarkdown/branch fields — the two env vars entrypoint.sh
// needs to satisfy the implement skill's documented assumptions.
func TestAgentRepoEnv_FeatureBuildIncludesAdrMarkdownAndBranch(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Add dark mode",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
			},
			"githubToken": "ghs_write-scoped-token",
			"adrMarkdown": "# Add dark mode\n\n...",
			"branch":      "yggdrasil/add-dark-mode-feat-456",
		})
	}))
	defer server.Close()

	featureID := "feat-456"
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindFeatureBuild, FeatureID: &featureID}
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	env, _, err := agentRepoEnv(context.Background(), cfg, job)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotQuery != "kind=feature_build" {
		t.Fatalf("expected query %q, got %q", "kind=feature_build", gotQuery)
	}
	if env["ADR_MARKDOWN"] != "# Add dark mode\n\n..." {
		t.Fatalf("expected ADR_MARKDOWN to be set from the response, got %q", env["ADR_MARKDOWN"])
	}
	if env["FEATURE_BRANCH"] != "yggdrasil/add-dark-mode-feat-456" {
		t.Fatalf("expected FEATURE_BRANCH to be set from the response, got %q", env["FEATURE_BRANCH"])
	}
}

func TestAgentRepoEnv_ScriptTestRunIncludesGroupAndFeatureRef(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Add dark mode",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
			},
			"githubToken": "ghs_read-scoped-token",
			"branch":      "yggdrasil/add-dark-mode-feat-456",
			"scriptName":  "unit",
		})
	}))
	defer server.Close()

	featureID := "feat-456"
	testGroup := "unit"
	job := &queue.Job{
		ID: "job-1", ProjectID: "proj-123", Kind: queue.KindScriptTestRun,
		FeatureID: &featureID, TestGroup: &testGroup,
	}
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	env, spec, err := agentRepoEnv(context.Background(), cfg, job)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotQuery != "kind=script_test_run&scriptName=unit" {
		t.Fatalf("expected script query, got %q", gotQuery)
	}
	if env["FEATURE_REF"] != "yggdrasil/add-dark-mode-feat-456" {
		t.Fatalf("expected FEATURE_REF to be set, got %q", env["FEATURE_REF"])
	}
	if spec.ScriptName != "unit" {
		t.Fatalf("expected script name to be returned, got %q", spec.ScriptName)
	}
}

// Proves a spec_grill or feature_build job dispatched without a feature_id
// fails loudly instead of silently fetching nothing — this is a dispatch
// bug (ADR 002 always sets one), not a runtime condition worth falling
// back from.
func TestAgentRepoEnv_ErrorsWhenFeatureIDMissing(t *testing.T) {
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindSpecGrill, FeatureID: nil}
	cfg := Config{APIClient: apiclient.New("http://unused.invalid", "test-token")}

	_, _, err := agentRepoEnv(context.Background(), cfg, job)
	if err == nil {
		t.Fatal("expected an error when job.FeatureID is nil")
	}
}

// Proves a failed spec fetch surfaces as an error rather than silently
// running the job with an empty/missing GitHub token and repo list.
func TestAgentRepoEnv_SurfacesFetchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	featureID := "feat-456"
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindSpecGrill, FeatureID: &featureID}
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	_, _, err := agentRepoEnv(context.Background(), cfg, job)
	if err == nil {
		t.Fatal("expected an error when the API returns a non-200 response")
	}
}

// Proves runAgentJob actually merges agentRepoEnv's output into the
// created pod's env for a spec_grill job — not just that agentRepoEnv
// computes the right map in isolation.
func TestRunAgentJob_SpecGrillIncludesFetchedRepoAndTokenEnv(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/secrets"):
			_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{}})
		case strings.HasSuffix(r.URL.Path, "/spec"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"title": "Add dark mode",
				"repos": []map[string]any{
					{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
				},
				"githubToken": "ghs_test-token",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	projectID := "test-" + time.Now().Format("150405")
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset.Interface, projectID)
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.Interface.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	featureID := "feat-456"
	jobID := "specgrill-" + time.Now().Format("150405")
	job := &queue.Job{ID: jobID, ProjectID: projectID, Kind: queue.KindSpecGrill, FeatureID: &featureID}

	cfg := Config{
		APIClient:         apiclient.New(server.URL, "test-token"),
		PlaceholderImage:  "busybox:1.36",
		PlaceholderScript: "exit 0",
	}

	if err := runAgentJob(ctx, clientset, job, namespace, cfg); err != nil {
		t.Fatalf("runAgentJob failed: %v", err)
	}

	createdJob, err := clientset.Interface.BatchV1().Jobs(namespace).Get(ctx, "job-"+jobID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch created job: %v", err)
	}

	env := map[string]string{}
	for _, e := range createdJob.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["GITHUB_TOKEN"] != "ghs_test-token" {
		t.Fatalf("expected GITHUB_TOKEN to be injected into the pod, got %v", env)
	}
	if !strings.Contains(env["TARGET_REPOS"], "acme/web") {
		t.Fatalf("expected TARGET_REPOS to include the fetched repo, got %q", env["TARGET_REPOS"])
	}
}
