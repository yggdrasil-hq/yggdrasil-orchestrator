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
	"k8s.io/client-go/kubernetes"
)

// testClient connects to whatever cluster KUBECONFIG (or in-cluster config)
// points at, skipping the test if none is reachable — same pattern used by
// internal/k8s and internal/helm.
func testClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	clientset, err := k8s.NewClient()
	if err != nil {
		t.Skipf("no Kubernetes config available; skipping: %v", err)
	}
	if _, err := clientset.Discovery().ServerVersion(); err != nil {
		t.Skipf("Kubernetes cluster unreachable; skipping: %v", err)
	}
	return clientset
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
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, projectID)
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
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
