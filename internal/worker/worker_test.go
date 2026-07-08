package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
)

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
