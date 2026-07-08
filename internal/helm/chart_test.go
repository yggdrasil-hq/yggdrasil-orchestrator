package helm_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A minimal chart map shaped exactly like what apiclient.FetchProjectChart
// returns (path -> raw file content) — proving LoadChartFromFiles works for
// a real per-project chart fetched over HTTP, not just the embedded
// placeholder.
func fetchedChartFiles() map[string][]byte {
	return map[string][]byte{
		"Chart.yaml": []byte("apiVersion: v2\nname: fetched-test-chart\nversion: 0.1.0\n"),
		"values.yaml": []byte(`replicaCount: 1
image:
  repository: nginxdemos/hello
  tag: latest
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi
`),
		"templates/deployment.yaml": []byte(`apiVersion: apps/v1
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
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
`),
	}
}

func TestLoadChartFromFiles_ProducesDeployableChart(t *testing.T) {
	chrt, err := helm.LoadChartFromFiles(fetchedChartFiles())
	if err != nil {
		t.Fatalf("expected chart to load, got: %v", err)
	}
	if chrt.Metadata.Name != "fetched-test-chart" {
		t.Fatalf("expected chart name %q, got %q", "fetched-test-chart", chrt.Metadata.Name)
	}

	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg, err := helm.NewConfiguration(namespace)
	if err != nil {
		t.Fatalf("failed to build helm configuration: %v", err)
	}

	if err := helm.Deploy(ctx, cfg, namespace, "primary", chrt, nil); err != nil {
		t.Fatalf("expected deploy of the fetched-shaped chart to succeed, got: %v", err)
	}

	deployments, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list deployments: %v", err)
	}
	if len(deployments.Items) != 1 {
		t.Fatalf("expected 1 deployment in namespace %s, got %d", namespace, len(deployments.Items))
	}
}
