package helm_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const testReleaseName = "primary"

func TestDeploy_InstallsPlaceholderChart(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg, err := helm.NewConfiguration(mustRESTConfig(t), namespace)
	if err != nil {
		t.Fatalf("failed to build helm configuration: %v", err)
	}
	chrt, err := helm.LoadPlaceholderChart()
	if err != nil {
		t.Fatalf("failed to load placeholder chart: %v", err)
	}

	if err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected deploy to succeed, got: %v", err)
	}

	deployments, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list deployments: %v", err)
	}
	if len(deployments.Items) != 1 {
		t.Fatalf("expected 1 deployment in namespace %s, got %d", namespace, len(deployments.Items))
	}
}

func TestDeploy_Idempotent(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg, err := helm.NewConfiguration(mustRESTConfig(t), namespace)
	if err != nil {
		t.Fatalf("failed to build helm configuration: %v", err)
	}
	chrt, err := helm.LoadPlaceholderChart()
	if err != nil {
		t.Fatalf("failed to load placeholder chart: %v", err)
	}

	if err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected first deploy (install) to succeed, got: %v", err)
	}
	// A second deploy of the same release simulates a second merge to main
	// (ADR 003 §11) — this must go through Helm's upgrade path cleanly, not
	// error out or duplicate resources.
	if err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected second deploy (upgrade) to succeed, got: %v", err)
	}
}

func TestDeploy_SecretsChecksumChangeForcesRollout(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg, err := helm.NewConfiguration(mustRESTConfig(t), namespace)
	if err != nil {
		t.Fatalf("failed to build helm configuration: %v", err)
	}
	chrt, err := helm.LoadPlaceholderChart()
	if err != nil {
		t.Fatalf("failed to load placeholder chart: %v", err)
	}

	if err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, map[string]interface{}{"secretsChecksum": "v1"}); err != nil {
		t.Fatalf("expected first deploy to succeed, got: %v", err)
	}
	firstPod := onlyPodName(ctx, t, clientset, namespace)

	// A deploy triggered only by a secrets change (no chart/value diff
	// otherwise) must still roll the Pod — Kubernetes does not restart Pods
	// just because a referenced Secret's content changed.
	if err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, map[string]interface{}{"secretsChecksum": "v2"}); err != nil {
		t.Fatalf("expected second deploy to succeed, got: %v", err)
	}
	secondPod := onlyPodName(ctx, t, clientset, namespace)

	if firstPod == secondPod {
		t.Fatalf("expected a new Pod after secretsChecksum changed, got the same Pod %q both times", firstPod)
	}
}

func onlyPodName(ctx context.Context, t *testing.T, clientset kubernetes.Interface, namespace string) string {
	t.Helper()
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list pods in namespace %s: %v", namespace, err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected exactly 1 pod in namespace %s, got %d", namespace, len(pods.Items))
	}
	return pods.Items[0].Name
}
