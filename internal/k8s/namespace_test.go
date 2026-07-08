package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureProjectNamespace_Idempotent(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	projectID := testProjectID(t)

	ns1, err := k8s.EnsureProjectNamespace(ctx, clientset, projectID)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), ns1, metav1.DeleteOptions{})
	})

	ns2, err := k8s.EnsureProjectNamespace(ctx, clientset, projectID)
	if err != nil {
		t.Fatalf("second call (should be a no-op) failed: %v", err)
	}
	if ns1 != ns2 {
		t.Fatalf("expected the same namespace both times, got %s and %s", ns1, ns2)
	}

	if _, err := clientset.CoreV1().ResourceQuotas(ns1).Get(ctx, "project-quota", metav1.GetOptions{}); err != nil {
		t.Fatalf("expected a resource quota to exist in %s: %v", ns1, err)
	}
}
