package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureProjectSecret_CreatesAndUpdates(t *testing.T) {
	clientset := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	if err := k8s.EnsureProjectSecret(ctx, clientset, namespace, map[string]string{"FOO": "bar"}); err != nil {
		t.Fatalf("expected create to succeed, got: %v", err)
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, k8s.ProjectSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch created secret: %v", err)
	}
	if string(secret.Data["FOO"]) != "bar" {
		t.Fatalf("expected FOO=bar, got: %v", secret.Data)
	}

	// A second call with different data simulates a project's secrets
	// changing between deploys — must update in place, not error or leave
	// the old value.
	if err := k8s.EnsureProjectSecret(ctx, clientset, namespace, map[string]string{"FOO": "baz"}); err != nil {
		t.Fatalf("expected update to succeed, got: %v", err)
	}

	updated, err := clientset.CoreV1().Secrets(namespace).Get(ctx, k8s.ProjectSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch updated secret: %v", err)
	}
	if string(updated.Data["FOO"]) != "baz" {
		t.Fatalf("expected FOO=baz after update, got: %v", updated.Data)
	}
}
