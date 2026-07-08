package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureProjectIngress_CreatesAndUpdates(t *testing.T) {
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

	err = k8s.EnsureProjectIngress(
		ctx, clientset, namespace,
		"acme.apps.yggdrasil.local", "primary", 80,
		"traefik", "primary-tls", "selfsigned-issuer",
	)
	if err != nil {
		t.Fatalf("expected create to succeed, got: %v", err)
	}

	ingress, err := clientset.NetworkingV1().Ingresses(namespace).Get(ctx, k8s.PrimaryIngressName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch created ingress: %v", err)
	}
	if len(ingress.Spec.Rules) != 1 || ingress.Spec.Rules[0].Host != "acme.apps.yggdrasil.local" {
		t.Fatalf("expected host %q, got rules: %+v", "acme.apps.yggdrasil.local", ingress.Spec.Rules)
	}

	// A slug (and therefore host) changing between deploys must update in
	// place, not error or leave the stale host.
	err = k8s.EnsureProjectIngress(
		ctx, clientset, namespace,
		"acme-renamed.apps.yggdrasil.local", "primary", 80,
		"traefik", "primary-tls", "selfsigned-issuer",
	)
	if err != nil {
		t.Fatalf("expected update to succeed, got: %v", err)
	}

	updated, err := clientset.NetworkingV1().Ingresses(namespace).Get(ctx, k8s.PrimaryIngressName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch updated ingress: %v", err)
	}
	if len(updated.Spec.Rules) != 1 || updated.Spec.Rules[0].Host != "acme-renamed.apps.yggdrasil.local" {
		t.Fatalf("expected updated host %q, got rules: %+v", "acme-renamed.apps.yggdrasil.local", updated.Spec.Rules)
	}
}
