package helm_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// These tests pin the revision contract ADR 022 depends on. They drive a real
// cluster because Helm's install/upgrade/rollback paths reach for a live
// Kubernetes client (existingResourceConflict calls resource.NewHelper, which
// dereferences the resource's client) — an in-memory driver plus Helm's
// kube/fake client is not enough to exercise them, so like the rest of this
// package's tests they need a reachable cluster and skip/fail accordingly.
//
// What they exist to prove is the single most counter-intuitive part of the
// design, and the part a reader is most likely to get wrong: `helm rollback`
// does NOT rewind the revision counter. Rolling back from revision 2 to
// revision 1 produces revision 3, whose content matches revision 1. That is
// why the deploy ledger records both the produced revision and the requested
// target, and why the newest ledger entry — not the target — is what the next
// rollback must target.

func TestDeploy_ReturnsProducedRevision(t *testing.T) {
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

	// First deploy installs, so Helm numbers it revision 1. If this ever
	// stopped holding, project_deploys would record a revision that does not
	// identify the deployed manifest and a rollback would target the wrong one.
	first, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil)
	if err != nil {
		t.Fatalf("expected first deploy (install) to succeed, got: %v", err)
	}
	if first != 1 {
		t.Fatalf("expected the first deploy to produce revision 1, got %d", first)
	}

	second, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil)
	if err != nil {
		t.Fatalf("expected second deploy (upgrade) to succeed, got: %v", err)
	}
	if second != 2 {
		t.Fatalf("expected the second deploy to produce revision 2, got %d", second)
	}
}

func TestRollback_CreatesANewRevisionInsteadOfRewinding(t *testing.T) {
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

	if _, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected first deploy to succeed, got: %v", err)
	}
	if _, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected second deploy to succeed, got: %v", err)
	}

	produced, err := helm.Rollback(ctx, cfg, namespace, testReleaseName, 1)
	if err != nil {
		t.Fatalf("expected rollback to revision 1 to succeed, got: %v", err)
	}
	if produced != 3 {
		t.Fatalf("expected the rollback to produce revision 3 (a new revision, not a rewind to 1), got %d", produced)
	}

	// The counter advanced rather than resetting, so the next deploy is
	// revision 4 — which is what makes "the newest ledger entry is the current
	// revision" a safe assumption for the rollback UI.
	next, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil)
	if err != nil {
		t.Fatalf("expected deploy after rollback to succeed, got: %v", err)
	}
	if next != 4 {
		t.Fatalf("expected the deploy after a rollback to produce revision 4, got %d", next)
	}
}

// TestRollback_RejectsAnUnknownRevision documents that the Orchestrator needs
// no validation of its own: Helm refuses a revision the release never had, so
// a stale target fails the job loudly instead of silently no-opping. The API
// also rejects an unknown target before enqueueing, making this the second
// line of defence for the race where a revision is pruned between request and
// claim.
func TestRollback_RejectsAnUnknownRevision(t *testing.T) {
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

	if _, err := helm.Deploy(ctx, cfg, namespace, testReleaseName, chrt, nil); err != nil {
		t.Fatalf("expected first deploy to succeed, got: %v", err)
	}

	_, err = helm.Rollback(ctx, cfg, namespace, testReleaseName, 99)
	if err == nil {
		t.Fatal("expected rollback to an unknown revision to fail, got nil error")
	}
	// The failure is surfaced verbatim as the job's last_error in the Web app,
	// so it has to name the revision the operator asked for.
	if !strings.Contains(err.Error(), "99") {
		t.Fatalf("expected the error to name the target revision 99, got: %v", err)
	}
}
