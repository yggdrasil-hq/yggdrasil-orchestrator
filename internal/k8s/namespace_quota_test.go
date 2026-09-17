package k8s_test

import (
	"context"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// These run against a fake clientset rather than a real cluster, so the ADR 030
// quota path is actually verifiable in this environment — unlike the
// cluster-backed tests in this package, which need namespace-create rights.
//
// This is the behaviour ADR 030 §5 added: before it, the quota was written once
// at namespace creation and never revisited, so an admin's change could never
// take effect on an existing namespace.

const quotaName = "project-quota"

func hardQuota(clientset *fake.Clientset, namespace string, t *testing.T) corev1.ResourceList {
	t.Helper()
	quota, err := clientset.CoreV1().ResourceQuotas(namespace).Get(
		context.Background(), quotaName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("expected a resource quota in %s: %v", namespace, err)
	}
	return quota.Spec.Hard
}

func TestEnsureProjectNamespace_AppliesTheGivenQuota(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	quota := k8s.ResourceQuota{CPUmillicores: 2000, MemoryMiB: 4096, Pods: 4}

	namespace, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-1", quota)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hard := hardQuota(clientset, namespace, t)
	want := map[corev1.ResourceName]string{
		corev1.ResourceRequestsCPU:    "2",
		corev1.ResourceRequestsMemory: "4Gi",
		corev1.ResourcePods:           "4",
	}
	for key, wantValue := range want {
		got, ok := hard[key]
		if !ok {
			t.Fatalf("quota is missing %s (have %v)", key, hard)
		}
		if got.Cmp(resource.MustParse(wantValue)) != 0 {
			t.Fatalf("quota %s = %s, want %s", key, got.String(), wantValue)
		}
	}
}

func TestEnsureProjectNamespace_RendersNormalizedUnitsAsQuantities(t *testing.T) {
	// 300m CPU and 512Mi memory are the shapes an admin will actually pick;
	// they exercise the millicore and MiB formatting rather than round numbers.
	clientset := fake.NewSimpleClientset()
	quota := k8s.ResourceQuota{CPUmillicores: 300, MemoryMiB: 512, Pods: 2}

	namespace, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-2", quota)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hard := hardQuota(clientset, namespace, t)
	if got := hard[corev1.ResourceRequestsCPU]; got.String() != "300m" {
		t.Fatalf("cpu = %s, want 300m", got.String())
	}
	if got := hard[corev1.ResourceRequestsMemory]; got.String() != "512Mi" {
		t.Fatalf("memory = %s, want 512Mi", got.String())
	}
}

func TestEnsureProjectNamespace_UpdatesAnExistingQuota(t *testing.T) {
	// The load-bearing case: the namespace already exists (every job after the
	// first), and the admin has since changed the project's limits.
	clientset := fake.NewSimpleClientset()
	first := k8s.ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10}
	second := k8s.ResourceQuota{CPUmillicores: 1000, MemoryMiB: 2048, Pods: 3}

	namespace, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-3", first)
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	if _, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-3", second); err != nil {
		t.Fatalf("second apply failed: %v", err)
	}

	hard := hardQuota(clientset, namespace, t)
	if got := hard[corev1.ResourceRequestsCPU]; got.String() != "1" {
		t.Fatalf("cpu = %s, want 1 (the updated limit)", got.String())
	}
	if got := hard[corev1.ResourcePods]; got.String() != "3" {
		t.Fatalf("pods = %s, want 3 (the updated limit)", got.String())
	}
}

func TestEnsureProjectNamespace_IsIdempotentForAnUnchangedQuota(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	quota := k8s.ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10}

	namespace, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-4", quota)
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	before := hardQuota(clientset, namespace, t)["cpu"]

	// Repeated calls must be a no-op, not an endless rewrite: this runs on every
	// single job, and churning resourceVersion on each one would be noise for
	// anyone watching the cluster.
	for i := 0; i < 3; i++ {
		if _, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-4", quota); err != nil {
			t.Fatalf("repeat apply %d failed: %v", i, err)
		}
	}

	after := hardQuota(clientset, namespace, t)["cpu"]
	if before.Cmp(after) != 0 {
		t.Fatalf("cpu changed across no-op applies: %s -> %s", before.String(), after.String())
	}
}

func TestEnsureProjectNamespace_LeavesForeignQuotaKeysAlone(t *testing.T) {
	// A quota may legitimately carry limits this package does not manage, so an
	// update must not drop them — the comparison and write are scoped to the
	// three keys ADR 030 owns.
	clientset := fake.NewSimpleClientset()
	namespace, err := k8s.EnsureProjectNamespace(
		context.Background(), clientset, "proj-5",
		k8s.ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10},
	)
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}

	existing, err := clientset.CoreV1().ResourceQuotas(namespace).Get(
		context.Background(), quotaName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	// Something else manages persistent storage limits.
	existing.Spec.Hard[corev1.ResourceRequestsStorage] = resource.MustParse("10Gi")
	if _, err := clientset.CoreV1().ResourceQuotas(namespace).Update(
		context.Background(), existing, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("seeding a foreign key failed: %v", err)
	}

	if _, err := k8s.EnsureProjectNamespace(
		context.Background(), clientset, "proj-5",
		k8s.ResourceQuota{CPUmillicores: 1000, MemoryMiB: 2048, Pods: 3},
	); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	hard := hardQuota(clientset, namespace, t)
	if _, ok := hard[corev1.ResourceRequestsStorage]; !ok {
		t.Fatalf("the foreign storage limit was dropped by the update: %v", hard)
	}
	if got := hard[corev1.ResourceRequestsCPU]; got.String() != "1" {
		t.Fatalf("cpu = %s, want 1", got.String())
	}
}

func TestEnsureProjectNamespace_CreatesTheNamespaceOnceAndLabelsIt(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	quota := k8s.ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10}

	namespace, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-6", quota)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if namespace != "proj-proj-6" {
		t.Fatalf("namespace = %s, want proj-proj-6", namespace)
	}

	ns, err := clientset.CoreV1().Namespaces().Get(context.Background(), namespace, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("namespace should exist: %v", err)
	}
	if ns.Labels["yggdrasil.dev/project-id"] != "proj-6" {
		t.Fatalf("namespace is missing its project label: %v", ns.Labels)
	}

	// Second call must tolerate the existing namespace rather than erroring.
	if _, err := k8s.EnsureProjectNamespace(context.Background(), clientset, "proj-6", quota); err != nil {
		t.Fatalf("second call should be a no-op, got: %v", err)
	}
}

func TestEnsureProjectNamespace_DoesNotFailWhenTheNamespaceAlreadyExists(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-proj-7"},
	})

	if _, err := k8s.EnsureProjectNamespace(
		context.Background(), clientset, "proj-7",
		k8s.ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10},
	); err != nil {
		t.Fatalf("a pre-existing namespace should not be an error, got: %v", err)
	}

	if _, err := clientset.CoreV1().ResourceQuotas("proj-proj-7").Get(
		context.Background(), quotaName, metav1.GetOptions{},
	); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatal("expected the quota to be created in the pre-existing namespace")
		}
		t.Fatalf("unexpected error reading the quota: %v", err)
	}
}

func TestResourceQuotaDefaults(t *testing.T) {
	// These are the numbers ADR 003 §17 shipped with and ADR 030 kept as the
	// platform default, so a project with no override behaves exactly as it did
	// before caps were configurable.
	defaults := k8s.DefaultResourceQuota()
	if defaults.CPUmillicores != 4000 || defaults.MemoryMiB != 8192 || defaults.Pods != 10 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
}
