package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Default per-project resource quota (ADR 003 §17, made configurable by
// ADR 030 §5). Used when the API cannot be reached for a project's effective
// quota — and, before ADR 030, used unconditionally.
//
// The API holds the authoritative copy of these numbers (its
// DEFAULT_RESOURCE_QUOTA in api/src/allocations/types.ts) because it is the
// surface an admin edits; this copy exists purely so a quota-read failure
// degrades to a sane limit instead of failing the job. The two must agree —
// ADR 030 §6 records why the duplication is accepted.
const (
	defaultQuotaCPU    = "4"
	defaultQuotaMemory = "8Gi"
	defaultQuotaPods   = "10"
)

// ResourceQuota is a project's namespace limits in normalized units, matching
// what the API stores (millicores and MiB). Formatting into Kubernetes
// quantity strings happens here, in the one component that actually talks to
// Kubernetes, rather than being round-tripped through the API as text.
type ResourceQuota struct {
	CPUmillicores int
	MemoryMiB     int
	Pods          int
}

// DefaultResourceQuota returns the fallback used when a project's effective
// quota could not be fetched.
func DefaultResourceQuota() ResourceQuota {
	return ResourceQuota{CPUmillicores: 4000, MemoryMiB: 8192, Pods: 10}
}

// spec renders the quota as the resource list Kubernetes admission enforces.
func (q ResourceQuota) spec() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceRequestsCPU:    resource.MustParse(fmt.Sprintf("%dm", q.CPUmillicores)),
		corev1.ResourceRequestsMemory: resource.MustParse(fmt.Sprintf("%dMi", q.MemoryMiB)),
		corev1.ResourcePods:           *resource.NewQuantity(int64(q.Pods), resource.DecimalSI),
	}
}

// ProjectNamespace computes the deterministic namespace name for a project
// (ADR 003 §5 — one namespace per project).
func ProjectNamespace(projectID string) string {
	return "proj-" + projectID
}

// EnsureProjectNamespace idempotently creates the namespace and applies the
// project's ResourceQuota, returning the namespace name.
//
// The quota is *applied*, not merely created: before ADR 030 it was written
// once and never revisited, so a namespace that already existed kept whatever
// numbers it was first given and an admin's change could never take effect.
// (The same create-only limitation is why secret.go documents needing its own
// update path.) Updating an existing quota is what makes the allocations page
// real rather than cosmetic.
func EnsureProjectNamespace(ctx context.Context, clientset kubernetes.Interface, projectID string, quota ResourceQuota) (string, error) {
	name := ProjectNamespace(projectID)

	_, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"yggdrasil.dev/project-id": projectID,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create namespace %s: %w", name, err)
	}

	desired := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "project-quota"},
		Spec:       corev1.ResourceQuotaSpec{Hard: quota.spec()},
	}

	existing, getErr := clientset.CoreV1().ResourceQuotas(name).Get(ctx, "project-quota", metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		if _, err := clientset.CoreV1().ResourceQuotas(name).Create(ctx, desired, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create resource quota in namespace %s: %w", name, err)
		}
		return name, nil
	}
	if getErr != nil {
		return "", fmt.Errorf("failed to read resource quota in namespace %s: %w", name, getErr)
	}

	// Only write when something actually differs, so the common case (no
	// change) costs one GET and no write, and so a quota's resourceVersion is
	// not churned on every single job.
	if quotaEqual(existing.Spec.Hard, quota) {
		return name, nil
	}

	// Merge rather than replace: a quota may carry hard limits this package does
	// not manage (a future chart adding storage, say), and assigning
	// `existing.Spec.Hard = quota.spec()` would silently delete them — which the
	// fake-clientset test for exactly this case caught.
	merged := corev1.ResourceList{}
	for key, value := range existing.Spec.Hard {
		merged[key] = value
	}
	for key, value := range quota.spec() {
		merged[key] = value
	}
	existing.Spec.Hard = merged
	if _, err := clientset.CoreV1().ResourceQuotas(name).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return "", fmt.Errorf("failed to update resource quota in namespace %s: %w", name, err)
	}
	return name, nil
}

// quotaEqual compares the parts of a live quota this package manages. A
// ResourceQuota may carry other keys (Kubernetes adds `used`, and a future
// chart could add more hard limits), so this compares only the three this
// package sets rather than requiring full equality — otherwise every job would
// rewrite the quota forever.
func quotaEqual(existing corev1.ResourceList, quota ResourceQuota) bool {
	want := quota.spec()
	for key, wantValue := range want {
		got, ok := existing[key]
		if !ok || got.Cmp(wantValue) != 0 {
			return false
		}
	}
	return true
}
