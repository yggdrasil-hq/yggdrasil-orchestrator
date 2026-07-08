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

// Default per-project resource quota (ADR 003 §17). Not yet configurable per
// project — real workloads will inform sensible limits; this is a starting
// point, not the final word.
const (
	defaultQuotaCPU    = "4"
	defaultQuotaMemory = "8Gi"
	defaultQuotaPods   = "10"
)

// ProjectNamespace computes the deterministic namespace name for a project
// (ADR 003 §5 — one namespace per project).
func ProjectNamespace(projectID string) string {
	return "proj-" + projectID
}

// EnsureProjectNamespace idempotently creates the namespace and default
// ResourceQuota for a project and returns the namespace name.
func EnsureProjectNamespace(ctx context.Context, clientset kubernetes.Interface, projectID string) (string, error) {
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

	_, err = clientset.CoreV1().ResourceQuotas(name).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "project-quota"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU:    resource.MustParse(defaultQuotaCPU),
				corev1.ResourceRequestsMemory: resource.MustParse(defaultQuotaMemory),
				corev1.ResourcePods:           resource.MustParse(defaultQuotaPods),
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create resource quota in namespace %s: %w", name, err)
	}

	return name, nil
}
