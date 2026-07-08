package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ProjectSecretName is the dedicated Kubernetes Secret a project's primary
// deployment references via envFrom (ADR 003 §16) — pushed directly by the
// Orchestrator, independent of Helm's own release-storage Secret.
const ProjectSecretName = "project-env"

// EnsureProjectSecret idempotently creates or updates the project-env
// Secret in namespace with the given key/value data. Unlike
// EnsureProjectNamespace, this needs a real update path (not just
// create-and-ignore-AlreadyExists) since secret values change between
// deploys.
func EnsureProjectSecret(ctx context.Context, clientset kubernetes.Interface, namespace string, data map[string]string) error {
	stringData := make(map[string]string, len(data))
	for k, v := range data {
		stringData[k] = v
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProjectSecretName,
			Namespace: namespace,
		},
		StringData: stringData,
		Type:       corev1.SecretTypeOpaque,
	}

	_, err := clientset.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create secret %s in namespace %s: %w", ProjectSecretName, namespace, err)
	}

	// Kubernetes requires the current resourceVersion for an update
	// (optimistic concurrency) — Get before Update rather than blindly
	// retrying Create's object.
	existing, err := clientset.CoreV1().Secrets(namespace).Get(ctx, ProjectSecretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch existing secret %s in namespace %s: %w", ProjectSecretName, namespace, err)
	}
	existing.StringData = stringData
	existing.Data = nil // clear stale base64 Data so StringData fully replaces the value set

	if _, err := clientset.CoreV1().Secrets(namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update secret %s in namespace %s: %w", ProjectSecretName, namespace, err)
	}
	return nil
}
