package k8s

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const defaultPollInterval = 2 * time.Second

// Default per-container resource requests/limits, applied when a JobSpec
// doesn't specify its own. EnsureProjectNamespace's ResourceQuota requires
// every pod in the namespace to declare requests.cpu/requests.memory
// explicitly, so RunJob must always set these — a Job whose pod omits them
// is rejected by admission control and silently never starts.
const (
	defaultRequestCPU    = "100m"
	defaultRequestMemory = "128Mi"
	defaultLimitCPU      = "500m"
	defaultLimitMemory   = "256Mi"
)

// JobSpec describes a single ephemeral run to execute as a Kubernetes Job.
type JobSpec struct {
	Namespace string
	Name      string
	Image     string
	Command   []string
	Env       map[string]string

	// RuntimeClassName is left nil unless the deployment has a sandboxed
	// runtime (gVisor/Kata, ADR 003 §6) installed on its nodes — not every
	// cluster (e.g. a local k3d dev cluster) has one available.
	RuntimeClassName *string

	// Resources overrides the default request/limit if set. Leave nil to use
	// the package defaults (fine for the Phase 2 placeholder container).
	Resources *corev1.ResourceRequirements
}

// RunJob creates spec as a Kubernetes Job and blocks until it succeeds,
// fails, or ctx is cancelled. Retries are intentionally left to the
// Orchestrator's own Postgres-backed queue (ADR 003) rather than Kubernetes's
// Job backoff, so BackoffLimit is always 0 — letting both layers retry would
// double up retry/attempt accounting.
func RunJob(ctx context.Context, clientset kubernetes.Interface, spec JobSpec) error {
	var envVars []corev1.EnvVar
	for k, v := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}

	backoffLimit := int32(0)
	ttlSecondsAfterFinished := int32(300)

	resources := spec.Resources
	if resources == nil {
		resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(defaultRequestCPU),
				corev1.ResourceMemory: resource.MustParse(defaultRequestMemory),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(defaultLimitCPU),
				corev1.ResourceMemory: resource.MustParse(defaultLimitMemory),
			},
		}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					RuntimeClassName: spec.RuntimeClassName,
					Containers: []corev1.Container{
						{
							Name:      "run",
							Image:     spec.Image,
							Command:   spec.Command,
							Env:       envVars,
							Resources: *resources,
						},
					},
				},
			},
		},
	}

	if _, err := clientset.BatchV1().Jobs(spec.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create job %s/%s: %w", spec.Namespace, spec.Name, err)
	}

	return waitForCompletion(ctx, clientset, spec.Namespace, spec.Name)
}

func waitForCompletion(ctx context.Context, clientset kubernetes.Interface, namespace, name string) error {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			job, err := clientset.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get job %s/%s: %w", namespace, name, err)
			}
			if job.Status.Succeeded > 0 {
				return nil
			}
			if job.Status.Failed > 0 {
				return fmt.Errorf("job %s/%s failed", namespace, name)
			}
		}
	}
}
