package k8s

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	// Agent RPC jobs (spec_grill/feature_build, ADR 006/010) run a real Pi
	// coding-agent session — cloning repos, installing dependencies,
	// running builds/tests — not the tiny placeholder script the package
	// defaults above were sized for. A run that hits defaultLimitMemory
	// (256Mi) gets silently OOM-killed with no Kubernetes Event recorded
	// (verified against a real run: the pod's own container status still
	// carries the OOMKilled reason/exit code, which is why
	// PodContainerTerminationReason reads it directly instead of relying on
	// the event stream), surfacing to the Orchestrator only as an attach
	// stream that "ended unexpectedly" with no further detail.
	agentRequestCPU    = "300m"
	agentRequestMemory = "512Mi"
	agentLimitCPU      = "2"
	agentLimitMemory   = "2Gi"
)

// AgentResources returns the resource requests/limits agent RPC jobs
// (spec_grill/feature_build) should run with — see the agent* constants'
// doc comment for why these are much higher than buildJob's own default.
func AgentResources() *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(agentRequestCPU),
			corev1.ResourceMemory: resource.MustParse(agentRequestMemory),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(agentLimitCPU),
			corev1.ResourceMemory: resource.MustParse(agentLimitMemory),
		},
	}
}

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

	// Stdin, when true, opens the container's stdin (PodSpec.Stdin) so the
	// Orchestrator can later Attach to it and drive an interactive process —
	// e.g. Pi's JSONL RPC mode (ADR 006). Leave false for one-shot jobs whose
	// container reads no stdin (the placeholder script; `deploy` never uses
	// this path at all).
	Stdin bool
}

// buildJob constructs the Kubernetes Job object for spec. Pure and
// side-effect-free so CreateJob is the only place that talks to the API
// server.
func buildJob(spec JobSpec) *batchv1.Job {
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

	return &batchv1.Job{
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
							Stdin:     spec.Stdin,
							Resources: *resources,
						},
					},
				},
			},
		},
	}
}

// CreateJob creates spec as a Kubernetes Job and returns as soon as the API
// server accepts it — it does not wait for the pod to start or finish.
// RunJob (waits for standard Job success/failure) and attach-driven callers
// (ADR 006, which decide completion from an RPC event stream instead of Job
// status) both build on this.
func CreateJob(ctx context.Context, clientset kubernetes.Interface, spec JobSpec) error {
	job := buildJob(spec)
	if _, err := clientset.BatchV1().Jobs(spec.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create job %s/%s: %w", spec.Namespace, spec.Name, err)
	}
	return nil
}

// RunJob creates spec as a Kubernetes Job and blocks until it succeeds,
// fails, or ctx is cancelled. Retries are intentionally left to the
// Orchestrator's own Postgres-backed queue (ADR 003) rather than Kubernetes's
// Job backoff, so BackoffLimit is always 0 — letting both layers retry would
// double up retry/attempt accounting.
//
// Not used for attach-driven jobs (ADR 006): Pi's RPC-mode process never
// exits on its own, so Job.Status never reaches Succeeded and this would
// block forever. Those callers use CreateJob + WaitForJobPod + Attach, and
// tear the Job down explicitly via DeleteJob once the RPC event stream
// signals completion.
func RunJob(ctx context.Context, clientset kubernetes.Interface, spec JobSpec) error {
	if err := CreateJob(ctx, clientset, spec); err != nil {
		return err
	}
	return waitForCompletion(ctx, clientset, spec.Namespace, spec.Name)
}

// WaitForJobPod blocks until a Job's pod exists and is running, returning
// its name. A Job's pod name isn't known until Kubernetes schedules it
// (unlike a Deployment's fixed name), so Attach needs this first.
func WaitForJobPod(ctx context.Context, clientset kubernetes.Interface, namespace, jobName string) (string, error) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "job-name=" + jobName,
			})
			if err != nil {
				return "", fmt.Errorf("failed to list pods for job %s/%s: %w", namespace, jobName, err)
			}
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case corev1.PodRunning:
					return pod.Name, nil
				case corev1.PodFailed:
					return "", fmt.Errorf("pod %s/%s failed before becoming attachable", namespace, pod.Name)
				}
			}
		}
	}
}

// DeleteJob deletes a Job (and, via Foreground propagation, its pod).
// Attach-driven job kinds (ADR 006) decide completion from the RPC event
// stream rather than Kubernetes Job status, so nothing else ever deletes
// the Job for them — this is that explicit teardown call. Deleting an
// already-gone Job is not an error.
func DeleteJob(ctx context.Context, clientset kubernetes.Interface, namespace, name string) error {
	propagation := metav1.DeletePropagationForeground
	err := clientset.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete job %s/%s: %w", namespace, name, err)
	}
	return nil
}

// PodContainerTerminationReason best-effort describes why the pod's "run"
// container (buildJob's fixed container name) most recently exited — e.g.
// "container exited with code 137 (OOMKilled)" — or "" if the pod is
// already gone or nothing terminated has been recorded yet. Meant to be
// called right after an attach stream ends with no error and no
// turn-ending event: remotecommand reports that the same way whether the
// container exited cleanly, crashed, or was OOM-killed, so the attach
// error itself carries no detail — but the pod object (not yet torn down
// by the caller's own deferred DeleteJob) still does, until Kubernetes GCs
// it.
func PodContainerTerminationReason(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) string {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "run" || cs.State.Terminated == nil {
			continue
		}
		t := cs.State.Terminated
		reason := t.Reason
		if reason == "" {
			reason = "unknown reason"
		}
		desc := fmt.Sprintf("container exited with code %d (%s)", t.ExitCode, reason)
		if t.Message != "" {
			desc += ": " + t.Message
		}
		return desc
	}
	return ""
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
