package k8s

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

/*
Issue #29: a fresh install cannot pull the agent images, and the failure looks
like a coding problem rather than a setup gap.

Every agent job runs an image published by `yggdrasil-agent-images` to GHCR
(ADR 004), and at least one of those packages (`test_run`) answers only a
credentialed pull. Nothing provisioned the credential, and nothing said so: the
pod sat in `ImagePullBackOff`, and the two ways that surfaced were both
unhelpful —

- for an attach-driven kind (`spec_grill`, `feature_build`, …), `WaitForJobPod`
  never returned, because it recognised only `Running` and `Failed` and a pod
  that cannot pull stays `Pending`. The job eventually died on the session's
  context deadline, reporting "context deadline exceeded" — indistinguishable
  from a hung agent;
- for a blocking kind (`script_test_run`, the placeholder), `waitForCompletion`
  blocked the same way, because a Job whose pod never starts never reaches a
  terminal status.

This file is the "turns the failure into a message" half of the issue's options.
It does two things:

1. **Supplies the missing wiring.** Job pods were created with no
   `imagePullSecrets` at all, so an operator who created the credential in the
   namespace would *still* not have been pulled from it: a namespaced
   docker-config object is only used when a pod or its service account
   references it. `JOB_IMAGE_PULL_SECRET` now lands in the pod spec — see
   `JobSpec.ImagePullSecret`.
2. **Reports the failure it can prove.** `ImagePullFailure` reads the kubelet's
   own waiting reason off the pod — the authoritative evidence — and both wait
   paths now fail fast on it with the same actionable message instead of
   blocking until a deadline.

The pre-emptive half (asking the registry, before the pod exists, whether the
image needs a credential at all) lives in `registry.go`, because it needs a
different kind of evidence and a different failure mode.
*/

// RunContainerName is the name buildJob gives the single container in every job
// pod. Exported because three parts of the system have to find that container by
// name — buildJob when it creates it, the pull-failure inspection, and (in the
// worker package) Attach, which streams the Pi session into it — and a second
// spelling of "run" is how those would drift apart.
const RunContainerName = "run"

// defaultServiceAccount is the service account every job pod runs as: buildJob
// sets no ServiceAccountName, so Kubernetes assigns `default`. A service
// account's imagePullSecrets apply to pods that use it, which is the second (and
// more convenient) way an operator can provision the credential — and because
// this Orchestrator never picks another account, "does the default service
// account reference one" is an exact question, not an approximation.
const defaultServiceAccount = "default"

// SetupError is a failure the *operator* has to fix before a job can run at all,
// as opposed to a failure in the code the job is building or testing.
//
// A distinct type rather than a formatted string, because the distinction is the
// whole point of issue #29: a job whose pod cannot pull its own image must not
// read as "the agent tried and failed", and the worker can only say so if the
// error says so. `Error()` renders all three parts, because `jobs.last_error`
// and the Orchestrator's log are the two places a human meets this and neither
// can be relied on to show more than one line.
type SetupError struct {
	// Summary is the one-line statement of what is wrong.
	Summary string
	// Detail is the observed evidence: the kubelet's reason and message, or the
	// registry's own answer.
	Detail string
	// Remedy is what the operator should do. Never empty — an error a reader
	// cannot act on is the failure mode this type exists to prevent.
	Remedy string
}

func (e *SetupError) Error() string {
	return fmt.Sprintf("setup error: %s. %s. %s", e.Summary, e.Detail, e.Remedy)
}

// imagePullWaitingReasons are the container waiting reasons that mean the image
// itself never arrived. Keyed on Kubernetes' own kubelet reasons rather than a
// substring match on the message, so an unrelated message that happens to
// mention a registry cannot be misclassified.
var imagePullWaitingReasons = map[string]string{
	// The kubelet is backing off after repeated pull failures. This is the one
	// a fresh install actually sits in.
	"ImagePullBackOff": "the image could not be pulled",
	// A single pull attempt failed and has not backed off yet. This is what the
	// dev cluster reports for the `test_run` package, which needs a credential.
	"ErrImagePull": "the image could not be pulled",
	// The reference itself is malformed (a typo'd *_IMAGE env var, most
	// likely), so no pull was even attempted.
	"InvalidImageName": "the image reference is not valid",
	// The pull succeeded but the image could not be read: a corrupt or
	// incompatible manifest.
	"ImageInspectError": "the image could not be inspected after pulling",
}

// ImagePullFailure reports the setup error behind a pod that cannot pull its
// container image, or nil when the pod shows no such failure.
//
// Reads `Status.ContainerStatuses` (which needs the pod to have been scheduled)
// and falls back to the pod-level `Status.Reason`, because a pod whose first
// pull has not yet produced a container status carries it there instead — and
// that is a state a fresh install passes through.
func ImagePullFailure(pod *corev1.Pod) *SetupError {
	image := containerImage(pod, RunContainerName)

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != RunContainerName || status.State.Waiting == nil {
			continue
		}
		describe, ok := imagePullWaitingReasons[status.State.Waiting.Reason]
		if !ok {
			continue
		}
		if image == "" {
			image = status.Image
		}
		return pullSetupError(image, describe, status.State.Waiting.Reason, status.State.Waiting.Message)
	}

	if describe, ok := imagePullWaitingReasons[pod.Status.Reason]; ok {
		return pullSetupError(image, describe, pod.Status.Reason, pod.Status.Message)
	}
	return nil
}

// ImagePullFailureInPods returns the first pull failure among a job's pods, or
// nil. A Job can briefly have more than one pod (a failed attempt replaced by a
// retry), so "any pod shows the failure" is the right question for a caller that
// only needs to know whether to stop waiting.
func ImagePullFailureInPods(pods []corev1.Pod) *SetupError {
	for i := range pods {
		if failure := ImagePullFailure(&pods[i]); failure != nil {
			return failure
		}
	}
	return nil
}

// pullSetupError builds the finished error for an observed pull failure. The
// remedy names both provisioning routes, because which one an operator uses
// depends on whether they want the credential to apply to this install's job
// pods alone or to everything in the namespace — and the second route needs no
// Orchestrator configuration at all. It also names making the package public,
// which is the other legitimate answer for an install that would rather not
// manage a credential.
func pullSetupError(image, describe, reason, message string) *SetupError {
	if image == "" {
		image = "an unnamed image"
	}
	detail := fmt.Sprintf("%s: %s", reason, describe)
	if message != "" {
		detail += ". The cluster said: " + truncateForMessage(message, 300)
	}

	return &SetupError{
		Summary: fmt.Sprintf("the agent image %q could not be pulled, so this job never started", image),
		Detail:  detail,
		Remedy: "create a registry credential for the target cluster and reference it — " +
			"`kubectl -n <project namespace> create secret docker-registry <name> " +
			"--docker-server=ghcr.io --docker-username=<github user> --docker-password=<token with read:packages>`, " +
			"then either set JOB_IMAGE_PULL_SECRET=<name> on the Orchestrator or attach it to that namespace's " +
			"`default` service account. Making the package public is the other way to fix it. " +
			"See orchestrator/docs/overview/setup.md. " +
			"This is a deployment configuration gap, not a problem with the project's code.",
	}
}

// imageRegistryHost extracts the registry host from an image reference, or ""
// when there is none (a bare `name:tag`, which Docker resolves to docker.io).
//
// Follows the rule Docker and Kubernetes both use: the first path segment is the
// registry only if it contains a `.` or a `:` port, or is exactly `localhost`.
// Without that rule `yggdrasil-agent-images/spec_grill:latest` would look like it
// named a registry called `yggdrasil-agent-images`.
func imageRegistryHost(image string) string {
	base := image
	// A digest (`@sha256:…`) is not part of the repository path.
	if at := strings.IndexByte(base, '@'); at >= 0 {
		base = base[:at]
	}
	// Strip the tag from the *last* path segment only — an earlier segment may
	// legitimately contain a colon, which is a registry port.
	lastSlash := strings.LastIndexByte(base, '/')
	if lastColon := strings.LastIndexByte(base, ':'); lastColon > lastSlash {
		base = base[:lastColon]
	}

	if firstSlash := strings.IndexByte(base, '/'); firstSlash >= 0 {
		first := base[:firstSlash]
		if first == "localhost" || strings.ContainsAny(first, ".:") {
			return first
		}
	}
	return ""
}

// serviceAccountHasPullSecret reports whether the namespace's default service
// account already carries a registry credential reference.
//
// A *missing* service account is reported as "no reference" — the read
// succeeding is not what makes the account usable, and a namespace without one
// has nothing for a pod to inherit. Any other read error is reported as "has
// one": this only ever adds a warning, and a permissions gap or a transient API
// error must not produce a misleading one. Nothing depends on the answer either
// way, because the evidence-based `ImagePullFailure` path is what fails a job.
func serviceAccountHasPullSecret(ctx context.Context, clientset kubernetes.Interface, namespace string) bool {
	sa, err := clientset.CoreV1().ServiceAccounts(namespace).Get(ctx, defaultServiceAccount, metav1.GetOptions{})
	if err != nil {
		return !apierrors.IsNotFound(err)
	}
	return len(sa.ImagePullSecrets) > 0
}

// containerImage returns the image reference of a named container in a pod, or
// "" when the pod does not declare that container.
func containerImage(pod *corev1.Pod, name string) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return container.Image
		}
	}
	return ""
}

// truncateForMessage bounds a message taken from the cluster before it goes into
// an error that lands in `jobs.last_error`. Kubelet messages are normally short;
// this is here so an unexpectedly long one cannot push the remedy — the part a
// human actually needs — out of view in a UI that truncates.
func truncateForMessage(message string, limit int) string {
	message = strings.TrimSpace(message)
	if len(message) <= limit {
		return message
	}
	return message[:limit] + "…"
}
