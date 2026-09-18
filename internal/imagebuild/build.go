package imagebuild

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// resultPollInterval is how often the build's outcome is checked. A build takes
// tens of seconds at least, so this is about noticing the end promptly rather
// than about throughput, and it matches the rest of this package's polling.
const resultPollInterval = 2 * time.Second

// Result is what a build produced.
type Result struct {
	// Built is false when the build was deliberately skipped — a repository with
	// no Dockerfile at the contract's path, or a ref that is not on the remote
	// yet. A skipped build is not a failure: a project that has not added a
	// Dockerfile still gets a preview (of its chart's declared image), and so does
	// one whose feature branch has not been pushed. Saying so is the honest
	// outcome rather than inventing an image reference that does not exist.
	Built bool
	// Image is the pushed reference, empty when Built is false.
	Image string
	// Reason explains a skip in the operator's terms.
	Reason string
}

// Build creates the build Job, waits for it, and reports what it produced.
//
// A build failure is returned as an error with the builder's own output attached:
// "the build failed" is not actionable, and a Dockerfile error is the single most
// likely reason a project's first preview build does not work.
func Build(ctx context.Context, cfg Config) (Result, error) {
	job := buildJob(cfg)

	// A previous attempt with the same name is removed first: the name is
	// deterministic so a retry does not pile up Jobs, but a leftover from a
	// crashed replica would otherwise make this create fail with AlreadyExists.
	if err := deleteJob(ctx, cfg.Clientset, cfg.Namespace, job.Name); err != nil {
		return Result{}, err
	}
	if _, err := cfg.Clientset.BatchV1().Jobs(cfg.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return Result{}, fmt.Errorf("failed to create the build job %s/%s: %w", cfg.Namespace, job.Name, err)
	}

	buildCtx, cancel := context.WithTimeout(ctx, cfg.timeout())
	defer cancel()

	return awaitResult(buildCtx, cfg.Clientset, cfg.Namespace, job.Name, cfg.Destination)
}

// awaitResult waits for the build Job to reach a terminal state and translates
// that state into a Result or an error.
//
// Reads the *pod's* container status rather than the Job's counters, because the
// two skip cases are exit codes (NoDockerfileExitCode, RefUnavailableExitCode)
// and a Job only reports success/failure. That is also why this does not reuse
// the k8s package's blocking RunJob, which knows nothing about exit codes. Both
// skip codes are emitted by the context container (see buildJob), and the build
// container never runs in either case.
func awaitResult(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, jobName, destination string,
) (Result, error) {
	ticker := time.NewTicker(resultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort cleanup so a timed-out build does not keep consuming a
			// node; the deployment's own Job TTL would collect it eventually.
			_ = deleteJob(context.Background(), clientset, namespace, jobName)
			return Result{}, fmt.Errorf("build %s/%s did not finish: %w", namespace, jobName, ctx.Err())
		case <-ticker.C:
			pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "job-name=" + jobName,
			})
			if err != nil {
				return Result{}, fmt.Errorf("failed to list pods for build %s/%s: %w", namespace, jobName, err)
			}

			for _, pod := range pods.Items {
				outcome, done := buildOutcome(&pod)
				if !done {
					continue
				}
				switch outcome.exitCode {
				case 0:
					return Result{Built: true, Image: destination}, nil
				case NoDockerfileExitCode:
					return Result{
						Built: false,
						Reason: "the repository has no " + DefaultDockerfilePath +
							" at its root (ADR 003 §12), so there is nothing to build",
					}, nil
				case RefUnavailableExitCode:
					// Ordinarily reached because a preview is created before the agent
					// has pushed the branch it is previewing — see the exit code's own
					// comment. Not a failure, and deliberately not silent either: the
					// reason is what tells an operator looking at a preview that shows
					// the old app whether that is expected or a broken build.
					return Result{
						Built: false,
						Reason: "the ref being previewed is not on the remote yet, so there is " +
							"nothing to build from; the preview shows the chart's declared image",
					}, nil
				default:
					return Result{}, fmt.Errorf(
						"the image build failed in %s/%s: %s", namespace, jobName, outcome.describe(),
					)
				}
			}

			// A Job whose pod was never created (a scheduling failure, a quota
			// rejection) never produces a container status, so the Job's own
			// conditions are the only signal.
			if failure := jobFailureCondition(ctx, clientset, namespace, jobName); failure != "" {
				return Result{}, fmt.Errorf("the image build could not start in %s/%s: %s", namespace, jobName, failure)
			}
		}
	}
}

// podOutcome is the terminal state of the build container.
type podOutcome struct {
	exitCode int32
	reason   string
	message  string
}

func (o podOutcome) describe() string {
	detail := fmt.Sprintf("container exited with code %d (%s)", o.exitCode, o.reason)
	if o.message != "" {
		detail += ": " + o.message
	}
	return detail
}

// buildOutcome reports whether the pod has finished, and how. The build
// container is named buildContainerName (see buildJob); a pod that failed before
// it ran is reported with the *context* container's state instead, because "the
// build context could not be prepared" is the real cause an operator needs to
// see rather than a bare build failure.
//
// Note that the two deliberate skip codes (NoDockerfileExitCode,
// RefUnavailableExitCode) also originate in that container, and both are handled
// by exit code in awaitResult — which is why reporting the context container's
// state here is not the same as reporting a failure.
func buildOutcome(pod *corev1.Pod) (podOutcome, bool) {
	if pod.Status.Phase == corev1.PodSucceeded {
		return podOutcome{exitCode: 0, reason: "Completed"}, true
	}

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != buildContainerName || status.State.Terminated == nil {
			continue
		}
		terminated := status.State.Terminated
		reason := terminated.Reason
		if reason == "" {
			reason = "unknown reason"
		}
		return podOutcome{
			exitCode: terminated.ExitCode,
			reason:   reason,
			message:  terminated.Message,
		}, true
	}

	// The context container is where a skip lands (both of its exit codes mean
	// "nothing to build" rather than a failure — see awaitResult), and also where
	// a clone failure lands, including the common case of a token that cannot read
	// the repository.
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name != prepareContainerName || status.State.Terminated == nil {
			continue
		}
		terminated := status.State.Terminated
		if terminated.ExitCode == 0 {
			continue
		}
		reason := terminated.Reason
		if reason == "" {
			reason = "unknown reason"
		}
		return podOutcome{
			exitCode: terminated.ExitCode,
			reason:   "the build context could not be prepared (" + reason + ")",
			message:  terminated.Message,
		}, true
	}

	return podOutcome{}, false
}

// jobFailureCondition reports a Job-level reason the build never ran, or "".
func jobFailureCondition(ctx context.Context, clientset kubernetes.Interface, namespace, jobName string) string {
	job, err := clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := condition.Message
			if message == "" {
				message = condition.Reason
			}
			return message
		}
	}
	return ""
}

// deleteJob removes a build Job and waits for it to be gone, because Kubernetes
// refuses to create a Job whose pod is still terminating under the same name.
func deleteJob(ctx context.Context, clientset kubernetes.Interface, namespace, name string) error {
	propagation := metav1.DeletePropagationBackground
	err := clientset.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to clear the previous build job %s/%s: %w", namespace, name, err)
	}
	return nil
}
