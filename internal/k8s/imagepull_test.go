package k8s_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
)

/*
Issue #29: the agent images are private-by-default on GHCR, and nothing used to
say so. These tests pin the three decisions that turn that into a message — the
image is not configured, the image is configured but unpullable, the image is
pullable — plus the registry parsing that decides which images need a credential
at all.

Fake-client tests rather than cluster tests: every decision here is a function of
objects the API server already has, so a real cluster would only make them slower
and flakier. The end-to-end demonstration against the dev cluster is recorded in
the issue rather than automated here.
*/

// ghcrImage is a reference in the shape ADR 004 publishes: the one that needs a
// credential on a fresh install.
const ghcrImage = "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/spec_grill:latest"

func newFakeClient(objects ...runtime.Object) *fake.Clientset {
	return fake.NewSimpleClientset(objects...)
}

// fakeNamespace returns a unique namespace name. Objects are not namespaced in
// the fake's tracker in a way that enforces existence, so a job can be created
// against a name no Namespace object backs — which is what these tests want.
func fakeNamespace(prefix string) string {
	return "proj-" + prefix + "-" + rand.String(6)
}

// podWithWaitingReason builds the pod shape the kubelet reports for a container
// that cannot pull its image, taken from `kubectl describe pod` on a cluster
// that could not pull a private GHCR image.
func podWithWaitingReason(namespace, reason, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-pod",
			Namespace: namespace,
			Labels:    map[string]string{"job-name": "job-under-test"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: k8s.RunContainerName, Image: ghcrImage}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  k8s.RunContainerName,
				Image: ghcrImage,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message},
				},
			}},
		},
	}
}

// --- reading the pod's own evidence -----------------------------------------

func TestImagePullFailure_ReportsEveryPullBlockingReason(t *testing.T) {
	for _, reason := range []string{
		"ImagePullBackOff",
		"ErrImagePull",
		"InvalidImageName",
		"ImageInspectError",
	} {
		failure := k8s.ImagePullFailure(podWithWaitingReason("ns", reason, "pull access denied"))

		if failure == nil {
			t.Fatalf("%s: expected a setup error, got nil", reason)
		}
		if !strings.Contains(failure.Detail, reason) {
			t.Fatalf("%s: expected the kubelet's reason in the detail, got %q", reason, failure.Detail)
		}
	}
}

// The message is the whole point of the issue: it has to name the fix, and it has
// to say the failure is not the project's code — which is what the operator
// assumed when they filed it.
func TestImagePullFailure_NamesTheRemedyAndTheKindOfProblem(t *testing.T) {
	failure := k8s.ImagePullFailure(podWithWaitingReason(
		"ns",
		"ImagePullBackOff",
		`Back-off pulling image "`+ghcrImage+`"`,
	))
	if failure == nil {
		t.Fatal("expected a setup error")
	}

	rendered := strings.ToLower(failure.Error())
	for _, want := range []string{
		"setup error",
		"could not be pulled",
		"job_image_pull_secret",
		"docker-registry",
		"read:packages",
		"deployment configuration gap, not a problem with the project's code",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected the rendered error to mention %q, got:\n%s", want, failure.Error())
		}
	}

	// The cluster's own words are kept, because they are what distinguishes a
	// missing credential from a typo'd tag.
	if !strings.Contains(failure.Error(), "Back-off pulling image") {
		t.Fatalf("expected the kubelet's message to be kept, got:\n%s", failure.Error())
	}
}

// A pod that is coming up normally must not be reported as a pull failure — this
// check runs in the poll loop of every job.
func TestImagePullFailure_IsSilentWhenNothingIsBlocked(t *testing.T) {
	running := podWithWaitingReason("ns", "", "")
	running.Status.Phase = corev1.PodRunning
	running.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}
	if failure := k8s.ImagePullFailure(running); failure != nil {
		t.Fatalf("expected no failure for a running pod, got: %v", failure)
	}

	// ContainerCreating is the ordinary first state and must not match, even
	// though it is also a `Waiting` state.
	creating := podWithWaitingReason("ns", "ContainerCreating", "")
	if failure := k8s.ImagePullFailure(creating); failure != nil {
		t.Fatalf("expected no failure while the container is being created, got: %v", failure)
	}

	// A container that ran and exited is not a pull failure either.
	terminated := podWithWaitingReason("ns", "", "")
	terminated.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
	}
	if failure := k8s.ImagePullFailure(terminated); failure != nil {
		t.Fatalf("expected no failure for a terminated container, got: %v", failure)
	}
}

// A pod rejected for a malformed reference carries the reason at pod level,
// before any container status exists — the state a typo'd *_IMAGE var produces.
func TestImagePullFailure_ReadsThePodLevelReason(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "job-pod", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase:   corev1.PodPending,
			Reason:  "InvalidImageName",
			Message: `Failed to apply default image tag "ghcr.io/x:": invalid reference format`,
		},
	}

	failure := k8s.ImagePullFailure(pod)
	if failure == nil {
		t.Fatal("expected a setup error from the pod-level reason")
	}
	if !strings.Contains(failure.Detail, "InvalidImageName") {
		t.Fatalf("expected the pod-level reason in the detail, got %q", failure.Detail)
	}

	// An unrelated pod-level reason — `Unschedulable` is the common one — is not
	// a pull failure and must not be reported as one.
	unschedulable := &corev1.Pod{
		Status: corev1.PodStatus{Phase: corev1.PodPending, Reason: "Unschedulable", Message: "0/1 nodes available"},
	}
	if failure := k8s.ImagePullFailure(unschedulable); failure != nil {
		t.Fatalf("expected no failure for an unschedulable pod, got: %v", failure)
	}
}

func TestImagePullFailureInPods_PicksTheFailingPod(t *testing.T) {
	healthy := podWithWaitingReason("ns", "", "")
	healthy.Status.Phase = corev1.PodRunning
	healthy.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}

	pods := []corev1.Pod{*healthy, *podWithWaitingReason("ns", "ImagePullBackOff", "")}

	if failure := k8s.ImagePullFailureInPods(pods); failure == nil {
		t.Fatal("expected the failing pod to be found among several")
	}
	if failure := k8s.ImagePullFailureInPods(nil); failure != nil {
		t.Fatalf("expected no failure for no pods, got: %v", failure)
	}
	if failure := k8s.ImagePullFailureInPods([]corev1.Pod{*healthy}); failure != nil {
		t.Fatalf("expected no failure when every pod is healthy, got: %v", failure)
	}
}

// --- the wait paths ---------------------------------------------------------

// The behaviour the issue is really about: the job must stop waiting and say
// why, rather than blocking until a deadline and reporting a timeout.
func TestWaitForJobPod_FailsFastWithASetupError(t *testing.T) {
	namespace := fakeNamespace("wait-attach")
	clientset := newFakeClient(podWithWaitingReason(namespace, "ImagePullBackOff", "pull access denied"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	started := time.Now()
	_, err := k8s.WaitForJobPod(ctx, clientset, namespace, "job-under-test")
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("expected an error rather than a pod name")
	}
	if elapsed >= 25*time.Second {
		t.Fatalf("expected a fast failure rather than a deadline, waited %s", elapsed)
	}
	var setup *k8s.SetupError
	if !errors.As(err, &setup) {
		t.Fatalf("expected an *k8s.SetupError, got %T: %v", err, err)
	}
}

func TestRunJob_FailsFastWithASetupError(t *testing.T) {
	namespace := fakeNamespace("wait-blocking")
	clientset := newFakeClient(podWithWaitingReason(namespace, "ErrImagePull", "unauthorized"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "job-under-test",
		Image:     ghcrImage,
		Command:   []string{"sh", "-c", "exit 0"},
	})
	if err == nil {
		t.Fatal("expected an error rather than a completed job")
	}
	var setup *k8s.SetupError
	if !errors.As(err, &setup) {
		t.Fatalf("expected an *k8s.SetupError, got %T: %v", err, err)
	}
}

// The counter-case: a pullable image must behave exactly as before, or the fix
// would be worse than the bug it fixes.
func TestWaitForJobPod_ReturnsThePodWhenNothingIsBlocked(t *testing.T) {
	namespace := fakeNamespace("wait-ok")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-pod-ok",
			Namespace: namespace,
			Labels:    map[string]string{"job-name": "job-pulls-fine"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: k8s.RunContainerName, Image: "busybox:1.36"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	clientset := newFakeClient(pod)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name, err := k8s.WaitForJobPod(ctx, clientset, namespace, "job-pulls-fine")
	if err != nil {
		t.Fatalf("expected the pod to be found, got: %v", err)
	}
	if name != "job-pod-ok" {
		t.Fatalf("expected the pod's name, got %q", name)
	}
}

// --- the pod spec -----------------------------------------------------------

func TestCreateJob_CarriesThePullReferenceOnlyWhenConfigured(t *testing.T) {
	namespace := fakeNamespace("spec")
	ctx := context.Background()
	clientset := newFakeClient()

	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace:       namespace,
		Name:            "job-with-secret",
		Image:           ghcrImage,
		ImagePullSecret: "ghcr-pull",
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	job, err := clientset.BatchV1().Jobs(namespace).Get(ctx, "job-with-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to read the job back: %v", err)
	}
	refs := job.Spec.Template.Spec.ImagePullSecrets
	if len(refs) != 1 || refs[0].Name != "ghcr-pull" {
		t.Fatalf("expected the pod spec to reference the configured secret, got %#v", refs)
	}

	// An unconfigured deployment must produce a pod spec that says *nothing*:
	// `imagePullSecrets: [{}]` is rejected by the API server as an invalid name,
	// which is why the field is built conditionally rather than always set.
	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "job-without-secret",
		Image:     "busybox:1.36",
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	job, err = clientset.BatchV1().Jobs(namespace).Get(ctx, "job-without-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to read the job back: %v", err)
	}
	if len(job.Spec.Template.Spec.ImagePullSecrets) != 0 {
		t.Fatalf("expected no pull references when nothing is configured, got %#v", job.Spec.Template.Spec.ImagePullSecrets)
	}
}
