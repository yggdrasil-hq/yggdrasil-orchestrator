package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

// Proves the full CreateJob -> WaitForJobPod -> Attach -> rpc.Client
// pipeline (ADR 006 items 1-4) actually round-trips a JSONL line through a
// real attached pod — without needing a real Pi image. `cat` echoes
// whatever it reads on stdin straight back to stdout, so a line sent via
// rpc.Client.Send must come back out as a parsed rpc.Event.
func TestAttach_RoundTripsJSONLThroughRealPod(t *testing.T) {
	clientset := testClient(t)
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Skipf("no Kubernetes REST config available; skipping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	jobName := "test-attach-" + rand.String(6)
	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      jobName,
		Image:     testImage,
		Command:   []string{"cat"},
		Stdin:     true,
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	t.Cleanup(func() {
		_ = k8s.DeleteJob(context.Background(), clientset, namespace, jobName)
	})

	podName, err := k8s.WaitForJobPod(ctx, clientset, namespace, jobName)
	if err != nil {
		t.Fatalf("pod never became attachable: %v", err)
	}

	rpcClient := rpc.NewClient()
	stdin, err := rpcClient.BeginTurn()
	if err != nil {
		t.Fatalf("failed to begin turn: %v", err)
	}
	attachErr := make(chan error, 1)
	go func() {
		attachErr <- k8s.Attach(ctx, clientset, restConfig, namespace, podName, "run", stdin, rpcClient, rpcClient)
	}()

	if err := rpcClient.Send(rpc.Command{Type: "ping", Message: "hello"}); err != nil {
		t.Fatalf("failed to send command: %v", err)
	}

	select {
	case ev := <-rpcClient.Events():
		if ev.Type != "ping" {
			t.Fatalf("expected the echoed event type to be %q, got %q (raw: %s)", "ping", ev.Type, ev.Raw)
		}
	case err := <-attachErr:
		t.Fatalf("attach stream ended before echoing anything back: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the echoed event")
	}

	if err := k8s.DeleteJob(ctx, clientset, namespace, jobName); err != nil {
		t.Fatalf("failed to delete job: %v", err)
	}
	rpcClient.Close()
}

// Proves WaitForJobPod doesn't hang forever (nor panic) when a job's pod
// never appears at all, e.g. an invalid image reference that Kubernetes
// can never schedule/pull — the context deadline should surface as a plain
// error instead.
func TestWaitForJobPod_TimesOutOnUnschedulablePod(t *testing.T) {
	clientset := testClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, testProjectID(t))
	if err != nil {
		t.Fatalf("failed to provision namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = clientset.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	jobName := "test-attach-timeout-" + rand.String(6)
	if err := k8s.CreateJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      jobName,
		Image:     "yggdrasil-test/does-not-exist:invalid",
		Command:   []string{"cat"},
		Stdin:     true,
	}); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	t.Cleanup(func() {
		_ = k8s.DeleteJob(context.Background(), clientset, namespace, jobName)
	})

	shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shortCancel()

	if _, err := k8s.WaitForJobPod(shortCtx, clientset, namespace, jobName); err == nil {
		t.Fatal("expected an error waiting for a pod that can never become attachable")
	}
}
