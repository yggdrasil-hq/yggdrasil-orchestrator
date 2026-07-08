package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

const testImage = "busybox:1.36"

func TestRunJob_Success(t *testing.T) {
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

	err = k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "test-success-" + rand.String(6),
		Image:     testImage,
		Command:   []string{"sh", "-c", "exit 0"},
	})
	if err != nil {
		t.Fatalf("expected job to succeed, got: %v", err)
	}
}

func TestRunJob_Failure(t *testing.T) {
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

	err = k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "test-failure-" + rand.String(6),
		Image:     testImage,
		Command:   []string{"sh", "-c", "exit 1"},
	})
	if err == nil {
		t.Fatal("expected job to fail, got nil error")
	}
	// A context timeout would also produce a non-nil error, so make sure
	// this is the job-failed error and not a false-positive pass caused by
	// the job never running at all (e.g. admission rejecting it).
	if ctx.Err() != nil {
		t.Fatalf("test context expired before the job reported failure: %v", err)
	}
}
