package k8s_test

import (
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
)

// testClient connects to whatever cluster KUBECONFIG (or in-cluster config)
// points at, skipping the test if none is reachable — same pattern as the
// queue package's DATABASE_URL skip.
func testClient(t *testing.T) kubernetes.Interface {
	t.Helper()

	clientset, err := k8s.NewClient()
	if err != nil {
		t.Skipf("no Kubernetes config available; skipping: %v", err)
	}
	if _, err := clientset.Discovery().ServerVersion(); err != nil {
		t.Skipf("Kubernetes cluster unreachable; skipping: %v", err)
	}
	return clientset
}

func testProjectID(t *testing.T) string {
	t.Helper()
	return "test-" + rand.String(8)
}
