package helm_test

import (
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// testClient connects to whatever cluster KUBECONFIG (or in-cluster config)
// points at, skipping the test if none is reachable — same pattern used by
// internal/k8s and internal/queue.
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

// mustRESTConfig resolves the ambient REST config for a helm test that needs
// one, failing (not skipping) if it can't — a caller of mustRESTConfig has
// already passed testClient, so a config should always be resolvable here.
func mustRESTConfig(t *testing.T) *rest.Config {
	t.Helper()
	restConfig, err := k8s.RESTConfig()
	if err != nil {
		t.Fatalf("failed to resolve REST config: %v", err)
	}
	return restConfig
}

func testProjectID(t *testing.T) string {
	t.Helper()
	return "test-" + rand.String(8)
}
