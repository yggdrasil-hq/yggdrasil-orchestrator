package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

const validTestKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
  user: {}
`

// A synthetic minimal invalid kubeconfig that RESTConfigFromKubeConfig will
// reject, proving a bad org kubeconfig surfaces as an error (and is cached),
// rather than the poll loop silently retrying forever.
const invalidTestKubeconfig = "this is not a kubeconfig"

func TestAPIClusterProvider_ResolvesAndCachesClientByOrg(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if !strings.HasSuffix(r.URL.Path, "/organization-cluster") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"organizationId": "org-1",
			"kubeconfig":     validTestKubeconfig,
		})
	}))
	defer server.Close()

	provider := NewAPIClusterProvider(apiclient.New(server.URL, "test-token"))
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindSpecGrill}

	first, err := provider.Resolve(context.Background(), job)
	if err != nil {
		t.Fatalf("expected first resolve to succeed, got: %v", err)
	}
	if first == nil || first.Interface == nil || first.Config == nil {
		t.Fatal("expected a resolved client with both Interface and Config")
	}

	second, err := provider.Resolve(context.Background(), job)
	if err != nil {
		t.Fatalf("expected second resolve to succeed, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected the API to be hit once and the client cached, got %d call(s)", calls)
	}
	if second != first {
		t.Fatal("expected the cached client to be returned on the second resolve")
	}
}

func TestAPIClusterProvider_NoClusterConfiguredIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	provider := NewAPIClusterProvider(apiclient.New(server.URL, "test-token"))
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindDeploy}

	_, err := provider.Resolve(context.Background(), job)
	if err == nil {
		t.Fatal("expected an error when the org has no cluster configured, got nil")
	}
}

func TestAPIClusterProvider_InvalidKubeconfigIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"organizationId": "org-1",
			"kubeconfig":     invalidTestKubeconfig,
		})
	}))
	defer server.Close()

	provider := NewAPIClusterProvider(apiclient.New(server.URL, "test-token"))
	job := &queue.Job{ID: "job-1", ProjectID: "proj-123", Kind: queue.KindSpecGrill}

	_, err := provider.Resolve(context.Background(), job)
	if err == nil {
		t.Fatal("expected an error when the kubeconfig is invalid, got nil")
	}
}