// ClusterProvider implementations and the production cached client resolver
// (ADR 016 item 13): dynamic, cached, per-Organization Kubernetes clients
// replace the old single static client built once at startup.
package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// apiClusterCacheEntry is one resolved org's client, plus the instant it was
// built so the cache can be refreshed when an org updates its kubeconfig.
type apiClusterCacheEntry struct {
	orgID   string
	client  *k8s.Client
	builtAt time.Time
}

// APIClusterProvider resolves a job's target Kubernetes client via the API
// (project -> organization -> stored kubeconfig), caching resolved clients
// per organization. It implements ClusterProvider.
type APIClusterProvider struct {
	api      *apiclient.Client
	mu       sync.Mutex
	byOrgID  map[string]*apiClusterCacheEntry
	byProjID map[string]string // projectID -> organizationID
	cacheTTL time.Duration
}

// NewAPIClusterProvider builds the production per-org cluster resolver
// backed by the API's internal organization-cluster endpoint.
func NewAPIClusterProvider(api *apiclient.Client) *APIClusterProvider {
	return &APIClusterProvider{
		api:      api,
		byOrgID:  map[string]*apiClusterCacheEntry{},
		byProjID: map[string]string{},
		cacheTTL: 5 * time.Minute,
	}
}

// Resolve returns the client for the job's project's organization. If the
// org has no configured cluster (no platform default exists, ADR 016 item
// 11) or the kubeconfig fails to build a client, it returns an error.
func (p *APIClusterProvider) Resolve(ctx context.Context, job *queue.Job) (*k8s.Client, error) {
	// Fast path: the project -> org mapping and that org's client are both
	// cached and still fresh.
	p.mu.Lock()
	orgID, projKnown := p.byProjID[job.ProjectID]
	if projKnown {
		if entry, ok := p.byOrgID[orgID]; ok && time.Since(entry.builtAt) < p.cacheTTL {
			client := entry.client
			p.mu.Unlock()
			return client, nil
		}
	}
	p.mu.Unlock()

	// Slow path: resolve the project's org (and its kubeconfig) from the API.
	orgID, kubeconfig, err := p.api.FetchOrganizationCluster(ctx, job.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("resolve cluster for project %s: %w", job.ProjectID, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Another goroutine may have resolved this org's client while we were
	// fetching; reuse it if it's still fresh for the same kubeconfig.
	if entry, ok := p.byOrgID[orgID]; ok && time.Since(entry.builtAt) < p.cacheTTL {
		p.byProjID[job.ProjectID] = orgID
		return entry.client, nil
	}

	client, err := k8s.NewClientFromKubeconfig([]byte(kubeconfig))
	if err != nil {
		log.Printf("worker: failed to build Kubernetes client for org %s: %v", orgID, err)
		// Purge the stale project/organization entries so the next claim
		// retries cleanly rather than reusing a poisoned projection.
		delete(p.byProjID, job.ProjectID)
		delete(p.byOrgID, orgID)
		return nil, err
	}
	p.byProjID[job.ProjectID] = orgID
	p.byOrgID[orgID] = &apiClusterCacheEntry{orgID: orgID, client: client, builtAt: time.Now()}
	return client, nil
}

// testClusterProvider returns a fixed client for every job — the injectable
// cluster resolver used by worker tests that already hand the lower-level
// funcs a concrete clientset.
type testClusterProvider struct {
	client *k8s.Client
}

func (p *testClusterProvider) Resolve(_ context.Context, _ *queue.Job) (*k8s.Client, error) {
	if p.client == nil {
		return nil, fmt.Errorf("no test cluster client configured")
	}
	return p.client, nil
}