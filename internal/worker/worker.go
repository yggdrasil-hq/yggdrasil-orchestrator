// Package worker runs the Orchestrator's job-claim poll loop.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"helm.sh/helm/v3/pkg/chart"
	"k8s.io/client-go/kubernetes"
)

// primaryReleaseName is constant within a project's own namespace — no
// collision risk since each project already gets its own namespace
// (ADR 003 §5), and each project has exactly one always-on primary
// deployment (ADR 003 §9).
const primaryReleaseName = "primary"

const (
	defaultPollInterval     = 2 * time.Second
	defaultPlaceholderImage = "busybox:1.36"
	// The Phase 2 (ADR 003) stand-in for the Pi agent — pi.dev's RPC/SDK
	// surface is still an open TODO (docs/concepts/pi-agent.md), so this
	// placeholder container just proves the "claim -> run in k8s -> report
	// result" mechanics work, independently of Pi.
	defaultPlaceholderScript = "echo job $JOB_ID kind=$JOB_KIND; exit 0"
)

// Config configures a worker's poll loop.
type Config struct {
	WorkerID     string
	PollInterval time.Duration

	// PlaceholderImage/PlaceholderScript stand in for a real Pi agent run
	// until pi.dev's RPC surface is defined (see docs/concepts/pi-agent.md).
	PlaceholderImage  string
	PlaceholderScript string

	// RuntimeClassName is left nil unless the target cluster has a sandboxed
	// runtime (gVisor/Kata, ADR 003 §6) installed — not available on every
	// cluster (e.g. a local k3d dev cluster).
	RuntimeClassName *string

	// APIClient fetches decrypted project secrets at deploy time (ADR 003
	// §16). Required for `deploy` jobs.
	APIClient *apiclient.Client
}

// Run polls the queue on an interval, claiming at most one job per tick, and
// blocks until ctx is cancelled.
func Run(ctx context.Context, q *queue.Queue, clientset kubernetes.Interface, cfg Config) {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if cfg.PlaceholderImage == "" {
		cfg.PlaceholderImage = defaultPlaceholderImage
	}
	if cfg.PlaceholderScript == "" {
		cfg.PlaceholderScript = defaultPlaceholderScript
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processOne(ctx, q, clientset, cfg)
		}
	}
}

func processOne(ctx context.Context, q *queue.Queue, clientset kubernetes.Interface, cfg Config) {
	job, err := q.Claim(ctx, cfg.WorkerID)
	if err != nil {
		log.Printf("worker %s: claim failed: %v", cfg.WorkerID, err)
		return
	}
	if job == nil {
		return
	}

	log.Printf("worker %s: claimed job %s (kind=%s project=%s)", cfg.WorkerID, job.ID, job.Kind, job.ProjectID)

	if err := runInCluster(ctx, clientset, job, cfg); err != nil {
		log.Printf("worker %s: job %s failed: %v", cfg.WorkerID, job.ID, err)
		if failErr := q.Fail(ctx, job.ID, err); failErr != nil {
			log.Printf("worker %s: failed to record failure for job %s: %v", cfg.WorkerID, job.ID, failErr)
		}
		return
	}

	log.Printf("worker %s: job %s completed", cfg.WorkerID, job.ID)
	if err := q.Complete(ctx, job.ID); err != nil {
		log.Printf("worker %s: failed to complete job %s: %v", cfg.WorkerID, job.ID, err)
	}
}

// runInCluster executes a claimed job in the project's namespace (ADR 003):
// `deploy` jobs apply the project's Helm chart to update its always-on
// primary deployment (ADR 003 §9-13); every other kind runs as a real
// Kubernetes Job via the placeholder-Pi-agent path (Phase 2).
func runInCluster(ctx context.Context, clientset kubernetes.Interface, job *queue.Job, cfg Config) error {
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, job.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to provision namespace: %w", err)
	}

	if job.Kind == queue.KindDeploy {
		return runDeploy(ctx, clientset, job.ProjectID, namespace, cfg)
	}

	return k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace: namespace,
		Name:      "job-" + job.ID,
		Image:     cfg.PlaceholderImage,
		Command:   []string{"sh", "-c", cfg.PlaceholderScript},
		Env: map[string]string{
			"JOB_ID":     job.ID,
			"JOB_KIND":   string(job.Kind),
			"PROJECT_ID": job.ProjectID,
		},
		RuntimeClassName: cfg.RuntimeClassName,
	})
}

// runDeploy applies the project's Helm chart to its namespace via
// `helm upgrade --install` (ADR 003 §13). It fetches the project's real
// chart scaffolded into its primary repo (Phase 3c); if none is scaffolded
// yet (old project, or scaffold failed), it falls back to the Orchestrator's
// embedded placeholder chart so deploys never hard-fail on a missing chart.
// Project secrets (ADR 003 §16) are fetched from the API and pushed into a
// dedicated Kubernetes Secret before the chart is applied, so the
// Deployment's envFrom reference resolves on first rollout.
func runDeploy(ctx context.Context, clientset kubernetes.Interface, projectID, namespace string, cfg Config) error {
	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	if err := k8s.EnsureProjectSecret(ctx, clientset, namespace, secrets); err != nil {
		return fmt.Errorf("failed to push project secrets: %w", err)
	}

	chrt, err := resolveChart(ctx, cfg, projectID)
	if err != nil {
		return err
	}

	helmCfg, err := helm.NewConfiguration(namespace)
	if err != nil {
		return fmt.Errorf("failed to initialize helm: %w", err)
	}
	values := map[string]interface{}{"secretsChecksum": secretsChecksum(secrets)}
	return helm.Deploy(ctx, helmCfg, namespace, primaryReleaseName, chrt, values)
}

// resolveChart fetches the project's scaffolded chart (Phase 3c), falling
// back to the embedded placeholder if the project has none yet.
func resolveChart(ctx context.Context, cfg Config, projectID string) (*chart.Chart, error) {
	files, found, err := cfg.APIClient.FetchProjectChart(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch project chart: %w", err)
	}
	if !found {
		log.Printf("project %s has no scaffolded chart yet; falling back to placeholder", projectID)
		return helm.LoadPlaceholderChart()
	}

	byteFiles := make(map[string][]byte, len(files))
	for path, content := range files {
		byteFiles[path] = []byte(content)
	}
	chrt, err := helm.LoadChartFromFiles(byteFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to load project %s's scaffolded chart: %w", projectID, err)
	}
	return chrt, nil
}

// secretsChecksum hashes a project's secrets deterministically (sorted
// keys) so the chart's pod template annotation changes whenever secret
// *content* changes, forcing a rollout even when nothing else did.
func secretsChecksum(secrets map[string]string) string {
	keys := make([]string, 0, len(secrets))
	for k := range secrets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(secrets[k]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
