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

// modelEnvKeys are the per-project model config keys a job pod needs (ADR
// 004) — stored as project_secrets rows, decrypted by FetchProjectSecrets,
// and injected as plain job-pod env vars, same delivery path as the scoped
// GitHub token. Only these three are ever copied into a job pod; the rest
// of a project's secrets (if any) are not (that's the primary deployment's
// concern, handled separately by runDeploy/EnsureProjectSecret).
var modelEnvKeys = []string{"MODEL_BASE_URL", "MODEL_API_KEY", "MODEL_ID"}

// Config configures a worker's poll loop.
type Config struct {
	WorkerID     string
	PollInterval time.Duration

	// Images maps a job kind to a real agent-images image (ADR 004:
	// yggdrasil-agent-images, resolved from SPEC_GRILL_IMAGE/
	// FEATURE_BUILD_IMAGE/TEST_RUN_IMAGE). A kind absent from this map falls
	// back to PlaceholderImage/PlaceholderScript below — lets a deployment
	// keep working before agent-images has published real images anywhere.
	Images map[queue.JobKind]string

	// PlaceholderImage/PlaceholderScript stand in for a real Pi agent image,
	// for any job kind not present in Images.
	PlaceholderImage  string
	PlaceholderScript string

	// RuntimeClassName is left nil unless the target cluster has a sandboxed
	// runtime (gVisor/Kata, ADR 003 §6) installed — not available on every
	// cluster (e.g. a local k3d dev cluster).
	RuntimeClassName *string

	// APIClient fetches decrypted project secrets at deploy time (ADR 003
	// §16). Required for `deploy` jobs.
	APIClient *apiclient.Client

	// AppsDomain/IngressClassName/CertIssuerName build the primary
	// deployment's Ingress (ADR 003 §15: <project-slug>.apps.<domain>).
	// Config values, not code, are what change between a local k3d dev
	// cluster (Traefik, self-signed cert) and a self-hosted/managed cluster
	// (ingress-nginx, a real ACME ClusterIssuer).
	AppsDomain       string
	IngressClassName string
	CertIssuerName   string
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
// primary deployment (ADR 003 §9-13); every other kind runs an agent-images
// container (ADR 004) via runAgentJob.
func runInCluster(ctx context.Context, clientset kubernetes.Interface, job *queue.Job, cfg Config) error {
	namespace, err := k8s.EnsureProjectNamespace(ctx, clientset, job.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to provision namespace: %w", err)
	}

	if job.Kind == queue.KindDeploy {
		return runDeploy(ctx, clientset, job.ProjectID, namespace, cfg)
	}

	return runAgentJob(ctx, clientset, job, namespace, cfg)
}

// runAgentJob runs a spec_grill/feature_build/test_run job as a Kubernetes
// Job. It resolves which container image to use (a real agent-images image
// per ADR 004 if configured for this job kind, else the placeholder dev
// stand-in) and injects the project's model config (ADR 004: MODEL_BASE_URL/
// MODEL_API_KEY/MODEL_ID, decrypted from project_secrets) as plain job-pod
// env vars — the same delivery path already used for the scoped GitHub
// token, not a Kubernetes Secret object.
func runAgentJob(ctx context.Context, clientset kubernetes.Interface, job *queue.Job, namespace string, cfg Config) error {
	env := map[string]string{
		"JOB_ID":     job.ID,
		"JOB_KIND":   string(job.Kind),
		"PROJECT_ID": job.ProjectID,
	}

	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, job.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	for k, v := range filterModelEnv(secrets) {
		env[k] = v
	}

	image, command := resolveAgentImage(cfg, job.Kind)

	return k8s.RunJob(ctx, clientset, k8s.JobSpec{
		Namespace:        namespace,
		Name:             "job-" + job.ID,
		Image:            image,
		Command:          command,
		Env:              env,
		RuntimeClassName: cfg.RuntimeClassName,
	})
}

// resolveAgentImage returns the image to run for a job kind and the command
// to run inside it. A kind with a configured real agent-images image (ADR
// 004, cfg.Images) runs with no command override, so the image's own
// ENTRYPOINT (agent-images' entrypoint.sh, which execs `pi --mode rpc`)
// takes over. A kind with no configured image falls back to the placeholder
// dev stand-in and its shell-script Command.
func resolveAgentImage(cfg Config, kind queue.JobKind) (image string, command []string) {
	if img, ok := cfg.Images[kind]; ok && img != "" {
		return img, nil
	}
	return cfg.PlaceholderImage, []string{"sh", "-c", cfg.PlaceholderScript}
}

// filterModelEnv picks the per-project model config keys (ADR 004) out of a
// project's full decrypted secrets map, so only those three ever land in a
// job pod's env — not whatever else a project happens to store in
// project_secrets.
func filterModelEnv(secrets map[string]string) map[string]string {
	env := make(map[string]string, len(modelEnvKeys))
	for _, key := range modelEnvKeys {
		if v, ok := secrets[key]; ok {
			env[key] = v
		}
	}
	return env
}

// runDeploy applies the project's Helm chart to its namespace via
// `helm upgrade --install` (ADR 003 §13). It fetches the project's real
// chart scaffolded into its primary repo (Phase 3c); if none is scaffolded
// yet (old project, or scaffold failed), it falls back to the Orchestrator's
// embedded placeholder chart so deploys never hard-fail on a missing chart.
// Project secrets (ADR 003 §16) are fetched from the API and pushed into a
// dedicated Kubernetes Secret before the chart is applied, so the
// Deployment's envFrom reference resolves on first rollout. Once the
// Deployment/Service exist, an Ingress (ADR 003 §15) makes the primary
// deployment reachable at <project-slug>.apps.<domain>.
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
	if err := helm.Deploy(ctx, helmCfg, namespace, primaryReleaseName, chrt, values); err != nil {
		return err
	}

	slug, err := cfg.APIClient.FetchProjectMetadata(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to fetch project metadata: %w", err)
	}
	host := fmt.Sprintf("%s.apps.%s", slug, cfg.AppsDomain)
	if err := k8s.EnsureProjectIngress(
		ctx, clientset, namespace, host, primaryReleaseName, 80,
		cfg.IngressClassName, "primary-tls", cfg.CertIssuerName,
	); err != nil {
		return fmt.Errorf("failed to ensure ingress: %w", err)
	}
	return nil
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
