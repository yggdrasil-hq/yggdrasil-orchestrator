// Package worker runs the Orchestrator's job-claim poll loop.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/messages"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"helm.sh/helm/v3/pkg/chart"
)

// primaryReleaseName is constant within a project's own namespace — no
// collision risk since each project already gets its own namespace
// (ADR 003 §5), and each project has exactly one always-on primary
// deployment (ADR 003 §9).
const primaryReleaseName = "primary"

const (
	defaultPollInterval     = 2 * time.Second
	defaultPlaceholderImage = "busybox:1.36"
	// defaultMaxConcurrentJobs bounds how many jobs a single Orchestrator
	// replica runs at once (ADR 006 item 1). Needed once spec_grill can hold
	// a job open for minutes waiting on a human's reply — without this, one
	// slow-to-answer grill would stall every other job this replica could
	// otherwise be claiming and running.
	defaultMaxConcurrentJobs = 5
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

	// MaxConcurrentJobs bounds how many claimed jobs this replica runs at
	// once (ADR 006 item 1). Zero/negative falls back to
	// defaultMaxConcurrentJobs. Multiple replicas remain safe exactly as
	// before (SKIP LOCKED, ADR 003) — this only makes a single replica
	// internally concurrent too.
	MaxConcurrentJobs int

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

	// Clusters resolves each job's target Kubernetes cluster (clientset +
	// REST config) from its project -> organization, replacing the old single
	// static client built once at startup (ADR 016 item 13). Required: there
	// is no platform-default cluster, so every job resolves its org's own.
	Clusters ClusterProvider

	// Messages delivers a human's reply to a running spec_grill job's
	// ask_user question back into its RPC session (ADR 006 items 9-10), via
	// Postgres LISTEN/NOTIFY on the same database the job queue lives in.
	Messages *messages.Store

	// AppsDomain/IngressClassName/CertIssuerName build the primary
	// deployment's Ingress (ADR 003 §15: <project-slug>.apps.<domain>).
	// Config values, not code, are what change between a local k3d dev
	// cluster (Traefik, self-signed cert) and a self-hosted/managed cluster
	// (ingress-nginx, a real ACME ClusterIssuer).
	AppsDomain       string
	IngressClassName string
	CertIssuerName   string
}

// ClusterProvider resolves the Kubernetes client a job should run against,
// derived from the job's project -> organization (ADR 016 item 13). Returns
// the error when the org has no cluster configured — a hard failure, not a
// fallback, since there is no platform-default cluster anymore.
type ClusterProvider interface {
	Resolve(ctx context.Context, job *queue.Job) (*k8s.Client, error)
}

// limiter bounds how many jobs this replica runs concurrently (ADR 006
// item 1) — a buffered channel used as a semaphore. A zero-value limiter
// (nil channel) blocks forever on both ends, so always construct one via
// newLimiter.
type limiter chan struct{}

func newLimiter(n int) limiter {
	if n <= 0 {
		n = 1
	}
	return make(limiter, n)
}

// tryAcquire reports whether a slot was available and, if so, claims it.
// Never blocks — a saturated limiter just means "not this tick."
func (l limiter) tryAcquire() bool {
	select {
	case l <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l limiter) release() {
	<-l
}

// Run polls the queue on an interval and blocks until ctx is cancelled.
// Each claimed job runs in its own goroutine (ADR 006 item 1), bounded by
// cfg.MaxConcurrentJobs, instead of blocking the poll loop until that one
// job finishes — a spec_grill session can sit open for minutes waiting on a
// human's reply, and shouldn't stall every other job this replica could
// otherwise claim.
func Run(ctx context.Context, q *queue.Queue, cfg Config) {
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
	maxConcurrent := cfg.MaxConcurrentJobs
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentJobs
	}
	sem := newLimiter(maxConcurrent)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			claimAndDispatch(ctx, q, cfg, sem)
		}
	}
}

// claimAndDispatch claims at most one job — only if a concurrency slot is
// free — and runs it in its own goroutine, releasing the slot when that job
// finishes. If the limiter is already saturated, it skips claiming entirely
// this tick: claiming a job this replica has no free slot to start would
// just leave it stuck in "running" status until a slot frees up, when
// another replica (or this one, next tick) could have picked it up instead.
func claimAndDispatch(ctx context.Context, q *queue.Queue, cfg Config, sem limiter) {
	if !sem.tryAcquire() {
		return
	}

	job, err := q.Claim(ctx, cfg.WorkerID)
	if err != nil {
		sem.release()
		log.Printf("worker %s: claim failed: %v", cfg.WorkerID, err)
		return
	}
	if job == nil {
		sem.release()
		return
	}

	log.Printf("worker %s: claimed job %s (kind=%s project=%s)", cfg.WorkerID, job.ID, job.Kind, job.ProjectID)

	go func() {
		defer sem.release()
		runClaimedJob(ctx, q, cfg, job)
	}()
}

// runClaimedJob runs a single already-claimed job to completion and records
// its outcome on the queue. It resolves the job's target cluster first (ADR
// 016 item 13) and fails the job if the org has no cluster configured.
func runClaimedJob(ctx context.Context, q *queue.Queue, cfg Config, job *queue.Job) {
	client, err := cfg.Clusters.Resolve(ctx, job)
	if err != nil {
		// The org has no (valid) cluster — a hard gate failure, recorded
		// exactly like a run crash via q.Fail (ADR 012 retry semantics apply).
		log.Printf("worker %s: job %s cluster resolution failed: %v", cfg.WorkerID, job.ID, err)
		if failErr := q.Fail(ctx, job.ID, err); failErr != nil {
			log.Printf("worker %s: failed to record failure for job %s: %v", cfg.WorkerID, job.ID, failErr)
		}
		return
	}

	if err := runInCluster(ctx, q, client, job, cfg); err != nil {
		if errors.Is(err, errJobCancelled) {
			log.Printf("worker %s: job %s was cancelled", cfg.WorkerID, job.ID)
		} else {
			log.Printf("worker %s: job %s failed: %v", cfg.WorkerID, job.ID, err)
		}
		// Fail is a no-op if the API's cancel endpoint already moved this job
		// to 'cancelled' (queue.Queue.Fail is guarded to only touch a
		// 'running' row) — so this call is safe either way, no branch needed.
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
// primary deployment (ADR 003 §9-13). A spec_grill, feature_build, or
// agentic_review (ADR 015 item 13 / Track B6) job with a real image
// configured for its kind (cfg.Images) drives Pi's RPC session directly
// (ADR 006 items 2-4, 7, 11; widened to feature_build by ADR 010 item 4 and
// to agentic_review by ADR 015) via runAgentRPCJob; every other case (either
// kind with no real image configured yet, test_run, or the non-Pi
// script_test_run — ADR 015 item 10) runs the standalone non-Pi image through
// the blocking k8s.RunJob path; its entrypoint posts the canonical report
// before returning. Jobs without a configured script image fail explicitly.
func runInCluster(ctx context.Context, q *queue.Queue, client *k8s.Client, job *queue.Job, cfg Config) error {
	namespace, err := k8s.EnsureProjectNamespace(ctx, client.Interface, job.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to provision namespace: %w", err)
	}

	if job.Kind == queue.KindDeploy {
		return runDeploy(ctx, client, job.ProjectID, namespace, cfg)
	}

	if job.Kind == queue.KindSpecGrill ||
		job.Kind == queue.KindFeatureBuild ||
		job.Kind == queue.KindTestRun ||
		job.Kind == queue.KindAgenticReview ||
		job.Kind == queue.KindDesignGrill {
		if img, ok := cfg.Images[job.Kind]; ok && img != "" {
			return runAgentRPCJob(ctx, q, client, job, namespace, cfg)
		}
	}

	if job.Kind == queue.KindScriptTestRun {
		if _, ok := cfg.Images[job.Kind]; !ok || cfg.Images[job.Kind] == "" {
			return fmt.Errorf("no image configured for %s", job.Kind)
		}
	}

	return runAgentJob(ctx, client, job, namespace, cfg)
}

// runAgentJob runs a spec_grill/feature_build/test_run job as a Kubernetes
// Job via the blocking k8s.RunJob (waits for standard Job success/failure).
// It resolves which container image to use (a real agent-images image per
// ADR 004 if configured for this job kind, else the placeholder dev
// stand-in) and delegates env assembly to buildAgentEnv.
//
// A spec_grill job with a real image configured never reaches this
// function — runInCluster routes it to runSpecGrillJob (specgrill.go)
// instead, since RunJob's "wait for Job status" model doesn't apply to a
// process that never exits on its own.
func runAgentJob(ctx context.Context, client *k8s.Client, job *queue.Job, namespace string, cfg Config) error {
	env, _, err := buildAgentEnv(ctx, cfg, job)
	if err != nil {
		return err
	}

	image, command := resolveAgentImage(cfg, job.Kind)

	return k8s.RunJob(ctx, client.Interface, k8s.JobSpec{
		Namespace:        namespace,
		Name:             "job-" + job.ID,
		Image:            image,
		Command:          command,
		Env:              env,
		RuntimeClassName: cfg.RuntimeClassName,
	})
}

// buildAgentEnv assembles the common env vars every agent job pod needs —
// JOB_ID/JOB_KIND/PROJECT_ID and the project's model config (ADR 004:
// MODEL_BASE_URL/MODEL_API_KEY/MODEL_ID, decrypted from project_secrets) as
// plain job-pod env vars, the same delivery path already used for the
// scoped GitHub token, not a Kubernetes Secret object — plus, for
// spec_grill, feature_build, and design_grill (ADR 010/014),
// TARGET_REPOS/GITHUB_TOKEN
// (and, feature_build only, ADR_MARKDOWN/FEATURE_BRANCH) via agentRepoEnv.
// Also returns the fetched FeatureSpec (zero value for job kinds that don't
// fetch one) so runAgentRPCJob can reuse spec.Title/spec.FeatureType for
// the initial RPC prompt without fetching it a second time.
func buildAgentEnv(ctx context.Context, cfg Config, job *queue.Job) (map[string]string, apiclient.FeatureSpec, error) {
	env := map[string]string{
		"JOB_ID":     job.ID,
		"JOB_KIND":   string(job.Kind),
		"PROJECT_ID": job.ProjectID,
	}

	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, job.ProjectID)
	if err != nil {
		return nil, apiclient.FeatureSpec{}, fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	for k, v := range filterModelEnv(secrets) {
		env[k] = v
	}

	var spec apiclient.FeatureSpec
	if job.Kind == queue.KindSpecGrill ||
		job.Kind == queue.KindFeatureBuild ||
		job.Kind == queue.KindTestRun ||
		job.Kind == queue.KindScriptTestRun ||
		job.Kind == queue.KindAgenticReview ||
		job.Kind == queue.KindDesignGrill {
		specEnv, fetchedSpec, err := agentRepoEnv(ctx, cfg, job)
		if err != nil {
			return nil, apiclient.FeatureSpec{}, err
		}
		for k, v := range specEnv {
			env[k] = v
		}
		spec = fetchedSpec
		if spec.TestMarkdown != "" {
			env["TEST_MARKDOWN"] = spec.TestMarkdown
		}
		if spec.Ref != "" {
			env["FEATURE_REF"] = spec.Ref
		}
		if job.Ref != nil && *job.Ref != "" {
			env["FEATURE_REF"] = *job.Ref
		}
		if job.Kind == queue.KindTestRun {
			slug, err := cfg.APIClient.FetchProjectMetadata(ctx, job.ProjectID)
			if err != nil {
				return nil, apiclient.FeatureSpec{}, fmt.Errorf("failed to resolve test preview URL: %w", err)
			}
			env["PREVIEW_URL"] = fmt.Sprintf(
				"https://%s-test-run-%s.preview.%s",
				slug,
				job.ID,
				cfg.AppsDomain,
			)
		}
		if job.Kind == queue.KindScriptTestRun {
			env["SCRIPT_NAME"] = spec.ScriptName
			baseURL, token := cfg.APIClient.InternalEndpoint()
			env["YGGDRASIL_API_URL"] = baseURL
			env["YGGDRASIL_API_TOKEN"] = token
		}
	}

	return env, spec, nil
}

// agentRepoEnv fetches a repo-backed job's payload
// (ADR 006 item 5, widened by ADR 010 item 2) and returns both the extra
// job-pod env vars it needs and the fetched spec itself:
//   - TARGET_REPOS (JSON-encoded FeatureSpecRepo list, consumed by
//     base/entrypoint.sh's clone step, ADR 006 item 6) and GITHUB_TOKEN (a
//     fresh, job-scoped installation token minted by that same API call —
//     not a project_secrets value, since installation tokens are
//     short-lived and per-job, ADR 005 §14/§16; scoped read-only for
//     spec_grill, contents:write+pull-requests:write for feature_build) —
//     both always set.
//   - ADR_MARKDOWN/FEATURE_BRANCH or FEATURE_REF (consumed by entrypoint.sh's
//     branch-checkout/ADR-file-write step), plus TEST_MARKDOWN for test_run.
//
// Scheduled test_run jobs have no feature_id and fetch their test payload
// directly, defaulting to main; feature-stage test runs carry a feature_id
// and target the persisted ref.
func agentRepoEnv(ctx context.Context, cfg Config, job *queue.Job) (map[string]string, apiclient.FeatureSpec, error) {
	var spec apiclient.FeatureSpec
	var err error
	if job.FeatureID == nil {
		if job.Kind == queue.KindDesignGrill {
			spec, err = cfg.APIClient.FetchDesignSpec(ctx, job.ProjectID, job.ID)
		} else if job.Kind != queue.KindTestRun || job.TestID == nil {
			return nil, apiclient.FeatureSpec{}, fmt.Errorf("job %s (kind=%s) has no feature_id", job.ID, job.Kind)
		} else {
			ref := "main"
			if job.Ref != nil && *job.Ref != "" {
				ref = *job.Ref
			}
			spec, err = cfg.APIClient.FetchTestSpec(ctx, job.ProjectID, *job.TestID, ref)
		}
	} else {
		testID := ""
		if job.TestID != nil {
			testID = *job.TestID
		}
		scriptName := ""
		if job.TestGroup != nil {
			scriptName = *job.TestGroup
		}
		spec, err = cfg.APIClient.FetchFeatureSpec(
			ctx,
			job.ProjectID,
			*job.FeatureID,
			string(job.Kind),
			testID,
			scriptName,
		)
	}
	if err != nil {
		return nil, apiclient.FeatureSpec{}, fmt.Errorf("failed to fetch feature spec: %w", err)
	}

	targetRepos, err := json.Marshal(spec.Repos)
	if err != nil {
		return nil, apiclient.FeatureSpec{}, fmt.Errorf("failed to encode target repos: %w", err)
	}

	env := map[string]string{
		"TARGET_REPOS": string(targetRepos),
		"GITHUB_TOKEN": spec.GithubToken,
	}
	if spec.AdrMarkdown != "" {
		env["ADR_MARKDOWN"] = spec.AdrMarkdown
	}
	if spec.Branch != "" {
		if job.Kind == queue.KindTestRun ||
			job.Kind == queue.KindScriptTestRun ||
			job.Kind == queue.KindAgenticReview {
			env["FEATURE_REF"] = spec.Branch
		} else {
			env["FEATURE_BRANCH"] = spec.Branch
		}
	}
	return env, spec, nil
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
func runDeploy(ctx context.Context, client *k8s.Client, projectID, namespace string, cfg Config) error {
	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	if err := k8s.EnsureProjectSecret(ctx, client.Interface, namespace, secrets); err != nil {
		return fmt.Errorf("failed to push project secrets: %w", err)
	}

	chrt, err := resolveChart(ctx, cfg, projectID)
	if err != nil {
		return err
	}

	helmCfg, err := helm.NewConfiguration(client.Config, namespace)
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
		ctx, client.Interface, namespace, host, primaryReleaseName, 80,
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
