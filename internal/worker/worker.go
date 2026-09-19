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
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/preview"
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

// modelSessionEnv is the env var this component contributes to the "who is this
// request for" contract (issue #37). It is a two-part name on purpose:
//
//   - this side owns the *value* — the job id, which makes a gateway's cost and
//     traffic breakdown per-run rather than per-project;
//   - agent-images owns the *header* the value ends up in
//     (`x-opencode-session`, declared in `models.json.template`), because the
//     pod is what actually issues the provider request. The Orchestrator does
//     not talk to a model provider at all.
//
// So the constant here names the env var, not the header. Deliberately not
// logged: it is not a credential, but there is no reason to put a run
// identifier into the worker's output either.
const modelSessionEnv = "MODEL_SESSION_ID"

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

	// ImagePullSecret names a docker-config object in the target cluster that
	// has to be presented to pull the images above (issue #29), or "" when they
	// need none.
	//
	// Only the agent images need this: they come from GHCR, where packages are
	// private by default (ADR 004), whereas the placeholder default is a public
	// docker.io image. Without it, a namespaced credential object is unused —
	// Docker and Kubernetes only send one for a pod that (or whose service
	// account) references it — so this field is what makes provisioning it
	// actually take effect.
	ImagePullSecret string

	// PullPreflight answers, once per namespace and once per repository per
	// process, whether this job's image can be pulled at all (issue #29). Run
	// fills it in when nil; tests pass their own so a finding is observable
	// without a process boundary, and so a fake registry can stand in for GHCR.
	PullPreflight *k8s.ImagePullPreflight

	// PlaceholderImage/PlaceholderScript stand in for a real Pi agent image,
	// for any job kind not present in Images.
	PlaceholderImage  string
	PlaceholderScript string

	// RuntimeClassName is left nil unless the target cluster has a sandboxed
	// runtime (gVisor/Kata, ADR 003 §6) installed — not available on every
	// cluster (e.g. a local k3d dev cluster).
	RuntimeClassName *string

	// RecordingMaxBytes bounds the screen recording the Orchestrator will collect
	// out of a finished test_run pod and upload (ADR 029). A value <= 0 disables
	// collection entirely, which is how a deployment that does not want
	// recordings turns the feature off without touching the API.
	//
	// Deliberately unlike MaxConcurrentPreviews' "0 means apply the default"
	// convention: here 0 is a meaningful, distinct instruction (store nothing),
	// so the unset case cannot share it. cmd/server/main.go resolves unset and
	// unparseable values to DefaultRecordingMaxBytes before this struct is built,
	// which is what leaves 0 free to mean "off".
	//
	// Should agree with the API's RECORDING_MAX_BYTES. It is duplicated rather
	// than fetched because the two sides fail differently and independently: the
	// Orchestrator's copy avoids moving megabytes to be told no, and the API's is
	// the authoritative one that a caller cannot lie its way past.
	RecordingMaxBytes int64

	// ScreenshotMaxBytes bounds each step screenshot the Orchestrator will collect
	// out of a finished job pod and upload (issue #22). A value <= 0 disables
	// screenshot collection entirely, on the same terms as RecordingMaxBytes: it is
	// how a deployment that does not want the artifacts turns the feature off
	// without touching the API.
	//
	// Same convention, and same reasoning, as RecordingMaxBytes above — 0 means
	// "off", not "use the default", so cmd/server/main.go resolves unset and
	// unparseable values to DefaultScreenshotMaxBytes before this struct is built.
	//
	// Should agree with the API's SCREENSHOT_MAX_BYTES (2 MB by default), and is
	// duplicated for the same reason: this copy avoids shipping bytes only to be
	// declined, while the API's is the authoritative one.
	//
	// Note this is a *per-screenshot* cap, not a per-job one. The API also enforces
	// a per-job count (SCREENSHOT_MAX_PER_JOB) which is deliberately not mirrored
	// here — see screenshotCollector's own comment for why a second, silently
	// divergent bound is worse than a logged decline.
	ScreenshotMaxBytes int64

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
	// (ingress-nginx, a real ACME ClusterIssuer). The same values build an
	// ephemeral preview's Ingress and certificate.
	AppsDomain       string
	IngressClassName string
	CertIssuerName   string

	// MaxConcurrentPreviews caps simultaneously-active ephemeral preview
	// deployments per project (ADR 003 §17). Zero means the documented
	// default of 3; a negative value disables previews entirely (and with
	// them the queue's admission clause, which keeps the claim query free of
	// any reference to the previews table).
	MaxConcurrentPreviews int

	// PreviewTTL bounds how long a preview may outlive its job when nothing
	// else knows the job is gone (default 2h). PreviewSweepInterval is how
	// often the orphan sweep runs (default 15m).
	PreviewTTL           time.Duration
	PreviewSweepInterval time.Duration

	// ReplyTimeout bounds how long one `ask_user` question may go unanswered
	// before the run is failed (issue #82). Zero or negative takes
	// `defaultReplyTimeout`.
	//
	// **There is deliberately no value that disables it.** `previewTTL`'s "0 means
	// take the default" shape is followed exactly, and that is the point rather
	// than an oversight: an unbounded wait is the bug this exists to close, so no
	// configuration should be able to reproduce it. An install needing a longer
	// grace period raises this; nothing should need an infinite one.
	//
	// It bounds **one wait, not the whole run** — see the wait in
	// `driveAgentSession` for why that distinction is the substance of the fix.
	ReplyTimeout time.Duration

	// ImageRegistry is the registry a preview's image is built and pushed to,
	// from which the preview's Deployment then pulls (ADR 003 §14: "a bundled
	// registry alongside the bundled k3s cluster").
	//
	// **Empty disables the build entirely**, which is why it defaults to empty
	// rather than to a guessed in-cluster hostname: with no registry configured
	// there is nowhere to push and nowhere to pull from, so a preview falls back
	// to the chart's declared image exactly as it did before this existed. That
	// makes landing this a no-op for every install that has not opted in, which
	// matters because a preview that broke on a missing registry would be a
	// regression in a feature previews do not depend on.
	ImageRegistry string

	// ImageBuildAuthSecret names a docker-config Secret in the project's namespace
	// that carries push credentials for ImageRegistry. Empty is correct for the
	// bundled `registry:2` of ADR 003 §14, which takes unauthenticated pushes
	// inside the cluster.
	ImageBuildAuthSecret string
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
	if cfg.PullPreflight == nil {
		cfg.PullPreflight = k8s.NewImagePullPreflight()
	}
	if cfg.PlaceholderScript == "" {
		cfg.PlaceholderScript = defaultPlaceholderScript
	}
	maxConcurrent := cfg.MaxConcurrentJobs
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentJobs
	}
	sem := newLimiter(maxConcurrent)

	// The orphan sweep runs alongside the poll loop rather than inside it:
	// its job is to collect previews left behind by jobs this replica may
	// never have seen (ADR 003 §17), which is unrelated to how this replica
	// is claiming work. The first pass happens immediately, before any job is
	// claimed — after a restart that is exactly when leftover previews are
	// most likely to be sitting there.
	go runPreviewSweeps(ctx, cfg)

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

// runPreviewSweeps performs the orphan sweep once at startup and then on an
// interval until ctx ends (ADR 003 §17).
func runPreviewSweeps(ctx context.Context, cfg Config) {
	if cfg.maxConcurrentPreviews() == 0 {
		return
	}

	SweepPreviews(ctx, cfg)

	ticker := time.NewTicker(cfg.previewSweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			SweepPreviews(ctx, cfg)
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

	job, err := q.Claim(ctx, cfg.WorkerID, previewAdmission(cfg))
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
// before returning. A `script_test_run` whose image is not configured on this
// installation has its group recorded as skipped rather than failing the job
// (issue #44, see preflight.go).
//
// Three ADR 030 steps and one setup check bracket everything else: the token cap
// and quota resolve before any work happens, issue #29's image-pull preflight
// runs once the namespace exists and the image is known (so it can read the
// namespace and report before a pod exists), and the namespace quota applies
// when the namespace is provisioned.
func runInCluster(ctx context.Context, q *queue.Queue, client *k8s.Client, job *queue.Job, cfg Config) error {
	// ADR 030 §4: the token cap is checked before anything expensive happens,
	// so an over-cap job never provisions a namespace, starts a preview, or
	// mints a GitHub token. The API owns the decision; this asks it once.
	if err := enforceTokenCap(ctx, job, cfg); err != nil {
		return err
	}

	// Issue #44: a `script_test_run` this installation cannot run at all. Handled
	// before the quota fetch, the namespace, and the preview, because none of
	// them can help — there is no image to run — and leaving them out keeps a
	// broken setting from also costing a namespace and a GitHub token.
	if handled, err := skipWhenScriptImageUnconfigured(ctx, job, cfg); handled {
		return err
	}

	// ADR 030 §5: the project's configured quota, resolved by the API to
	// concrete numbers. A fetch failure falls back to the built-in defaults
	// rather than failing the job — quota sizing is a guardrail, not a
	// correctness dependency (ADR 030 §6).
	quota := k8s.DefaultResourceQuota()
	if fetched, err := cfg.APIClient.FetchProjectResourceQuota(ctx, job.ProjectID); err != nil {
		log.Printf("worker: using default resource quota for project %s: %v", job.ProjectID, err)
	} else {
		quota = k8s.ResourceQuota{
			CPUmillicores: fetched.CPUmillicores,
			MemoryMiB:     fetched.MemoryMiB,
			Pods:          fetched.Pods,
		}
	}

	namespace, err := k8s.EnsureProjectNamespace(ctx, client.Interface, job.ProjectID, quota)
	if err != nil {
		return fmt.Errorf("failed to provision namespace: %w", err)
	}

	// Issue #29: warn, before any pod exists, when this job's image cannot
	// plausibly be pulled. Placed here because the check reads the namespace
	// (which has just been provisioned) and because everything below — the
	// preview, the GitHub token, the pod — is work an unpullable image wastes.
	preflightImagePull(ctx, client, job, namespace, cfg)

	// The ephemeral preview exists for the whole job and is removed when this
	// function returns, however it returns (ADR 003 §10/§15). `defer` is what
	// covers the failure and cancellation paths as well as success — the run
	// can unwind from anywhere below, including a ctx cancellation while an
	// agent session is waiting on a human — so a preview cannot be left behind
	// by an outcome that was merely unexpected. The sweep in preview.go covers
	// the cases defer cannot reach at all (a crash, a restart mid-job).
	//
	// Created before the agent's env is built so a `test_run`'s PREVIEW_URL
	// points at a live environment rather than a hostname nothing serves: that
	// env var has always been set, but until now nothing created the deployment
	// behind it.
	livePreview := startJobPreview(ctx, client, job, namespace, cfg)
	defer livePreview.stop(ctx, client, cfg)
	if job.Kind == queue.KindDeploy {
		return runDeploy(ctx, client, job, namespace, cfg)
	}

	if job.Kind == queue.KindRollback {
		return runRollback(ctx, client, job, namespace, cfg)
	}

	if job.Kind == queue.KindSpecGrill ||
		job.Kind == queue.KindFeatureBuild ||
		job.Kind == queue.KindTestRun ||
		job.Kind == queue.KindAgenticReview ||
		job.Kind == queue.KindDesignGrill {
		if configuredImage(cfg, job.Kind) != "" {
			return runAgentRPCJob(ctx, q, client, job, namespace, cfg)
		}
	}

	// A `script_test_run` without an image never reaches here — that case is
	// decided in skipWhenScriptImageUnconfigured, above.
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
		ImagePullSecret:  cfg.ImagePullSecret,
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
// Model config is resolved for the job's own feature when it has one, so a
// per-feature override reaches the pod rather than stopping at the API's
// dispatch gate (ADR 018 amendment, issue #5). Also returns the fetched
// FeatureSpec (zero value for job kinds that don't fetch one) so
// runAgentRPCJob can reuse spec.Title/spec.FeatureType for the initial RPC
// prompt without fetching it a second time.
func buildAgentEnv(ctx context.Context, cfg Config, job *queue.Job) (map[string]string, apiclient.FeatureSpec, error) {
	env := map[string]string{
		"JOB_ID":     job.ID,
		"JOB_KIND":   string(job.Kind),
		"PROJECT_ID": job.ProjectID,
	}

	// Always set, even with no gateway in front of the provider: sending the
	// header unconditionally is the safe default (a provider with no use for it
	// ignores it), whereas the gateway this product is deployed against refuses
	// a request without it:
	//
	//	400 {"type":"MissingSessionID","message":"Error from provider (Console
	//	     Go): Request is missing x-opencode-session and cannot be routed
	//	     efficiently."}
	//
	// See modelSessionEnv for which side owns what.
	env[modelSessionEnv] = job.ID

	// Empty for a job with no feature (a scheduled test_run, a deploy, or a
	// design session — ADR 014 keeps design jobs project-scoped), which makes
	// the fetch behave exactly as it did before the feature tier existed.
	featureID := ""
	if job.FeatureID != nil {
		featureID = *job.FeatureID
	}
	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, job.ProjectID, string(job.Kind), featureID)
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
			// The one place the preview URL scheme is spelled (internal/preview),
			// so this env var and the Ingress a preview creates cannot disagree
			// about which host the environment is actually on.
			env["PREVIEW_URL"] = "https://" + preview.Host(slug, job.Kind, job.ID, cfg.AppsDomain)
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
	spec, err := fetchJobSpec(ctx, cfg, job)
	if err != nil {
		return nil, apiclient.FeatureSpec{}, err
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

// fetchJobSpec resolves the API payload a job's pod payload is built from — the
// feature or test it belongs to, the project's linked repositories, and a
// freshly minted job-scoped GitHub token.
//
// Split out of agentRepoEnv for issue #19: the preview's image build needs the
// same repositories and the same token, and it runs *before* the agent's env is
// assembled (a preview exists for the whole job, and the build has to finish
// before the preview is deployed). Duplicating the dispatch below — which has
// three branches across two job shapes and a design-session special case — would
// be the kind of copy that drifts the first time a job kind is added.
func fetchJobSpec(ctx context.Context, cfg Config, job *queue.Job) (apiclient.FeatureSpec, error) {
	var spec apiclient.FeatureSpec
	var err error
	if job.FeatureID == nil {
		if job.Kind == queue.KindDesignGrill {
			spec, err = cfg.APIClient.FetchDesignSpec(ctx, job.ProjectID, job.ID)
		} else if job.Kind != queue.KindTestRun || job.TestID == nil {
			return apiclient.FeatureSpec{}, fmt.Errorf("job %s (kind=%s) has no feature_id", job.ID, job.Kind)
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
		args := []string{testID, scriptName}
		if job.Kind == queue.KindSpecGrill {
			args = append(args, job.ID)
		}
		spec, err = cfg.APIClient.FetchFeatureSpec(
			ctx,
			job.ProjectID,
			*job.FeatureID,
			string(job.Kind),
			args...,
		)
	}
	if err != nil {
		return apiclient.FeatureSpec{}, fmt.Errorf("failed to fetch feature spec: %w", err)
	}
	return spec, nil
}

// resolveAgentImage returns the image to run for a job kind and the command
// to run inside it. A kind with a configured real agent-images image (ADR
// 004, cfg.Images) runs with no command override, so the image's own
// ENTRYPOINT (agent-images' entrypoint.sh, which execs `pi --mode rpc`)
// takes over. A kind with no configured image falls back to the placeholder
// dev stand-in and its shell-script Command.
func resolveAgentImage(cfg Config, kind queue.JobKind) (image string, command []string) {
	if image := configuredImage(cfg, kind); image != "" {
		return image, nil
	}
	return cfg.PlaceholderImage, []string{"sh", "-c", cfg.PlaceholderScript}
}

// configuredImage returns the real agent-images image for a job kind, or "" when
// this installation has none.
//
// One predicate rather than the `img, ok := cfg.Images[kind]; ok && img != ""`
// idiom repeated at each call site: an empty-string entry and an absent one mean
// the same thing ("not configured") and are easy to treat differently by
// accident — a map that holds a kind mapped to "" is exactly what an
// `.env`-driven config produces when an env var is set but blank.
func configuredImage(cfg Config, kind queue.JobKind) string {
	if img, ok := cfg.Images[kind]; ok && img != "" {
		return img
	}
	return ""
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
//
// The Helm revision this deploy produced is reported back to the API
// (ADR 022) so the project has a deploy ledger to show and to roll back to.
func runDeploy(ctx context.Context, client *k8s.Client, job *queue.Job, namespace string, cfg Config) error {
	// The trailing "" is the feature id: a deploy is project-scoped, so it has
	// no feature whose model tier could apply.
	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, job.ProjectID, string(queue.KindDeploy), "")
	if err != nil {
		return fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	if err := k8s.EnsureProjectSecret(ctx, client.Interface, namespace, secrets); err != nil {
		return fmt.Errorf("failed to push project secrets: %w", err)
	}

	chrt, err := resolveChart(ctx, cfg, job.ProjectID)
	if err != nil {
		return err
	}

	helmCfg, err := helm.NewConfiguration(client.Config, namespace)
	if err != nil {
		return fmt.Errorf("failed to initialize helm: %w", err)
	}
	values := map[string]interface{}{"secretsChecksum": secretsChecksum(secrets)}
	revision, err := helm.Deploy(ctx, helmCfg, namespace, primaryReleaseName, chrt, values)
	if err != nil {
		// Reported even on failure: the API records the attempt either way, so
		// the ledger shows a failed deploy rather than silently omitting it.
		reportDeployResult(ctx, cfg, job.ID, apiclient.DeployResultInput{LastError: err.Error()})
		return err
	}
	reportDeployResult(ctx, cfg, job.ID, apiclient.DeployResultInput{Revision: revision})

	slug, err := cfg.APIClient.FetchProjectMetadata(ctx, job.ProjectID)
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

// runRollback reverts a project's primary release to an earlier Helm
// revision (ADR 022) — the safety net ADR 003 §9 left out when it shipped
// auto-deploy-on-merge.
//
// It deliberately does *not* re-apply the chart, re-push project secrets, or
// re-ensure the Ingress: those are inputs to a release, not part of it. Only
// the release's own manifests move backwards. Two consequences worth being
// explicit about, both documented in ADR 022: project secrets are applied
// imperatively outside Helm (ADR 003 §16), so a rollback does not roll back
// rotated credentials; and the Ingress points at the release's Service name,
// which a rollback does not change.
//
// The target revision is read from the job row rather than resolved here, so
// it stays pinned to what the operator actually chose even if newer deploys
// land between the request and this claim (ADR 022).
func runRollback(ctx context.Context, client *k8s.Client, job *queue.Job, namespace string, cfg Config) error {
	if job.TargetRevision == nil {
		return errors.New("rollback job has no target revision")
	}
	helmCfg, err := helm.NewConfiguration(client.Config, namespace)
	if err != nil {
		return fmt.Errorf("failed to initialize helm: %w", err)
	}

	revision, err := helm.Rollback(ctx, helmCfg, namespace, primaryReleaseName, *job.TargetRevision)
	if err != nil {
		reportDeployResult(ctx, cfg, job.ID, apiclient.DeployResultInput{
			TargetRevision: job.TargetRevision,
			LastError:      err.Error(),
		})
		return err
	}
	reportDeployResult(ctx, cfg, job.ID, apiclient.DeployResultInput{
		Revision:       revision,
		TargetRevision: job.TargetRevision,
	})
	return nil
}

// reportDeployResult posts one deploy/rollback outcome to the API's deploy
// ledger (ADR 022), retrying a transient failure first.
//
// A failed report is deliberately not fatal to the job: the release really was
// applied or rolled back, so failing the job would report a true outcome as a
// false one. The consequence of losing the call is not cosmetic though — the
// revision is then absent from the ledger, so it is not offered as a rollback
// target, and the user cannot undo a deploy that is live (issue #26). That is a
// worse outcome than a few seconds of retrying, which is why this is the one
// reporting call in this package that retries.
//
// Retrying is safe because the API's ingest is idempotent on the job id
// (migration 043): the case a retry cannot distinguish from a fresh attempt —
// "the write landed but the response didn't" — records one row, not two.
//
// It narrows the window rather than closing it: a pod that is evicted between
// the Helm operation and this call still loses the report, and no amount of
// retrying inside that pod can fix it. Closing it properly would need the
// revision to be recoverable from the cluster by something that outlives the
// job — recorded as a limitation in ADR 022 §5 rather than pretended away.
// deployReportBackoff is the wait before retrying a lost deploy report. Three
// entries means up to four attempts: long enough to ride out an API restart or
// a dropped packet (~7s), short enough that the job is not held open for it.
//
// A variable rather than a slice literal inside the loop so a test can shrink
// it; nothing in production reassigns it.
var deployReportBackoff = []time.Duration{
	time.Second, 2 * time.Second, 4 * time.Second,
}

func reportDeployResult(ctx context.Context, cfg Config, jobID string, result apiclient.DeployResultInput) {
	attempts := len(deployReportBackoff) + 1

	for attempt := 1; ; attempt++ {
		err := cfg.APIClient.ReportDeployResult(ctx, jobID, result)
		if err == nil {
			if attempt > 1 {
				log.Printf("worker: recorded deploy result for job %s (revision %d) on attempt %d", jobID, result.Revision, attempt)
			}
			return
		}

		if attempt >= attempts || ctx.Err() != nil {
			log.Printf(
				"worker: WARNING failed to record deploy result for job %s (revision %d) after %d attempt(s): %v — this revision will be missing from the project's deploy history",
				jobID, result.Revision, attempt, err,
			)
			return
		}

		delay := deployReportBackoff[attempt-1]
		log.Printf("worker: failed to record deploy result for job %s (attempt %d/%d): %v; retrying in %s", jobID, attempt, attempts, err, delay)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
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
