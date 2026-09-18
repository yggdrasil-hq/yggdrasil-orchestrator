package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/preview"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// defaultPreviewTTL bounds how long a preview may outlive its job when nothing
// else knows the job is gone. It exists for one case that no other mechanism
// can cover: a job that crashed hard is left `running` forever (nothing reaps
// a stale job), so a sweep keyed only on "the job is no longer running" would
// never collect that preview. Two hours comfortably exceeds a normal build or
// test run while still bounding the leak from a crash.
const defaultPreviewTTL = 2 * time.Hour

// defaultPreviewSweepInterval is how often the orphan sweep runs. Previews are
// normally removed by their job's own deferred teardown; this exists for the
// cases that teardown cannot cover (a crash, a restart mid-job, a failed
// teardown), so it does not need to be frequent.
const defaultPreviewSweepInterval = 15 * time.Minute

// defaultPreviewSweepBatch bounds how many previews one sweep pass removes, so
// a sweep after a long outage cannot try to uninstall hundreds of releases in
// a single pass.
const defaultPreviewSweepBatch = 100

// previewAdmission is the queue-claim policy for ADR 003 §17's per-project cap
// on simultaneous temporary deployments. Disabled (zero) when the configured cap
// is negative; an unset or zero cap means the default (preview.DefaultMaxConcurrent),
// which also keeps the claim query free of the previews table.
func previewAdmission(cfg Config) queue.Admission {
	return queue.NewAdmission(cfg.maxConcurrentPreviews(), preview.EligibleKinds())
}

func (c Config) maxConcurrentPreviews() int {
	if c.MaxConcurrentPreviews < 0 {
		return 0
	}
	if c.MaxConcurrentPreviews == 0 {
		return preview.DefaultMaxConcurrent
	}
	return c.MaxConcurrentPreviews
}

func (c Config) previewTTL() time.Duration {
	if c.PreviewTTL <= 0 {
		return defaultPreviewTTL
	}
	return c.PreviewTTL
}

func (c Config) previewSweepInterval() time.Duration {
	if c.PreviewSweepInterval <= 0 {
		return defaultPreviewSweepInterval
	}
	return c.PreviewSweepInterval
}

// jobPreview is a live preview deployment plus what is needed to remove it.
type jobPreview struct {
	jobID     string
	namespace string
	release   string
	host      string
}

// ensureJobPreview brings up a preview-eligible job's ephemeral deployment and
// returns a handle for tearing it down (ADR 003 §10/§15).
//
// The release is the project's own chart applied under a preview-specific
// release name, with the project's env pushed first so the app starts with the
// same configuration its primary deployment has. Chart resources are named
// after the release, so the preview coexists with `primary` — and with other
// previews — inside the one per-project namespace (§5).
//
// The chart is the *same* one the primary deployment uses, but with an image
// override (issue #19): `buildPreviewImage` builds and pushes the primary
// repository's Dockerfile at this job's ref and the resulting reference is
// passed as `image.repository`/`image.tag`, so the preview serves the branch
// under construction rather than the image the chart declares. When there is
// nothing to build — no registry configured, no Dockerfile yet, or a ref not yet
// pushed — the override is absent and the preview falls back to the chart's image
// exactly as it did before that work landed.
func ensureJobPreview(
	ctx context.Context,
	client *k8s.Client,
	job *queue.Job,
	namespace string,
	cfg Config,
) (*jobPreview, error) {
	slug, err := cfg.APIClient.FetchProjectMetadata(ctx, job.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch project metadata: %w", err)
	}

	host := preview.Host(slug, job.Kind, job.ID, cfg.AppsDomain)
	release := preview.ReleaseName(job.ID)

	// The app the preview runs reads the same `project-env` Secret the
	// primary deployment does (the chart's envFrom), so it must exist before
	// Helm rolls the release. A preview is project-scoped, so it resolves
	// secrets at the project/org tier exactly as a deploy does — the trailing
	// "" is the feature id that does not apply.
	secrets, err := cfg.APIClient.FetchProjectSecrets(ctx, job.ProjectID, string(queue.KindDeploy), "")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch project secrets: %w", err)
	}
	if err := k8s.EnsureProjectSecret(ctx, client.Interface, namespace, secrets); err != nil {
		return nil, fmt.Errorf("failed to push project secrets: %w", err)
	}

	chrt, err := resolveChart(ctx, cfg, job.ProjectID)
	if err != nil {
		return nil, err
	}

	helmCfg, err := helm.NewConfiguration(client.Config, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize helm: %w", err)
	}

	values := map[string]interface{}{"secretsChecksum": secretsChecksum(secrets)}
	for key, value := range previewImageValues(buildPreviewImage(ctx, client, job, namespace, cfg)) {
		values[key] = value
	}

	if _, err := preview.Ensure(ctx, preview.Config{
		Clientset:        client.Interface,
		HelmConfig:       helmCfg,
		Namespace:        namespace,
		ReleaseName:      release,
		Host:             host,
		IngressClassName: cfg.IngressClassName,
		CertIssuerName:   cfg.CertIssuerName,
		Chart:            chrt,
		Values:           values,
	}); err != nil {
		return nil, err
	}

	return &jobPreview{jobID: job.ID, namespace: namespace, release: release, host: host}, nil
}

// stop removes the preview's Ingress and release and frees its slot in the
// API's registry. Every step is best-effort and logged rather than returned:
// this runs from a deferred call on the job's exit path, where a teardown
// failure must not rewrite the job's own outcome. The API registry is updated
// last, so a partial teardown leaves the row `active` and the orphan sweep
// picks it up on its next pass — failing closed towards "try again" rather than
// towards "forget it".
func (p *jobPreview) stop(ctx context.Context, client *k8s.Client, cfg Config) {
	if p == nil {
		return
	}

	helmCfg, err := helm.NewConfiguration(client.Config, p.namespace)
	if err != nil {
		log.Printf("worker: failed to init helm for preview teardown of job %s: %v", p.jobID, err)
	} else if err := preview.Teardown(ctx, preview.Config{
		Clientset:   client.Interface,
		HelmConfig:  helmCfg,
		Namespace:   p.namespace,
		ReleaseName: p.release,
		Host:        p.host,
	}); err != nil {
		log.Printf(
			"worker: WARNING failed to remove preview for job %s (release %s in %s): %v — the orphan sweep will retry",
			p.jobID, p.release, p.namespace, err,
		)
		return
	}

	if err := cfg.APIClient.ReportPreviewTeardown(ctx, p.jobID); err != nil {
		log.Printf(
			"worker: WARNING preview for job %s was removed but the API was not told (%v) — its slot stays occupied until the sweep catches up",
			p.jobID, err,
		)
	}
}

// startJobPreview is the job-lifecycle entry point: it decides whether a job
// gets a preview, brings one up if so, and reports the outcome to the API.
//
// A preview that cannot be created does NOT fail the job. The preview is
// additive to a job kind that worked before it existed, so turning a chart
// problem into a failed grill/build/test would be a regression in reliability
// for a feature those jobs do not depend on. The failure is logged and
// recorded in the registry, which is what keeps it visible instead of silent —
// the user sees a failed preview row rather than wondering where it went.
func startJobPreview(
	ctx context.Context,
	client *k8s.Client,
	job *queue.Job,
	namespace string,
	cfg Config,
) *jobPreview {
	if !preview.Eligible(job.Kind) || cfg.maxConcurrentPreviews() == 0 {
		return nil
	}

	handle, err := ensureJobPreview(ctx, client, job, namespace, cfg)
	if err != nil {
		log.Printf("worker: failed to create preview for job %s: %v", job.ID, err)
		// Best effort, and with a short deadline: this is the job's own
		// ability to describe a failure, so it must not be able to stall the
		// job it is describing.
		reportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		slug, slugErr := cfg.APIClient.FetchProjectMetadata(reportCtx, job.ProjectID)
		host := ""
		if slugErr == nil {
			host = preview.Host(slug, job.Kind, job.ID, cfg.AppsDomain)
		}
		if reportErr := cfg.APIClient.RegisterPreview(reportCtx, job.ID, host, err.Error()); reportErr != nil {
			log.Printf("worker: WARNING could not record preview failure for job %s: %v", job.ID, reportErr)
		}
		return nil
	}

	if err := cfg.APIClient.RegisterPreview(ctx, job.ID, handle.host, ""); err != nil {
		// The preview exists in the cluster but the API does not know. Tear it
		// down rather than leaking it: without a registry row the sweep cannot
		// see it, so leaving it up would be a permanent leak. The job proceeds
		// without a preview, which is the same degraded-but-working outcome as
		// a preview that failed outright.
		log.Printf("worker: WARNING failed to register preview for job %s: %v — removing it", job.ID, err)
		handle.stop(ctx, client, cfg)
		return nil
	}

	log.Printf("worker: preview for job %s live at %s", job.ID, handle.host)
	return handle
}

// SweepPreviews removes previews the API reports as stale (ADR 003 §17).
//
// This is the cleanup path for everything a job's own deferred teardown cannot
// cover: an Orchestrator restart mid-job, a crash that left the job `running`
// forever, or a teardown that failed. It is deliberately driven by the API's
// registry and not by listing cluster releases, because the registry is where
// "what should exist" lives — a release with no row is not distinguishable from
// one belonging to a job this replica has never seen.
//
// One project's failure does not stop the pass: a stale org kubeconfig or an
// unreachable cluster would otherwise block cleanup for every other project.
func SweepPreviews(ctx context.Context, cfg Config) {
	if cfg.APIClient == nil || cfg.Clusters == nil || cfg.maxConcurrentPreviews() == 0 {
		return
	}

	stale, err := cfg.APIClient.FetchStalePreviews(ctx, int(cfg.previewTTL().Seconds()), defaultPreviewSweepBatch)
	if err != nil {
		log.Printf("worker: preview sweep could not fetch its work list: %v", err)
		return
	}
	if len(stale) == 0 {
		return
	}

	log.Printf("worker: preview sweep removing %d stale preview(s)", len(stale))
	removed := 0
	for _, stalePreview := range stale {
		if ctx.Err() != nil {
			return
		}

		// Resolve walks project -> organization -> kubeconfig, so this
		// survives a project whose org cluster has changed since the preview
		// was created (the old cluster keeps an unreachable preview, which is
		// the best this can do — and the registry row is closed either way so
		// the cap is not held forever).
		client, err := cfg.Clusters.Resolve(ctx, &queue.Job{ProjectID: stalePreview.ProjectID})
		if err != nil {
			log.Printf(
				"worker: preview sweep could not resolve a cluster for project %s (job %s): %v",
				stalePreview.ProjectID, stalePreview.JobID, err,
			)
			continue
		}

		namespace := k8s.ProjectNamespace(stalePreview.ProjectID)
		helmCfg, err := helm.NewConfiguration(client.Config, namespace)
		if err != nil {
			log.Printf("worker: preview sweep could not init helm for %s: %v", namespace, err)
			continue
		}

		if err := preview.Teardown(ctx, preview.Config{
			Clientset:   client.Interface,
			HelmConfig:  helmCfg,
			Namespace:   namespace,
			ReleaseName: preview.ReleaseName(stalePreview.JobID),
			Host:        stalePreview.Host,
		}); err != nil {
			log.Printf("worker: preview sweep failed to remove preview for job %s: %v", stalePreview.JobID, err)
			continue
		}

		if err := cfg.APIClient.ReportPreviewTeardown(ctx, stalePreview.JobID); err != nil {
			log.Printf("worker: preview sweep removed job %s's preview but could not report it: %v", stalePreview.JobID, err)
			continue
		}
		removed++
	}

	log.Printf("worker: preview sweep removed %d of %d stale preview(s)", removed, len(stale))
}
