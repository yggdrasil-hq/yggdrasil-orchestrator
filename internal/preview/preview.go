// Package preview manages ephemeral "temporary deployments" — one per
// preview-eligible job, reachable at a per-run subdomain, torn down when the
// job ends (ADR 003 §10 and §15).
//
// It is the implementation of the half of ADR 003 that was designed but never
// built: §10 gives `spec_grill`, `feature_build` and `test_run` an ephemeral
// deployment each, §15 fixes the URL scheme
// (`<project-slug>-<kind>-<id>.preview.<domain>`), and §17 caps how many may
// run at once. Before this package none of that existed in code — the only
// trace was a PREVIEW_URL env var handed to a `test_run` pod pointing at a
// hostname nothing ever served (see worker.buildAgentEnv).
//
// Identity is computed here and nowhere else. The API stores whatever host
// this package reports rather than re-deriving it, so the URL scheme cannot
// drift between the two languages.
package preview

import (
	"context"
	"fmt"
	"strings"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/helm"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"k8s.io/client-go/kubernetes"
)

// DefaultMaxConcurrent limits simultaneously-running previews per project
// (ADR 003 §17's "initial default: 3"). The cap is enforced at job-claim time
// — see queue.Claim's admission clause — not here, so that a job which cannot
// be admitted stays `pending` instead of occupying a worker slot.
const DefaultMaxConcurrent = 3

// releasePrefix namespaces preview releases inside a project's namespace.
// Releases (and the chart's own resources, which are named after the release)
// are what make several previews coexist with each other and with the
// project's `primary` release in one namespace — ADR 003 §5 puts every one of
// them in the same per-project namespace on purpose.
const releasePrefix = "preview-"

// eligibleKinds is the set of job kinds ADR 003 §10 backs with a temporary
// deployment. Data, not a switch, so changing the policy is a one-line edit.
//
// `spec_grill` is included because §10 names it explicitly, though its value
// is the weakest of the three: a grill session is a read-only conversation
// against docs and never produces anything to look at, so its preview only
// ever serves the app as it already exists. It also competes for the same
// per-project slot budget as a build or a test run.
var eligibleKinds = map[queue.JobKind]bool{
	queue.KindSpecGrill:    true,
	queue.KindFeatureBuild: true,
	queue.KindTestRun:      true,
}

// Eligible reports whether a job kind gets a preview deployment.
func Eligible(kind queue.JobKind) bool {
	return eligibleKinds[kind]
}

// EligibleKinds returns the eligible kinds as a slice, for callers that need
// to pass the policy to something else (the queue's admission clause takes it
// as a query parameter rather than importing this package).
func EligibleKinds() []string {
	kinds := make([]string, 0, len(eligibleKinds))
	for _, kind := range []queue.JobKind{
		queue.KindSpecGrill,
		queue.KindFeatureBuild,
		queue.KindTestRun,
	} {
		if eligibleKinds[kind] {
			kinds = append(kinds, string(kind))
		}
	}
	return kinds
}

// ReleaseName is the Helm release name for a job's preview. Keyed on the job
// id, not the feature or branch: a retried build is a new job row (ADR 012),
// so it gets its own preview rather than colliding with the attempt it
// replaced — and the sweep below can map any preview release back to exactly
// one job.
func ReleaseName(jobID string) string {
	return releasePrefix + jobID
}

// jobIDFromReleaseName is ReleaseName's inverse, for the orphan sweep. ok is
// false for any release that is not a preview (including the project's
// `primary` release).
func jobIDFromReleaseName(name string) (string, bool) {
	if !strings.HasPrefix(name, releasePrefix) {
		return "", false
	}
	id := strings.TrimPrefix(name, releasePrefix)
	if id == "" {
		return "", false
	}
	return id, true
}

// KindToken renders a job kind for use in a hostname: ADR 003 §15 writes the
// scheme with hyphens (`<kind>`), while the job kind itself is underscored
// (`test_run`), and a DNS label cannot carry an underscore.
func KindToken(kind queue.JobKind) string {
	return strings.ReplaceAll(string(kind), "_", "-")
}

// Host is the preview hostname for one job (ADR 003 §15):
// `<project-slug>-<kind>-<id>.preview.<domain>`.
//
// A single DNS label, so the whole thing must fit in 63 characters. A project
// slug is capped well below that and a UUID is 36, so the sum can exceed it on
// a long slug; hostname truncates the *slug* (the only variable-length part)
// to keep the label legal rather than emitting something the ingress
// controller would reject.
func Host(slug string, kind queue.JobKind, jobID, appsDomain string) string {
	suffix := fmt.Sprintf("-%s-%s", KindToken(kind), jobID)
	const maxLabel = 63
	if max := maxLabel - len(suffix); len(slug) > max {
		slug = slug[:max]
	}
	// Trim a trailing hyphen the truncation may have left, which would
	// otherwise produce an empty or malformed label segment.
	slug = strings.TrimRight(slug, "-")
	return fmt.Sprintf("%s%s.preview.%s", slug, suffix, appsDomain)
}

// Config is everything needed to place a preview in a project's namespace.
type Config struct {
	Clientset        kubernetes.Interface
	HelmConfig       *action.Configuration
	Namespace        string
	ReleaseName      string
	Host             string
	IngressClassName string
	CertIssuerName   string
	Chart            *chart.Chart
	// Values overrides the chart's defaults. Preview releases are ephemeral
	// by definition, so callers use this to keep a preview from claiming
	// resources the always-on primary deployment needs — most importantly any
	// persistent volumes, which a preview must never mount: two releases
	// sharing one PVC is a data-corruption bug, not a scheduling one.
	Values map[string]interface{}
}

// Ensure creates or updates a job's preview release and its Ingress, and
// returns the host it is reachable at.
//
// Idempotent, because a retried job or a re-run of the same step must not fail
// on a preview that already exists. The Ingress is created with a
// preview-specific TLS secret name so cert-manager issues a certificate for
// this host rather than reusing the primary deployment's single-host
// certificate — one cert per preview is what §15's wildcard-certificate
// arrangement is for.
func Ensure(ctx context.Context, cfg Config) (string, error) {
	if _, err := helm.Deploy(ctx, cfg.HelmConfig, cfg.Namespace, cfg.ReleaseName, cfg.Chart, cfg.Values); err != nil {
		return "", fmt.Errorf("failed to deploy preview release %s: %w", cfg.ReleaseName, err)
	}

	if err := k8s.EnsureNamedIngress(
		ctx,
		cfg.Clientset,
		cfg.ReleaseName,
		cfg.Namespace,
		cfg.Host,
		// The chart names its Service after the release (see the scaffolded
		// chart's service.yaml), so the release name is also the Service name.
		cfg.ReleaseName,
		80,
		cfg.IngressClassName,
		cfg.ReleaseName+"-tls",
		cfg.CertIssuerName,
	); err != nil {
		return "", fmt.Errorf("failed to ensure preview ingress: %w", err)
	}

	return cfg.Host, nil
}

// Teardown removes a preview's Ingress and Helm release.
//
// Both steps are attempted even if the first fails, and neither treats
// "already gone" as an error: teardown runs from a deferred call on the job's
// exit path, where it must be safe to invoke for a preview that was never
// created (a job that failed before Ensure ran) and for one whose resources
// something else already removed.
func Teardown(ctx context.Context, cfg Config) error {
	ingressErr := k8s.DeleteIngress(ctx, cfg.Clientset, cfg.Namespace, cfg.ReleaseName)
	uninstallErr := teardownRelease(ctx, cfg.HelmConfig, cfg.ReleaseName)

	if ingressErr != nil {
		return fmt.Errorf("failed to delete preview ingress: %w", ingressErr)
	}
	return uninstallErr
}

// teardownRelease uninstalls a preview release, tolerating one that does not
// exist — a job that never reached Ensure, or a sweep racing the job's own
// deferred teardown.
func teardownRelease(ctx context.Context, cfg *action.Configuration, releaseName string) error {
	if err := helm.Uninstall(ctx, cfg, releaseName); err != nil {
		if isReleaseNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// isReleaseNotFound reports whether err is Helm's "no such release" condition,
// which action.NewUninstall surfaces as a plain error string rather than a
// typed sentinel.
func isReleaseNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "release: not found")
}

// ListOrphans returns the job ids of preview releases in the namespace that
// callers should tidy up, i.e. every preview release (primary is not one).
//
// Used by the orphan sweep when the API has no preview row for a release — a
// job that died before registering, or whose registration was lost. Callers
// decide which of the returned ids still deserve to exist by consulting the
// API's preview registry, since that is where "what should exist" lives.
func ListOrphans(ctx context.Context, cfg *action.Configuration) ([]string, error) {
	releases, err := helm.List(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to list releases for preview sweep: %w", err)
	}

	ids := make([]string, 0, len(releases))
	for _, name := range releases {
		if jobID, ok := jobIDFromReleaseName(name); ok {
			ids = append(ids, jobID)
		}
	}
	return ids, nil
}
