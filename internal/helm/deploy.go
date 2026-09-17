package helm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/storage/driver"
)

const defaultTimeout = 5 * time.Minute

// LoadPlaceholderChart exposes the embedded fallback chart to callers (see
// internal/worker) that need it when a project has no scaffolded chart yet.
func LoadPlaceholderChart() (*chart.Chart, error) {
	return loadPlaceholderChart()
}

// Deploy applies chrt to namespace as releaseName, using `helm upgrade
// --install` semantics (ADR 003 §13): installs the release if it doesn't
// exist yet, upgrades it in place otherwise. This mirrors the
// install-fallback check the `helm` CLI itself does for `upgrade --install`
// (action.Upgrade.Install is purely informative and does not trigger an
// install on its own). chrt is caller-resolved (internal/worker picks a
// project's real scaffolded chart or falls back to the embedded
// placeholder) — Deploy itself doesn't care where it came from.
//
// valueOverrides is shallow-merged over the chart's own defaults — used by
// the caller to pass a secretsChecksum (see internal/worker) so a Pod
// actually rolls when only the referenced project-env Secret's *content*
// changed. Kubernetes does not restart Pods on a Secret update by itself:
// envFrom values are read once at container start.
//
// Returns the release revision this call produced (ADR 022): Helm numbers
// revisions monotonically per release, and the revision *after* an operation
// is what identifies it — it is the handle a later rollback targets, so the
// caller records it rather than re-deriving it from history afterwards (a
// concurrent operation would otherwise make "the latest revision" ambiguous).
func Deploy(ctx context.Context, cfg *action.Configuration, namespace, releaseName string, chrt *chart.Chart, valueOverrides map[string]interface{}) (int, error) {
	values := mergeValues(chrt.Values, valueOverrides)

	histClient := action.NewHistory(cfg)
	histClient.Max = 1
	_, err := histClient.Run(releaseName)

	if errors.Is(err, driver.ErrReleaseNotFound) {
		install := action.NewInstall(cfg)
		install.ReleaseName = releaseName
		install.Namespace = namespace
		install.Wait = true
		install.Timeout = defaultTimeout
		rel, err := install.RunWithContext(ctx, chrt, values)
		if err != nil {
			return 0, fmt.Errorf("helm install failed: %w", err)
		}
		return rel.Version, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to check release history: %w", err)
	}

	upgrade := action.NewUpgrade(cfg)
	upgrade.Namespace = namespace
	upgrade.Install = true
	upgrade.Wait = true
	upgrade.Timeout = defaultTimeout
	rel, err := upgrade.RunWithContext(ctx, releaseName, chrt, values)
	if err != nil {
		return 0, fmt.Errorf("helm upgrade failed: %w", err)
	}
	return rel.Version, nil
}

// Rollback reverts releaseName in namespace to targetRevision (ADR 022),
// using `helm rollback` semantics (`action.Rollback`).
//
// Returns the revision the rollback *itself* produced, which is what makes a
// rollback different from a reset: Helm does not rewind the revision counter,
// so rolling back to revision 3 from revision 9 creates revision 10 whose
// content matches revision 3. Callers must record both numbers — the produced
// revision is the new "latest" (and the next rollback target), while
// targetRevision is what the operator asked for.
//
// targetRevision must be an existing revision of this release; Helm rejects
// an unknown revision, and refuses outright when the release is mid-operation
// (its status is pending-install/pending-upgrade/pending-rollback), which is
// the backstop for two deploys racing on one release (see ADR 022).
func Rollback(ctx context.Context, cfg *action.Configuration, namespace, releaseName string, targetRevision int) (int, error) {
	// Kept in the signature by symmetry with Deploy (same caller, same shape),
	// though Helm's rollback action has neither: action.Rollback carries no
	// namespace (it is fixed by the action.Configuration, which was Init'd
	// with it) and Run takes no context.
	_ = ctx
	_ = namespace
	rollback := action.NewRollback(cfg)
	rollback.Version = targetRevision
	rollback.Wait = true
	rollback.Timeout = defaultTimeout
	if err := rollback.Run(releaseName); err != nil {
		return 0, fmt.Errorf("helm rollback to revision %d failed: %w", targetRevision, err)
	}

	// action.Rollback.Run returns only an error, so the produced revision has
	// to be read back from the release history. Take the maximum revision
	// rather than the first element: action.History's Max field does not
	// actually trim the slice (its Run only delegates to the storage driver),
	// and the driver's ordering is not part of its contract, so index 0 would
	// be relying on unspecified behavior.
	history, err := action.NewHistory(cfg).Run(releaseName)
	if err != nil {
		return 0, fmt.Errorf("rollback succeeded but reading back the revision failed: %w", err)
	}
	latest := 0
	for _, rel := range history {
		if rel.Version > latest {
			latest = rel.Version
		}
	}
	if latest == 0 {
		return 0, errors.New("rollback succeeded but the release has no history")
	}
	return latest, nil
}

func mergeValues(base, overrides map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{}, len(base)+len(overrides))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	return merged
}
