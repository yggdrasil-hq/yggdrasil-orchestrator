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
func Deploy(ctx context.Context, cfg *action.Configuration, namespace, releaseName string, chrt *chart.Chart, valueOverrides map[string]interface{}) error {
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
		if _, err := install.RunWithContext(ctx, chrt, values); err != nil {
			return fmt.Errorf("helm install failed: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check release history: %w", err)
	}

	upgrade := action.NewUpgrade(cfg)
	upgrade.Namespace = namespace
	upgrade.Install = true
	upgrade.Wait = true
	upgrade.Timeout = defaultTimeout
	if _, err := upgrade.RunWithContext(ctx, releaseName, chrt, values); err != nil {
		return fmt.Errorf("helm upgrade failed: %w", err)
	}
	return nil
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
