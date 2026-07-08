package helm

import (
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
)

// The Phase 3 (ADR 003) stand-in for a project's real, per-project Helm
// chart — that chart is scaffolded into the project's primary repo during
// project_init, which isn't built yet (tracked as Phase 3c). This lets the
// Orchestrator prove "deploy job -> helm upgrade --install -> running
// primary deployment" mechanically, independent of where the chart comes
// from.
//
//go:embed charts/placeholder
var placeholderChartFS embed.FS

const placeholderChartRoot = "charts/placeholder"

func loadPlaceholderChart() (*chart.Chart, error) {
	var files []*loader.BufferedFile
	err := fs.WalkDir(placeholderChartFS, placeholderChartRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := placeholderChartFS.ReadFile(path)
		if err != nil {
			return err
		}
		relPath := strings.TrimPrefix(path, placeholderChartRoot+"/")
		files = append(files, &loader.BufferedFile{Name: relPath, Data: data})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded placeholder chart: %w", err)
	}

	c, err := loader.LoadFiles(files)
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded placeholder chart: %w", err)
	}
	return c, nil
}
