package helm

import (
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
)

// A fallback for projects with no chart scaffolded yet (Phase 3c scaffolds
// a real chart into each project's primary repo during project creation;
// this stands in when that hasn't happened — an old project, or a failed
// scaffold) — so deploys never hard-fail due to a missing chart.
//
//go:embed charts/placeholder
var placeholderChartFS embed.FS

const placeholderChartRoot = "charts/placeholder"

func loadPlaceholderChart() (*chart.Chart, error) {
	files := make(map[string][]byte)
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
		files[relPath] = data
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded placeholder chart: %w", err)
	}

	c, err := LoadChartFromFiles(files)
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded placeholder chart: %w", err)
	}
	return c, nil
}

// LoadChartFromFiles builds a Helm chart from an in-memory file map keyed
// by path relative to the chart root (e.g. "Chart.yaml",
// "templates/deployment.yaml") — shared by the embedded placeholder loader
// above and the real per-project chart fetched via apiclient.FetchProjectChart.
func LoadChartFromFiles(files map[string][]byte) (*chart.Chart, error) {
	bufferedFiles := make([]*loader.BufferedFile, 0, len(files))
	for name, data := range files {
		bufferedFiles = append(bufferedFiles, &loader.BufferedFile{Name: name, Data: data})
	}

	c, err := loader.LoadFiles(bufferedFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to load chart: %w", err)
	}
	return c, nil
}
