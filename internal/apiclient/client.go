// Package apiclient is the Orchestrator's first (and so far only) outbound
// call path back to the API — used at deploy time to fetch decrypted
// project secrets (ADR 003 §16) via a shared-bearer-token-authenticated
// internal endpoint, rather than smuggling plaintext through the Postgres
// job queue.
package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL:    baseURL,
		token:      token,
		httpClient: &http.Client{},
	}
}

type secretsResponse struct {
	Secrets map[string]string `json:"secrets"`
}

// FetchProjectSecrets fetches a project's decrypted env vars/secrets. The
// returned map is empty (not an error) if the project has none configured.
func (c *Client) FetchProjectSecrets(ctx context.Context, projectID string) (map[string]string, error) {
	url := fmt.Sprintf("%s/internal/projects/%s/secrets", c.baseURL, projectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d fetching secrets for project %s", resp.StatusCode, projectID)
	}

	var parsed secretsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to decode secrets response: %w", err)
	}
	return parsed.Secrets, nil
}

type chartResponse struct {
	Files map[string]string `json:"files"`
}

// FetchProjectChart fetches a project's scaffolded Helm chart (ADR 003
// §12) — files keyed by path relative to the chart root (e.g.
// "Chart.yaml", "templates/deployment.yaml"). found is false (not an
// error) if the project has no chart scaffolded yet, so callers can fall
// back to the Orchestrator's embedded placeholder chart.
func (c *Client) FetchProjectChart(ctx context.Context, projectID string) (files map[string]string, found bool, err error) {
	url := fmt.Sprintf("%s/internal/projects/%s/chart", c.baseURL, projectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("API returned status %d fetching chart for project %s", resp.StatusCode, projectID)
	}

	var parsed chartResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, fmt.Errorf("failed to decode chart response: %w", err)
	}
	return parsed.Files, true, nil
}
