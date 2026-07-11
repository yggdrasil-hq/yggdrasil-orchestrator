// Package apiclient is the Orchestrator's outbound call path back to the
// API — a shared-bearer-token-authenticated internal endpoint per need
// (decrypted project secrets at deploy time, ADR 003 §16; a spec_grill
// job's payload at claim time, ADR 006 item 5), rather than smuggling any
// of this through the Postgres job queue.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
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

type slugResponse struct {
	Slug string `json:"slug"`
}

// FetchProjectMetadata fetches a project's slug, used to build its primary
// deployment's ingress host (ADR 003 §15: <project-slug>.apps.<domain>).
func (c *Client) FetchProjectMetadata(ctx context.Context, projectID string) (slug string, err error) {
	url := fmt.Sprintf("%s/internal/projects/%s/slug", c.baseURL, projectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d fetching slug for project %s", resp.StatusCode, projectID)
	}

	var parsed slugResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("failed to decode slug response: %w", err)
	}
	return parsed.Slug, nil
}

// FeatureSpecRepo is one of a feature's linked repos, as needed to clone it
// (ADR 006 item 6 — consumed by base/entrypoint.sh's TARGET_REPOS).
type FeatureSpecRepo struct {
	CloneURL  string `json:"cloneUrl"`
	IsPrimary bool   `json:"isPrimary"`
}

// FeatureSpec is a job's feature payload: the feature title (the first
// prompt sent to Pi for spec_grill), its FeatureType, the project's linked
// repos, and a job-scoped GitHub installation token freshly minted by the
// API for this fetch — short-lived (ADR 005 §14), unlike the model config
// secrets, which is why it isn't delivered through
// FetchProjectSecrets/project_secrets.
//
// AdrMarkdown and Branch are only populated when kind is "feature_build"
// (ADR 010 item 1) — the approved ADR to implement and the feature branch
// already expected checked out, per feature_build/skills/implement/
// SKILL.md's own documented assumptions. Both are empty for spec_grill,
// which has no ADR yet and clones each repo's default branch.
//
// FeatureType ("normal" | "project_init") lets buildInitialPrompt
// (specgrill.go) pick which skill governs a spec_grill run explicitly,
// instead of the model inferring it from Title alone (ADR 008 item 1-2) —
// Title for a project_init feature is a fixed, non-descriptive string
// ("Project initialization"), so it carries no information the container
// could use to tell the two cases apart on its own.
type FeatureSpec struct {
	Title       string            `json:"title"`
	FeatureType string            `json:"featureType"`
	Repos       []FeatureSpecRepo `json:"repos"`
	GithubToken string            `json:"githubToken"`
	AdrMarkdown string            `json:"adrMarkdown"`
	Branch      string            `json:"branch"`
}

// FetchFeatureSpec fetches a job's feature payload (ADR 006 item 5, widened
// by ADR 010 item 1). A feature is scoped to its project, so both IDs are
// required — mirrors the API's own FeatureRepository.findById(projectId,
// featureId). kind (e.g. "spec_grill"/"feature_build") is passed through as
// a query param so the API can decide both the response shape (AdrMarkdown/
// Branch) and the minted token's scope (read-only for spec_grill,
// contents:write+pull-requests:write for feature_build) — this is the
// existing internal, bearer-token-only surface, not user-facing, so a
// caller-supplied kind carries no privilege-escalation risk beyond what an
// internal service is already trusted with.
func (c *Client) FetchFeatureSpec(ctx context.Context, projectID, featureID, kind string) (FeatureSpec, error) {
	reqURL := fmt.Sprintf("%s/internal/projects/%s/features/%s/spec?%s",
		c.baseURL, projectID, featureID, url.Values{"kind": {kind}}.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return FeatureSpec{}, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return FeatureSpec{}, fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return FeatureSpec{}, fmt.Errorf("API returned status %d fetching spec for feature %s", resp.StatusCode, featureID)
	}

	var spec FeatureSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return FeatureSpec{}, fmt.Errorf("failed to decode feature spec response: %w", err)
	}
	return spec, nil
}

type jobEventRequest struct {
	Type     string `json:"type"`
	Question string `json:"question,omitempty"`
	Markdown string `json:"markdown,omitempty"`
	Message  string `json:"message,omitempty"`
	Status   string `json:"status,omitempty"`
	PRUrl    string `json:"prUrl,omitempty"`
	Summary  string `json:"summary,omitempty"`
}

// PostJobEvent relays one curated event (ADR 006 items 7-8) from a running
// job's Pi RPC session to the API for persistence. Errors are the caller's
// to decide how to handle — a failed relay shouldn't necessarily fail the
// job itself, since the job's actual outcome (e.g. an ADR submitted) is
// independent of whether this side-channel post succeeded.
func (c *Client) PostJobEvent(ctx context.Context, jobID string, event rpc.CuratedEvent) error {
	body, err := json.Marshal(jobEventRequest{
		Type:     string(event.Type),
		Question: event.Question,
		Markdown: event.Markdown,
		Message:  event.Message,
		Status:   event.Status,
		PRUrl:    event.PRUrl,
		Summary:  event.Summary,
	})
	if err != nil {
		return fmt.Errorf("failed to encode job event: %w", err)
	}

	url := fmt.Sprintf("%s/internal/jobs/%s/events", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("API returned status %d posting event for job %s", resp.StatusCode, jobID)
	}
	return nil
}
