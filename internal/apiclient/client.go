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
	"strconv"

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

// InternalEndpoint returns the API URL and bearer token needed by a
// script_test_run container to submit its canonical report. The same
// short-lived internal credentials already used for the Orchestrator's
// payload fetch are scoped by the API to the specific job id in the event
// endpoint.
func (c *Client) InternalEndpoint() (baseURL, token string) {
	return c.baseURL, c.token
}

type secretsResponse struct {
	Secrets map[string]string `json:"secrets"`
}

// FetchProjectSecrets fetches a project's decrypted env vars/secrets, with
// model config (MODEL_BASE_URL/MODEL_API_KEY/MODEL_ID) resolved for the given
// job kind (ADR 018 — provider/model catalog + per-job-kind defaults). The
// returned map is empty (not an error) if the project has none configured.
//
// featureID is the feature a feature-owned job belongs to, and is optional:
// pass "" for a job that has no feature (a scheduled test_run, a deploy). It
// is sent only when present, and the API ignores it unless it belongs to the
// project, so an empty featureID produces exactly the request this call made
// before the feature tier existed — the two sides can be deployed in either
// order (ADR 018 amendment, issue #5).
func (c *Client) FetchProjectSecrets(ctx context.Context, projectID string, jobKind string, featureID string) (map[string]string, error) {
	query := url.Values{"jobKind": {jobKind}}
	if featureID != "" {
		query.Set("featureId", featureID)
	}
	reqURL := fmt.Sprintf("%s/internal/projects/%s/secrets?%s", c.baseURL, projectID, query.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
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

type organizationClusterResponse struct {
	OrganizationID string `json:"organizationId"`
	Kubeconfig     string `json:"kubeconfig"`
}

// FetchOrganizationCluster resolves the Organization a project belongs to and
// returns that org's configured (decrypted) Kubernetes kubeconfig — ADR 016
// items 11-13. There is no platform-default cluster: the API returns 409 when
// the org has no cluster configured, which the Orchestrator must treat as a
// hard failure (not a fallback to some static client).
func (c *Client) FetchOrganizationCluster(ctx context.Context, projectID string) (organizationID, kubeconfig string, err error) {
	url := fmt.Sprintf("%s/internal/projects/%s/organization-cluster", c.baseURL, projectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("API returned status %d resolving cluster for project %s", resp.StatusCode, projectID)
	}

	var parsed organizationClusterResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", "", fmt.Errorf("failed to decode organization-cluster response: %w", err)
	}
	return parsed.OrganizationID, parsed.Kubeconfig, nil
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
// AdrMarkdown and Branch are populated for feature_build and test_run
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
	Title              string            `json:"title"`
	FeatureType        string            `json:"featureType"`
	ProjectName        string            `json:"projectName"`
	ProjectDescription string            `json:"projectDescription"`
	Repos              []FeatureSpecRepo `json:"repos"`
	GithubToken        string            `json:"githubToken"`
	AdrMarkdown        string            `json:"adrMarkdown"`
	Branch             string            `json:"branch"`
	TestID             string            `json:"testId"`
	TestMarkdown       string            `json:"testMarkdown"`
	Ref                string            `json:"ref"`
	DesignName         string            `json:"name"`
	DesignSlug         string            `json:"slug"`
	DesignDescription  string            `json:"description"`
	ScriptName         string            `json:"scriptName"`
	SpecContext        *SpecGrillContext `json:"specContext,omitempty"`
}

type DesignSnapshotContext struct {
	SessionID string            `json:"sessionId"`
	Snapshot  map[string]string `json:"snapshot"`
}

type SpecGrillContext struct {
	PreviousAdrMarkdown    string                    `json:"previousAdrMarkdown"`
	GrillTranscriptSummary string                    `json:"grillTranscriptSummary"`
	KickbackReason         string                    `json:"kickbackReason"`
	RequestedActionItems   []rpc.RequestedActionItem `json:"requestedActionItems"`
	DesignSnapshots        []DesignSnapshotContext   `json:"designSnapshots"`
	// RestartFromMessage marks a per-message "restart from here" (ADR 024):
	// the user rewound the interview to an earlier turn and asked for the
	// conversation from that point to be redone. It changes only how the prompt
	// is worded — the same fields carry the same kind of content either way —
	// because a rewind has no kickback reason and its transcript ends
	// mid-conversation with no conclusion, so describing it as a continuation
	// of a blocked implementation would be actively misleading.
	RestartFromMessage bool `json:"restartFromMessage,omitempty"`
}

// FetchDesignSpec fetches the project-scoped payload for a design_grill job.
func (c *Client) FetchDesignSpec(ctx context.Context, projectID, sessionID string) (FeatureSpec, error) {
	reqURL := fmt.Sprintf("%s/internal/projects/%s/designs/%s/spec", c.baseURL, projectID, sessionID)
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
		return FeatureSpec{}, fmt.Errorf("API returned status %d fetching design session %s", resp.StatusCode, sessionID)
	}
	var spec FeatureSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return FeatureSpec{}, fmt.Errorf("failed to decode design session response: %w", err)
	}
	return spec, nil
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
func (c *Client) FetchFeatureSpec(ctx context.Context, projectID, featureID, kind string, testIDs ...string) (FeatureSpec, error) {
	query := url.Values{"kind": {kind}}
	if len(testIDs) > 0 && testIDs[0] != "" {
		query.Set("testId", testIDs[0])
	}
	if len(testIDs) > 1 && testIDs[1] != "" {
		query.Set("scriptName", testIDs[1])
	}
	if len(testIDs) > 2 && testIDs[2] != "" {
		query.Set("jobId", testIDs[2])
	}
	reqURL := fmt.Sprintf("%s/internal/projects/%s/features/%s/spec?%s",
		c.baseURL, projectID, featureID, query.Encode())
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

func (c *Client) FetchTestSpec(ctx context.Context, projectID, testID, ref string) (FeatureSpec, error) {
	query := url.Values{"ref": {ref}}
	reqURL := fmt.Sprintf("%s/internal/projects/%s/tests/%s/spec?%s",
		c.baseURL, projectID, testID, query.Encode())
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
		return FeatureSpec{}, fmt.Errorf("API returned status %d fetching spec for test %s", resp.StatusCode, testID)
	}
	var spec FeatureSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return FeatureSpec{}, fmt.Errorf("failed to decode test spec response: %w", err)
	}
	return spec, nil
}

// DeployResultInput is what the Orchestrator reports back after a `deploy`
// or `rollback` job reaches a terminal state (ADR 022). Deploy jobs were
// previously silent: only the `jobs` row recorded the outcome, so the Helm
// revision a deploy produced existed only inside the cluster and there was
// nothing to roll back *to*.
//
// Revision is the revision the operation produced (Helm numbers revisions
// monotonically; a rollback does not rewind that counter, it creates a new
// revision whose content matches the target). TargetRevision is set only for
// a rollback — the earlier revision the operator asked for. LastError is
// empty on success.
type DeployResultInput struct {
	Revision       int
	TargetRevision *int
	LastError      string
}

// ReportDeployResult records the outcome of a deploy/rollback job so the API
// can persist it in the project's deploy ledger (ADR 022). Errors are the
// caller's to decide how to handle: like PostJobEvent, a failed report is a
// visibility gap rather than a job failure — the deployment itself either
// happened or didn't, independent of whether this side-channel post landed.
func (c *Client) ReportDeployResult(ctx context.Context, jobID string, result DeployResultInput) error {
	body, err := json.Marshal(struct {
		Revision       int    `json:"revision"`
		TargetRevision *int   `json:"targetRevision,omitempty"`
		LastError      string `json:"lastError,omitempty"`
	}{
		Revision:       result.Revision,
		TargetRevision: result.TargetRevision,
		LastError:      result.LastError,
	})
	if err != nil {
		return fmt.Errorf("failed to encode deploy result: %w", err)
	}

	reqURL := fmt.Sprintf("%s/internal/jobs/%s/deploy-result", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
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
		return fmt.Errorf("API returned status %d reporting deploy result for job %s", resp.StatusCode, jobID)
	}
	return nil
}

type jobEventRequest struct {
	Type     string `json:"type"`
	Question string `json:"question,omitempty"`
	Markdown string `json:"markdown,omitempty"`
	Message  string `json:"message,omitempty"`
	Status   string `json:"status,omitempty"`
	PRUrl    string `json:"prUrl,omitempty"`
	Summary  string `json:"summary,omitempty"`
	// Verdict is set for submit_review (ADR 015 item 14-16 / Track B6): the
	// internal Agentic Review verdict "approved" | "changes_requested".
	Verdict string `json:"verdict,omitempty"`
	// ActionItems is set for request_action_item (ADR 015 item 8 / Track B3):
	// the needed items the blocked implement skill reported, or the batch
	// returned by submit_adr.
	ActionItems      []rpc.RequestedActionItem `json:"actionItems,omitempty"`
	TestName         string                    `json:"testName,omitempty"`
	TestStatus       string                    `json:"testStatus,omitempty"`
	TestDetails      string                    `json:"testDetails,omitempty"`
	ScreenshotPath   string                    `json:"screenshotPath,omitempty"`
	Passed           *int                      `json:"passed,omitempty"`
	Failed           *int                      `json:"failed,omitempty"`
	Skipped          *int                      `json:"skipped,omitempty"`
	Total            *int                      `json:"total,omitempty"`
	CoveragePercent  *float64                  `json:"coveragePercent,omitempty"`
	FailingTests     []string                  `json:"failingTests,omitempty"`
	RecordingPath    string                    `json:"recordingPath,omitempty"`
	Snapshot         map[string]string         `json:"snapshot,omitempty"`
	HasDesignSurface *bool                     `json:"hasDesignSurface,omitempty"`
}

// PostJobEvent relays one curated event (ADR 006 items 7-8) from a running
// job's Pi RPC session to the API for persistence. Errors are the caller's
// to decide how to handle — a failed relay shouldn't necessarily fail the
// job itself, since the job's actual outcome (e.g. an ADR submitted) is
// independent of whether this side-channel post succeeded.
func (c *Client) PostJobEvent(ctx context.Context, jobID string, event rpc.CuratedEvent) error {
	body, err := json.Marshal(jobEventRequest{
		Type:             string(event.Type),
		Question:         event.Question,
		Markdown:         event.Markdown,
		Message:          event.Message,
		Status:           event.Status,
		PRUrl:            event.PRUrl,
		Summary:          event.Summary,
		Verdict:          event.Verdict,
		ActionItems:      event.ActionItems,
		TestName:         event.TestName,
		TestStatus:       event.TestStatus,
		TestDetails:      event.TestDetails,
		ScreenshotPath:   event.ScreenshotPath,
		Passed:           event.Passed,
		Failed:           event.Failed,
		Skipped:          event.Skipped,
		Total:            event.Total,
		CoveragePercent:  event.CoveragePercent,
		FailingTests:     event.FailingTests,
		RecordingPath:    event.RecordingPath,
		Snapshot:         event.Snapshot,
		HasDesignSurface: event.HasDesignSurface,
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

// JobUsage is one job's token/cost accounting (ADR 023), as reported by Pi's
// own get_session_stats and posted once the job's session ends. Every count is
// provider-reported — Yggdrasil never tokenizes or estimates.
//
// CostUSD and DurationMs are pointers so "not reported" stays distinguishable
// from a genuine zero all the way into the database: a model billed at nothing
// is a real fact, whereas a missing figure must be stored NULL rather than as
// free / instantaneous.
type JobUsage struct {
	// ModelID is the literal model-id string the job ran with (the pod's
	// MODEL_ID env var), which is what actually served the run. The API
	// resolves the provider/tier itself; the Orchestrator never sees a
	// provider name, and never sends a key.
	ModelID          string   `json:"modelId,omitempty"`
	InputTokens      int64    `json:"inputTokens"`
	OutputTokens     int64    `json:"outputTokens"`
	CacheReadTokens  int64    `json:"cacheReadTokens"`
	CacheWriteTokens int64    `json:"cacheWriteTokens"`
	TotalTokens      int64    `json:"totalTokens"`
	CostUSD          *float64 `json:"costUsd"`
	DurationMs       *int64   `json:"durationMs"`
}

// PostJobUsage reports a finished job's token/cost accounting (ADR 023).
// Like PostJobEvent this is a side channel: the caller decides how to handle
// an error, and a failure here must never change the job's outcome, since the
// job already succeeded or failed on its own terms by the time this is sent.
func (c *Client) PostJobUsage(ctx context.Context, jobID string, usage JobUsage) error {
	body, err := json.Marshal(usage)
	if err != nil {
		return fmt.Errorf("failed to encode job usage: %w", err)
	}

	url := fmt.Sprintf("%s/internal/jobs/%s/usage", c.baseURL, jobID)
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
		return fmt.Errorf("API returned status %d posting usage for job %s", resp.StatusCode, jobID)
	}
	return nil
}

// PostJobRecording uploads a job's screen recording (ADR 029) as the raw
// bytes, not as JSON.
//
// Binary rather than base64-in-JSON because a recording is orders of magnitude
// larger than every other payload this client sends: base64 would inflate it by
// a third in transit for no benefit, and the API's JSON body parser has a 2 MB
// limit that would reject most recordings before the handler ever ran. The API
// route carries its own raw parser at the recording size cap instead.
//
// Like PostJobUsage this is a side channel: the caller decides what an error
// means, and a failure here must never change the job's outcome. The API
// answers 202 (not 4xx) for an artifact it declines to store — oversized, wrong
// format, or a job kind that does not record — so a rejection is reported as a
// normal error here and logged by the caller rather than treated as a fault. A
// 201 means stored.
func (c *Client) PostJobRecording(
	ctx context.Context,
	jobID string,
	contentType string,
	data []byte,
) error {
	url := fmt.Sprintf("%s/internal/jobs/%s/recording", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", contentType)
	// The API enforces the authoritative cap; setting a body length lets it
	// refuse an over-size artifact before buffering rather than mid-stream.
	req.ContentLength = int64(len(data))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		return nil
	}
	// 202 is the API's "declined, and here is why" — worth surfacing verbatim,
	// since the reason is what an operator needs to act on (raise the cap, fix
	// the format) and is not an error in this service.
	if resp.StatusCode == http.StatusAccepted {
		var declined struct {
			Reason string `json:"reason"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&declined); decodeErr == nil && declined.Reason != "" {
			return fmt.Errorf("API declined the recording for job %s: %s", jobID, declined.Reason)
		}
		return fmt.Errorf("API declined the recording for job %s", jobID)
	}
	return fmt.Errorf("API returned status %d posting a recording for job %s", resp.StatusCode, jobID)
}

// StalePreview is one entry of the API's stale-preview work list (ADR 003
// §17): a preview the Orchestrator should tear down, either because its job is
// no longer running or because it has outlived the TTL.
type StalePreview struct {
	JobID     string `json:"jobId"`
	ProjectID string `json:"projectId"`
	Host      string `json:"host"`
}

// RegisterPreview records a job's ephemeral preview deployment (ADR 003 §15).
// Pass a non-empty errMsg when the preview could not be brought up: the API
// records a failure rather than an active preview, so a broken preview does not
// occupy one of the project's §17 slots and shows up as failed in the UI
// instead of silently missing.
//
// The API derives the project from the job row, so there is nothing else to
// send — a caller cannot attribute a preview to another project.
func (c *Client) RegisterPreview(ctx context.Context, jobID, host, errMsg string) error {
	body, err := json.Marshal(struct {
		Host  string `json:"host"`
		Error string `json:"error,omitempty"`
	}{Host: host, Error: errMsg})
	if err != nil {
		return fmt.Errorf("failed to encode preview: %w", err)
	}

	reqURL := fmt.Sprintf("%s/internal/jobs/%s/preview", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
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
		return fmt.Errorf("API returned status %d registering preview for job %s", resp.StatusCode, jobID)
	}
	return nil
}

// ReportPreviewTeardown tells the API a job's preview is gone, which is what
// frees its ADR 003 §17 slot. Idempotent server-side, so the job's own
// deferred teardown and the orphan sweep can both call it.
func (c *Client) ReportPreviewTeardown(ctx context.Context, jobID string) error {
	reqURL := fmt.Sprintf("%s/internal/jobs/%s/preview/teardown", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader([]byte("{}")))
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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status %d reporting preview teardown for job %s", resp.StatusCode, jobID)
	}
	return nil
}

// FetchStalePreviews returns previews the API considers safe to remove.
// ttlSeconds bounds how long a preview may outlive its job when nothing else
// knows the job is gone — a hard-crashed job stays 'running' forever, so job
// status alone cannot collect its preview.
func (c *Client) FetchStalePreviews(ctx context.Context, ttlSeconds, limit int) ([]StalePreview, error) {
	query := url.Values{
		"ttlSeconds": {strconv.Itoa(ttlSeconds)},
		"limit":      {strconv.Itoa(limit)},
	}
	reqURL := fmt.Sprintf("%s/internal/previews/stale?%s", c.baseURL, query.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
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
		return nil, fmt.Errorf("API returned status %d fetching stale previews", resp.StatusCode)
	}

	var parsed struct {
		Previews []StalePreview `json:"previews"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to decode stale previews response: %w", err)
	}
	return parsed.Previews, nil
}
