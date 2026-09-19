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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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
	// ForkFromJobID and ForkEntryID mark ADR 032 item 3's "resume from here":
	// the run is seeded from a *stored Pi session* rather than from a transcript,
	// and branches at a Pi entry id.
	//
	// **Two ids, because neither is derivable from the other.** ForkFromJobID names
	// the finished job whose session artifact is to be restored — that is where the
	// bytes come from, and it is a Yggdrasil job id. ForkEntryID is Pi's own entry
	// id for the branch point, from `get_fork_messages`; the two live in different
	// id spaces and the ADR's trade-offs say so explicitly (an id *mapping* between
	// them is exactly what item 2 rejects). Both are required together: a fork with
	// no entry id has no branch point, and one with no job id has no bytes.
	//
	// Present only on a fork. Its absence is what keeps ADR 024's seeded rewind on
	// its existing path — the two controls are separate gestures and share a job
	// kind, not an implementation.
	ForkFromJobID string `json:"forkFromJobId,omitempty"`
	ForkEntryID   string `json:"forkEntryId,omitempty"`
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
	// Findings is set for submit_review when the reviewing agent listed its
	// issues per location (issue #73).
	//
	// A pointer to a slice, like the CuratedEvent field it comes from, and
	// `omitempty` so an absent list sends **no key at all**. The API reads
	// `undefined` as SQL NULL and `[]` as an empty jsonb array, and treats those
	// as different answers ("prose, count unknowable" versus "structured, none") —
	// so a plain slice here would make every prose review arrive claiming zero
	// findings. This is the hop #38 lost its `options` at, one struct after
	// `Translate` had already been fixed.
	Findings *[]rpc.ReviewFinding `json:"findings,omitempty"`
	// ActionItems is set for request_action_item (ADR 015 item 8 / Track B3):
	// the needed items the blocked implement skill reported, or the batch
	// returned by submit_adr.
	ActionItems     []rpc.RequestedActionItem `json:"actionItems,omitempty"`
	TestName        string                    `json:"testName,omitempty"`
	TestStatus      string                    `json:"testStatus,omitempty"`
	TestDetails     string                    `json:"testDetails,omitempty"`
	ScreenshotPath  string                    `json:"screenshotPath,omitempty"`
	Passed          *int                      `json:"passed,omitempty"`
	Failed          *int                      `json:"failed,omitempty"`
	Skipped         *int                      `json:"skipped,omitempty"`
	Total           *int                      `json:"total,omitempty"`
	CoveragePercent *float64                  `json:"coveragePercent,omitempty"`
	FailingTests    []string                  `json:"failingTests,omitempty"`
	RecordingPath   string                    `json:"recordingPath,omitempty"`
	// SkipReason is set for submit_test_report when a group was skipped rather
	// than run: "no_script" or "runner_unavailable" (issue #53). The API's schema
	// is a closed enum over exactly these two values, so this must stay optional —
	// absent is the pre-existing behaviour for every runner that does not report it.
	SkipReason string `json:"skipReason,omitempty"`
	// ForkStage is set for fork_failed (ADR 032 item 3): which of the fork's three
	// steps stopped. A closed set the API validates, sent only on that one event.
	ForkStage        string            `json:"forkStage,omitempty"`
	Snapshot         map[string]string `json:"snapshot,omitempty"`
	HasDesignSurface *bool             `json:"hasDesignSurface,omitempty"`
	// QuestionHeader / QuestionMultiSelect / QuestionOptions are the structured
	// half of an ask_user question (issue #38).
	//
	// **These were the second place the fields were dropped**, and the reason the
	// round-trip test walks all the way to this marshalled body rather than
	// stopping at `rpc.Translate`: this struct is an explicit field list, so a
	// field that survives translation and is missing here is discarded with no
	// error — the same shape as #59's verdict, one hop further along.
	//
	// Pointers for the reason `rpc.CuratedEvent` documents at length: the API reads
	// the *presence* of `options` as "render a picker", so an absent field has to
	// stay absent rather than becoming `false` or `[]`.
	QuestionHeader      string                `json:"header,omitempty"`
	QuestionMultiSelect *bool                 `json:"multiSelect,omitempty"`
	QuestionOptions     *[]rpc.QuestionOption `json:"options,omitempty"`
}

// PostJobEvent relays one curated event (ADR 006 items 7-8) from a running
// job's Pi RPC session to the API for persistence. Errors are the caller's
// to decide how to handle — a failed relay shouldn't necessarily fail the
// job itself, since the job's actual outcome (e.g. an ADR submitted) is
// independent of whether this side-channel post succeeded.
func (c *Client) PostJobEvent(ctx context.Context, jobID string, event rpc.CuratedEvent) error {
	body, err := json.Marshal(jobEventRequest{
		Type:                string(event.Type),
		Question:            event.Question,
		Markdown:            event.Markdown,
		Message:             event.Message,
		Status:              event.Status,
		PRUrl:               event.PRUrl,
		Summary:             event.Summary,
		Verdict:             event.Verdict,
		Findings:            event.Findings,
		ActionItems:         event.ActionItems,
		TestName:            event.TestName,
		TestStatus:          event.TestStatus,
		TestDetails:         event.TestDetails,
		ScreenshotPath:      event.ScreenshotPath,
		Passed:              event.Passed,
		Failed:              event.Failed,
		Skipped:             event.Skipped,
		Total:               event.Total,
		CoveragePercent:     event.CoveragePercent,
		FailingTests:        event.FailingTests,
		RecordingPath:       event.RecordingPath,
		SkipReason:          event.SkipReason,
		ForkStage:           event.ForkStage,
		Snapshot:            event.Snapshot,
		HasDesignSurface:    event.HasDesignSurface,
		QuestionHeader:      event.QuestionHeader,
		QuestionMultiSelect: event.QuestionMultiSelect,
		QuestionOptions:     event.QuestionOptions,
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

// SessionArtifact is what the Orchestrator reports about one job's Pi session
// file (ADR 032 item 1), and it is the cross-service contract the next wave's
// reader depends on.
//
// It lives here, in the apiclient package, rather than in internal/worker where
// it is produced: this is the *wire* shape, and the two fields a reader branches
// on (Outcome, and whether bytes accompanied it) are part of the API's contract,
// not of the collection logic. The worker has its own `sessionArtifact` with the
// same JSON tags and an explicit conversion, so a change to one is a compile
// error at the other rather than a silent divergence.
//
// The bytes are deliberately **not** a field: they are the request body (see
// PostJobSession), because a session is megabytes of JSONL and base64-in-JSON
// would inflate it by a third for nothing.
type SessionArtifact struct {
	JobID string `json:"jobId"`
	// Outcome is one of "collected", "not_collected", "unavailable", "disabled".
	//
	// A string rather than an enum type here on purpose: the API stores and
	// returns it verbatim, so a Go type would buy nothing and would need a
	// mapping table on the way out. The worker defines the named constants.
	Outcome string `json:"outcome"`
	// SessionID is Pi's own session id — what a `switch_session` resumes by — and
	// is empty when Pi reported none.
	SessionID string `json:"sessionId,omitempty"`
	// There is deliberately **no ByteSize field.** The bytes are the request body,
	// so the API knows the size from what it receives — and that number is
	// authoritative, where a value sent alongside could disagree with the body it
	// describes. An earlier draft carried one and never sent it, which is the
	// "declared, marshalled, discarded" shape this suite has already had to fix
	// several times; the field is gone rather than wired up, because the second
	// source is the thing to avoid.
	// PodFilePath is the pod-local path the artifact was read from — evidence of
	// *which* file was read, not a storage key (the API derives the key from the
	// job id, the way recordingKey does). Worthless once the pod is gone, and
	// carried anyway for the one case it is for: a session that turns out not to
	// hold what an operator expected.
	PodFilePath string `json:"podFilePath,omitempty"`
}

// PostJobSession reports what became of a finished job's Pi session (ADR 032
// item 1), uploading the JSONL bytes when there are any.
//
// **One call, always made, even when nothing was collected** — and that is the
// design rather than an accident. ADR 032 item 5 requires "this run has no
// session" to be distinguishable from "this run's session could not be
// retrieved", and a route that is only called on success cannot express the
// difference. So the outcome travels with every call and the body is simply empty
// when the outcome is one of the failing three.
//
// The bytes are raw, matching PostJobRecording's shape and for the same two
// reasons with the sizes shifted down: a session is text but routinely megabytes
// (Pi appends tool results verbatim), so base64 would inflate it by a third for
// nothing, and the API's JSON body parser has a 2 MB limit a long grill would
// exceed before the handler ever ran. The API route carries its own raw parser at
// the session size cap instead.
//
// The artifact's small fields ride as query parameters rather than as a JSON
// envelope, because the body is already spoken for by the bytes — the same
// arrangement, and the same reasoning, as PostJobScreenshot's `stepName`.
//
// Like PostJobUsage and PostJobRecording this is a side channel: the caller
// decides what an error means, and a failure here must never change the job's
// outcome. The API answers 202 (not 4xx) for an artifact it declines to store —
// oversized, or a job kind that does not persist sessions — so a rejection is
// reported as a normal error here and logged by the caller rather than treated as
// a fault. A 201 means recorded.
func (c *Client) PostJobSession(
	ctx context.Context,
	artifact SessionArtifact,
	data []byte,
) error {
	query := url.Values{"outcome": {string(artifact.Outcome)}}
	if artifact.SessionID != "" {
		query.Set("sessionId", artifact.SessionID)
	}
	if artifact.PodFilePath != "" {
		query.Set("podFilePath", artifact.PodFilePath)
	}

	endpoint := fmt.Sprintf(
		"%s/internal/jobs/%s/session?%s",
		c.baseURL, artifact.JobID, query.Encode(),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/x-ndjson")
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
	// 202 is the API's "declined, and here is why" — the same convention
	// PostJobRecording reads, and worth surfacing verbatim for the same reason:
	// the reason is what an operator acts on (raise the cap) and is not an error in
	// this service.
	if resp.StatusCode == http.StatusAccepted {
		var declined struct {
			Reason string `json:"reason"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&declined); decodeErr == nil && declined.Reason != "" {
			return fmt.Errorf("API declined the session for job %s: %s", artifact.JobID, declined.Reason)
		}
		return fmt.Errorf("API declined the session for job %s", artifact.JobID)
	}
	return fmt.Errorf("API returned status %d posting a session for job %s", resp.StatusCode, artifact.JobID)
}

// ForkPointOutcome is what became of this run's `get_fork_messages` question (ADR
// 032 item 2), and the API stores it verbatim.
//
// **Two values, and neither of them is "there are none".** An unanswered question
// (`unavailable`) and an answered one with an empty list are different facts — the
// first means nobody found out, the second means Pi said this session has no
// previous user messages to fork from. The API's read path turns a *missing* record
// into a third state (`unknown`) rather than guessing, so a lost post reads as "we
// were never told" and never as "there are none".
const (
	// ForkPointsCaptured — Pi answered, and Points is what it said (possibly empty).
	ForkPointsCaptured = "captured"
	// ForkPointsUnavailable — Pi was asked and did not answer: the terminal turn's
	// read failed or the grace elapsed. Points carries nothing.
	ForkPointsUnavailable = "unavailable"
)

// forkPointsRequest is the JSON body of PostJobForkPoints.
//
// `points` is omitted entirely for an unanswered question rather than sent as an
// empty slice, so the payload cannot be misread as "there are none" by a reader
// that looks at the array before the outcome.
type forkPointsRequest struct {
	Outcome string          `json:"outcome"`
	Points  []rpc.ForkPoint `json:"points,omitempty"`
}

// PostJobForkPoints reports which previous user messages a finished job's session
// can be forked from (ADR 032 item 2), which is what makes ADR 032 item 3's
// non-destructive "resume from here" possible.
//
// **A second call, not a field on PostJobSession, and that is forced rather than
// chosen.** That route's body is the raw JSONL artifact — deliberately, because a
// session is routinely megabytes and base64-in-JSON would inflate it by a third —
// so it cannot also carry a structured list, and the list is too large and too
// variable to ride as a query parameter. The API carries a sibling route for it
// (`POST /internal/jobs/:jobId/session/fork-points`), and the outcome travels in the
// body beside the points it describes.
//
// **The cost of the split is one failure mode, and it is closed by the outcome.**
// This call can fail while the session post succeeded, leaving a stored session and
// no fork-point record — which the API reports as `unknown`, the honest answer, and
// never as an empty list. A caller must therefore never treat this error as fatal:
// like every artifact post, it is a side channel, and the run it describes has
// already decided its own outcome.
//
// `points` must be nil for ForkPointsUnavailable and may be empty for
// ForkPointsCaptured; the API refuses the two mismatches with a 202 and a reason.
func (c *Client) PostJobForkPoints(
	ctx context.Context,
	jobID string,
	outcome string,
	points []rpc.ForkPoint,
) error {
	body, err := json.Marshal(forkPointsRequest{Outcome: outcome, Points: points})
	if err != nil {
		return fmt.Errorf("failed to encode fork points: %w", err)
	}

	endpoint := fmt.Sprintf(
		"%s/internal/jobs/%s/session/fork-points",
		c.baseURL, jobID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
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

	if resp.StatusCode == http.StatusCreated {
		return nil
	}
	// 202 is the API's "declined, and here is why" — the same convention
	// PostJobSession and PostJobRecording read, surfaced verbatim for the same reason:
	// the reason is what an operator acts on.
	if resp.StatusCode == http.StatusAccepted {
		var declined struct {
			Reason string `json:"reason"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&declined); decodeErr == nil && declined.Reason != "" {
			return fmt.Errorf("API declined the fork points for job %s: %s", jobID, declined.Reason)
		}
		return fmt.Errorf("API declined the fork points for job %s", jobID)
	}
	return fmt.Errorf("API returned status %d posting fork points for job %s", resp.StatusCode, jobID)
}

// PostJobRecording uploads a job's screen recording (ADR 029) as the raw
// bytes, not as JSON.
// //
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

// PostJobScreenshot uploads one step's screenshot (issue #22) as the raw bytes,
// the same binary-over-JSON choice PostJobRecording makes and for the same two
// reasons: base64 would inflate the image by a third in transit for nothing, and
// the API's JSON body parser has a 2 MB limit that would reject the larger
// screenshots before the handler ever ran. The API route carries its own raw
// parser at the screenshot size cap instead.
//
// **The step name is a query parameter, not part of the path**, matching the
// API's own contract. A step name is a `##` heading from the project's test
// markdown — arbitrary text, potentially long, and needing URL encoding that a
// path segment makes awkward to get right for every case. `url.Values` encodes
// it correctly here without this side having to reason about which characters
// are safe in a path.
//
// Like PostJobUsage and PostJobRecording this is a side channel: the caller
// decides what an error means, and a failure here must never change the job's
// outcome. The API answers 202 for an artifact it declines (oversized, wrong
// format, or a kind that does not report steps) and 400 for a step name it
// cannot store, so both are surfaced as errors for the caller to log rather than
// treated as faults. A 201 means stored.
func (c *Client) PostJobScreenshot(
	ctx context.Context,
	jobID, stepName, contentType string,
	data []byte,
) error {
	query := url.Values{"stepName": {stepName}}
	reqURL := fmt.Sprintf(
		"%s/internal/jobs/%s/screenshot?%s",
		c.baseURL, jobID, query.Encode(),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(data))
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
	// the format) and is not an error in this service. The screenshot endpoint's
	// declines carry the same `reason` field as the recording endpoint's.
	if resp.StatusCode == http.StatusAccepted {
		var declined struct {
			Reason string `json:"reason"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&declined); decodeErr == nil && declined.Reason != "" {
			return fmt.Errorf("API declined the screenshot for step %q on job %s: %s", stepName, jobID, declined.Reason)
		}
		return fmt.Errorf("API declined the screenshot for step %q on job %s", stepName, jobID)
	}
	return fmt.Errorf("API returned status %d posting a screenshot for job %s", resp.StatusCode, jobID)
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

// TokenCapDecision is the API's answer to "may this project run one more
// job of this kind right now?" (ADR 030 §4).
//
// The API owns both halves of the question — the stored cap and the
// consumption it is measured against — so the Orchestrator never needs to know
// the cap's period semantics or the usage table's shape. It asks about one job
// and acts on Allowed.
type TokenCapDecision struct {
	Allowed     bool   `json:"allowed"`
	Cap         *int64 `json:"cap"`
	UsedTokens  int64  `json:"usedTokens"`
	Exceeded    bool   `json:"exceeded"`
	PeriodStart string `json:"periodStart"`
}

// CheckProjectTokenCap asks whether a job of this kind may run for this
// project. A cap that does not apply (no cap set, or a kind that consumes no
// tokens) answers Allowed:true.
//
// A failure to reach the API is returned as an error rather than defaulting
// to "allowed": the caller decides what an unanswerable question means, and
// silently running unbounded work because a check could not be performed is
// the one failure mode a spend cap exists to prevent.
func (c *Client) CheckProjectTokenCap(ctx context.Context, projectID, jobKind string) (*TokenCapDecision, error) {
	url := fmt.Sprintf("%s/internal/projects/%s/token-cap?kind=%s", c.baseURL, projectID, url.QueryEscape(jobKind))
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
		return nil, fmt.Errorf("API returned status %d checking the token cap for project %s", resp.StatusCode, projectID)
	}

	var parsed TokenCapDecision
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to decode token cap response: %w", err)
	}
	return &parsed, nil
}

// ProjectResourceQuota is a project's effective namespace resource limits
// (ADR 030 §5), already resolved to concrete numbers by the API — an override
// where the organization set one, the platform default otherwise — so the
// Orchestrator applies exactly what an admin sees on /allocations/infra.
type ProjectResourceQuota struct {
	CPUmillicores int  `json:"cpuMillicores"`
	MemoryMiB     int  `json:"memoryMib"`
	Pods          int  `json:"pods"`
	FromOverride  bool `json:"fromOverride"`
}

// FetchProjectResourceQuota fetches the quota to apply to a project's
// namespace. Callers treat a failure as "use the built-in defaults" rather
// than failing the job: quota sizing is a guardrail, so a job must not be
// lost because a limit could not be read (ADR 030 §6).
func (c *Client) FetchProjectResourceQuota(ctx context.Context, projectID string) (*ProjectResourceQuota, error) {
	url := fmt.Sprintf("%s/internal/projects/%s/resource-quota", c.baseURL, projectID)
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
		return nil, fmt.Errorf("API returned status %d fetching the resource quota for project %s", resp.StatusCode, projectID)
	}

	var parsed ProjectResourceQuota
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to decode resource quota response: %w", err)
	}
	return &parsed, nil
}

// FetchJobSession reads back a stored job session's bytes (ADR 032 item 3).
//
// **This is the Orchestrator fetching, not the pod.** ADR 032 item 3 decided that
// the restored session is *written into* the pod rather than pulled by it, and the
// reasoning is about which process is allowed to talk to the API: a pod clones a
// user's repository and runs their build code, so giving it an API token — even one
// scoped to a single session — would add an outbound channel and a third secret to
// the least-trusted process in the system. This client already holds the internal
// token and already fetches a job's payload, its secrets and its chart; reading one
// more artifact through the same path adds nothing to the pod.
//
// Returns the raw JSONL, and the status alongside it. The route answers 404 when
// nothing was stored, when the stored outcome was a failure, or when the object is
// gone, and 410 when retention reclaimed it — three different facts that the
// *caller* turns into different refusals rather than flattening into one error,
// which is why the status is reported rather than mapped to a bare error here.
func (c *Client) FetchJobSession(ctx context.Context, jobID string) ([]byte, int, error) {
	endpoint := fmt.Sprintf("%s/internal/jobs/%s/session/content", c.baseURL, jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to reach API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// The body carries the API's own wording ("This session was removed after
		// its retention window"), which is what a user is shown — so it is read even
		// on a failure rather than discarded for the status alone.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, resp.StatusCode, fmt.Errorf(
			"API returned status %d for the session of job %s: %s",
			resp.StatusCode, jobID, strings.TrimSpace(string(body)),
		)
	}

	// Bounded, because a reader must not be where a runaway response is buffered —
	// and generously, because the API's own cap is the authority on what it stored,
	// so this is only a guard against an unbounded body rather than a second policy.
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSessionReadBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read the session for job %s: %w", jobID, err)
	}
	return data, resp.StatusCode, nil
}

// maxSessionReadBytes bounds what FetchJobSession will buffer. Above the API's
// default `SESSION_MAX_BYTES` (5 MB) by a wide factor, so a deployment that raised
// the cap is not silently truncated here.
const maxSessionReadBytes = 64 << 20
