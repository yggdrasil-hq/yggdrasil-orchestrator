package apiclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

func TestFetchProjectSecrets_SendsBearerTokenAndParsesResponse(t *testing.T) {
	var gotAuthHeader, gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"secrets": map[string]string{"DATABASE_URL": "postgres://example"},
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	secrets, err := client.FetchProjectSecrets(context.Background(), "proj-123", "feature_build", "")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/projects/proj-123/secrets" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/secrets", gotPath)
	}
	if secrets["DATABASE_URL"] != "postgres://example" {
		t.Fatalf("expected DATABASE_URL secret to be parsed, got: %v", secrets)
	}
}

func TestFetchProjectSecrets_ReturnsErrorOnNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "wrong-token")
	_, err := client.FetchProjectSecrets(context.Background(), "proj-123", "feature_build", "")
	if err == nil {
		t.Fatal("expected an error for a non-200 response, got nil")
	}
}

func TestFetchProjectSecrets_EmptySecretsIsNotAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{}})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	secrets, err := client.FetchProjectSecrets(context.Background(), "proj-123", "feature_build", "")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(secrets) != 0 {
		t.Fatalf("expected empty secrets map, got: %v", secrets)
	}
}

// Proves a feature-owned job's feature id reaches the API as a query param, so
// the per-feature model tier takes part in resolution (ADR 018 amendment,
// issue #5). Without it the API resolves at the project/org tier and a
// feature override silently has no effect on the model the pod runs with.
func TestFetchProjectSecrets_SendsFeatureIdWhenPresent(t *testing.T) {
	var gotJobKind, gotFeatureID string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotJobKind = r.URL.Query().Get("jobKind")
		gotFeatureID = r.URL.Query().Get("featureId")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"secrets": map[string]string{"MODEL_ID": "feature-model"},
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	secrets, err := client.FetchProjectSecrets(
		context.Background(), "proj-123", "feature_build", "feat-456",
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotFeatureID != "feat-456" {
		t.Fatalf("expected featureId query param %q, got %q", "feat-456", gotFeatureID)
	}
	if gotJobKind != "feature_build" {
		t.Fatalf("expected jobKind query param %q, got %q", "feature_build", gotJobKind)
	}
	if secrets["MODEL_ID"] != "feature-model" {
		t.Fatalf("expected the feature-tier model to be parsed, got: %v", secrets)
	}
}

// The param must be absent, not merely empty, when a job has no feature: an
// API that predates the feature tier then sees exactly the request it saw
// before, which is what keeps the two sides deployable in either order.
func TestFetchProjectSecrets_OmitsFeatureIdWhenJobHasNoFeature(t *testing.T) {
	var query url.Values

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{}})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	// A scheduled test_run is the realistic no-feature case.
	if _, err := client.FetchProjectSecrets(
		context.Background(), "proj-123", "test_run", "",
	); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if _, present := query["featureId"]; present {
		t.Fatalf("expected no featureId query param, got %q", query.Get("featureId"))
	}
	if query.Get("jobKind") != "test_run" {
		t.Fatalf("expected jobKind query param %q, got %q", "test_run", query.Get("jobKind"))
	}
}

func TestFetchProjectChart_FoundParsesFiles(t *testing.T) {
	var gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"files": map[string]string{
				"Chart.yaml": "apiVersion: v2\nname: primary\n",
			},
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	files, found, err := client.FetchProjectChart(context.Background(), "proj-123")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if gotPath != "/internal/projects/proj-123/chart" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/chart", gotPath)
	}
	if files["Chart.yaml"] == "" {
		t.Fatalf("expected Chart.yaml content to be parsed, got: %v", files)
	}
}

func TestFetchProjectChart_NotFoundIsNotAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	files, found, err := client.FetchProjectChart(context.Background(), "proj-123")
	if err != nil {
		t.Fatalf("expected no error for a 404, got: %v", err)
	}
	if found {
		t.Fatal("expected found=false")
	}
	if files != nil {
		t.Fatalf("expected nil files, got: %v", files)
	}
}

func TestFetchProjectChart_ReturnsErrorOnOtherNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	_, _, err := client.FetchProjectChart(context.Background(), "proj-123")
	if err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

func TestFetchProjectMetadata_ParsesSlug(t *testing.T) {
	var gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"slug": "acme-web"})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	slug, err := client.FetchProjectMetadata(context.Background(), "proj-123")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotPath != "/internal/projects/proj-123/slug" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/slug", gotPath)
	}
	if slug != "acme-web" {
		t.Fatalf("expected slug %q, got %q", "acme-web", slug)
	}
}

func TestFetchProjectMetadata_ReturnsErrorOnNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	_, err := client.FetchProjectMetadata(context.Background(), "proj-123")
	if err == nil {
		t.Fatal("expected an error for a 404 response, got nil")
	}
}

func TestFetchOrganizationCluster_ParsesOrgAndKubeconfig(t *testing.T) {
	var gotAuthHeader, gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"organizationId": "org-1",
			"kubeconfig":     "apiVersion: v1\nclusters: []\n",
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	orgID, kubeconfig, err := client.FetchOrganizationCluster(context.Background(), "proj-123")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/projects/proj-123/organization-cluster" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/organization-cluster", gotPath)
	}
	if orgID != "org-1" {
		t.Fatalf("expected org id %q, got %q", "org-1", orgID)
	}
	if kubeconfig != "apiVersion: v1\nclusters: []\n" {
		t.Fatalf("expected kubeconfig to be parsed, got %q", kubeconfig)
	}
}

func TestFetchOrganizationCluster_ReturnsErrorWhenNoClusterConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	_, _, err := client.FetchOrganizationCluster(context.Background(), "proj-123")
	if err == nil {
		t.Fatal("expected an error for a 409 (org has no cluster), got nil")
	}
}

func TestFetchFeatureSpec_SendsBearerTokenAndParsesResponse(t *testing.T) {
	var gotAuthHeader, gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Add dark mode",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
				{"cloneUrl": "https://github.com/acme/worker.git", "isPrimary": false},
			},
			"githubToken": "ghs_minted-token",
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	spec, err := client.FetchFeatureSpec(context.Background(), "proj-123", "feat-456", "spec_grill")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/projects/proj-123/features/feat-456/spec" {
		t.Fatalf("expected path %q, got %q", "/internal/projects/proj-123/features/feat-456/spec", gotPath)
	}
	if spec.Title != "Add dark mode" {
		t.Fatalf("expected title %q, got %q", "Add dark mode", spec.Title)
	}
	if len(spec.Repos) != 2 || spec.Repos[0].CloneURL != "https://github.com/acme/web.git" || !spec.Repos[0].IsPrimary {
		t.Fatalf("unexpected repos: %+v", spec.Repos)
	}
	if spec.GithubToken != "ghs_minted-token" {
		t.Fatalf("expected githubToken %q, got %q", "ghs_minted-token", spec.GithubToken)
	}
}

// Proves the kind param actually reaches the API as a query string, and that
// the feature_build-only fields (ADR 010 item 1) get decoded when present.
func TestFetchFeatureSpec_PassesKindAndDecodesBuildFields(t *testing.T) {
	var gotQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Add dark mode",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
			},
			"githubToken": "ghs_write-scoped-token",
			"adrMarkdown": "# Add dark mode\n\n...",
			"branch":      "yggdrasil/add-dark-mode-feat-456",
		})
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	spec, err := client.FetchFeatureSpec(context.Background(), "proj-123", "feat-456", "feature_build")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotQuery != "kind=feature_build" {
		t.Fatalf("expected query %q, got %q", "kind=feature_build", gotQuery)
	}
	if spec.AdrMarkdown != "# Add dark mode\n\n..." {
		t.Fatalf("expected adrMarkdown to be decoded, got %q", spec.AdrMarkdown)
	}
	if spec.Branch != "yggdrasil/add-dark-mode-feat-456" {
		t.Fatalf("expected branch to be decoded, got %q", spec.Branch)
	}
}

func TestFetchFeatureSpec_ReturnsErrorOnNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	_, err := client.FetchFeatureSpec(context.Background(), "proj-123", "feat-456", "spec_grill")
	if err == nil {
		t.Fatal("expected an error for a 404 response, got nil")
	}
}

func TestFetchDesignSpecDecodesDesignPayload(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":        "Checkout flow",
			"slug":        "checkout",
			"description": "A responsive checkout mockup.",
			"branch":      "yggdrasil/design-checkout-session-1",
			"repos":       []map[string]any{{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true}},
			"githubToken": "ghs_design-token",
		})
	}))
	defer server.Close()

	spec, err := apiclient.New(server.URL, "test-token").
		FetchDesignSpec(context.Background(), "proj-123", "session-1")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotPath != "/internal/projects/proj-123/designs/session-1/spec" {
		t.Fatalf("unexpected design payload path: %q", gotPath)
	}
	if spec.DesignName != "Checkout flow" || spec.DesignSlug != "checkout" ||
		spec.DesignDescription != "A responsive checkout mockup." {
		t.Fatalf("unexpected design payload: %+v", spec)
	}
}

func TestFetchTestSpecSendsRefAndParsesMarkdown(t *testing.T) {
	var gotAuthHeader, gotPath, gotRef string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotRef = r.URL.Query().Get("ref")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title":        "Checkout flow",
			"testId":       "test-1",
			"testMarkdown": "## Checkout",
			"ref":          "main",
		})
	}))
	defer server.Close()

	spec, err := apiclient.New(server.URL, "test-token").
		FetchTestSpec(context.Background(), "proj-123", "test-1", "main")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected bearer auth, got %q", gotAuthHeader)
	}
	if gotPath != "/internal/projects/proj-123/tests/test-1/spec" || gotRef != "main" {
		t.Fatalf("unexpected test spec request: path=%q ref=%q", gotPath, gotRef)
	}
	if spec.TestMarkdown != "## Checkout" || spec.Ref != "main" {
		t.Fatalf("unexpected test spec: %+v", spec)
	}
}

func TestPostJobEvent_SendsBearerTokenAndBody(t *testing.T) {
	var gotAuthHeader, gotPath string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobEvent(context.Background(), "job-123", rpc.CuratedEvent{
		Type:     rpc.EventAskUser,
		Question: "Which auth model?",
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/jobs/job-123/events" {
		t.Fatalf("expected path %q, got %q", "/internal/jobs/job-123/events", gotPath)
	}
	if gotBody["type"] != "ask_user" || gotBody["question"] != "Which auth model?" {
		t.Fatalf("unexpected request body: %v", gotBody)
	}
}

func TestPostJobEvent_ReturnsErrorOnNon201(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobEvent(context.Background(), "job-123", rpc.CuratedEvent{Type: rpc.EventSubmitADR})
	if err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

func TestReportDeployResult_PostsRevisionWithBearerToken(t *testing.T) {
	var gotAuthHeader, gotPath, gotMethod string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.ReportDeployResult(context.Background(), "job-1", apiclient.DeployResultInput{Revision: 7})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("expected method %q, got %q", http.MethodPost, gotMethod)
	}
	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/jobs/job-1/deploy-result" {
		t.Fatalf("expected path %q, got %q", "/internal/jobs/job-1/deploy-result", gotPath)
	}
	if gotBody["revision"] != float64(7) {
		t.Fatalf("expected revision 7 in the body, got %v", gotBody["revision"])
	}
	// A successful deploy has no target revision and no error, and both must be
	// *absent* rather than sent as zero/empty — an empty lastError on a
	// rollback record would read as "succeeded" in the ledger.
	if _, present := gotBody["targetRevision"]; present {
		t.Fatalf("expected no targetRevision key for a plain deploy, got %v", gotBody["targetRevision"])
	}
	if _, present := gotBody["lastError"]; present {
		t.Fatalf("expected no lastError key on success, got %v", gotBody["lastError"])
	}
}

func TestReportDeployResult_SendsTargetRevisionAndErrorForAFailedRollback(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	target := 3
	client := apiclient.New(server.URL, "test-token")
	err := client.ReportDeployResult(context.Background(), "job-2", apiclient.DeployResultInput{
		TargetRevision: &target,
		LastError:      "helm rollback to revision 3 failed: release not found",
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotBody["targetRevision"] != float64(3) {
		t.Fatalf("expected targetRevision 3, got %v", gotBody["targetRevision"])
	}
	// A failed operation produces no new revision, so revision stays 0 and the
	// API stores NULL for it — the ledger row records an attempt that changed
	// nothing, which is exactly what makes "which revisions can I roll back
	// to" unambiguous.
	if gotBody["revision"] != float64(0) {
		t.Fatalf("expected revision 0 for a failed operation, got %v", gotBody["revision"])
	}
	if gotBody["lastError"] == nil || gotBody["lastError"] == "" {
		t.Fatalf("expected the failure reason to be reported, got %v", gotBody["lastError"])
	}
}

func TestReportDeployResult_ErrorsOnNonCreatedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.ReportDeployResult(context.Background(), "job-3", apiclient.DeployResultInput{Revision: 1})
	if err == nil {
		t.Fatal("expected an error when the API rejects the report, got nil")
	}
}

// ADR 023: the usage side channel posts provider-reported token/cost
// accounting to the internal endpoint, with the same auth as every other
// internal call.
func TestPostJobUsage_SendsBearerTokenAndBody(t *testing.T) {
	var gotAuthHeader, gotPath, gotContentType string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	cost := 0.45
	durationMs := int64(90_000)
	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobUsage(context.Background(), "job-123", apiclient.JobUsage{
		ModelID:          "anthropic/claude-sonnet-4",
		InputTokens:      50_000,
		OutputTokens:     10_000,
		CacheReadTokens:  40_000,
		CacheWriteTokens: 5_000,
		TotalTokens:      105_000,
		CostUSD:          &cost,
		DurationMs:       &durationMs,
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/jobs/job-123/usage" {
		t.Fatalf("expected path %q, got %q", "/internal/jobs/job-123/usage", gotPath)
	}
	if gotContentType != "application/json" {
		t.Fatalf("expected JSON content type, got %q", gotContentType)
	}
	if gotBody["modelId"] != "anthropic/claude-sonnet-4" {
		t.Fatalf("expected modelId in the body, got %v", gotBody["modelId"])
	}
	if gotBody["totalTokens"] != float64(105_000) {
		t.Fatalf("expected totalTokens 105000, got %v", gotBody["totalTokens"])
	}
	if gotBody["costUsd"] != 0.45 {
		t.Fatalf("expected costUsd 0.45, got %v", gotBody["costUsd"])
	}
}

// An unreported cost or duration must serialize as an explicit null rather
// than being dropped: the API distinguishes "not reported" from zero, and a
// missing key would otherwise be indistinguishable from an older client.
func TestPostJobUsage_SendsNullForUnreportedCost(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobUsage(context.Background(), "job-123", apiclient.JobUsage{TotalTokens: 10})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	value, present := gotBody["costUsd"]
	if !present {
		t.Fatal("expected costUsd to be present in the payload")
	}
	if value != nil {
		t.Fatalf("expected costUsd null, got %v", value)
	}
}

func TestPostJobUsage_ReturnsErrorOnNon201(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobUsage(context.Background(), "job-123", apiclient.JobUsage{}); err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

// ADR 029: the recording side channel posts the video itself as the request
// body, not as JSON, with the same auth as every other internal call.
func TestPostJobRecording_SendsRawBytesWithBearerToken(t *testing.T) {
	var gotAuthHeader, gotPath, gotContentType, gotMethod string
	var gotBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	payload := []byte{0x1a, 0x45, 0xdf, 0xa3, 0x00, 0xff}
	err := client.PostJobRecording(context.Background(), "job-123", "video/webm", payload)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("expected method %q, got %q", http.MethodPost, gotMethod)
	}
	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/jobs/job-123/recording" {
		t.Fatalf("expected path %q, got %q", "/internal/jobs/job-123/recording", gotPath)
	}
	if gotContentType != "video/webm" {
		t.Fatalf("expected the video content type, got %q", gotContentType)
	}
	// Byte-for-byte: a recording is binary, so any accidental encoding on the
	// way out (base64, JSON wrapping) would corrupt it silently.
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("expected the raw bytes to survive, got %v", gotBody)
	}
}

// A 202 is the API declining to store an artifact (too large, wrong format, or
// a job kind that does not record). It must read as an error to the caller —
// which logs it — but crucially NOT as a failure that changes the job's
// outcome, which is the caller's decision, not this method's.
func TestPostJobRecording_SurfacesTheAPIsDeclineReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"stored":false,"reason":"Recording exceeds the 25 MB limit (30 MB)"}`))
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobRecording(context.Background(), "job-123", "video/webm", []byte("x"))
	if err == nil {
		t.Fatal("expected an error when the API declines the recording")
	}
	// The reason is the actionable part — it is what tells an operator to raise
	// the cap — so it must survive into the error rather than being flattened.
	if !strings.Contains(err.Error(), "25 MB") {
		t.Fatalf("expected the API's reason to be surfaced, got %q", err.Error())
	}
}

func TestPostJobRecording_ErrorsOnAnUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobRecording(context.Background(), "job-123", "video/webm", []byte("x")); err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

/*
Issue #22: the screenshot upload. The step name travels as a query parameter
because a step is a `##` heading from the project's own test markdown — arbitrary
text, potentially long — so the encoding is the risky part of this method and is
what these tests pin.
*/

func TestPostJobScreenshot_SendsRawBytesWithTheStepNameEncoded(t *testing.T) {
	var gotAuthHeader, gotPath, gotContentType, gotStepName, gotRawQuery, gotMethod string
	var gotBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotRawQuery = r.URL.RawQuery
		gotStepName = r.URL.Query().Get("stepName")
		gotContentType = r.Header.Get("Content-Type")
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	payload := []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}

	// A step name carrying the characters a heading really can: spaces, a
	// slash, an ampersand and a `#`. All of them must survive, because the step
	// name is the API's identity for the row and a mangled one would store the
	// screenshot against a step that does not exist.
	const stepName = "opens checkout & pays: step 2/3 #happy-path"
	if err := client.PostJobScreenshot(context.Background(), "job-123", stepName, "image/png", payload); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("expected method %q, got %q", http.MethodPost, gotMethod)
	}
	if gotAuthHeader != "Bearer test-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer test-token", gotAuthHeader)
	}
	if gotPath != "/internal/jobs/job-123/screenshot" {
		t.Fatalf("expected path %q, got %q", "/internal/jobs/job-123/screenshot", gotPath)
	}
	if gotStepName != stepName {
		t.Fatalf("expected the step name to round-trip %q, got %q", stepName, gotStepName)
	}
	// The name must be in the query, not smuggled into the path: a `/` in a
	// heading would otherwise become a path separator and 404.
	if !strings.Contains(gotRawQuery, "stepName=") {
		t.Fatalf("expected stepName in the query string, got %q", gotRawQuery)
	}
	if gotContentType != "image/png" {
		t.Fatalf("expected the image content type, got %q", gotContentType)
	}
	// Byte-for-byte: a screenshot is binary, so any accidental encoding on the way
	// out (base64, JSON wrapping) would corrupt it silently.
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("expected the raw bytes to survive, got %v", gotBody)
	}
}

// A step name with a `/` must not become a path separator — the single case where
// a query parameter and a path segment genuinely differ.
func TestPostJobScreenshot_ASlashInTheStepNameStaysInTheQuery(t *testing.T) {
	var gotPath, gotStepName string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotStepName = r.URL.Query().Get("stepName")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobScreenshot(context.Background(), "job-1", "runs/lints", "image/png", []byte("x")); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotPath != "/internal/jobs/job-1/screenshot" {
		t.Fatalf("expected the step name not to reach the path, got %q", gotPath)
	}
	if gotStepName != "runs/lints" {
		t.Fatalf("expected the step name intact, got %q", gotStepName)
	}
}

// A 202 is the API declining to store an artifact (too large, wrong format, or a
// job kind that does not report steps). It must read as an error to the caller —
// which logs it — but NOT as a failure that changes the job's outcome, which is
// the caller's decision, not this method's.
func TestPostJobScreenshot_SurfacesTheAPIsDeclineReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"stored":false,"reason":"Screenshot exceeds the 2000000 byte limit"}`))
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	err := client.PostJobScreenshot(context.Background(), "job-123", "step", "image/png", []byte("x"))
	if err == nil {
		t.Fatal("expected an error when the API declines the screenshot")
	}
	// The reason is the actionable part — it is what tells an operator to raise
	// the cap — so it must survive into the error rather than being flattened.
	if !strings.Contains(err.Error(), "2000000 byte limit") {
		t.Fatalf("expected the API's reason to be surfaced, got %q", err.Error())
	}
	// And the step must be named, so a run with many steps says which one failed.
	if !strings.Contains(err.Error(), `"step"`) {
		t.Fatalf("expected the step name in the error, got %q", err.Error())
	}
}

// A 400 is the API's answer to a step name it cannot store (empty, or beyond its
// length limit). Surfaced like the decline — logged by the caller, non-fatal —
// rather than silently swallowed, since it means a screenshot was not stored.
func TestPostJobScreenshot_ErrorsOnARejectedStepName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobScreenshot(context.Background(), "job-123", "", "image/png", []byte("x")); err == nil {
		t.Fatal("expected an error for a 400 response, got nil")
	}
}

func TestPostJobScreenshot_ErrorsOnAnUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobScreenshot(context.Background(), "job-123", "step", "image/png", []byte("x")); err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

// The API's route is mounted at a path the Orchestrator must match exactly; a
// typo here would 404 on every upload and the failure would look like a missing
// endpoint rather than a client bug.
func TestPostJobScreenshot_UsesTheDocumentedRoute(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobScreenshot(context.Background(), "job-abc", "s", "image/webp", []byte("x")); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gotPath != "/internal/jobs/job-abc/screenshot" {
		t.Fatalf("unexpected route %q", gotPath)
	}
	// Sanity: the recording endpoint is a path sibling, not the same path.
	if strings.Contains(gotPath, "recording") {
		t.Fatalf("screenshot upload must not target the recording route, got %q", gotPath)
	}
}

/*
 * Issue #38's transport, tested as a **whole path** rather than hop by hop.
 *
 * The structured half of an `ask_user` question crosses two places that each
 * drop unknown fields silently:
 *
 *	Pi event → rpc.Translate → rpc.CuratedEvent → PostJobEvent → JSON body
 *	              (struct)                          (explicit field list)
 *
 * `Translate` builds a `CuratedEvent` literal, so a field it does not name is
 * lost; `jobEventRequest` is an explicit field list, so a field missing there is
 * lost *again*, one hop later. Either drop is invisible at runtime — the question
 * still arrives, just without its choices — which is the same shape as #59's
 * verdict, validated and acted on and then discarded.
 *
 * So these cases start from the raw Pi JSON and assert on the marshalled body. A
 * test that stopped at `Translate` would not have caught the second drop, and one
 * that only checked presence would not have caught an option list arriving with
 * its labels stripped.
 */
func TestPostJobEvent_CarriesAStructuredQuestionThroughBothHops(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	raw := `{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{` +
		`"kind":"ask_user",` +
		`"question":"Which database should the API use?",` +
		`"header":"Database",` +
		`"multiSelect":false,` +
		`"options":[` +
		`{"label":"PostgreSQL","description":"Matches the existing API stack"},` +
		`{"label":"SQLite"}` +
		`]},"terminate":true}}`

	curated, ok := rpc.Translate(rpc.Event{Type: "tool_execution_end", Raw: json.RawMessage(raw)})
	if !ok {
		t.Fatal("expected the ask_user tool call to translate")
	}

	// Hop one: the fields must exist on the curated event, not just be present in
	// the JSON Pi sent.
	if curated.QuestionHeader != "Database" {
		t.Fatalf("Translate dropped the header, got %q", curated.QuestionHeader)
	}
	if curated.QuestionMultiSelect == nil || *curated.QuestionMultiSelect {
		t.Fatalf("Translate dropped multiSelect, got %v", curated.QuestionMultiSelect)
	}
	if curated.QuestionOptions == nil || len(*curated.QuestionOptions) != 2 {
		t.Fatalf("Translate dropped the options, got %v", curated.QuestionOptions)
	}

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobEvent(context.Background(), "job-structured", curated); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Hop two: and the values must survive the request body. Asserting the values
	// rather than the keys is the point — `jobEventRequest` would happily send an
	// empty options array if the slice were dropped.
	if gotBody["header"] != "Database" {
		t.Fatalf("expected header in the body, got %v", gotBody["header"])
	}
	// Present-and-false, not absent: the API reads presence of `options` as
	// "structured", and an omitted multiSelect would be the API's problem to
	// guess. It defaults to false, so a *missing* key here still means false —
	// but a dropped one is indistinguishable from an unasked question, so it is
	// asserted explicitly.
	multiSelect, present := gotBody["multiSelect"]
	if !present || multiSelect != false {
		t.Fatalf("expected an explicit multiSelect=false in the body, got %v (present=%v)", multiSelect, present)
	}

	options, ok := gotBody["options"].([]any)
	if !ok || len(options) != 2 {
		t.Fatalf("expected two options in the body, got %v", gotBody["options"])
	}
	first, ok := options[0].(map[string]any)
	if !ok {
		t.Fatalf("expected the first option to be an object, got %T", options[0])
	}
	if first["label"] != "PostgreSQL" || first["description"] != "Matches the existing API stack" {
		t.Fatalf("option values did not survive the marshalling: %v", first)
	}
	// The option with no description must send no description key rather than an
	// empty string — the API normalises absent to null, and an empty string would
	// render as a blank line under the label.
	second := options[1].(map[string]any)
	if _, present := second["description"]; present {
		t.Fatalf("expected no description key for an option that gave none, got %v", second["description"])
	}
}

func TestPostJobEvent_OmitsTheStructuredFieldsForAProseQuestion(t *testing.T) {
	/*
	 * The other half of the contract, and the one a careless change breaks: the
	 * API decides between a picker and a text box by whether `options` is
	 * *present*, so a prose question must send none of the three. Emitting
	 * `multiSelect: false` or `options: []` here would make every plain grill
	 * question render as a picker with nothing in it.
	 */
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	raw := `{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{` +
		`"kind":"ask_user","question":"What problem does this solve?"},"terminate":true}}`

	curated, ok := rpc.Translate(rpc.Event{Type: "tool_execution_end", Raw: json.RawMessage(raw)})
	if !ok {
		t.Fatal("expected the ask_user tool call to translate")
	}
	if curated.QuestionMultiSelect != nil || curated.QuestionOptions != nil {
		t.Fatalf("a prose question must carry no structured fields, got %v / %v",
			curated.QuestionMultiSelect, curated.QuestionOptions)
	}

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobEvent(context.Background(), "job-prose", curated); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if gotBody["question"] != "What problem does this solve?" {
		t.Fatalf("expected the question itself, got %v", gotBody["question"])
	}
	for _, key := range []string{"header", "multiSelect", "options"} {
		if _, present := gotBody[key]; present {
			t.Fatalf("expected %q to be absent from a prose question, got %v", key, gotBody[key])
		}
	}
}

/*
 * Issue #88's transport, tested the same way #38's was and for the same reason.
 *
 * `submit_review`'s `findings` crossed the same two silent drops #38's question
 * half did:
 *
 *	Pi event → rpc.Translate → rpc.CuratedEvent → PostJobEvent → JSON body
 *	              (struct)                          (explicit field list)
 *
 * `grep -rn findings orchestrator/ --include=*.go` returned **zero hits** before
 * this — the tool emitted the field, the API accepted and stored it, and nothing
 * in between carried it. Every test on both sides passed, and the feature looked
 * unbuilt rather than broken. That is four instances of one shape now (#38, #59,
 * #73, #88), so this file's job is to make the next dropped field fail loudly
 * rather than silently.
 *
 * These cases therefore start from raw Pi JSON and assert on the *marshalled
 * body*. A test stopping at `Translate` would miss the second drop, and one
 * asserting mere presence would miss a finding arriving with its `body` — the one
 * required field — stripped.
 */
func TestPostJobEvent_CarriesReviewFindingsThroughBothHops(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	// Shaped exactly as the contract extension emits it
	// (agent-images/extensions/yggdrasil-contract/src/index.ts). The second
	// finding deliberately omits `blocking` and carries no `path`/`line`, because
	// those absences are part of the contract rather than incidental: the API
	// defaults an omitted flag to `true`, and the SKILL tells the agent not to
	// invent a location to fill the field.
	raw := `{"type":"tool_execution_end","toolName":"submit_review","result":{"details":{` +
		`"kind":"submit_review",` +
		`"verdict":"changes_requested",` +
		`"comment":"Two issues, one blocking.",` +
		`"findings":[` +
		`{"path":"src/app/page.tsx","line":42,"body":"Missing null check","blocking":true},` +
		`{"body":"Requirement implemented nowhere"}` +
		`]},"terminate":true}}`

	curated, ok := rpc.Translate(rpc.Event{Type: "tool_execution_end", Raw: json.RawMessage(raw)})
	if !ok {
		t.Fatal("expected the submit_review tool call to translate")
	}

	// Hop one: the fields must exist on the curated event, not merely in the JSON
	// Pi sent. Asserting the *values* here is deliberate — a length-only check
	// would pass for a list whose entries had all been zeroed.
	if curated.Findings == nil {
		t.Fatal("Translate dropped the findings list entirely")
	}
	findings := *curated.Findings
	if len(findings) != 2 {
		t.Fatalf("expected two findings after Translate, got %d (%v)", len(findings), findings)
	}
	if findings[0].Path != "src/app/page.tsx" || findings[0].Body != "Missing null check" {
		t.Fatalf("Translate mangled the first finding: %+v", findings[0])
	}
	if findings[0].Line == nil || *findings[0].Line != 42 {
		t.Fatalf("Translate dropped the first finding's line, got %v", findings[0].Line)
	}
	if findings[0].Blocking == nil || !*findings[0].Blocking {
		t.Fatalf("Translate dropped the first finding's blocking flag, got %v", findings[0].Blocking)
	}
	// The absences must survive as absences, not become zero values.
	if findings[1].Blocking != nil {
		t.Fatalf("an omitted blocking flag must stay absent, got %v", *findings[1].Blocking)
	}
	if findings[1].Line != nil {
		t.Fatalf("an omitted line must stay absent, got %v", *findings[1].Line)
	}
	if findings[1].Path != "" {
		t.Fatalf("expected no path on the second finding, got %q", findings[1].Path)
	}

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobEvent(context.Background(), "job-review", curated); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Hop two: this is where #38 lost `options`, so the assertions matter more
	// here than at hop one.
	got, ok := gotBody["findings"].([]any)
	if !ok {
		t.Fatalf("expected a findings array in the request body, got %v", gotBody["findings"])
	}
	if len(got) != 2 {
		t.Fatalf("expected two findings in the body, got %d", len(got))
	}

	first, ok := got[0].(map[string]any)
	if !ok {
		t.Fatalf("expected the first finding to be an object, got %T", got[0])
	}
	if first["path"] != "src/app/page.tsx" {
		t.Fatalf("path did not survive the request body: %v", first["path"])
	}
	if first["body"] != "Missing null check" {
		t.Fatalf("body did not survive the request body: %v", first["body"])
	}
	// JSON numbers decode as float64 through a `map[string]any`.
	if line, ok := first["line"].(float64); !ok || line != 42 {
		t.Fatalf("line did not survive the request body: %v", first["line"])
	}
	if blocking, ok := first["blocking"].(bool); !ok || !blocking {
		t.Fatalf("blocking did not survive the request body: %v", first["blocking"])
	}

	second, ok := got[1].(map[string]any)
	if !ok {
		t.Fatalf("expected the second finding to be an object, got %T", got[1])
	}
	if second["body"] != "Requirement implemented nowhere" {
		t.Fatalf("the second finding's body did not survive: %v", second["body"])
	}
	/*
	 * The two absences that decide behaviour, asserted as absences.
	 *
	 * `blocking`: the API defaults an omitted flag to **true** (`finding.blocking
	 * ?? true`) and documents why — on a `changes_requested` verdict an omitted flag
	 * means "these are the blockers", and a `false` default would let a review pass
	 * its gate while displaying the findings that should stop it. So marshalling a
	 * plain `false` here would invert that decision, and the API's carefully-reasoned
	 * default would be unreachable from the only producer that exists.
	 *
	 * `path`/`line`: omitting them is a legitimate finding about the change as a
	 * whole, so an empty string or a zero line would be a fabricated location.
	 */
	for _, key := range []string{"blocking", "path", "line"} {
		if _, present := second[key]; present {
			t.Fatalf("an omitted %s must send no key, got %v", key, second[key])
		}
	}
}

/*
 * The other half of that contract, and the one a careless change breaks.
 *
 * The API stores an absent `findings` as SQL NULL and an empty list as `[]`, and
 * calls those **two different answers to "how many blocking issues"**: absent means
 * *prose, the count is not knowable*, `[]` means *structured, and there are
 * genuinely none*. A plain (non-pointer) slice collapses them — every review written
 * as a paragraph would arrive claiming zero findings, which turns "we cannot say"
 * into a confident statement of fact in the reassuring direction.
 *
 * So this asserts the absence *on the wire*, not just on the struct.
 */
func TestPostJobEvent_OmitsReviewFindingsWhenTheReviewWasProse(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	// A review written as a paragraph, which is what every review before #73
	// looked like and what the SKILL says remains a legitimate choice.
	raw := `{"type":"tool_execution_end","toolName":"submit_review","result":{"details":{` +
		`"kind":"submit_review",` +
		`"verdict":"approved",` +
		`"comment":"Implements the ADR as written."` +
		`},"terminate":true}}`

	curated, ok := rpc.Translate(rpc.Event{Type: "tool_execution_end", Raw: json.RawMessage(raw)})
	if !ok {
		t.Fatal("expected the submit_review tool call to translate")
	}
	if curated.Findings != nil {
		t.Fatalf("a prose review must carry no findings list, got %v", curated.Findings)
	}

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobEvent(context.Background(), "job-prose-review", curated); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if _, present := gotBody["findings"]; present {
		t.Fatalf("expected no findings key at all for a prose review, got %v", gotBody["findings"])
	}
	if gotBody["verdict"] != "approved" {
		t.Fatalf("expected the verdict itself, got %v", gotBody["verdict"])
	}
}

/*
 * And the state between those two: a structured review that looked and found
 * nothing. `[]` is a *claim* — "there are no findings" — which is only honest when
 * the agent actually enumerated. It must reach the API as an empty array rather
 * than collapsing back to absent, or a reviewer cannot tell "checked, nothing
 * wrong" from "wrote a paragraph instead".
 */
func TestPostJobEvent_SendsAnEmptyReviewFindingsListAsAnEmptyArray(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	raw := `{"type":"tool_execution_end","toolName":"submit_review","result":{"details":{` +
		`"kind":"submit_review",` +
		`"verdict":"approved",` +
		`"comment":"Nothing to report.",` +
		`"findings":[]` +
		`},"terminate":true}}`

	curated, ok := rpc.Translate(rpc.Event{Type: "tool_execution_end", Raw: json.RawMessage(raw)})
	if !ok {
		t.Fatal("expected the submit_review tool call to translate")
	}
	if curated.Findings == nil {
		t.Fatal("an explicit empty list must stay present, not become absent")
	}
	if len(*curated.Findings) != 0 {
		t.Fatalf("expected zero findings, got %d", len(*curated.Findings))
	}

	client := apiclient.New(server.URL, "test-token")
	if err := client.PostJobEvent(context.Background(), "job-empty-findings", curated); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Present-and-empty, which is a different fact from absent.
	got, present := gotBody["findings"]
	if !present {
		t.Fatal("an empty findings list must be sent, or the API cannot tell it from prose")
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected an array, got %T (%v)", got, got)
	}
	if len(arr) != 0 {
		t.Fatalf("expected an empty array, got %v", arr)
	}
}
