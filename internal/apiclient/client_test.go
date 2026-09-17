package apiclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
