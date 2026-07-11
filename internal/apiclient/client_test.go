package apiclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	secrets, err := client.FetchProjectSecrets(context.Background(), "proj-123")
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
	_, err := client.FetchProjectSecrets(context.Background(), "proj-123")
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
	secrets, err := client.FetchProjectSecrets(context.Background(), "proj-123")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(secrets) != 0 {
		t.Fatalf("expected empty secrets map, got: %v", secrets)
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
