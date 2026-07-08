package apiclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
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
