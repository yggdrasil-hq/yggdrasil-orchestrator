package apiclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
)

// ADR 030's two internal reads. Verified against a fake API rather than a
// cluster: both are plain HTTP contracts, so this is the whole surface.

func TestCheckProjectTokenCap_SendsBearerTokenAndParsesTheDecision(t *testing.T) {
	var gotAuth, gotPath, gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"allowed": false,
			"cap": 5000,
			"usedTokens": 5000,
			"exceeded": true,
			"periodStart": "2026-09-01T00:00:00.000Z"
		}`))
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	decision, err := client.CheckProjectTokenCap(context.Background(), "proj-9", "feature_build")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAuth != "Bearer test-token" {
		t.Fatalf("expected the internal bearer token, got %q", gotAuth)
	}
	if gotPath != "/internal/projects/proj-9/token-cap" {
		t.Fatalf("unexpected path %q", gotPath)
	}
	if gotQuery != "kind=feature_build" {
		t.Fatalf("expected the job kind in the query, got %q", gotQuery)
	}
	if decision.Allowed || !decision.Exceeded {
		t.Fatalf("expected a refusal, got %+v", decision)
	}
	if decision.Cap == nil || *decision.Cap != 5000 || decision.UsedTokens != 5000 {
		t.Fatalf("unexpected cap figures: %+v", decision)
	}
}

func TestCheckProjectTokenCap_AcceptsAnUncappedProject(t *testing.T) {
	// `cap` is null when uncapped, which must decode as a nil pointer rather
	// than a zero value — 0 is a real cap meaning "permit nothing further".
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"allowed": true, "cap": null, "usedTokens": 12345, "exceeded": false, "periodStart": "2026-09-01T00:00:00.000Z"}`))
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	decision, err := client.CheckProjectTokenCap(context.Background(), "proj-9", "spec_grill")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("an uncapped project must be allowed, got %+v", decision)
	}
	if decision.Cap != nil {
		t.Fatalf("expected a nil cap for an uncapped project, got %v", *decision.Cap)
	}
}

func TestCheckProjectTokenCap_ErrorsOnANonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if _, err := client.CheckProjectTokenCap(context.Background(), "proj-9", "spec_grill"); err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

func TestFetchProjectResourceQuota_ParsesTheEffectiveQuota(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"cpuMillicores": 2000, "memoryMib": 4096, "pods": 4, "fromOverride": true}`))
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	quota, err := client.FetchProjectResourceQuota(context.Background(), "proj-9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotPath != "/internal/projects/proj-9/resource-quota" {
		t.Fatalf("unexpected path %q", gotPath)
	}
	if quota.CPUmillicores != 2000 || quota.MemoryMiB != 4096 || quota.Pods != 4 || !quota.FromOverride {
		t.Fatalf("unexpected quota: %+v", quota)
	}
}

func TestFetchProjectResourceQuota_ErrorsOnANonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := apiclient.New(server.URL, "test-token")
	if _, err := client.FetchProjectResourceQuota(context.Background(), "proj-9"); err == nil {
		t.Fatal("expected an error for a 404 response")
	}
}
