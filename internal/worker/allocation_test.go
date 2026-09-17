package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// ADR 030 §4's enforcement point, exercised against a fake API rather than a
// cluster — the decision is entirely the API's, so this is verifiable here even
// though the job itself could not run without a cluster.

func capServer(t *testing.T, decision map[string]any) (*httptest.Server, *[]string) {
	t.Helper()
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		_ = json.NewEncoder(w).Encode(decision)
	}))
	t.Cleanup(server.Close)
	return server, &paths
}

func TestEnforceTokenCap_AllowsAJobUnderTheCap(t *testing.T) {
	server, paths := capServer(t, map[string]any{
		"allowed": true, "cap": 1000, "usedTokens": 900, "exceeded": false,
		"periodStart": "2026-09-01T00:00:00.000Z",
	})
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
	job := &queue.Job{ID: "job-1", ProjectID: "proj-1", Kind: queue.KindFeatureBuild}

	if err := enforceTokenCap(context.Background(), job, cfg); err != nil {
		t.Fatalf("expected the job to be allowed, got: %v", err)
	}
	if len(*paths) != 1 || !strings.Contains((*paths)[0], "kind=feature_build") {
		t.Fatalf("expected one cap check naming the job kind, got %v", *paths)
	}
}

func TestEnforceTokenCap_BlocksAnOverCapJobWithALegibleReason(t *testing.T) {
	server, _ := capServer(t, map[string]any{
		"allowed": false, "cap": 1000, "usedTokens": 1500, "exceeded": true,
		"periodStart": "2026-09-01T00:00:00.000Z",
	})
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
	job := &queue.Job{ID: "job-2", ProjectID: "proj-2", Kind: queue.KindSpecGrill}

	err := enforceTokenCap(context.Background(), job, cfg)
	if err == nil {
		t.Fatal("expected an over-cap job to be refused")
	}
	// The reason lands in jobs.last_error and is what a human reads, so it has
	// to carry the spend, the cap, the period and the remedy.
	for _, want := range []string{"1500", "1000", "2026-09-01", "raise or clear the cap"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q is missing %q", err.Error(), want)
		}
	}
}

func TestEnforceTokenCap_SkipsTheCheckForDeterministicKinds(t *testing.T) {
	// A project over its model budget must still be able to deploy, roll back,
	// and run script tests. These kinds never even ask the API.
	server, paths := capServer(t, map[string]any{"allowed": false, "cap": 0, "usedTokens": 9_999_999, "exceeded": true})
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	for _, kind := range []queue.JobKind{queue.KindDeploy, queue.KindRollback, queue.KindScriptTestRun} {
		job := &queue.Job{ID: "job-3", ProjectID: "proj-3", Kind: kind}
		if err := enforceTokenCap(context.Background(), job, cfg); err != nil {
			t.Fatalf("kind %s must not be blocked by a spend cap, got: %v", kind, err)
		}
	}
	if len(*paths) != 0 {
		t.Fatalf("expected no cap checks for deterministic kinds, got %v", *paths)
	}
}

func TestEnforceTokenCap_FailsOpenWhenTheAPIIsUnreachable(t *testing.T) {
	// A cap check that cannot be performed must not become a work stoppage: the
	// alternative turns an API restart into every project being blocked. The
	// overshoot this permits is bounded by one job, exactly like the check-to-run
	// race the ADR already accepts.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	server.Close() // unreachable

	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
	job := &queue.Job{ID: "job-4", ProjectID: "proj-4", Kind: queue.KindDesignGrill}

	if err := enforceTokenCap(context.Background(), job, cfg); err != nil {
		t.Fatalf("an unreachable API should not block the job, got: %v", err)
	}
}

func TestEnforceTokenCap_ChecksEveryTokenConsumingKind(t *testing.T) {
	consuming := []queue.JobKind{
		queue.KindSpecGrill,
		queue.KindFeatureBuild,
		queue.KindTestRun,
		queue.KindAgenticReview,
		queue.KindDesignGrill,
	}
	for _, kind := range consuming {
		server, paths := capServer(t, map[string]any{
			"allowed": false, "cap": 10, "usedTokens": 10, "exceeded": true,
			"periodStart": "2026-09-01T00:00:00.000Z",
		})
		cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}
		job := &queue.Job{ID: "job-5", ProjectID: "proj-5", Kind: kind}

		if err := enforceTokenCap(context.Background(), job, cfg); err == nil {
			t.Fatalf("kind %s consumes tokens and must be checked", kind)
		}
		if len(*paths) != 1 {
			t.Fatalf("expected one check for %s, got %v", kind, *paths)
		}
		server.Close()
	}
}

func TestQueueConsumesTokens_MatchesTheAPIsRule(t *testing.T) {
	// The API is authoritative, but the Orchestrator's mirror must not drift:
	// a kind missing here would skip its check entirely.
	wantConsuming := map[queue.JobKind]bool{
		queue.KindSpecGrill:     true,
		queue.KindFeatureBuild:  true,
		queue.KindTestRun:       true,
		queue.KindAgenticReview: true,
		queue.KindDesignGrill:   true,
		queue.KindDeploy:        false,
		queue.KindRollback:      false,
		queue.KindScriptTestRun: false,
	}
	for kind, want := range wantConsuming {
		if got := queue.ConsumesTokens(kind); got != want {
			t.Fatalf("ConsumesTokens(%s) = %v, want %v", kind, got, want)
		}
	}
}
