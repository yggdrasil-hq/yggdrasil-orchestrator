package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/capabilities"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected ok, got %q", body["status"])
	}
}

// Issue #63: `resolveAgentImages` decides which kinds this process has an image
// for, and `capabilities.ImageBackedKinds` decides which kinds it will publish a
// claim about. If the two ever drift, the failure is silent in the dangerous
// direction: a kind added to one list and not the other simply gets no row, and
// the API reads "no row" as *runnable* — so the install would keep dispatching
// work it cannot do, which is the bug #63 exists to fix, now hidden behind a
// feature that looks implemented.
//
// Asserting the coupling here rather than in either package is deliberate: this
// is the one place that can see both, and neither package should have to know
// about the other's list.
func TestResolveAgentImages_MatchesTheKindsCapabilitiesPublishes(t *testing.T) {
	// Every kind's env var set, so the returned map is the full set this process
	// can recognise — not just the subset configured in this environment.
	for _, spec := range []struct{ envVar, kind string }{
		{"SPEC_GRILL_IMAGE", "spec_grill"},
		{"FEATURE_BUILD_IMAGE", "feature_build"},
		{"TEST_RUN_IMAGE", "test_run"},
		{"SCRIPT_TEST_RUN_IMAGE", "script_test_run"},
		{"AGENTIC_REVIEW_IMAGE", "agentic_review"},
		{"DESIGN_GRILL_IMAGE", "design_grill"},
	} {
		t.Setenv(spec.envVar, "ghcr.io/example/"+spec.kind+":latest")
	}

	configured := resolveAgentImages()

	published := make(map[string]bool)
	for _, kind := range capabilities.ImageBackedKinds() {
		published[string(kind)] = true
	}

	for kind := range configured {
		if !published[string(kind)] {
			t.Fatalf(
				"%s has an image env var in resolveAgentImages but is not in capabilities.ImageBackedKinds, "+
					"so no capability claim is published for it and the API will keep dispatching it",
				kind,
			)
		}
	}
	for kind := range published {
		if _, ok := configured[queue.JobKind(kind)]; !ok {
			t.Fatalf(
				"%s is published by capabilities.ImageBackedKinds but has no image env var in "+
					"resolveAgentImages, so its claim would always read as unrunnable",
				kind,
			)
		}
	}

	if len(configured) != len(published) {
		t.Fatalf("expected %d kinds on both sides, got %d configured and %d published",
			len(published), len(configured), len(published))
	}
}

// The default must be the package's own, so a deployment that sets nothing gets
// the interval the reader's trust window was designed against.
func TestResolveCapabilityReportInterval_DefaultsAndReadsTheEnvVar(t *testing.T) {
	t.Setenv("CAPABILITY_REPORT_INTERVAL", "")
	if got := resolveCapabilityReportInterval(); got != capabilities.DefaultReportInterval {
		t.Fatalf("expected the package default %s, got %s", capabilities.DefaultReportInterval, got)
	}

	t.Setenv("CAPABILITY_REPORT_INTERVAL", "90s")
	if got := resolveCapabilityReportInterval(); got != 90*time.Second {
		t.Fatalf("expected 90s, got %s", got)
	}

	// An unparseable value falls back rather than disabling reporting: there is no
	// useful state in which this process stops saying what it can run.
	t.Setenv("CAPABILITY_REPORT_INTERVAL", "not-a-duration")
	if got := resolveCapabilityReportInterval(); got != capabilities.DefaultReportInterval {
		t.Fatalf("expected the package default on a bad value, got %s", got)
	}
}
