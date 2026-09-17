package worker

import (
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// ADR 023: the payload posted to the API carries what actually served the run,
// so the model id must come from the pod's own env (ground truth) rather than
// from whatever the API's config resolution currently says.
func TestUsageReportFrom_CarriesModelAndProviderReportedTotals(t *testing.T) {
	cost := 0.45
	stats := rpc.SessionStats{
		InputTokens:      50_000,
		OutputTokens:     10_000,
		CacheReadTokens:  40_000,
		CacheWriteTokens: 5_000,
		TotalTokens:      105_000,
		CostUSD:          &cost,
	}

	usage := usageReportFrom(stats, 90*time.Second, map[string]string{
		"MODEL_ID":       "anthropic/claude-sonnet-4",
		"MODEL_BASE_URL": "https://openrouter.ai/api/v1",
		"JOB_KIND":       "feature_build",
		// Never sent: a provider key must not leave the pod even though the
		// same env map carries it.
		"MODEL_API_KEY": "sk-should-never-be-reported",
	})

	if usage.ModelID != "anthropic/claude-sonnet-4" {
		t.Fatalf("expected the pod's own model id, got %q", usage.ModelID)
	}
	if usage.InputTokens != 50_000 || usage.OutputTokens != 10_000 ||
		usage.CacheReadTokens != 40_000 || usage.CacheWriteTokens != 5_000 ||
		usage.TotalTokens != 105_000 {
		t.Fatalf("token counts were not carried through verbatim: %+v", usage)
	}
	if usage.CostUSD == nil || *usage.CostUSD != 0.45 {
		t.Fatalf("expected the provider-reported cost to survive, got %v", usage.CostUSD)
	}
	if usage.DurationMs == nil || *usage.DurationMs != 90_000 {
		t.Fatalf("expected 90000ms, got %v", usage.DurationMs)
	}
}

// A job pod with no model config at all (a kind that never resolved one) still
// reports its usage; the model id is simply absent rather than invented.
func TestUsageReportFrom_OmitsModelWhenEnvHasNone(t *testing.T) {
	usage := usageReportFrom(rpc.SessionStats{TotalTokens: 10}, 0, map[string]string{})

	if usage.ModelID != "" {
		t.Fatalf("expected no model id, got %q", usage.ModelID)
	}
}

// An unreported cost must stay distinguishable from a real zero: the column is
// nullable and "we don't know" is not the same fact as "free".
func TestUsageReportFrom_LeavesUnreportedCostNil(t *testing.T) {
	usage := usageReportFrom(rpc.SessionStats{TotalTokens: 10}, time.Second, map[string]string{})

	if usage.CostUSD != nil {
		t.Fatalf("expected an unreported cost to stay nil, got %v", *usage.CostUSD)
	}
	if usage.DurationMs == nil {
		t.Fatal("expected a duration even for a zero-length session — the run happened")
	}
}
