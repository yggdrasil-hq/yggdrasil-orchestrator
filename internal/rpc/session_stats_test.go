package rpc_test

import (
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

func TestParseSessionStats_ReadsProviderReportedTotals(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_session_stats","success":true,"data":{"sessionId":"abc123","userMessages":5,"assistantMessages":5,"toolCalls":12,"toolResults":12,"totalMessages":22,"tokens":{"input":50000,"output":10000,"cacheRead":40000,"cacheWrite":5000,"total":105000},"cost":0.45,"contextUsage":{"tokens":60000,"contextWindow":200000,"percent":30}}}`)

	stats, ok := rpc.ParseSessionStats(ev)
	if !ok {
		t.Fatal("expected a successful get_session_stats response to parse")
	}
	if stats.InputTokens != 50000 {
		t.Fatalf("expected 50000 input tokens, got %d", stats.InputTokens)
	}
	if stats.OutputTokens != 10000 {
		t.Fatalf("expected 10000 output tokens, got %d", stats.OutputTokens)
	}
	if stats.CacheReadTokens != 40000 {
		t.Fatalf("expected 40000 cache-read tokens, got %d", stats.CacheReadTokens)
	}
	if stats.CacheWriteTokens != 5000 {
		t.Fatalf("expected 5000 cache-write tokens, got %d", stats.CacheWriteTokens)
	}
	if stats.TotalTokens != 105000 {
		t.Fatalf("expected Pi's own total (105000) to be used verbatim, got %d", stats.TotalTokens)
	}
	if stats.CostUSD == nil || *stats.CostUSD != 0.45 {
		t.Fatalf("expected cost 0.45, got %v", stats.CostUSD)
	}
}

func TestParseSessionStats_IgnoresEveryOtherEventType(t *testing.T) {
	// The stats response is read off the same stream that carries the agent's
	// whole event vocabulary, so a non-response line must never be mistaken
	// for an accounting result.
	cases := map[string]string{
		"agent_text message_end": `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}`,
		"message_update usage":   `{"type":"message_update","usage":{"input":100,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":101,"cost":{"total":0}},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hello "}}`,
		"agent_start":            `{"type":"agent_start"}`,
		"agent_settled":          `{"type":"agent_settled"}`,
	}
	for name, line := range cases {
		if _, ok := rpc.ParseSessionStats(rawEvent(t, line)); ok {
			t.Fatalf("expected %s not to parse as session stats", name)
		}
	}
}

func TestParseSessionStats_RejectsAnotherCommandsResponse(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,"data":{"tokens":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"total":2}}}`)

	if _, ok := rpc.ParseSessionStats(ev); ok {
		t.Fatal("expected a different command's response envelope to be rejected")
	}
}

func TestParseSessionStats_RejectsFailedResponse(t *testing.T) {
	// success:false must not be read as a genuine all-zero accounting result —
	// otherwise a rejected call would silently record "this job used nothing".
	ev := rawEvent(t, `{"type":"response","command":"get_session_stats","success":false,"error":"session unavailable"}`)

	if _, ok := rpc.ParseSessionStats(ev); ok {
		t.Fatal("expected a failed response to be rejected rather than parsed as zeros")
	}
}

func TestParseSessionStats_KeepsZeroCostDistinctFromAbsentCost(t *testing.T) {
	free := rawEvent(t, `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"input":10,"output":5,"cacheRead":0,"cacheWrite":0,"total":15},"cost":0}}`)
	absent := rawEvent(t, `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"input":10,"output":5,"cacheRead":0,"cacheWrite":0,"total":15}}}`)

	freeStats, ok := rpc.ParseSessionStats(free)
	if !ok {
		t.Fatal("expected the zero-cost response to parse")
	}
	if freeStats.CostUSD == nil || *freeStats.CostUSD != 0 {
		t.Fatalf("expected a reported cost of zero to survive as a real zero, got %v", freeStats.CostUSD)
	}

	absentStats, ok := rpc.ParseSessionStats(absent)
	if !ok {
		t.Fatal("expected the cost-less response to parse")
	}
	if absentStats.CostUSD != nil {
		t.Fatalf("expected an unreported cost to stay nil, got %v", *absentStats.CostUSD)
	}
}

func TestParseSessionStats_RecoversTotalFromComponents(t *testing.T) {
	// A provider that reports usage without a total is a reporting gap, not a
	// job that consumed nothing: storing 0 would understate every aggregate
	// built on the column, and the sum is the same quantity.
	ev := rawEvent(t, `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"input":100,"output":20,"cacheRead":5,"cacheWrite":1,"total":0}}}`)

	stats, ok := rpc.ParseSessionStats(ev)
	if !ok {
		t.Fatal("expected the response to parse")
	}
	if stats.TotalTokens != 126 {
		t.Fatalf("expected the total to fall back to the component sum (126), got %d", stats.TotalTokens)
	}
}

func TestParseSessionStats_GenuineEmptySessionStaysZero(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}}`)

	stats, ok := rpc.ParseSessionStats(ev)
	if !ok {
		t.Fatal("expected the response to parse")
	}
	if stats.TotalTokens != 0 {
		t.Fatalf("expected a genuinely empty session to stay at zero, got %d", stats.TotalTokens)
	}
}
