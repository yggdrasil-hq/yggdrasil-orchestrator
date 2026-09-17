package rpc

import "encoding/json"

// CommandGetSessionStats is Pi's RPC command asking for the session's own
// token/cost accounting (see the "Session" section of Pi's rpc.md). It is the
// only command in this suite that reads state rather than driving the agent,
// which is why it lives here beside Client.Send rather than in the curated
// event vocabulary: its response is not an event to relay but a payload this
// package parses (ParseSessionStats).
const CommandGetSessionStats = "get_session_stats"

// SessionStats is the token/cost accounting Pi reports for a whole session
// (ADR 023). Deliberately the provider-reported totals — Yggdrasil never
// tokenizes or estimates anything itself, so these are only ever as
// trustworthy as the provider's own accounting.
//
// CostUSD is a pointer because "the provider reported no cost" and "the
// provider reported a cost of exactly zero" are different facts: a model
// billed at nothing is a real, useful zero, while a missing figure must not be
// silently recorded as free (the usage table stores it NULL).
type SessionStats struct {
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	TotalTokens      int64
	CostUSD          *float64
}

// sessionStatsResponse mirrors just enough of Pi's get_session_stats response
// envelope (`{"type":"response","command":"get_session_stats","success":true,
// "data":{...}}`) to read the usage block. The response envelope is shared
// with every other command, so Command is checked rather than assumed.
type sessionStatsResponse struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Success bool   `json:"success"`
	Data    *struct {
		Tokens struct {
			Input      int64 `json:"input"`
			Output     int64 `json:"output"`
			CacheRead  int64 `json:"cacheRead"`
			CacheWrite int64 `json:"cacheWrite"`
			Total      int64 `json:"total"`
		} `json:"tokens"`
		Cost *float64 `json:"cost"`
	} `json:"data"`
}

// ParseSessionStats decodes a get_session_stats response, returning ok=false
// for every other line on the stream — the overwhelmingly common case, since
// this is read from the same Events channel that carries the agent's whole
// event stream. A failed response (success:false) is not a stats reading and
// is also rejected, so a caller can never mistake an error envelope for a
// genuine all-zero accounting result.
//
// TotalTokens falls back to the sum of the four components when the provider
// reports a zero total alongside non-zero parts. That combination is a
// provider-reporting gap rather than a real zero, and storing it as-is would
// understate every aggregate built on top of this column; the sum is
// arithmetically the same quantity, so this normalizes without inventing data.
func ParseSessionStats(ev Event) (SessionStats, bool) {
	if ev.Type != "response" {
		return SessionStats{}, false
	}

	var parsed sessionStatsResponse
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return SessionStats{}, false
	}
	if parsed.Command != CommandGetSessionStats || !parsed.Success || parsed.Data == nil {
		return SessionStats{}, false
	}

	stats := SessionStats{
		InputTokens:      parsed.Data.Tokens.Input,
		OutputTokens:     parsed.Data.Tokens.Output,
		CacheReadTokens:  parsed.Data.Tokens.CacheRead,
		CacheWriteTokens: parsed.Data.Tokens.CacheWrite,
		TotalTokens:      parsed.Data.Tokens.Total,
		CostUSD:          parsed.Data.Cost,
	}
	if stats.TotalTokens == 0 {
		stats.TotalTokens = stats.InputTokens + stats.OutputTokens +
			stats.CacheReadTokens + stats.CacheWriteTokens
	}
	return stats, true
}
