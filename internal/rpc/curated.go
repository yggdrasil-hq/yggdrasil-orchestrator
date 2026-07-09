package rpc

import "encoding/json"

// CuratedEventType is the small, product-meaningful event vocabulary the
// Orchestrator forwards on (ADR 006 item 7) — deliberately narrower than
// Pi's own raw RPC event taxonomy.
type CuratedEventType string

const (
	// EventAskUser: the yggdrasil-contract extension's ask_user tool fired.
	// Ends the current Pi *turn*, not the job run — the run stays open
	// waiting for a human reply (ADR 006 items 9-10, not yet built).
	EventAskUser CuratedEventType = "ask_user"
	// EventSubmitADR: the yggdrasil-contract extension's submit_adr tool
	// fired. This — not agent_end, and not the ask_user tool's own
	// terminate:true (both tools set that flag; only the tool identity
	// distinguishes them) — is what ends a spec_grill run (ADR 006 item 11).
	EventSubmitADR CuratedEventType = "submit_adr"
	// EventRunFailed is synthesized locally (never present in Pi's own
	// stream) when the attach stream itself ends unexpectedly — the
	// container crashed, the connection dropped, or ctx was cancelled
	// before a terminating contract event was ever seen.
	EventRunFailed CuratedEventType = "run_failed"
	// EventRunCancelled is synthesized locally (never present in Pi's own
	// stream), like EventRunFailed, but for the expected case: a human
	// asked to stop the run (queue.Queue.WatchCancellation), not an error.
	EventRunCancelled CuratedEventType = "run_cancelled"
)

// CuratedEvent is one product-meaningful event translated from Pi's raw
// RPC stream (or synthesized locally for EventRunFailed/EventRunCancelled).
type CuratedEvent struct {
	Type     CuratedEventType
	Question string // set for EventAskUser
	Markdown string // set for EventSubmitADR
	Message  string // set for EventRunFailed/EventRunCancelled
}

// Terminal reports whether this event ends the whole job run (ADR 006 item
// 11): the Orchestrator should stop driving the session and tear the pod
// down, rather than waiting for more events.
func (e CuratedEvent) Terminal() bool {
	return e.Type == EventSubmitADR || e.Type == EventRunFailed || e.Type == EventRunCancelled
}

// contractToolResult mirrors the shape yggdrasil-contract's tool
// implementations return (agent-images/extensions/yggdrasil-contract/src/
// index.ts) — this suite's own code, so decoded with confidence, unlike
// Pi's own internal event shapes.
type contractToolResult struct {
	Details struct {
		Kind     string `json:"kind"`
		Question string `json:"question"`
		Markdown string `json:"markdown"`
	} `json:"details"`
}

type toolExecutionEndEvent struct {
	ToolName string             `json:"toolName"`
	Result   contractToolResult `json:"result"`
}

// Translate maps a raw Pi RPC event to a curated Event (ADR 006 item 7),
// scoped for now to the yggdrasil-contract extension's tool-call-based
// signals (ask_user/submit_adr) — the ones needed to detect completion
// (item 11), and the only ones this suite can decode with full confidence
// today, since the extension's shape is this suite's own code. Plain
// assistant text (agent_text) is intentionally not translated yet: Pi's own
// message event shapes aren't confirmed against a real integration.
//
// ok is false for any event this suite doesn't curate (including
// tool_execution_end for tools other than ask_user/submit_adr, e.g. a
// non-contract bash call) — the caller should just keep reading.
func Translate(ev Event) (curated CuratedEvent, ok bool) {
	if ev.Type != "tool_execution_end" {
		return CuratedEvent{}, false
	}

	var parsed toolExecutionEndEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return CuratedEvent{}, false
	}

	switch parsed.Result.Details.Kind {
	case "ask_user":
		return CuratedEvent{Type: EventAskUser, Question: parsed.Result.Details.Question}, true
	case "submit_adr":
		return CuratedEvent{Type: EventSubmitADR, Markdown: parsed.Result.Details.Markdown}, true
	default:
		return CuratedEvent{}, false
	}
}
