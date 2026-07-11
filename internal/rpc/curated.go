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
	// EventRunFailed is either synthesized locally (by reportSessionError,
	// worker/specgrill.go) when the attach stream itself ends unexpectedly —
	// the container crashed, the connection dropped, or ctx was cancelled
	// before a terminating contract event was ever seen — or translated
	// directly from Pi's own agent_end event (see translateAgentEnd) when Pi
	// reports a request-level error (e.g. the configured model rejecting the
	// call) without ever exiting or closing the stream itself.
	EventRunFailed CuratedEventType = "run_failed"
	// EventRunCancelled is synthesized locally (never present in Pi's own
	// stream), like EventRunFailed, but for the expected case: a human
	// asked to stop the run (queue.Queue.WatchCancellation), not an error.
	EventRunCancelled CuratedEventType = "run_cancelled"
	// EventSubmitBuildResult: the yggdrasil-contract extension's
	// submit_build_result tool fired (feature_build only, ADR 010 item 7) —
	// the implement skill's single terminating call, analogous to
	// submit_adr for spec_grill. Always ends the run (Terminal() is true
	// regardless of Status): the two outcomes (Status "success"/"failure")
	// are distinguished by the caller, not by whether the run ended.
	EventSubmitBuildResult CuratedEventType = "submit_build_result"
	// EventRunStarted is synthesized locally (never present in Pi's own
	// stream, like EventRunFailed/EventRunCancelled) the moment the job's
	// pod is confirmed up (k8s.WaitForJobPod succeeds), before Pi has even
	// received its first prompt (ADR 011 item 2). Fired from the one call
	// site shared by spec_grill and feature_build, with no job-kind
	// branching: the API's guarded write this drives is what makes it a
	// no-op for spec_grill (whose feature sits in 'draft', not 'queued').
	// Never terminal.
	EventRunStarted CuratedEventType = "run_started"
)

// CuratedEvent is one product-meaningful event translated from Pi's raw
// RPC stream (or synthesized locally for EventRunFailed/EventRunCancelled).
type CuratedEvent struct {
	Type     CuratedEventType
	Question string // set for EventAskUser
	Markdown string // set for EventSubmitADR
	Message  string // set for EventRunFailed/EventRunCancelled
	Status   string // set for EventSubmitBuildResult: "success" | "failure"
	PRUrl    string // set for EventSubmitBuildResult on success
	Summary  string // set for EventSubmitBuildResult
}

// Terminal reports whether this event ends the whole job run (ADR 006 item
// 11): the Orchestrator should stop driving the session and tear the pod
// down, rather than waiting for more events.
func (e CuratedEvent) Terminal() bool {
	return e.Type == EventSubmitADR || e.Type == EventRunFailed || e.Type == EventRunCancelled || e.Type == EventSubmitBuildResult
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
		Status   string `json:"status"`
		PRUrl    string `json:"prUrl"`
		Summary  string `json:"summary"`
	} `json:"details"`
}

type toolExecutionEndEvent struct {
	ToolName string             `json:"toolName"`
	Result   contractToolResult `json:"result"`
}

// agentEndEvent mirrors just enough of Pi's own agent_end event (raw RPC
// taxonomy, not this suite's code, so decoded best-effort like the rest of
// Pi's own shapes) to detect one specific case: the agent ending with an
// unrecoverable request-level error (e.g. the configured MODEL_ID/base URL
// rejecting the call with a 404) rather than a normal contract-tool-driven
// turn end. Verified against a real run hitting an invalid model.
type agentEndEvent struct {
	Messages []struct {
		StopReason   string `json:"stopReason"`
		ErrorMessage string `json:"errorMessage"`
	} `json:"messages"`
}

// Translate maps a raw Pi RPC event to a curated Event (ADR 006 item 7),
// scoped for now to the yggdrasil-contract extension's tool-call-based
// signals (ask_user/submit_adr for spec_grill, submit_build_result for
// feature_build, ADR 010 item 7) — the ones needed to detect completion
// (item 11) — plus one raw Pi event, agent_end, but only far enough to catch
// a request-level failure (translateAgentEnd); a clean agent_end is left
// untranslated since a contract tool call, not agent_end, is what ends a
// turn normally. Plain assistant text (agent_text) is intentionally not
// translated yet: Pi's own message event shapes aren't confirmed against a
// real integration beyond what's needed here.
//
// ok is false for any event this suite doesn't curate (including
// tool_execution_end for tools other than ask_user/submit_adr, e.g. a
// non-contract bash call, and a clean agent_end) — the caller should just
// keep reading.
func Translate(ev Event) (curated CuratedEvent, ok bool) {
	switch ev.Type {
	case "tool_execution_end":
		return translateToolExecutionEnd(ev)
	case "agent_end":
		return translateAgentEnd(ev)
	default:
		return CuratedEvent{}, false
	}
}

func translateToolExecutionEnd(ev Event) (CuratedEvent, bool) {
	var parsed toolExecutionEndEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return CuratedEvent{}, false
	}

	switch parsed.Result.Details.Kind {
	case "ask_user":
		return CuratedEvent{Type: EventAskUser, Question: parsed.Result.Details.Question}, true
	case "submit_adr":
		return CuratedEvent{Type: EventSubmitADR, Markdown: parsed.Result.Details.Markdown}, true
	case "submit_build_result":
		return CuratedEvent{
			Type:    EventSubmitBuildResult,
			Status:  parsed.Result.Details.Status,
			PRUrl:   parsed.Result.Details.PRUrl,
			Summary: parsed.Result.Details.Summary,
		}, true
	default:
		return CuratedEvent{}, false
	}
}

// translateAgentEnd catches what the read loop used to miss entirely
// (worker/specgrill.go's runTurn): Pi's RPC process doesn't exit or close
// the stream after a request-level failure like a 404 from an invalid
// model — it just goes idle waiting for the next command, so nothing else
// in this package would ever notice the run was over. A clean agent_end
// (no message with stopReason "error") isn't curated here — ok is false —
// since that's not how a successful turn ends (see Translate's doc comment).
func translateAgentEnd(ev Event) (CuratedEvent, bool) {
	var parsed agentEndEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return CuratedEvent{}, false
	}

	for _, m := range parsed.Messages {
		if m.StopReason != "error" {
			continue
		}
		message := m.ErrorMessage
		if message == "" {
			message = "agent run ended with an error"
		}
		return CuratedEvent{Type: EventRunFailed, Message: message}, true
	}
	return CuratedEvent{}, false
}
