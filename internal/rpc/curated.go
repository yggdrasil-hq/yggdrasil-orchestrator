package rpc

import (
	"encoding/json"
	"strings"
)

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
	// EventAgentText carries Pi's own plain assistant text — the model's
	// prose, as opposed to a contract tool call — decoded from a
	// message_end event (translateMessageEnd). Never terminal: unlike every
	// other curated event, runTurn (worker/specgrill.go) forwards this one
	// live via handle and keeps reading, rather than ending the turn on it.
	// Exists so a turn that settles without ever calling a contract tool
	// (the agent_settled failure path) leaves a record of what the model
	// actually said, instead of just "ended without submitting a result".
	EventAgentText CuratedEventType = "agent_text"
	// EventAgentTextDelta is one streaming chunk of an assistant message, decoded
	// from Pi's message_update event while the model is still talking
	// (translateMessageUpdate). ADR 019 item 13: this is what makes the grill
	// transcript grow token by token instead of appearing in one block at
	// message_end.
	//
	// Never terminal, and a *provisional* view of text that EventAgentText will
	// later supersede: the deltas for one message concatenate to the text that
	// message_end eventually delivers whole. The API relays it without storing it
	// (one row per token would multiply the append-only job_events table and
	// bloat the catch-up read), so it is best-effort by construction — a lost
	// delta costs a moment of smoothness, never content, because the
	// authoritative message still arrives over the ordinary agent_text path.
	EventAgentTextDelta CuratedEventType = "agent_text_delta"
	// EventRequestActionItem: the yggdrasil-contract extension's
	// request_action_item tool fired (feature_build only, ADR 015 item 7-8 &
	// Track B3) — the implement skill's terminal "I'm blocked" call,
	// structurally distinct from a generic crash/`submit_build_result
	// success:false`. The API lands the feature back in `draft` and dispatches
	// a context-seeded spec_grill. Always terminal.
	EventRequestActionItem CuratedEventType = "request_action_item"
	// EventSubmitReview: the yggdrasil-contract extension's submit_review tool
	// fired (agentic_review only, ADR 015 item 14-16 & Track B6) — the
	// reviewing agent's terminal internal verdict, never a real GitHub PR
	// review (which would collide with ADR 013's human-review webhook). The
	// API maps `approved` -> in_review, `changes_requested` -> returned with
	// reason agentic_review. Always terminal.
	EventSubmitReview     CuratedEventType = "submit_review"
	EventReportTestStep   CuratedEventType = "report_test_step"
	EventSubmitTestReport CuratedEventType = "submit_test_report"
	// EventUpdateDesignPreview carries the complete current design folder
	// snapshot and ends only the current turn, not the session.
	EventUpdateDesignPreview CuratedEventType = "update_design_preview"
	// EventSubmitDesign carries the finalized design snapshot and ends the
	// design_grill session.
	EventSubmitDesign CuratedEventType = "submit_design"
	// EventMergeConflicts is synthesized locally (issue #27, ADR 021 follow-up
	// 1), never translated from Pi: it reports that this build's entrypoint
	// resolved conflicts between the feature branch and its base before the
	// agent started, which the agent's own stream has no way to know.
	//
	// It exists because a conflict resolution is the highest-risk part of a
	// build's diff — it is where the agent guessed at how two changes should
	// coexist, the judgement ADR 021 §9 says cannot be automated — and until
	// now nothing in the product said one had happened, so a reviewer read the
	// resolution interleaved with ordinary work with no signal at all.
	//
	// Never terminal: it is context for a reviewer, not a result.
	EventMergeConflicts CuratedEventType = "merge_conflicts"

	// EventForkFailed is ADR 032 item 3's preamble stopping before the first turn:
	// the stored session could not be placed in the pod, would not load, or the
	// chosen entry id was rejected.
	//
	// **Its own event type rather than a `run_failed` with a longer message.** A
	// fork can stop at three different places, and each one asks a different thing
	// of the operator: a *write* failure means the artifact never reached the
	// container; a *switch* failure means it arrived and Pi loaded nothing from it;
	// a *fork* failure means the session was fine and the resume point was not.
	// Those are three different diagnoses, so they must not arrive as three
	// sentences of identical shape — the same argument ADR 032 item 5 makes for
	// `unavailable` versus `not_collected`, applied to dispatch. Terminal: the run
	// never got as far as an agent turn.
	EventForkFailed CuratedEventType = "fork_failed"
)

// CuratedEvent is one product-meaningful event translated from Pi's raw
// RPC stream (or synthesized locally for EventRunFailed/EventRunCancelled).
type CuratedEvent struct {
	Type     CuratedEventType
	Question string // set for EventAskUser
	// QuestionHeader / QuestionMultiSelect / QuestionOptions are set for
	// EventAskUser when the agent offered the answer as a choice (issue #38),
	// and are the *rendering* half of a question that Question already carries as
	// prose.
	//
	// Pointers, not values, and deliberately: a question asked in prose has none of
	// them, and Go's zero values would make "absent" indistinguishable from
	// "present and false". That distinction is the whole contract — the API treats
	// the presence of `options` as "render a picker" and its absence as "render a
	// text box" — so `MultiSelect bool` would silently turn an explicit
	// single-select into prose, and `Options []QuestionOption` would make an empty
	// list look like no list. A `*bool` and a `*[]T` keep all three states distinct
	// through the JSON round trip.
	QuestionHeader      string
	QuestionMultiSelect *bool
	QuestionOptions     *[]QuestionOption
	Markdown            string // set for EventSubmitADR
	HasDesignSurface    *bool  // set when project_init answers the UI question
	Message             string // set for EventRunFailed/EventRunCancelled/EventAgentText/EventMergeConflicts
	Status              string // set for EventSubmitBuildResult: "success" | "failure"
	PRUrl               string // set for EventSubmitBuildResult on success
	Summary             string // set for EventSubmitBuildResult
	// Verdict is set for EventSubmitReview: "approved" | "changes_requested".
	Verdict string
	// Findings is set for EventSubmitReview when the reviewing agent listed its
	// issues per location (issue #73).
	//
	// A pointer to a slice for the same reason `QuestionOptions` is: the API
	// distinguishes an absent list (prose — the count is not knowable) from an
	// empty one (structured, and there are none), and Go's zero value would make
	// `Findings []ReviewFinding` send those two as the same thing. See the field's
	// counterpart on `contractToolResult.Details` for the full argument.
	Findings *[]ReviewFinding
	// ActionItems is set for submit_adr and request_action_item: the Action
	// Item batch or the needed items the blocked implement skill reported.
	ActionItems     []RequestedActionItem
	TestName        string
	TestStatus      string
	TestDetails     string
	ScreenshotPath  string
	Passed          *int
	Failed          *int
	Skipped         *int
	Total           *int
	CoveragePercent *float64
	FailingTests    []string
	RecordingPath   string
	// SkipReason is set for EventSubmitTestReport when a group was skipped rather
	// than run, and says *why*: "no_script" or "runner_unavailable" (issue #53).
	//
	// The two causes look identical in the counts — both report
	// `passed: 0, failed: 0, skipped: 1, total: 1` — but they mean opposite things
	// to the Testing gate: a project with no test-unit.sh has disabled the group by
	// its own choice (ADR 015 item 10) and nothing is wrong, whereas an install with
	// no image for the kind could not run a group it has, so advancing would put a
	// review over unverified work. Absent means "the runner did not say", which the
	// API treats exactly as it did before the field existed.
	SkipReason string
	// ForkStage is set for EventForkFailed: which of the fork's three steps
	// stopped ("write" | "switch" | "fork").
	//
	// A string rather than an enum shared with the worker: this package is the wire
	// vocabulary and does not import the worker, and the API validates the closed
	// set on receipt (the same arrangement `status` and `skipReason` use). Absent on
	// every other event, which is what `omitempty` on the request field is for.
	ForkStage string
	Snapshot  map[string]string
}

// RequestedActionItem is one item feature_build reported it needs via
// request_action_item (ADR 015 item 8): a type ("secret_request",
// "subtask_feature", "design_grill", "test_request") and a description.
type RequestedActionItem struct {
	Type              string `json:"type"`
	Description       string `json:"description"`
	SecretKey         string `json:"secretKey,omitempty"`
	DraftTestMarkdown string `json:"draftTestMarkdown,omitempty"`
}

// QuestionOption is one choice offered by a structured `ask_user` question
// (issue #38).
//
// `Description` is optional because it often is — "PostgreSQL" needs no gloss,
// while "SQLite" is only a real choice once you know it is "simplest for local
// development". The API normalises an absent description to an explicit null, so
// a client reads one shape rather than checking for a key per option.
type QuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

// ReviewFinding is one issue an `agentic_review` run located (issue #73), as
// emitted by `submit_review`'s `findings` list.
//
// **The field names and their optionality match the API's schema exactly**
// (`api/src/jobs/internal-routes.ts`, `reviewFindingSchema`'s `findings`), which
// in turn matches the tool's own arguments — the same rule `ask_user`'s
// structured half follows, so a reader comparing
// `agent-images/agentic_review/skills/review/SKILL.md`, the contract extension
// and this struct is comparing the same words.
//
// `Path` and `Line` are optional because a finding legitimately may carry
// neither: a remark about the change as a whole has no location, and neither
// does one about a requirement implemented *nowhere* — the SKILL tells the agent
// not to invent a location to fill the field, so an invented one must not be
// manufactured here either.
//
// **`Blocking` is a `*bool`, and the pointer is load-bearing.** The API defaults
// an absent flag to `true` at ingest (`finding.blocking ?? true`), deliberately
// fail-closed: on a `changes_requested` verdict an omitted flag means "these are
// the blockers", and defaulting to `false` would let a review pass its gate
// while displaying the findings that should stop it. A plain `bool` would send
// `false` for every finding the agent left the flag off, which is the exact
// inversion of that decision — so the absence has to survive this hop rather
// than being resolved here.
type ReviewFinding struct {
	Path     string `json:"path,omitempty"`
	Line     *int   `json:"line,omitempty"`
	Body     string `json:"body"`
	Blocking *bool  `json:"blocking,omitempty"`
}

// Terminal reports whether this event ends the whole job run (ADR 006 item
// 11): the Orchestrator should stop driving the session and tear the pod
// down, rather than waiting for more events.
//
// EventAgentTextDelta is deliberately absent: like EventAgentText it is a
// mid-turn observation, so runTurn forwards it and keeps reading.
func (e CuratedEvent) Terminal() bool {
	return e.Type == EventSubmitADR || e.Type == EventRunFailed || e.Type == EventRunCancelled ||
		e.Type == EventSubmitBuildResult || e.Type == EventRequestActionItem ||
		e.Type == EventSubmitReview || e.Type == EventSubmitTestReport ||
		e.Type == EventSubmitDesign
}

// contractToolResult mirrors the shape yggdrasil-contract's tool
// implementations return (agent-images/extensions/yggdrasil-contract/src/
// index.ts) — this suite's own code, so decoded with confidence, unlike
// Pi's own internal event shapes.
type contractToolResult struct {
	Details struct {
		Kind             string `json:"kind"`
		Question         string `json:"question"`
		Markdown         string `json:"markdown"`
		HasDesignSurface *bool  `json:"hasDesignSurface,omitempty"`
		Status           string `json:"status"`
		PRUrl            string `json:"prUrl"`
		Summary          string `json:"summary"`
		Comment          string `json:"comment,omitempty"`
		// Verdict carries submit_review's "approved" | "changes_requested".
		Verdict string `json:"verdict,omitempty"`
		// Header / MultiSelect / Options are the structured half of an ask_user
		// question (issue #38), as emitted by
		// `agent-images/extensions/yggdrasil-contract/src/index.ts`.
		//
		// `omitempty` on all three matters for the same reason the CuratedEvent
		// fields are pointers: a prose question sends none of them, and the API
		// distinguishes the two modes by whether `options` is present at all. A
		// non-pointer `MultiSelect` with omitempty would drop an explicit `false`,
		// and without omitempty it would send `false` for a prose question that
		// never mentioned the field.
		Header      string            `json:"header,omitempty"`
		MultiSelect *bool             `json:"multiSelect,omitempty"`
		Options     *[]QuestionOption `json:"options,omitempty"`
		// Findings carries submit_review's per-location findings (issue #73).
		//
		// A **pointer to a slice**, and the pointer is the whole contract — the
		// same shape and the same reason as `Options` above. The API stores
		// `undefined` as SQL NULL and `[]` as an empty jsonb array, and calls the
		// two different answers to "how many blocking issues": absent means
		// *prose — the count is not knowable*, while `[]` means *structured, and
		// there are genuinely none*. A plain slice collapses them, so every review
		// written as a paragraph would arrive claiming zero findings — turning
		// "we cannot say" into a statement of fact, in the reassuring direction.
		//
		// `omitempty` on the tag is what makes the absent case send no key at all
		// rather than `"findings":null`, which is how #38's `options` contract
		// works too.
		Findings *[]ReviewFinding `json:"findings,omitempty"`
		// ActionItems carries the needed items for request_action_item.
		ActionItems     []RequestedActionItem `json:"actionItems,omitempty"`
		TestName        string                `json:"name,omitempty"`
		TestDetails     string                `json:"details,omitempty"`
		ScreenshotPath  string                `json:"screenshotPath,omitempty"`
		Passed          *int                  `json:"passed,omitempty"`
		Failed          *int                  `json:"failed,omitempty"`
		Skipped         *int                  `json:"skipped,omitempty"`
		Total           *int                  `json:"total,omitempty"`
		CoveragePercent *float64              `json:"coveragePercent,omitempty"`
		FailingTests    []string              `json:"failingTests,omitempty"`
		RecordingPath   string                `json:"recordingPath,omitempty"`
		Snapshot        map[string]string     `json:"snapshot,omitempty"`
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

// messageEndEvent mirrors just enough of Pi's own message_end event (raw
// RPC taxonomy, decoded best-effort like agentEndEvent) to extract an
// assistant message's plain text. Verified against a real k3s pod's raw
// log: Pi emits this same shape (role/content/timestamp) for its own echo
// of an injected prompt (role "user") and, by the same shape, for the
// model's own reply (role "assistant") — the case this suite actually
// wants.
type messageEndEvent struct {
	Message struct {
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"message"`
}

// messageUpdateEvent mirrors just enough of Pi's own message_update event (raw
// RPC taxonomy, decoded best-effort like agentEndEvent/messageEndEvent) to
// extract one streaming text chunk. Pi documents this shape explicitly and
// distinguishes it from message_end in a way that matters here: message_update
// carries "a delta event without a cumulative message snapshot", so the text has
// to be accumulated by the receiver rather than read off in full. It also means
// `assistantMessageEvent` is a union — text_start/text_delta/text_end,
// thinking_*, toolcall_* — of which only text_delta carries sayable prose, which
// is why translateMessageUpdate checks the type rather than trusting `delta` to
// be present.
//
// The top-level `usage` field is deliberately not decoded: ADR 023 accounts a
// job's tokens from get_session_stats at session end, so a per-delta cumulative
// figure would be a second, moving answer to a question that already has one.
type messageUpdateEvent struct {
	AssistantMessageEvent struct {
		Type  string `json:"type"`
		Delta string `json:"delta"`
	} `json:"assistantMessageEvent"`
}

// translateMessageUpdate extracts one streaming text chunk as
// EventAgentTextDelta (ADR 019 item 13). ok is false for everything that is not
// a text_delta, which is most of the union: text_start/text_end carry no prose,
// thinking_* is reasoning the model did not address to the user, and
// toolcall_* is a tool call being assembled — translating any of those would
// stream the wrong thing into the transcript.
//
// An empty delta is also dropped rather than forwarded: it carries no text, and
// the API would relay it as a frame that appends nothing.
func translateMessageUpdate(ev Event) (CuratedEvent, bool) {
	var parsed messageUpdateEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return CuratedEvent{}, false
	}
	if parsed.AssistantMessageEvent.Type != "text_delta" {
		return CuratedEvent{}, false
	}
	if parsed.AssistantMessageEvent.Delta == "" {
		return CuratedEvent{}, false
	}
	return CuratedEvent{
		Type:    EventAgentTextDelta,
		Message: parsed.AssistantMessageEvent.Delta,
	}, true
}

// translateMessageEnd extracts an assistant message's plain text as
// EventAgentText (curated.go's doc comment on that constant explains why
// this exists). ok is false for anything that isn't a genuinely-texty
// assistant message: a non-assistant role (most commonly Pi echoing back
// the prompt the Orchestrator just sent, role "user" — not something to
// show as the agent's own words), or an assistant message whose only
// content is a tool_use block (a normal ask_user/submit_adr-only turn with
// no separate prose) — translating that would produce an empty bubble
// alongside the tool call's own curated event.
func translateMessageEnd(ev Event) (CuratedEvent, bool) {
	var parsed messageEndEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return CuratedEvent{}, false
	}
	if parsed.Message.Role != "assistant" {
		return CuratedEvent{}, false
	}

	var text strings.Builder
	for _, part := range parsed.Message.Content {
		if part.Type != "text" || part.Text == "" {
			continue
		}
		if text.Len() > 0 {
			text.WriteString("\n\n")
		}
		text.WriteString(part.Text)
	}
	if text.Len() == 0 {
		return CuratedEvent{}, false
	}
	return CuratedEvent{Type: EventAgentText, Message: text.String()}, true
}

// Translate maps a raw Pi RPC event to a curated Event (ADR 006 item 7),
// scoped to the yggdrasil-contract extension's tool-call-based signals
// (ask_user/submit_adr for spec_grill, submit_build_result for
// feature_build, ADR 010 item 7) — the ones needed to detect completion
// (item 11) — plus three raw Pi events: agent_end, but only far enough to
// catch a request-level failure (translateAgentEnd; a clean agent_end is
// left untranslated since a contract tool call, not agent_end, is what ends
// a turn normally), message_end, translated into EventAgentText
// whenever it carries the model's own plain text (translateMessageEnd), and
// message_update, translated into EventAgentTextDelta while the model is still
// streaming (translateMessageUpdate, ADR 019 item 13).
//
// ok is false for any event this suite doesn't curate (including
// tool_execution_end for tools other than ask_user/submit_adr, e.g. a
// non-contract bash call, a clean agent_end, a message_update that isn't a text
// delta, and a message_end that isn't a texty assistant message) — the caller
// should just keep reading.
func Translate(ev Event) (curated CuratedEvent, ok bool) {
	switch ev.Type {
	case "tool_execution_end":
		return translateToolExecutionEnd(ev)
	case "agent_end":
		return translateAgentEnd(ev)
	case "message_end":
		return translateMessageEnd(ev)
	case "message_update":
		return translateMessageUpdate(ev)
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
		// Issue #38: the structured half rides along with the prose question. All
		// three are passed through as-is rather than normalised here — the API owns
		// the shape (it rejects options without a header and defaults an absent
		// multiSelect), and duplicating that policy in the transport would give two
		// places to disagree about what a valid question is.
		//
		// A prose question carries none of them, and because the two structs above
		// use pointers the absence survives: `nil` here means the tool never offered
		// choices, which is exactly what the API needs to decide to render text.
		return CuratedEvent{
			Type:                EventAskUser,
			Question:            parsed.Result.Details.Question,
			QuestionHeader:      parsed.Result.Details.Header,
			QuestionMultiSelect: parsed.Result.Details.MultiSelect,
			QuestionOptions:     parsed.Result.Details.Options,
		}, true
	case "submit_adr":
		return CuratedEvent{
			Type:             EventSubmitADR,
			Markdown:         parsed.Result.Details.Markdown,
			HasDesignSurface: parsed.Result.Details.HasDesignSurface,
			ActionItems:      parsed.Result.Details.ActionItems,
		}, true
	case "submit_build_result":
		return CuratedEvent{
			Type:    EventSubmitBuildResult,
			Status:  parsed.Result.Details.Status,
			PRUrl:   parsed.Result.Details.PRUrl,
			Summary: parsed.Result.Details.Summary,
		}, true
	case "request_action_item":
		return CuratedEvent{
			Type:        EventRequestActionItem,
			Message:     "feature_build requested action items mid-build",
			ActionItems: parsed.Result.Details.ActionItems,
		}, true
	case "submit_review":
		summary := parsed.Result.Details.Comment
		if summary == "" {
			summary = parsed.Result.Details.Summary
		}
		return CuratedEvent{
			Type:     EventSubmitReview,
			Verdict:  parsed.Result.Details.Verdict,
			Summary:  summary,
			Findings: parsed.Result.Details.Findings,
		}, true
	case "report_test_step":
		return CuratedEvent{
			Type:           EventReportTestStep,
			TestName:       parsed.Result.Details.TestName,
			TestStatus:     parsed.Result.Details.Status,
			TestDetails:    parsed.Result.Details.TestDetails,
			ScreenshotPath: parsed.Result.Details.ScreenshotPath,
		}, true
	case "submit_test_report":
		return CuratedEvent{
			Type:            EventSubmitTestReport,
			Passed:          parsed.Result.Details.Passed,
			Failed:          parsed.Result.Details.Failed,
			Skipped:         parsed.Result.Details.Skipped,
			Total:           parsed.Result.Details.Total,
			CoveragePercent: parsed.Result.Details.CoveragePercent,
			FailingTests:    parsed.Result.Details.FailingTests,
			Summary:         parsed.Result.Details.Summary,
			RecordingPath:   parsed.Result.Details.RecordingPath,
		}, true
	case "update_design_preview":
		return CuratedEvent{
			Type:     EventUpdateDesignPreview,
			Snapshot: parsed.Result.Details.Snapshot,
		}, true
	case "submit_design":
		return CuratedEvent{
			Type:     EventSubmitDesign,
			PRUrl:    parsed.Result.Details.PRUrl,
			Summary:  parsed.Result.Details.Summary,
			Snapshot: parsed.Result.Details.Snapshot,
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

// LastMessageStopReason decodes a raw agent_end event and returns the stop
// reason of its last message — one of Pi's own values ("stop", "length",
// "toolUse", "error", "aborted") — or ok=false if ev isn't an agent_end
// event or carries no messages. Used by runTurn (worker/specgrill.go) to
// remember why the most recent low-level agent run ended, for the message
// it builds if agent_settled arrives next with nothing else translated in
// between — agent_settled itself carries no detail of its own (verified
// against Pi's RPC docs: `{"type": "agent_settled"}`, no other fields).
func LastMessageStopReason(ev Event) (stopReason string, ok bool) {
	if ev.Type != "agent_end" {
		return "", false
	}
	var parsed agentEndEvent
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil || len(parsed.Messages) == 0 {
		return "", false
	}
	return parsed.Messages[len(parsed.Messages)-1].StopReason, true
}
