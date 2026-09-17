package rpc_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

func rawEvent(t *testing.T, jsonLine string) rpc.Event {
	t.Helper()
	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(jsonLine), &typed); err != nil {
		t.Fatalf("failed to build test event: %v", err)
	}
	return rpc.Event{Type: typed.Type, Raw: json.RawMessage(jsonLine)}
}

func TestTranslate_AskUserIsNotTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"ask_user","result":{"details":{"kind":"ask_user","question":"Which auth model?"},"terminate":true}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected ask_user to be curated")
	}
	if curated.Type != rpc.EventAskUser {
		t.Fatalf("expected type %q, got %q", rpc.EventAskUser, curated.Type)
	}
	if curated.Question != "Which auth model?" {
		t.Fatalf("expected question to be carried through, got %q", curated.Question)
	}
	if curated.Terminal() {
		t.Fatal("expected ask_user not to be terminal — it ends the turn, not the run, despite the tool's own terminate:true")
	}
}

func TestTranslate_SubmitADRIsTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"# ADR 1"},"terminate":true}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected submit_adr to be curated")
	}
	if curated.Type != rpc.EventSubmitADR {
		t.Fatalf("expected type %q, got %q", rpc.EventSubmitADR, curated.Type)
	}
	if curated.Markdown != "# ADR 1" {
		t.Fatalf("expected markdown to be carried through, got %q", curated.Markdown)
	}
	if !curated.Terminal() {
		t.Fatal("expected submit_adr to be terminal")
	}
}

func TestTranslate_SubmitADRCarriesActionItems(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"# ADR 1","actionItems":[{"type":"secret_request","description":"Need a key","secretKey":"API_KEY"},{"type":"test_request","description":"Add smoke coverage","draftTestMarkdown":"## Smoke"}]},"terminate":true}}`)

	curated, ok := rpc.Translate(ev)
	if !ok || len(curated.ActionItems) != 2 {
		t.Fatalf("expected two submit_adr Action Items, got %+v (ok=%v)", curated.ActionItems, ok)
	}
	if curated.ActionItems[0].SecretKey != "API_KEY" || curated.ActionItems[1].DraftTestMarkdown != "## Smoke" {
		t.Fatalf("expected Action Item fields to survive translation, got %+v", curated.ActionItems)
	}
}

func TestTranslate_SubmitADRCarriesDesignSurfaceAnswer(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_adr","result":{"details":{"kind":"submit_adr","markdown":"# Project ADR","hasDesignSurface":true},"terminate":true}}`)
	curated, ok := rpc.Translate(ev)
	if !ok || curated.HasDesignSurface == nil || !*curated.HasDesignSurface {
		t.Fatalf("expected submit_adr to carry hasDesignSurface=true, got %+v (ok=%v)", curated, ok)
	}
}

func TestTranslate_IgnoresNonContractToolCalls(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"bash","result":{"content":[{"type":"text","text":"ok"}]},"isError":false}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected a non-contract tool call not to be curated")
	}
}

func TestTranslate_IgnoresOtherEventTypes(t *testing.T) {
	ev := rawEvent(t, `{"type":"agent_start"}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected a non-tool_execution_end event not to be curated")
	}
}

func TestTranslate_IgnoresMalformedResult(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_adr","result":"not-an-object"}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected a malformed result to be ignored, not to panic or curate garbage")
	}
}

func TestTranslate_AgentEndWithErrorIsRunFailed(t *testing.T) {
	ev := rawEvent(t, `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"error","errorMessage":"404: {\"message\":\"Not Found\",\"code\":404}"}],"willRetry":false}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected an agent_end carrying a stopReason:error message to be curated")
	}
	if curated.Type != rpc.EventRunFailed {
		t.Fatalf("expected type %q, got %q", rpc.EventRunFailed, curated.Type)
	}
	if curated.Message != `404: {"message":"Not Found","code":404}` {
		t.Fatalf("expected errorMessage to be carried through, got %q", curated.Message)
	}
	if !curated.Terminal() {
		t.Fatal("expected this EventRunFailed to be terminal")
	}
}

func TestTranslate_CleanAgentEndIsNotCurated(t *testing.T) {
	ev := rawEvent(t, `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"endTurn"}],"willRetry":false}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected a clean agent_end (no error stopReason) not to be curated — a contract tool call ends the turn, not agent_end")
	}
}

func TestTranslate_SubmitBuildResultSuccessIsTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_build_result","result":{"details":{"kind":"submit_build_result","status":"success","prUrl":"https://github.com/acme/web/pull/42","summary":"Added dark mode toggle."},"terminate":true}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected submit_build_result to be curated")
	}
	if curated.Type != rpc.EventSubmitBuildResult {
		t.Fatalf("expected type %q, got %q", rpc.EventSubmitBuildResult, curated.Type)
	}
	if curated.Status != "success" {
		t.Fatalf("expected status %q, got %q", "success", curated.Status)
	}
	if curated.PRUrl != "https://github.com/acme/web/pull/42" {
		t.Fatalf("expected prUrl to be carried through, got %q", curated.PRUrl)
	}
	if curated.Summary != "Added dark mode toggle." {
		t.Fatalf("expected summary to be carried through, got %q", curated.Summary)
	}
	if !curated.Terminal() {
		t.Fatal("expected submit_build_result to be terminal")
	}
}

func TestTranslate_SubmitBuildResultFailureIsAlsoTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_build_result","result":{"details":{"kind":"submit_build_result","status":"failure","summary":"ADR referenced a package that doesn't exist."},"terminate":true}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected submit_build_result to be curated")
	}
	if curated.Status != "failure" {
		t.Fatalf("expected status %q, got %q", "failure", curated.Status)
	}
	if curated.PRUrl != "" {
		t.Fatalf("expected no prUrl on a failure result, got %q", curated.PRUrl)
	}
	if !curated.Terminal() {
		t.Fatal("expected a failed submit_build_result to still be terminal — it ends the run either way, only the caller decides success vs. failure")
	}
}

func TestTranslate_RequestActionItemIsTerminalAndCarriesItems(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"request_action_item","result":{"details":{"kind":"request_action_item","actionItems":[{"type":"secret_request","description":"Need a deploy token"}]},"terminate":true}}`)
	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected request_action_item to be curated")
	}
	if curated.Type != rpc.EventRequestActionItem {
		t.Fatalf("expected EventRequestActionItem, got %q", curated.Type)
	}
	if len(curated.ActionItems) != 1 || curated.ActionItems[0].Type != "secret_request" || curated.ActionItems[0].Description != "Need a deploy token" {
		t.Fatalf("unexpected action items: %+v", curated.ActionItems)
	}
	if !curated.Terminal() {
		t.Fatal("expected request_action_item to be terminal — it ends feature_build's run")
	}
}

func TestTranslate_SubmitReviewIsTerminalAndCarriesVerdict(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_review","result":{"details":{"kind":"submit_review","verdict":"changes_requested","comment":"The diff doesn't match the approved ADR."},"terminate":true}}`)
	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected submit_review to be curated")
	}
	if curated.Type != rpc.EventSubmitReview {
		t.Fatalf("expected EventSubmitReview, got %q", curated.Type)
	}
	if curated.Verdict != "changes_requested" {
		t.Fatalf("expected verdict to be decoded, got %q", curated.Verdict)
	}
	if curated.Summary != "The diff doesn't match the approved ADR." {
		t.Fatalf("expected comment to be decoded, got %q", curated.Summary)
	}
	if !curated.Terminal() {
		t.Fatal("expected submit_review to be terminal — it ends the review run")
	}
}

func TestTranslate_DesignPreviewIsNonTerminalAndCarriesSnapshot(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"update_design_preview","result":{"details":{"kind":"update_design_preview","snapshot":{"page.html":"<h1>Hello</h1>","styles.css":"body{}"}},"terminate":true}}`)
	curated, ok := rpc.Translate(ev)
	if !ok || curated.Type != rpc.EventUpdateDesignPreview {
		t.Fatalf("expected update_design_preview, got %+v (ok=%v)", curated, ok)
	}
	if curated.Snapshot["page.html"] != "<h1>Hello</h1>" || curated.Snapshot["styles.css"] != "body{}" {
		t.Fatalf("unexpected design snapshot: %+v", curated.Snapshot)
	}
	if curated.Terminal() {
		t.Fatal("expected update_design_preview to end only the current turn")
	}
}

func TestTranslate_SubmitDesignIsTerminalAndCarriesSnapshot(t *testing.T) {
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_design","result":{"details":{"kind":"submit_design","snapshot":{"page.html":"<main/>"},"prUrl":"https://github.com/acme/web/pull/9","summary":"Checkout mockup"},"terminate":true}}`)
	curated, ok := rpc.Translate(ev)
	if !ok || curated.Type != rpc.EventSubmitDesign {
		t.Fatalf("expected submit_design, got %+v (ok=%v)", curated, ok)
	}
	if curated.PRUrl != "https://github.com/acme/web/pull/9" || curated.Summary != "Checkout mockup" {
		t.Fatalf("unexpected design result: %+v", curated)
	}
	if !curated.Terminal() {
		t.Fatal("expected submit_design to end the session")
	}
}

func TestTranslate_AssistantMessageEndIsAgentTextAndNotTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Here's my thinking on the port question."}],"timestamp":1787514442356}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected an assistant message_end with text to be curated")
	}
	if curated.Type != rpc.EventAgentText {
		t.Fatalf("expected type %q, got %q", rpc.EventAgentText, curated.Type)
	}
	if curated.Message != "Here's my thinking on the port question." {
		t.Fatalf("expected the text to be carried through, got %q", curated.Message)
	}
	if curated.Terminal() {
		t.Fatal("expected agent_text not to be terminal — it must never end a turn on its own")
	}
}

func TestTranslate_UserMessageEndIsNotCurated(t *testing.T) {
	ev := rawEvent(t, `{"type":"message_end","message":{"role":"user","content":[{"type":"text","text":"bind next server to port 80"}],"timestamp":1787514442356}}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected Pi's own echo of the injected prompt (role: user) not to be curated as agent_text")
	}
}

func TestTranslate_AssistantMessageEndWithOnlyToolUseIsNotCurated(t *testing.T) {
	ev := rawEvent(t, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"tool_use","id":"1","name":"ask_user"}],"timestamp":1787514442356}}`)

	_, ok := rpc.Translate(ev)
	if ok {
		t.Fatal("expected an assistant message with no text content (only a tool_use block) not to be curated — would just be an empty bubble alongside the tool call's own event")
	}
}

// ADR 019 item 13: a streaming text chunk becomes agent_text_delta, and —
// critically — is NOT terminal, since the message it belongs to is still being
// written. If this ever became terminal the turn would end mid-sentence and the
// job would tear down before the model called a contract tool.
func TestTranslate_MessageUpdateTextDeltaIsAgentTextDeltaAndNotTerminal(t *testing.T) {
	ev := rawEvent(t, `{"type":"message_update","usage":{"input":100,"output":1,"totalTokens":101},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hello "}}`)

	curated, ok := rpc.Translate(ev)
	if !ok {
		t.Fatal("expected a message_update text_delta to be curated")
	}
	if curated.Type != rpc.EventAgentTextDelta {
		t.Fatalf("expected type %q, got %q", rpc.EventAgentTextDelta, curated.Type)
	}
	if curated.Message != "Hello " {
		t.Fatalf("expected the delta text to be carried through verbatim (including its trailing space), got %q", curated.Message)
	}
	if curated.Terminal() {
		t.Fatal("expected agent_text_delta not to be terminal — it must never end a turn on its own")
	}
}

// The delta and the finished message must not disagree about the text: Pi's docs
// are explicit that message_update carries a delta without a cumulative
// snapshot, so the deltas for one message concatenate to exactly what
// message_end later delivers whole. This is the property the Web app relies on
// when it drops its accumulated buffer in favour of agent_text.
func TestTranslate_DeltasConcatenateToTheMessageEndText(t *testing.T) {
	parts := []string{"Hello", " world", "!"}

	var built string
	for _, part := range parts {
		ev := rawEvent(t, `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":`+strconv.Quote(part)+`}}`)
		curated, ok := rpc.Translate(ev)
		if !ok {
			t.Fatalf("expected delta %q to be curated", part)
		}
		built += curated.Message
	}

	end := rawEvent(t, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello world!"}],"timestamp":1}}`)
	finished, ok := rpc.Translate(end)
	if !ok {
		t.Fatal("expected the message_end to be curated as agent_text")
	}
	if built != finished.Message {
		t.Fatalf("expected the concatenated deltas %q to equal the finished message %q", built, finished.Message)
	}
}

// Only text_delta is prose. Everything else in Pi's assistantMessageEvent union
// (block boundaries, thinking, tool-call assembly) must produce nothing, or the
// transcript would stream reasoning the model never addressed to the user, or
// raw tool arguments.
func TestTranslate_MessageUpdateNonTextDeltasAreNotCurated(t *testing.T) {
	cases := map[string]string{
		"text_start":      `{"type":"message_update","assistantMessageEvent":{"type":"text_start","contentIndex":0}}`,
		"text_end":        `{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"Hello world"}}`,
		"thinking_start":  `{"type":"message_update","assistantMessageEvent":{"type":"thinking_start","contentIndex":0}}`,
		"thinking_delta":  `{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"considering options"}}`,
		"thinking_end":    `{"type":"message_update","assistantMessageEvent":{"type":"thinking_end","contentIndex":0}}`,
		"toolcall_start":  `{"type":"message_update","assistantMessageEvent":{"type":"toolcall_start","contentIndex":1,"id":"call_abc123","toolName":"write"}}`,
		"toolcall_delta":  `{"type":"message_update","assistantMessageEvent":{"type":"toolcall_delta","contentIndex":1,"delta":"{\"path\":"}}`,
		"toolcall_end":    `{"type":"message_update","assistantMessageEvent":{"type":"toolcall_end","contentIndex":1}}`,
		"missing_event":   `{"type":"message_update"}`,
		"unknown_subtype": `{"type":"message_update","assistantMessageEvent":{"type":"something_new","delta":"ignored"}}`,
	}

	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := rpc.Translate(rawEvent(t, line)); ok {
				t.Fatalf("expected %s not to be curated as agent_text_delta", name)
			}
		})
	}
}

// An empty delta carries no text; relaying it would append nothing while still
// costing an HTTP request and a frame.
func TestTranslate_MessageUpdateEmptyDeltaIsNotCurated(t *testing.T) {
	ev := rawEvent(t, `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":""}}`)

	if _, ok := rpc.Translate(ev); ok {
		t.Fatal("expected an empty text_delta not to be curated")
	}
}

func TestTranslate_MessageUpdateMalformedIsNotCurated(t *testing.T) {
	for name, line := range map[string]string{
		"not_an_object":      `{"type":"message_update","assistantMessageEvent":"text_delta"}`,
		"delta_not_a_string": `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":42}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := rpc.Translate(rawEvent(t, line)); ok {
				t.Fatalf("expected malformed message_update (%s) not to be curated", name)
			}
		})
	}
}

func TestRunFailedIsTerminal(t *testing.T) {
	ev := rpc.CuratedEvent{Type: rpc.EventRunFailed, Message: "attach stream ended"}
	if !ev.Terminal() {
		t.Fatal("expected EventRunFailed to be terminal")
	}
}

func TestRunCancelledIsTerminal(t *testing.T) {
	ev := rpc.CuratedEvent{Type: rpc.EventRunCancelled, Message: "job cancelled"}
	if !ev.Terminal() {
		t.Fatal("expected EventRunCancelled to be terminal")
	}
}

func TestRunStartedIsNotTerminal(t *testing.T) {
	ev := rpc.CuratedEvent{Type: rpc.EventRunStarted}
	if ev.Terminal() {
		t.Fatal("expected EventRunStarted not to be terminal — it only signals the pod is up, the run has barely begun")
	}
}
