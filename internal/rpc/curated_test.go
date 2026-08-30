package rpc_test

import (
	"encoding/json"
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
	ev := rawEvent(t, `{"type":"tool_execution_end","toolName":"submit_review","result":{"details":{"kind":"submit_review","verdict":"changes_requested","summary":"The diff doesn't match the approved ADR."},"terminate":true}}`)
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
