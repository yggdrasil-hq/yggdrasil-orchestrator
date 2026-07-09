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
