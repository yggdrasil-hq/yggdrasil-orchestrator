package rpc_test

import (
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// rawEvent is declared in curated_test.go and shared across this package's tests.
// The response body is copied from Pi's own docs/rpc.md (the get_state section),
// so this test fails if Pi changes the shape the ADR 032 contract depends on.
func TestParseSessionFileReadsTheDocumentedGetStateResponse(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,`+
		`"data":{"model":null,"isStreaming":false,"sessionFile":"/root/.pi/agent/sessions/abc.jsonl",`+
		`"sessionId":"abc123","sessionName":"my-feature-work","messageCount":5}}`)

	session, ok := rpc.ParseSessionFile(ev)
	if !ok {
		t.Fatal("expected the documented get_state response to parse")
	}
	if session.FilePath != "/root/.pi/agent/sessions/abc.jsonl" {
		t.Errorf("FilePath = %q", session.FilePath)
	}
	if session.SessionID != "abc123" {
		t.Errorf("SessionID = %q", session.SessionID)
	}
}

func TestParseSessionFileRejectsEveryOtherLine(t *testing.T) {
	// This reads from the same stream as the agent's whole event stream, so the
	// common case is "not mine" — including the *other* command this suite reads,
	// which is the case that would silently cross the wires if the command were
	// not checked.
	cases := map[string]string{
		"an agent event":               `{"type":"message_update","delta":"hi"}`,
		"the session-stats response":   `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"total":1}}}`,
		"a different command's answer": `{"type":"response","command":"get_messages","success":true,"data":{"messages":[]}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := rpc.ParseSessionFile(rawEvent(t, line)); ok {
				t.Fatal("parsed a line that is not a get_state response")
			}
		})
	}
}

func TestParseSessionFileRejectsAFailedResponse(t *testing.T) {
	// A failure must not read as "this session has no file": ADR 032 item 5 needs
	// "Pi did not answer" to stay distinguishable from "Pi answered, and there is
	// no file", because only the first is `unavailable` and only the second is
	// `not_collected`.
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":false,"error":"nope"}`)

	if _, ok := rpc.ParseSessionFile(ev); ok {
		t.Fatal("a failed response must not parse as a session with no file")
	}
}

func TestParseSessionFileAcceptsADocumentedInMemorySession(t *testing.T) {
	// Pi documents `sessionFile` as part of the state object, but a session
	// created with --no-session legitimately reports none. That is a successful
	// answer with an empty path — not a failure — and the caller distinguishes it
	// from an unanswered question.
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,"data":{"sessionFile":""}}`)

	session, ok := rpc.ParseSessionFile(ev)
	if !ok {
		t.Fatal("an in-memory session's answer is still an answer")
	}
	if session.FilePath != "" {
		t.Errorf("FilePath = %q, want empty", session.FilePath)
	}
}

func TestParseSessionFileRejectsAMalformedBody(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,"data":"not-an-object"}`)

	if _, ok := rpc.ParseSessionFile(ev); ok {
		t.Fatal("a malformed data block must not parse")
	}
}
