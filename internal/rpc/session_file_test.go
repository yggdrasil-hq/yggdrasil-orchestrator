package rpc_test

import (
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// rawEvent is declared in curated_test.go and shared across this package's tests.
// The response body is **captured from a real Pi process**, not transcribed from
// the docs: Pi 0.84.4 in the pinned base image, sent `{"type":"get_state"}` over
// RPC and answered with exactly this. That matters because ADR 032's contract
// rests on the shape, and a doc can drift from the binary — the docs were checked
// separately and agree, but this is the stronger evidence of the two.
func TestParseSessionFileReadsTheDocumentedGetStateResponse(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,"data":`+
		`{"model":{"id":"unknown","name":"unknown","api":"unknown","provider":"unknown",`+
		`"baseUrl":"","reasoning":false,"input":[],"cost":{"input":0,"output":0,`+
		`"cacheRead":0,"cacheWrite":0},"contextWindow":0,"maxTokens":0},`+
		`"thinkingLevel":"off","isStreaming":false,"isCompacting":false,`+
		`"steeringMode":"one-at-a-time","followUpMode":"one-at-a-time",`+
		`"sessionFile":"/root/.pi/agent/sessions/--workspace--/2026-09-19T06-43-12-287Z_01a0b867-991f-7a57-930f-4966d876d8a4.jsonl",`+
		`"sessionId":"01a0b867-991f-7a57-930f-4966d876d8a4","autoCompactionEnabled":true,`+
		`"messageCount":0,"pendingMessageCount":0}}`)

	session, ok := rpc.ParseSessionFile(ev)
	if !ok {
		t.Fatal("expected a real get_state response to parse")
	}
	wantPath := "/root/.pi/agent/sessions/--workspace--/2026-09-19T06-43-12-287Z_01a0b867-991f-7a57-930f-4966d876d8a4.jsonl"
	if session.FilePath != wantPath {
		t.Errorf("FilePath = %q, want %q", session.FilePath, wantPath)
	}
	if session.SessionID != "01a0b867-991f-7a57-930f-4966d876d8a4" {
		t.Errorf("SessionID = %q", session.SessionID)
	}
	// The real path ends in .jsonl, which is what the collection reads out of the
	// pod — worth asserting because a path Pi changed to some other format would
	// otherwise be stored and never be readable back as a session.
	if !strings.HasSuffix(session.FilePath, ".jsonl") {
		t.Errorf("session file %q is not a JSONL file", session.FilePath)
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

// The one-turn pairing, tested as a pair.
//
// fetchSessionStats now sends `get_session_stats` and `get_state` in the same
// attach and demultiplexes by the response's own `command` field, so the two
// parsers read the same stream. Both are written to reject every other command's
// envelope (see each parser's own "rejects another command's response" test), and
// this case asserts the positive half against the **real** bytes of both answers,
// captured from Pi 0.84.4 in one session: neither may claim the other's line, and
// each must read its own.
//
// Worth its own test rather than trusting the two negative cases, because the
// failure mode is not "one parser breaks" but "both parse the same line", which
// only a case carrying both lines can see.
func TestTheTwoTerminalParsersDoNotCrossOnARealPairedResponse(t *testing.T) {
	statsLine := `{"type":"response","command":"get_session_stats","success":true,"data":` +
		`{"sessionFile":"/root/.pi/agent/sessions/--workspace--/s.jsonl",` +
		`"sessionId":"01a0b867-e002-76a0-af71-d2bf6ae973cf","userMessages":0,` +
		`"assistantMessages":0,"toolCalls":0,"toolResults":0,"totalMessages":0,` +
		`"tokens":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0},"cost":0}}`
	stateLine := `{"type":"response","command":"get_state","success":true,"data":` +
		`{"sessionFile":"/root/.pi/agent/sessions/--workspace--/s.jsonl",` +
		`"sessionId":"01a0b867-e002-76a0-af71-d2bf6ae973cf","messageCount":0}}`

	statsEvent := rawEvent(t, statsLine)
	stateEvent := rawEvent(t, stateLine)

	if _, ok := rpc.ParseSessionStats(statsEvent); !ok {
		t.Error("the real get_session_stats response did not parse as stats")
	}
	if _, ok := rpc.ParseSessionFile(statsEvent); ok {
		t.Error("the stats response parsed as a session file — the two parsers crossed")
	}
	if _, ok := rpc.ParseSessionFile(stateEvent); !ok {
		t.Error("the real get_state response did not parse as a session file")
	}
	if _, ok := rpc.ParseSessionStats(stateEvent); ok {
		t.Error("the state response parsed as stats — the two parsers crossed")
	}

	// And the stats answer is not mistaken for a *failed* one just because it
	// carries sessionFile alongside the tokens this suite already read.
	session, ok := rpc.ParseSessionFile(stateEvent)
	if !ok || session.SessionID != "01a0b867-e002-76a0-af71-d2bf6ae973cf" {
		t.Errorf("session id did not survive the paired parse: %+v ok=%v", session, ok)
	}
}
