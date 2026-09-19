package rpc

import (
	"encoding/json"
	"testing"
)

// The three commands ADR 032 item 3 needs, and the two behaviours a real Pi
// 0.84.4 produced that contradict the documentation. Every payload below is
// copied from that session's actual output.

func event(t *testing.T, raw string) Event {
	t.Helper()
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		t.Fatalf("test payload is not valid JSON: %v", err)
	}
	return Event{Type: probe.Type, Raw: json.RawMessage(raw)}
}

func TestParseSwitchSessionReadsTheFlagThatCannotBeTrustedAlone(t *testing.T) {
	// This is the exact answer Pi gives for a session path that DOES NOT EXIST —
	// success, not cancelled, no error. The parser must report it faithfully, and
	// the test exists to make the danger visible: nothing in this response says the
	// load failed, so a caller relying on it cannot tell a loaded session from an
	// empty one. The verification step is what catches it (see the worker's
	// verifySwitch), and this fixture is the reason that step exists.
	result, ok := ParseSwitchSession(event(t, `{
		"type":"response","command":"switch_session","success":true,
		"data":{"cancelled":false}}`))
	if !ok {
		t.Fatal("expected a switch_session response to parse")
	}
	if !result.Success || result.Cancelled {
		t.Fatalf("expected success=true cancelled=false, got %+v", result)
	}
}

func TestParseSwitchSessionReportsAnExtensionRefusal(t *testing.T) {
	result, ok := ParseSwitchSession(event(t, `{
		"type":"response","command":"switch_session","success":true,
		"data":{"cancelled":true}}`))
	if !ok || !result.Cancelled {
		t.Fatalf("expected a cancelled switch to be reported, got %+v ok=%v", result, ok)
	}
}

func TestParseSwitchSessionRejectsAnotherCommandsResponse(t *testing.T) {
	// The envelope is shared with every command, so the `command` field is checked
	// rather than assumed — otherwise a get_state answer arriving first would be
	// read as the switch's outcome.
	if _, ok := ParseSwitchSession(event(t, `{
		"type":"response","command":"get_state","success":true,
		"data":{"sessionFile":"/s.jsonl"}}`)); ok {
		t.Fatal("expected a get_state response not to parse as switch_session")
	}
}

func TestParseSwitchSessionRejectsAMissingDataObject(t *testing.T) {
	// success-with-no-data is not a shape Pi produces, so reporting it as
	// {Success:true, Cancelled:false} would assert a load nothing established.
	if _, ok := ParseSwitchSession(event(t, `{
		"type":"response","command":"switch_session","success":true}`)); ok {
		t.Fatal("expected a response with no data to be rejected")
	}
}

func TestParseForkReadsTheTextTheCallerMustResend(t *testing.T) {
	// The forked context ends *before* the fork point, so this text is the next
	// turn's prompt — which is what makes "resume from here" mean resuming rather
	// than restarting.
	result, ok := ParseFork(event(t, `{
		"type":"response","command":"fork","success":true,
		"data":{"text":"Second: add a projects section.","cancelled":false}}`))
	if !ok {
		t.Fatal("expected a fork response to parse")
	}
	if !result.Success || result.Text != "Second: add a projects section." {
		t.Fatalf("unexpected fork result: %+v", result)
	}
}

func TestParseForkReportsARejectedEntryIDAsAnAnswerNotASilence(t *testing.T) {
	// The opposite choice from ParseForkMessages, and deliberately: there,
	// success:false means the question went unanswered (Asked must stay false);
	// here it is Pi *answering* that the entry id is not forkable, and discarding it
	// would lose the only thing that tells a user which input was wrong.
	result, ok := ParseFork(event(t, `{
		"type":"response","command":"fork","success":false,
		"error":"Invalid entry ID for forking"}`))
	if !ok {
		t.Fatal("a refused fork is an answer and must parse")
	}
	if result.Success {
		t.Fatal("expected success=false")
	}
	if result.Error != "Invalid entry ID for forking" {
		t.Fatalf("expected Pi's own wording to survive, got %q", result.Error)
	}
}

func TestParseForkReadsACancelledFork(t *testing.T) {
	result, ok := ParseFork(event(t, `{
		"type":"response","command":"fork","success":true,
		"data":{"cancelled":true}}`))
	if !ok || !result.Cancelled {
		t.Fatalf("expected a cancelled fork to be reported, got %+v ok=%v", result, ok)
	}
}

func TestParseForkIgnoresAnotherCommandsResponse(t *testing.T) {
	if _, ok := ParseFork(event(t, `{
		"type":"response","command":"get_fork_messages","success":true,
		"data":{"messages":[]}}`)); ok {
		t.Fatal("expected get_fork_messages not to parse as fork")
	}
}

func TestParseSessionFileCarriesTheMessageCountTheVerificationNeeds(t *testing.T) {
	// The successful switch's own answer, verbatim: sessionFile set to the restored
	// path and messageCount 6. Both halves are read because either alone is
	// satisfiable by a failure — a path match passes for a file that exists but is
	// empty, and a count alone passes for a session that loaded but is not the one
	// named.
	session, ok := ParseSessionFile(event(t, `{
		"type":"response","command":"get_state","success":true,
		"data":{"sessionFile":"/tmp/restored.jsonl","sessionId":"abc","messageCount":6}}`))
	if !ok {
		t.Fatal("expected a get_state response to parse")
	}
	if session.FilePath != "/tmp/restored.jsonl" || session.MessageCount != 6 {
		t.Fatalf("unexpected session: %+v", session)
	}
}

func TestParseSessionFileReportsTheEmptySessionSwitchSessionAlleges(t *testing.T) {
	// The other half of the same finding: after a switch to a missing path, Pi
	// reports no sessionFile at all and messageCount 0. The parser must surface
	// that as an empty path with Asked=true — "Pi answered, and there is no
	// session" — which verifySwitch rejects, rather than as an unanswered question.
	session, ok := ParseSessionFile(event(t, `{
		"type":"response","command":"get_state","success":true,
		"data":{"messageCount":0}}`))
	if !ok {
		t.Fatal("expected a get_state response to parse")
	}
	if !session.Asked {
		t.Fatal("Pi answered, so Asked must be true")
	}
	if session.FilePath != "" || session.MessageCount != 0 {
		t.Fatalf("expected an empty session, got %+v", session)
	}
}
