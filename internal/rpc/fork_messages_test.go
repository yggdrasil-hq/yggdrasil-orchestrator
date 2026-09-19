package rpc_test

import (
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// forkMessagesResponseLine is **captured from a real Pi process**, not
// transcribed from the docs: Pi 0.84.4 in the pinned base image, sent
// `{"type":"get_fork_messages"}` over RPC against a session built by hand with
// three user messages, and answered with exactly this. The entry ids are that
// session's own. ADR 032's item-2 contract rests on this shape, and a doc can
// drift from the binary — the docs were checked separately and agree (the same
// two-source posture session_file_test.go documents).
const forkMessagesResponseLine = `{"type":"response","command":"get_fork_messages","success":true,` +
	`"data":{"messages":[{"entryId":"a1b2c3d4","text":"First: make me a portfolio site."},` +
	`{"entryId":"c3d4e5f6","text":"Second: add a projects section."},` +
	`{"entryId":"e5f6a7b8","text":"Third: use a dark theme instead."}]}}`

func TestParseForkMessagesReadsTheRealResponse(t *testing.T) {
	messages, ok := rpc.ParseForkMessages(rawEvent(t, forkMessagesResponseLine))
	if !ok {
		t.Fatal("expected a real get_fork_messages response to parse")
	}
	if !messages.Asked {
		t.Error("Asked = false for a response Pi answered")
	}
	if len(messages.Points) != 3 {
		t.Fatalf("got %d fork points, want 3", len(messages.Points))
	}
	// The ids and the texts are both load-bearing: the id is what `fork` is sent,
	// and the text is what the user is offered and what ADR 032 item 2 matches
	// against the transcript. Asserting only the count would pass on a parser that
	// swapped or dropped either.
	if messages.Points[0].EntryID != "a1b2c3d4" {
		t.Errorf("Points[0].EntryID = %q, want %q", messages.Points[0].EntryID, "a1b2c3d4")
	}
	if messages.Points[0].Text != "First: make me a portfolio site." {
		t.Errorf("Points[0].Text = %q", messages.Points[0].Text)
	}
	if messages.Points[2].EntryID != "e5f6a7b8" {
		t.Errorf("Points[2].EntryID = %q, want %q", messages.Points[2].EntryID, "e5f6a7b8")
	}
}

// TestParseForkMessagesReadsAnEmptyAnswerAsAnswered is the item-5 distinction at
// its narrowest, and it is why this type carries Asked at all.
//
// The line is captured from a real Pi 0.84.4 run with `--no-session`: asked for
// fork messages before any session had a user message, it answers success:true
// with an empty list. That is Pi *saying* there are none — a fact — and it must
// not be reported the same way as a question that was never answered.
func TestParseForkMessagesReadsAnEmptyAnswerAsAnswered(t *testing.T) {
	messages, ok := rpc.ParseForkMessages(rawEvent(t,
		`{"type":"response","command":"get_fork_messages","success":true,"data":{"messages":[]}}`))
	if !ok {
		t.Fatal("an empty-but-successful answer must parse as an answer")
	}
	if !messages.Asked {
		t.Error("Asked = false for a session Pi said has no fork points")
	}
	if len(messages.Points) != 0 {
		t.Errorf("got %d fork points, want 0", len(messages.Points))
	}
}

func TestParseForkMessagesRejectsEveryOtherLine(t *testing.T) {
	// This reads from the same stream as the agent's whole event stream and as the
	// other two commands this suite reads on the same turn, so the common case is
	// "not mine" — and a command check that was dropped would let the *state*
	// answer be read as a fork-messages answer.
	cases := map[string]string{
		"an agent event":             `{"type":"message_update","delta":"hi"}`,
		"the get_state response":     `{"type":"response","command":"get_state","success":true,"data":{"sessionFile":"/s.jsonl"}}`,
		"the session-stats response": `{"type":"response","command":"get_session_stats","success":true,"data":{"tokens":{"total":1}}}`,
		"a different command":        `{"type":"response","command":"get_entries","success":true,"data":{"entries":[]}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := rpc.ParseForkMessages(rawEvent(t, line)); ok {
				t.Fatal("parsed a line that is not a get_fork_messages response")
			}
		})
	}
}

// TestParseForkMessagesTreatsAFailedAnswerAsUnanswered pins the *other* half of
// the distinction, and it is the half that would otherwise be wrong in the
// dangerous direction.
//
// Pi reports a genuine failure as `success:false` with an `error` — a real Pi
// 0.84.4 answers a `fork` at an unknown entry id with
// `{"success":false,"error":"Invalid entry ID for forking"}`, so this envelope
// carries a refusal rather than an empty result. Treating it as "no fork points"
// would tell a user there is nothing to resume from when the truth is that
// nobody found out, which is the collapse ADR 032 item 5 exists to prevent.
func TestParseForkMessagesTreatsAFailedAnswerAsUnanswered(t *testing.T) {
	cases := map[string]string{
		"a refusal with an error": `{"type":"response","command":"get_fork_messages","success":false,"error":"Invalid entry ID for forking"}`,
		"success with no data":    `{"type":"response","command":"get_fork_messages","success":true}`,
		"success with null data":  `{"type":"response","command":"get_fork_messages","success":true,"data":null}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := rpc.ParseForkMessages(rawEvent(t, line)); ok {
				t.Fatal("an answer that carried no list must not parse as an answered empty list")
			}
		})
	}
}

// TestParseForkMessagesDropsAPointWithNoEntryID covers the one internal
// inconsistency Pi could hand us: a message with text but no id.
//
// `fork` is sent the id, and Pi rejects an unknown one with "Invalid entry ID for
// forking" — so a point with no id is one that would be *offered* to a user and
// then refuse them. Dropping it loses nothing (it cannot be forked from) and keeps
// the promise the list makes: every entry in it is one a fork will accept.
func TestParseForkMessagesDropsAPointWithNoEntryID(t *testing.T) {
	messages, ok := rpc.ParseForkMessages(rawEvent(t,
		`{"type":"response","command":"get_fork_messages","success":true,"data":{"messages":[`+
			`{"entryId":"a1b2c3d4","text":"keep me"},`+
			`{"text":"no id, so unforkable"},`+
			`{"entryId":"","text":"empty id"}]}}`))
	if !ok {
		t.Fatal("expected a response with one usable point to parse")
	}
	if !messages.Asked {
		t.Error("Asked = false, so the whole answer was discarded rather than filtered")
	}
	if len(messages.Points) != 1 || messages.Points[0].EntryID != "a1b2c3d4" {
		t.Fatalf("got %+v, want only the point with an entry id", messages.Points)
	}
}
