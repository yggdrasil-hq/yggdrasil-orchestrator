package rpc

import "encoding/json"

// CommandGetForkMessages is Pi's RPC command asking which previous user messages
// this session can be forked from (see the "Forking" section of Pi's rpc.md).
// Like CommandGetState and CommandGetSessionStats it is a read rather than a
// drive, so it lives here beside Client.Send instead of in the curated event
// vocabulary: its response is a payload this package parses (ParseForkMessages),
// not an event to relay.
//
// ADR 032 item 2 names this command as where the fork entry ids come from, and it
// is the right one — the alternative is to map grill event ids onto session entry
// ids, which the ADR rejects because compaction, tool-call turns and abandoned
// branches each break the one-to-one assumption.
//
// **This command needs a live process, which is why it is read on the terminal
// turn.** ADR 006 deletes the Job at the terminal event (a pod holds a live GitHub
// token and the model key), so once the run is over there is nobody left to ask —
// and re-deriving the answers from the stored JSONL would mean re-implementing
// Pi's branch and compaction semantics. The entry ids are therefore captured at
// the same seam the session file is (see the worker's collectSession).
const CommandGetForkMessages = "get_fork_messages"

// ForkPoint is one previous user message a session can be forked from.
//
// ADR 032 item 3's "resume from here" gesture operates on exactly this: `fork`
// takes an EntryID and returns the Text it is forking from, and the new session
// re-answers that message. For a grill, these texts *are* the human's replies —
// which is why they can be matched to the transcript by their own text rather
// than by an id mapping (ADR 032 item 2).
type ForkPoint struct {
	// EntryID is Pi's own entry id — a durable cursor into the session tree, and
	// a different id space from a grill `job_events` id. Both are kept and they
	// mean different things (ADR 032's trade-offs say so explicitly).
	EntryID string
	Text    string
}

// ForkMessages is what `get_fork_messages` reports about a live session.
//
// **Asked is the item-5 distinction applied to this second question**, and it is
// the reason this is not just a slice. There are two ways Points can be empty —
// Pi answered and this session has no forking points (a real, if unusual, fact:
// a session with no user messages has none, which a real Pi 0.84.4 confirms
// returns `"messages":[]`), or Pi was never successfully asked (the terminal turn
// failed, timed out, or the command was never sent) — and Go's zero value would
// make them identical.
//
// That distinction has to survive all the way to a user, which is why it is not
// resolved here: an empty list must render as "there are no earlier replies to
// resume from", while an unanswered ask must render as "we could not find out",
// and only the second is worth an operator looking at. It is the same shape as
// SessionFile.Asked, and it exists for the same reason.
type ForkMessages struct {
	Points []ForkPoint
	// Asked records that Pi answered the question, so an empty Points is a *fact*
	// rather than an absence of information.
	Asked bool
}

// forkMessagesResponse mirrors just enough of Pi's get_fork_messages response
// envelope (`{"type":"response","command":"get_fork_messages","success":true,
// "data":{"messages":[{"entryId":"…","text":"…"}]}}`) to read the list. The
// envelope is shared with every other command, so Command is checked rather than
// assumed — the same reasoning sessionStatsResponse gives.
type forkMessagesResponse struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Success bool   `json:"success"`
	Data    *struct {
		Messages []struct {
			EntryID string `json:"entryId"`
			Text    string `json:"text"`
		} `json:"messages"`
	} `json:"data"`
}

// ParseForkMessages decodes a get_fork_messages response, returning ok=false for
// every other line on the stream — the overwhelmingly common case, since this is
// read from the same Events channel that carries the agent's whole event stream.
//
// A failed response (success:false) is rejected rather than treated as "no fork
// points". Pi reports a genuine refusal this way — a real Pi 0.84.4 answers
// `{"type":"fork","entryId":"<unknown>"}` with
// `{"type":"response","command":"fork","success":false,"error":"Invalid entry ID
// for forking"}`, so `success:false` carries an *error* and not an empty result —
// and a caller must be able to tell "Pi says there are none" from "Pi did not
// answer the question". Returning ok=false forces the caller to treat it as an
// unanswered ask (`Asked=false`) rather than silently recording "there are none",
// which is exactly the collapse ADR 032 item 5 forbids.
//
// `data.messages` being *absent* is a failed answer in the same way: success:true
// with no messages array is not a shape Pi produces, and treating it as an empty
// list would assert a fact nothing established.
func ParseForkMessages(ev Event) (ForkMessages, bool) {
	if ev.Type != "response" {
		return ForkMessages{}, false
	}

	var parsed forkMessagesResponse
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return ForkMessages{}, false
	}
	if parsed.Command != CommandGetForkMessages || !parsed.Success || parsed.Data == nil {
		return ForkMessages{}, false
	}

	points := make([]ForkPoint, 0, len(parsed.Data.Messages))
	for _, message := range parsed.Data.Messages {
		// An entry with no id cannot be forked from — `fork` would be sent a
		// value Pi rejects as "Invalid entry ID for forking". Dropped rather than
		// carried, because a fork point that cannot be used is worse than one that
		// is absent: it would be offered to a user and then refuse them. Text is
		// allowed to be empty (it is a display string, and Pi's own answer is the
		// only source for it).
		if message.EntryID == "" {
			continue
		}
		points = append(points, ForkPoint{EntryID: message.EntryID, Text: message.Text})
	}

	return ForkMessages{Points: points, Asked: true}, true
}
