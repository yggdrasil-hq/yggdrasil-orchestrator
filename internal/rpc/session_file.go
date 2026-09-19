package rpc

import "encoding/json"

// CommandGetState is Pi's RPC command asking for the session's current state
// (see the "State" section of Pi's rpc.md). Like CommandGetSessionStats it is a
// read rather than a drive, so it lives beside Client.Send instead of in the
// curated event vocabulary: its response is a payload this package parses
// (ParseSessionFile), not an event to relay.
//
// ADR 032 item 1 names this command as where the session file path comes from,
// and it is the right one — `get_state` is the only command whose response
// documents `sessionFile` as its purpose rather than as a by-product. (See
// ParseSessionStats's comment for the by-product case, which the Orchestrator
// already reads.)
const CommandGetState = "get_state"

// SessionFile is what `get_state` reports about where the session lives.
//
// FilePath is the pod-local path to the session JSONL — the artifact ADR 032
// item 1 persists. SessionID is carried because it is the identifier Pi itself
// uses for the session, which is what a future `switch_session` would be
// resuming; the two are separate fields rather than one because a path is not an
// identity (Pi's own docs show both, and `resolveSessionPath` accepts either an
// id or a path).
//
// An empty FilePath is not an error here: Pi documents `sessionFile` as part of
// the state object, but a session created with `--no-session` (or
// `SessionManager.inMemory`) legitimately has none, and reporting that honestly
// is better than inventing a path. The caller decides what an absent path means
// (see collectSession's `unavailable` outcome).
type SessionFile struct {
	FilePath  string
	SessionID string
	// Asked records that Pi actually answered the question, so an empty FilePath
	// is a *fact* rather than an absence of information.
	//
	// **This field is why the type is not just two strings.** Pi documents
	// `sessionFile` as part of the state object, but a session created with
	// `--no-session` (or `SessionManager.inMemory`) legitimately has none — and a
	// terminal read that never completed also leaves the path empty. Those are
	// different facts with different consequences (ADR 032 item 5: `not_collected`
	// versus `unavailable`), and Go's zero value would make them identical.
	//
	// A separate field rather than a pointer to the whole struct because the two
	// strings are still worth carrying when the ask failed — an id Pi reported
	// before failing is not made useless by the failure.
	Asked bool
}

// sessionStateResponse mirrors just enough of Pi's get_state response envelope
// (`{"type":"response","command":"get_state","success":true,"data":{...}}`) to
// read where the session file is. The envelope is shared with every other
// command, so Command is checked rather than assumed — the same reasoning
// sessionStatsResponse gives.
type sessionStateResponse struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Success bool   `json:"success"`
	Data    *struct {
		SessionFile string `json:"sessionFile"`
		SessionID   string `json:"sessionId"`
	} `json:"data"`
}

// ParseSessionFile decodes a get_state response, returning ok=false for every
// other line on the stream — the overwhelmingly common case, since this is read
// from the same Events channel that carries the agent's whole event stream.
//
// A failed response (success:false) is rejected rather than treated as "no
// session file": a caller must be able to tell "Pi says this session has no file"
// from "Pi did not answer the question", because ADR 032 item 5 requires exactly
// that distinction to reach the user. Returning ok=false for a failure forces the
// caller to treat it as an unanswered question (the `unavailable` path) rather
// than silently recording a missing artifact.
func ParseSessionFile(ev Event) (SessionFile, bool) {
	if ev.Type != "response" {
		return SessionFile{}, false
	}

	var parsed sessionStateResponse
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return SessionFile{}, false
	}
	if parsed.Command != CommandGetState || !parsed.Success || parsed.Data == nil {
		return SessionFile{}, false
	}

	return SessionFile{
		FilePath:  parsed.Data.SessionFile,
		SessionID: parsed.Data.SessionID,
		Asked:     true,
	}, true
}
