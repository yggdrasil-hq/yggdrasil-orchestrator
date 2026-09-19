package rpc

import "encoding/json"

// The three commands that turn a stored session into a running fork (ADR 032
// item 3). Unlike get_state/get_session_stats/get_fork_messages these *drive* Pi
// — they replace the session and branch it — but like them their responses are
// payloads this package parses rather than curated events to relay, so they live
// here beside Client.Send. A fork is an operation the Orchestrator performs, not
// agent output a human watches.
const (
	// CommandSwitchSession loads a persisted session file as the active session.
	CommandSwitchSession = "switch_session"
	// CommandFork creates a new session file branched at a real entry id.
	CommandFork = "fork"
)

// SwitchSessionResult is what Pi reports about a `switch_session` request.
//
// **Cancelled is why this is a struct and not a bool.** An extension can decline
// the switch (`session_before_switch`), which Pi reports as `cancelled:true`
// rather than as a failure — a refusal to switch is a decision, not an error, and
// the two get different wording (ADR 032 item 5's `refused` vs a fault).
//
// **Success is NOT the load-bearing field, and that is the whole reason
// VerifySwitchState exists.** A real Pi 0.84.4 answers
// `{"success":true,"cancelled":false}` when pointed at a session path that does
// not exist: no error, no file created, and a subsequent `get_state` reports no
// `sessionFile`. So `success` says only that the *request was well-formed*, and a
// caller that treats it as "the session was loaded" cannot tell a loaded session
// from an empty one. `Success` is carried so the distinction is visible at the
// call site rather than implied, but the verification step is what decides.
type SwitchSessionResult struct {
	Success   bool
	Cancelled bool
}

// switchSessionResponse mirrors enough of the `switch_session` response envelope
// to read its two flags. The envelope is shared with every other command, so
// Command is checked rather than assumed — the same reasoning
// sessionStatsResponse gives.
type switchSessionResponse struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Success bool   `json:"success"`
	Data    *struct {
		Cancelled bool `json:"cancelled"`
	} `json:"data"`
}

// ParseSwitchSession decodes a switch_session response, returning ok=false for
// every other line on the stream — the overwhelmingly common case, since this is
// read from the Events channel that also carries the agent's whole event stream.
//
// A missing `data` object is treated as ok=false rather than as a zero result:
// success-with-no-data is not a shape Pi produces, and reporting it as
// `{Success:true, Cancelled:false}` would assert a load that nothing established.
func ParseSwitchSession(ev Event) (SwitchSessionResult, bool) {
	if ev.Type != "response" {
		return SwitchSessionResult{}, false
	}

	var parsed switchSessionResponse
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return SwitchSessionResult{}, false
	}
	if parsed.Command != CommandSwitchSession || parsed.Data == nil {
		return SwitchSessionResult{}, false
	}

	return SwitchSessionResult{
		Success:   parsed.Success,
		Cancelled: parsed.Data.Cancelled,
	}, true
}

// ForkResult is what Pi reports about a `fork` request.
//
// Text is the forked-from user message, returned so the caller can re-send it:
// the forked context ends *before* the fork point, so that message is the next
// thing the agent should answer. That is the right semantics for "resume from
// here" — the conversation is re-joined one reply earlier, which is precisely the
// reply the user chose.
type ForkResult struct {
	Success   bool
	Cancelled bool
	Text      string
	// Error carries Pi's own wording for a refusal — "Invalid entry ID for
	// forking" for an entry id that is not on the active branch. Kept rather than
	// collapsed into a bool because it is what a user acts on: it names *which*
	// input Pi rejected, whereas a generic "the fork failed" leaves them with no
	// next step.
	Error string
}

// forkResponse mirrors enough of the `fork` response envelope. `error` is a
// sibling of `data`, not a member of it, which is why it is a separate field:
// Pi answers an unknown entry id with
// `{"success":false,"error":"Invalid entry ID for forking"}` — an honest failure,
// unlike switch_session's silent success — and a caller must be able to surface
// that text.
type forkResponse struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Data    *struct {
		Text      string `json:"text"`
		Cancelled bool   `json:"cancelled"`
	} `json:"data"`
}

// ParseFork decodes a fork response, returning ok=false for every other line on
// the stream.
//
// **A `success:false` response is returned as ok=true with Success=false**, which
// is the opposite of what ParseForkMessages does, and deliberately: there,
// `success:false` means the question was not answered and the only honest report
// is `Asked=false`. Here it is an *answer* — Pi telling us the entry id is not
// forkable — and discarding it would lose the one piece of information that tells
// the user which entry id was wrong. `Error` carries Pi's own wording.
//
// `data` may legitimately be absent on a failure, so it is only read when
// present; a success with no data is still reported, because the caller's
// next step (re-read the state) does not depend on the payload.
func ParseFork(ev Event) (ForkResult, bool) {
	if ev.Type != "response" {
		return ForkResult{}, false
	}

	var parsed forkResponse
	if err := json.Unmarshal(ev.Raw, &parsed); err != nil {
		return ForkResult{}, false
	}
	if parsed.Command != CommandFork {
		return ForkResult{}, false
	}

	result := ForkResult{Success: parsed.Success, Error: parsed.Error}
	if parsed.Data != nil {
		result.Text = parsed.Data.Text
		result.Cancelled = parsed.Data.Cancelled
	}
	return result, true
}
