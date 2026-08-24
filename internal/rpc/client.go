// Package rpc speaks Pi's line-delimited JSON RPC protocol (`pi --mode
// rpc`) over a byte stream — in practice, a Kubernetes pod's attached
// stdin/stdout (see internal/k8s.Attach). See
// docs/adr/006-pi-rpc-orchestrator-integration.md for the design.
//
// Framing follows Pi's own RPC docs: strict JSONL, LF-only record
// delimiters. A trailing '\r' is stripped so a CRLF-emitting process still
// frames correctly, but this package does not use a general
// Unicode-aware line reader (Pi's docs call that out as non-compliant).
package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// Command is a single JSONL command sent to Pi's stdin. Only the fields
// this suite currently sends are typed; Pi's command surface is much
// wider (see docs/adr/006-pi-rpc-orchestrator-integration.md's summary of
// packages/coding-agent/docs/rpc.md) and can be extended here as needed.
type Command struct {
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
	// StreamingBehavior is required by Pi's `prompt` command if the agent is
	// already streaming when it's sent — "steer" or "followUp".
	StreamingBehavior string `json:"streamingBehavior,omitempty"`
}

// Event is a single JSONL event received from Pi's stdout. Only Type is
// parsed eagerly; Raw holds the full line so callers can decode
// event-specific fields themselves. Pi's event taxonomy is wide and still
// evolving (agent_start/end, turn_start/end, message_update,
// tool_execution_start/update/end, ...) — this package deliberately does
// not model all of it.
type Event struct {
	Type string
	Raw  json.RawMessage
}

// Client drives one Pi RPC session across possibly many turns: BeginTurn
// opens a fresh stdin pipe for one k8s.Attach call, Send writes a JSONL
// command to it, and EndTurn closes it once that turn's response has been
// read — letting the Attach call return without restarting the container
// process (see k8s.Attach's doc comment for why one attach handles exactly
// one turn, not the whole session). Write (making Client itself an
// io.Writer, wired into every turn's Attach call as the stdout argument)
// parses received bytes into framed JSONL lines and publishes them on
// Events, which persists across turns. Not safe for concurrent Send calls
// from multiple goroutines.
type Client struct {
	stdinR *os.File
	stdinW *os.File

	events chan Event
	errs   chan error

	mu  sync.Mutex // guards buf; Write is called from the Attach goroutine
	buf bytes.Buffer

	closeOnce sync.Once
}

// eventBuffer is the Events channel's capacity. Sized generously so a
// burst of events (e.g. several message_update deltas in a row) doesn't
// make Write block on a slow consumer — Write is called synchronously from
// the k8s.Attach goroutine reading the pod's stdout, so blocking there
// would stall the whole stream.
const eventBuffer = 256

// NewClient creates a Client with no turn open yet — call BeginTurn before
// the first (and every subsequent) attach.
func NewClient() *Client {
	return &Client{
		events: make(chan Event, eventBuffer),
		errs:   make(chan error, eventBuffer),
	}
}

// BeginTurn opens a fresh stdin pipe for one k8s.Attach call. Pass the
// returned reader as that call's stdin argument. Must be called before
// Send for every turn, including the first — reusing a reader from a
// previous, already-ended turn won't work, since that pipe is closed.
func (c *Client) BeginTurn() (io.Reader, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	c.stdinR = pr
	c.stdinW = pw
	return pr, nil
}

// EndTurn closes the current turn's stdin pipe, so its k8s.Attach call
// returns (its stdin-copy goroutine sees a clean EOF, not an error) once
// the turn's response has been read. Call this only after reading that
// response — closing stdin immediately after Send, before the container
// has replied, ends the whole attach call prematurely. Safe to call when
// no turn is open.
func (c *Client) EndTurn() error {
	if c.stdinW == nil {
		return nil
	}
	err := c.stdinW.Close()
	c.stdinW = nil
	c.stdinR = nil
	return err
}

// Send encodes cmd as one JSONL line and writes it to the current turn's
// stdin pipe (BeginTurn must have been called first). Blocks until the
// attach stream's reader consumes it.
func (c *Client) Send(cmd Command) error {
	line, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to encode RPC command %+v: %w", cmd, err)
	}
	line = append(line, '\n')
	if _, err := c.stdinW.Write(line); err != nil {
		return fmt.Errorf("failed to send RPC command: %w", err)
	}
	return nil
}

// Write implements io.Writer so a Client can be passed directly as every
// turn's k8s.Attach call's stdout argument: each call appends to an
// internal buffer and emits one Event per complete '\n'-terminated line —
// this buffer (and the Events channel) persists across turns, so an event
// split across a turn boundary can't happen in practice (each turn starts
// with a fresh attach, and Pi only emits a line once it's complete) but
// would still frame correctly if it did. Never returns an error — a
// malformed line is reported on Errs instead of aborting the stream, since
// one bad line from Pi shouldn't tear down an otherwise healthy session.
func (c *Client) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.buf.Write(p)
	for {
		idx := bytes.IndexByte(c.buf.Bytes(), '\n')
		if idx < 0 {
			break
		}
		line := bytes.TrimRight(c.buf.Next(idx+1), "\r\n")
		if len(line) == 0 {
			continue
		}
		c.emit(line)
	}
	return len(p), nil
}

func (c *Client) emit(line []byte) {
	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &typed); err != nil {
		c.reportErr(fmt.Errorf("failed to parse RPC event line %q: %w", string(line), err))
		return
	}

	raw := make(json.RawMessage, len(line))
	copy(raw, line)

	select {
	case c.events <- Event{Type: typed.Type, Raw: raw}:
	default:
		// A consumer that isn't keeping up with Events must not be allowed
		// to block the attach stream's reader goroutine indefinitely —
		// drop and report instead.
		c.reportErr(fmt.Errorf("events channel full, dropped event type %q", typed.Type))
	}
}

func (c *Client) reportErr(err error) {
	select {
	case c.errs <- err:
	default:
	}
}

// Events returns the channel of parsed RPC events, closed once Close is
// called.
func (c *Client) Events() <-chan Event {
	return c.events
}

// DrainStaleEvents discards, without blocking, any events already sitting
// in the buffer. Call this once per turn, right before BeginTurn/Send — not
// after EndTurn — so a turn that just ended can't leak into the one that's
// about to start.
//
// Why this is needed: Events persists across the whole session (many
// turns), by design (see Write's doc comment), so an event can't be lost
// *mid-turn*. But a contract tool call (ask_user/submit_adr/
// submit_build_result) ends a Yggdrasil "turn" the instant runTurn's read
// loop matches it — before Pi's own trailing per-turn bookkeeping
// (typically agent_end then agent_settled, emitted moments later as part
// of the same completed turn) has necessarily arrived. Nothing ever reads
// those trailing events off Events, because runTurn already returned; they
// just sit in the buffer. Left there, the *next* runTurn call — the one
// meant to process a fresh human reply — reads them first and misreads a
// previous turn's own "I'm done" signal as its own, failing the run before
// the new prompt was ever sent. Verified against a real k3s pod: two
// production failures of exactly this shape (run_failed within
// milliseconds of a reply being sent — too fast for any model round trip)
// traced back to this.
//
// Safe to call with no turn open: BeginTurn/Send for the new turn haven't
// happened yet at the call site, so nothing genuinely new could be in the
// channel — only leftovers from a turn whose attach call has already fully
// returned (closeOutTurn only returns after that), so its Write goroutine
// is guaranteed to have stopped producing more.
func (c *Client) DrainStaleEvents() {
	for {
		select {
		case <-c.events:
		default:
			return
		}
	}
}

// Errs returns non-fatal parse/backpressure errors observed while decoding
// the event stream — these don't end the session, just note a dropped or
// unparseable line.
func (c *Client) Errs() <-chan error {
	return c.errs
}

// Close ends the whole session (not just the current turn): closes
// whatever stdin pipe is currently open (if any) and closes the Events
// channel so range loops over it terminate. Call this once the session is
// over for good — a hard failure, or after the last turn's EndTurn. Safe
// to call multiple times, only the first call has an effect.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		if c.stdinW != nil {
			_ = c.stdinW.Close()
		}
		close(c.events)
	})
}
