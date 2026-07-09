package rpc_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

func TestClient_SendEncodesOneJSONLLine(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	stdin, err := c.BeginTurn()
	if err != nil {
		t.Fatalf("failed to begin turn: %v", err)
	}

	sendDone := make(chan error, 1)
	go func() { sendDone <- c.Send(rpc.Command{Type: "prompt", Message: "hello"}) }()

	buf := make([]byte, 256)
	n, err := stdin.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from stdin: %v", err)
	}

	line := string(buf[:n])
	if line[len(line)-1] != '\n' {
		t.Fatalf("expected the command to be newline-terminated, got %q", line)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line[:len(line)-1]), &decoded); err != nil {
		t.Fatalf("expected a single valid JSON line, got %q: %v", line, err)
	}
	if decoded["type"] != "prompt" || decoded["message"] != "hello" {
		t.Fatalf("unexpected command shape: %v", decoded)
	}

	if err := <-sendDone; err != nil {
		t.Fatalf("Send returned an error: %v", err)
	}
}

func TestClient_WriteEmitsOneEventPerLine(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	// Two JSONL events delivered in one Write call, as a real attach stream
	// might batch bytes rather than delivering exactly one line per Write.
	payload := []byte(`{"type":"agent_start"}` + "\n" + `{"type":"agent_end","messages":[]}` + "\n")
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}

	first := mustRecvEvent(t, c)
	if first.Type != "agent_start" {
		t.Fatalf("expected first event type agent_start, got %q", first.Type)
	}

	second := mustRecvEvent(t, c)
	if second.Type != "agent_end" {
		t.Fatalf("expected second event type agent_end, got %q", second.Type)
	}
	var decoded struct {
		Messages []any `json:"messages"`
	}
	if err := json.Unmarshal(second.Raw, &decoded); err != nil {
		t.Fatalf("expected Raw to hold the full decodable line: %v", err)
	}
}

func TestClient_WriteHandlesLineSplitAcrossCalls(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	if _, err := c.Write([]byte(`{"type":"mess`)); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}
	select {
	case ev := <-c.Events():
		t.Fatalf("expected no event before the line is complete, got %v", ev)
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := c.Write([]byte("age_update\"}\n")); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}
	ev := mustRecvEvent(t, c)
	if ev.Type != "message_update" {
		t.Fatalf("expected the reassembled event type message_update, got %q", ev.Type)
	}
}

func TestClient_WriteStripsCRLF(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	if _, err := c.Write([]byte("{\"type\":\"turn_end\"}\r\n")); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}
	ev := mustRecvEvent(t, c)
	if ev.Type != "turn_end" {
		t.Fatalf("expected event type turn_end, got %q", ev.Type)
	}
}

func TestClient_WriteSkipsBlankLines(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	if _, err := c.Write([]byte("\n\n" + `{"type":"turn_start"}` + "\n")); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}
	ev := mustRecvEvent(t, c)
	if ev.Type != "turn_start" {
		t.Fatalf("expected event type turn_start, got %q", ev.Type)
	}
}

func TestClient_WriteReportsMalformedLineWithoutBlocking(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	if _, err := c.Write([]byte("not json\n" + `{"type":"agent_start"}` + "\n")); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}

	select {
	case err := <-c.Errs():
		if err == nil {
			t.Fatal("expected a non-nil parse error")
		}
	case <-time.After(time.Second):
		t.Fatal("expected a parse error on Errs, got none")
	}

	// The malformed line must not have blocked the well-formed one after it.
	ev := mustRecvEvent(t, c)
	if ev.Type != "agent_start" {
		t.Fatalf("expected event type agent_start after the malformed line, got %q", ev.Type)
	}
}

// Send is backed by a real OS pipe (kernel-buffered), not io.Pipe — a
// small write like this one succeeds immediately whether or not anything
// has read the previous data, so this proves the *other* half of Close's
// contract: a Send made *after* Close fails, rather than trying (and
// fighting kernel pipe-buffer sizing) to catch one already blocked.
func TestClient_SendFailsAfterClose(t *testing.T) {
	c := rpc.NewClient()
	if _, err := c.BeginTurn(); err != nil {
		t.Fatalf("failed to begin turn: %v", err)
	}
	c.Close()

	done := make(chan error, 1)
	go func() { done <- c.Send(rpc.Command{Type: "prompt", Message: "after close"}) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Send to fail once the client is closed")
		}
	case <-time.After(time.Second):
		t.Fatal("Send neither failed nor returned after Close")
	}
}

func TestClient_EndTurnLetsBeginTurnOpenAFreshPipe(t *testing.T) {
	c := rpc.NewClient()
	t.Cleanup(c.Close)

	stdin1, err := c.BeginTurn()
	if err != nil {
		t.Fatalf("failed to begin first turn: %v", err)
	}
	if err := c.Send(rpc.Command{Type: "prompt", Message: "one"}); err != nil {
		t.Fatalf("failed to send in first turn: %v", err)
	}
	buf := make([]byte, 256)
	if _, err := stdin1.Read(buf); err != nil {
		t.Fatalf("failed to read first turn's stdin: %v", err)
	}
	if err := c.EndTurn(); err != nil {
		t.Fatalf("failed to end first turn: %v", err)
	}

	// The first turn's stdin reader should now be at EOF.
	if _, err := stdin1.Read(buf); err == nil {
		t.Fatal("expected the first turn's stdin to be at EOF after EndTurn")
	}

	stdin2, err := c.BeginTurn()
	if err != nil {
		t.Fatalf("failed to begin second turn: %v", err)
	}
	if err := c.Send(rpc.Command{Type: "prompt", Message: "two"}); err != nil {
		t.Fatalf("failed to send in second turn: %v", err)
	}
	n, err := stdin2.Read(buf)
	if err != nil {
		t.Fatalf("failed to read second turn's stdin: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf[:n-1], &decoded); err != nil {
		t.Fatalf("expected a valid JSON line from the second turn, got %q: %v", buf[:n], err)
	}
	if decoded["message"] != "two" {
		t.Fatalf("expected the second turn's own message, got %v", decoded)
	}
}

func TestClient_CloseClosesEventsChannel(t *testing.T) {
	c := rpc.NewClient()
	c.Close()

	select {
	case _, ok := <-c.Events():
		if ok {
			t.Fatal("expected Events to be closed after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Events channel was never closed")
	}
}

func mustRecvEvent(t *testing.T, c *rpc.Client) rpc.Event {
	t.Helper()
	select {
	case ev, ok := <-c.Events():
		if !ok {
			t.Fatal("Events channel closed unexpectedly")
		}
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for an event")
		return rpc.Event{}
	}
}
