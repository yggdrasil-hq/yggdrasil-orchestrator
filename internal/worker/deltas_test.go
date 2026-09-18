package worker

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
Issue #23: the orchestrator sends one HTTP POST per curated event, so an
un-coalesced token stream is one POST per token. These tests pin the two
properties the coalescer has to keep: fewer posts, and the same order.
*/

// collector records what the sink was handed, in order, behind a mutex because
// the flush timer fires on its own goroutine.
type collector struct {
	mu     sync.Mutex
	events []rpc.CuratedEvent
}

func (c *collector) forward(ev rpc.CuratedEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
}

func (c *collector) snapshot() []rpc.CuratedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rpc.CuratedEvent(nil), c.events...)
}

func delta(text string) rpc.CuratedEvent {
	return rpc.CuratedEvent{Type: rpc.EventAgentTextDelta, Message: text}
}

// A slow interval keeps the timer out of the way, so these tests exercise the
// flush points rather than racing them.
func newTestCoalescer(c *collector) *deltaCoalescer {
	return newDeltaCoalescer(c.forward, time.Hour, defaultDeltaMaxBytes)
}

func TestDeltaCoalescer_MergesConsecutiveDeltasIntoOneEvent(t *testing.T) {
	c := &collector{}
	co := newTestCoalescer(c)

	for _, chunk := range []string{"Hel", "lo ", "world"} {
		co.event(delta(chunk))
	}
	if got := len(c.snapshot()); got != 0 {
		t.Fatalf("expected deltas to be buffered, got %d forwards", got)
	}

	co.flush()

	events := c.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected one coalesced event, got %d", len(events))
	}
	if events[0].Message != "Hello world" {
		t.Fatalf("expected the concatenated text, got %q", events[0].Message)
	}
	if events[0].Type != rpc.EventAgentTextDelta {
		t.Fatalf("expected the delta type to survive, got %s", events[0].Type)
	}
}

// The load-bearing property: agent_text is the message's final form, and the Web
// client replaces accumulated delta text with it. If it overtook its own deltas
// the bubble would visibly revert.
func TestDeltaCoalescer_FlushesBeforeAnyNonDeltaEvent(t *testing.T) {
	c := &collector{}
	co := newTestCoalescer(c)

	co.event(delta("the "))
	co.event(delta("answer"))
	co.event(rpc.CuratedEvent{Type: rpc.EventAgentText, Message: "the answer"})

	events := c.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected a flush then the text, got %d events", len(events))
	}
	if events[0].Type != rpc.EventAgentTextDelta || events[0].Message != "the answer" {
		t.Fatalf("expected the delta first, got %s %q", events[0].Type, events[0].Message)
	}
	if events[1].Type != rpc.EventAgentText {
		t.Fatalf("expected the authoritative text second, got %s", events[1].Type)
	}
}

func TestDeltaCoalescer_PassesNonDeltaEventsThroughUnchanged(t *testing.T) {
	c := &collector{}
	co := newTestCoalescer(c)

	ask := rpc.CuratedEvent{Type: rpc.EventAskUser, Question: "Which database?"}
	co.event(ask)

	events := c.snapshot()
	if len(events) != 1 || events[0].Type != rpc.EventAskUser || events[0].Question != "Which database?" {
		t.Fatalf("expected the event forwarded verbatim, got %#v", events)
	}
}

func TestDeltaCoalescer_FlushesAtTheSizeCeiling(t *testing.T) {
	c := &collector{}
	co := newDeltaCoalescer(c.forward, time.Hour, 8)

	co.event(delta("12345"))
	if got := len(c.snapshot()); got != 0 {
		t.Fatalf("expected the ceiling not to have been reached, got %d forwards", got)
	}

	co.event(delta("6789")) // 9 bytes: over the ceiling

	events := c.snapshot()
	if len(events) != 1 || events[0].Message != "123456789" {
		t.Fatalf("expected one flush of the whole buffer, got %#v", events)
	}
}

// A timer flush is what makes a slow stream still feel live; a stream that stops
// mid-message must not sit in a buffer until something else happens.
func TestDeltaCoalescer_TimerFlushesWithoutFurtherEvents(t *testing.T) {
	c := &collector{}
	co := newDeltaCoalescer(c.forward, 10*time.Millisecond, defaultDeltaMaxBytes)
	defer co.stop()

	co.event(delta("late text"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.snapshot()) == 1 {
			if got := c.snapshot()[0].Message; got != "late text" {
				t.Fatalf("expected the buffered text, got %q", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the timer to flush the buffered delta")
}

// A finished run must not lose its tail, and must not leave a timer behind that
// keeps posting for a job that is over.
func TestDeltaCoalescer_StopFlushesTheTailAndStopsTheTimer(t *testing.T) {
	c := &collector{}
	co := newDeltaCoalescer(c.forward, 10*time.Millisecond, defaultDeltaMaxBytes)

	co.event(delta("tail"))
	co.stop()

	events := c.snapshot()
	if len(events) != 1 || events[0].Message != "tail" {
		t.Fatalf("expected the tail flushed once, got %#v", events)
	}

	// Long enough that a surviving timer would have fired several times.
	time.Sleep(50 * time.Millisecond)
	if got := len(c.snapshot()); got != 1 {
		t.Fatalf("expected no further forwards after stop, got %d", got)
	}
}

// After stop(), text is still forwarded rather than dropped: losing a run's last
// delta is worse than one extra POST.
func TestDeltaCoalescer_ForwardsRatherThanDropsAfterStop(t *testing.T) {
	c := &collector{}
	co := newTestCoalescer(c)
	co.stop()

	co.event(delta("after stop"))

	events := c.snapshot()
	if len(events) != 1 || events[0].Message != "after stop" {
		t.Fatalf("expected the late delta forwarded, got %#v", events)
	}
}

func TestDeltaCoalescer_FlushWithNothingBufferedForwardsNothing(t *testing.T) {
	c := &collector{}
	co := newTestCoalescer(c)

	co.flush()
	co.flush()

	if got := len(c.snapshot()); got != 0 {
		t.Fatalf("expected no forwards, got %d", got)
	}
}

// The point of the whole file: a turn's worth of chunks is a handful of posts,
// not one per chunk.
func TestDeltaCoalescer_CutsAPostPerChunkIntoAPostPerInterval(t *testing.T) {
	c := &collector{}
	co := newDeltaCoalescer(c.forward, 10*time.Millisecond, defaultDeltaMaxBytes)

	const chunks = 200
	for i := 0; i < chunks; i++ {
		co.event(delta("x"))
		if i%20 == 19 {
			// What a stream that spans several flush intervals looks like.
			time.Sleep(15 * time.Millisecond)
		}
	}
	co.stop()

	forwards := len(c.snapshot())
	if forwards >= chunks {
		t.Fatalf("expected far fewer forwards than chunks, got %d of %d", forwards, chunks)
	}
	if forwards == 0 {
		t.Fatal("expected at least one forward")
	}

	// And nothing was lost on the way.
	rebuilt := strings.Builder{}
	for _, ev := range c.snapshot() {
		rebuilt.WriteString(ev.Message)
	}
	if rebuilt.Len() != chunks {
		t.Fatalf("expected all %d chunks to survive, got %d bytes", chunks, rebuilt.Len())
	}
}
