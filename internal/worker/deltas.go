package worker

import (
	"strings"
	"sync"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
ADR 019 item 13 / issue #23: coalesce streaming deltas before forwarding them.

**The problem.** `handle` does one HTTP POST per curated event
(`apiclient.PostJobEvent`), and since ADR 019 item 13 a turn's token stream is one
curated event per chunk. So a long answer is a few thousand POSTs to the API, each
of which runs a `pg_notify`. The request count is therefore proportional to the
*model's* token rate — something Yggdrasil does not control — rather than to the
number of turns. It also makes a turn's wall-clock time grow with the number of
deltas, because a terminating event behind a long stream waits for those POSTs to
drain.

**The fix is local to this forwarding path.** Nothing about the protocol changes:
the API's endpoint, the no-persistence delta fan-out, the socket frame shape, and
the Web client's accumulate-then-discard-on-authoritative-`agent_text` behaviour
all stay exactly as they are. Only the number of POSTs changes.

**Timing.** ADR 019 says the honest sequence is measure, then pick ~50-100ms or a
size threshold, whichever comes first. 75ms is the middle of that range: well
under the point where a stream starts to *look* like it is arriving in lumps,
while cutting a few hundred chunks a second down to something near ten. The 4 KiB
ceiling bounds a fast model that would otherwise turn one flush into a large body,
keeping a flush in the same order of magnitude as the requests it replaces rather
than trading many small ones for a few huge.

**Ordering is the load-bearing property.** Every non-delta event flushes the
buffer before it is forwarded, so the authoritative `agent_text` — the message's
final form, which the Web client uses to *replace* accumulated delta text — can
never overtake the deltas it supersedes. Coalescing is a bandwidth optimisation,
not a licence to reorder, and a client that received them out of order would show
a bubble that briefly reverts.

**Text only.** Deltas carry the model's prose and nothing else (`Translate` drops
every `assistantMessageEvent` that is not a `text_delta`), so concatenating their
strings is the whole operation: no ids to merge, no per-chunk metadata to keep,
and no reason to hold anything but a builder.
*/

const (
	// defaultDeltaFlushInterval is how long a delta waits for company before
	// being forwarded on its own.
	defaultDeltaFlushInterval = 75 * time.Millisecond
	// defaultDeltaMaxBytes bounds one flush, so a fast stream produces a few
	// larger bodies rather than an unbounded one.
	defaultDeltaMaxBytes = 4096
)

// deltaCoalescer wraps a curated-event sink and merges consecutive agent-text
// deltas into fewer, larger events.
//
// Only the RPC read loop forwards events, which is what makes a single buffer
// correct — but the flush timer fires on its own goroutine, so every path holds
// the mutex. `forward` is called while holding it, deliberately: the sink does a
// blocking POST, and letting a timer flush interleave with the read loop's own
// forward would reorder deltas against the events around them. `forward` must
// therefore not re-enter this type.
type deltaCoalescer struct {
	forward func(rpc.CuratedEvent)

	flushInterval time.Duration
	maxBytes      int

	mu      sync.Mutex
	buffer  strings.Builder
	timer   *time.Timer
	stopped bool
}

func newDeltaCoalescer(
	forward func(rpc.CuratedEvent),
	flushInterval time.Duration,
	maxBytes int,
) *deltaCoalescer {
	return &deltaCoalescer{
		forward:       forward,
		flushInterval: flushInterval,
		maxBytes:      maxBytes,
	}
}

// event is the sink the session loop calls: deltas accumulate, anything else
// flushes first and is then forwarded unchanged.
func (c *deltaCoalescer) event(ev rpc.CuratedEvent) {
	if ev.Type == rpc.EventAgentTextDelta {
		c.add(ev)
		return
	}
	c.flush()
	c.forward(ev)
}

func (c *deltaCoalescer) add(ev rpc.CuratedEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		// After stop() forward directly rather than dropping: a late delta is
		// still information the API has not seen, and losing text is worse than
		// one extra POST. (It cannot be reordered relative to a later event,
		// because stop() is called once the session loop is finished.)
		c.forward(ev)
		return
	}

	c.buffer.WriteString(ev.Message)
	if c.buffer.Len() >= c.maxBytes {
		c.flushLocked()
		return
	}
	if c.timer == nil {
		c.timer = time.AfterFunc(c.flushInterval, c.flush)
	}
}

// flush forwards whatever has accumulated, if anything.
func (c *deltaCoalescer) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
}

func (c *deltaCoalescer) flushLocked() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if c.buffer.Len() == 0 {
		return
	}
	text := c.buffer.String()
	c.buffer.Reset()
	c.forward(rpc.CuratedEvent{Type: rpc.EventAgentTextDelta, Message: text})
}

// stop flushes and disables the timer. Called when the session ends, so a
// buffered tail is never the thing a finished run loses — and so a timer cannot
// outlive the job and keep posting.
func (c *deltaCoalescer) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
	c.stopped = true
}
