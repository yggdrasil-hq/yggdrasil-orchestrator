package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// rawEvent builds an rpc.Event from a JSONL line, the same helper the rpc
// package's own tests use.
func rawEvent(t *testing.T, line string) rpc.Event {
	t.Helper()
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return rpc.Event{Type: envelope.Type, Raw: json.RawMessage(line)}
}

// fakeSessionPoster records what collectSession reported, so a test can assert on
// the artifact *and* the bytes together — which is the point of the single-call
// shape.
type fakeSessionPoster struct {
	calls    int
	artifact apiclient.SessionArtifact
	data     []byte
	err      error
}

func (f *fakeSessionPoster) PostJobSession(
	_ context.Context,
	artifact apiclient.SessionArtifact,
	data []byte,
) error {
	f.calls++
	f.artifact = artifact
	f.data = data
	return f.err
}

// sessionFixture is a collection whose reader returns body, so each test states
// only the thing it is about.
func sessionFixture(poster *fakeSessionPoster, body []byte) sessionCollection {
	return sessionCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return body, nil
		},
		api:       poster,
		jobID:     "job-1",
		namespace: "ns",
		podName:   "pod-1",
		filePath:  "/root/.pi/agent/sessions/session.jsonl",
		sessionID: "sess-abc",
		// A successful ask, so fileUnknown is false — the fixture represents the
		// normal case and each test that needs an unanswered ask sets it.
		fileUnknown: false,
		maxBytes:    1000,
	}
}

func TestCollectSessionUploadsTheBytesAndReportsCollected(t *testing.T) {
	poster := &fakeSessionPoster{}
	got := collectSession(context.Background(), sessionFixture(poster, []byte("jsonl-bytes")))

	if poster.calls != 1 {
		t.Fatalf("expected one report, got %d", poster.calls)
	}
	if got != SessionCollected {
		t.Fatalf("outcome = %q, want %q", got, SessionCollected)
	}
	if !got.CollectsSession() {
		t.Error("a collected session must satisfy CollectsSession, or nothing can fork from it")
	}
	if poster.artifact.JobID != "job-1" {
		t.Errorf("reported job %q, want job-1", poster.artifact.JobID)
	}
	if string(poster.data) != "jsonl-bytes" {
		t.Errorf("uploaded %q", poster.data)
	}
	// The id and path are what a reader uses to identify the session, so they must
	// survive the round trip rather than being derivable-but-absent.
	if poster.artifact.SessionID != "sess-abc" {
		t.Errorf("reported session id %q, want sess-abc", poster.artifact.SessionID)
	}
	// No size field: the bytes are the body, so the API derives it from what it
	// receives rather than trusting a number sent alongside. Asserted by checking
	// the body is what the API would measure.
	if len(poster.data) == 0 {
		t.Error("no bytes accompanied a collected outcome, so the API has nothing to size")
	}
	if poster.artifact.PodFilePath != "/root/.pi/agent/sessions/session.jsonl" {
		t.Errorf("reported pod path %q", poster.artifact.PodFilePath)
	}
}

// The core of ADR 032 item 5: each failing case must land on the outcome that
// describes it, and the outcomes must stay distinct from each other and from
// success. A test per case, because collapsing any two is the bug this guards.
//
// Note that a read failure and an oversized artifact both land on `unavailable`,
// and that is correct rather than a gap: item 5 groups them ("the session file is
// expired or was never persisted") and puts the finer distinction in the *reason*,
// which an operator reads. What must not happen is either of them reading as
// `not_collected` — a run that never had a session — because that is a fact about
// the run and the other two are facts about the retrieval.
func TestCollectSessionReportsEachFailingOutcomeDistinctly(t *testing.T) {
	cases := map[string]struct {
		mutate func(*sessionCollection)
		want   SessionCollectionOutcome
	}{
		"a run that never wrote a session is not_collected": {
			mutate: func(c *sessionCollection) { c.filePath = "" },
			want:   SessionNotCollected,
		},
		"a read failure is unavailable, not not_collected": {
			mutate: func(c *sessionCollection) {
				c.read = func(context.Context, string, string, string, string) ([]byte, error) {
					return nil, errors.New("cat: no such file")
				}
			},
			want: SessionUnavailable,
		},
		"an oversized artifact is unavailable": {
			mutate: func(c *sessionCollection) { c.maxBytes = 4 },
			want:   SessionUnavailable,
		},
		"an unanswered terminal read is unavailable, not not_collected": {
			// The distinction that is easiest to get wrong, because from this
			// struct's point of view it looks exactly like "no session": both
			// leave an empty filePath. Only fileUnknown separates them.
			mutate: func(c *sessionCollection) { c.fileUnknown = true; c.filePath = "" },
			want:   SessionUnavailable,
		},
		"collection switched off is disabled": {
			mutate: func(c *sessionCollection) { c.maxBytes = 0 },
			want:   SessionDisabled,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			poster := &fakeSessionPoster{}
			collection := sessionFixture(poster, []byte("jsonl-bytes"))
			tc.mutate(&collection)

			got := collectSession(context.Background(), collection)

			if got != tc.want {
				t.Fatalf("outcome = %q, want %q", got, tc.want)
			}
			if got.CollectsSession() {
				t.Error("a failing outcome must not claim a session can be forked from")
			}
			// Reported even when failing — that is what makes the distinction
			// reachable by the API at all. `disabled` is the one exception and is
			// asserted separately below, because it is a fact about the
			// installation rather than about this run.
			if tc.want == SessionDisabled {
				if poster.calls != 0 {
					t.Fatalf("a disabled installation must not post a record per job, got %d", poster.calls)
				}
				return
			}
			if poster.calls != 1 {
				t.Fatalf("a failing outcome must still be reported, got %d calls", poster.calls)
			}
		})
	}
}

func TestSessionOutcomesAreDistinctValues(t *testing.T) {
	// The regression this guards is someone aliasing two of these constants — the
	// exact way ADR 032 item 5's distinction would be lost while every test above
	// still passed, since a test that names one constant cannot see another
	// sharing its value.
	all := map[string]SessionCollectionOutcome{
		"collected":     SessionCollected,
		"not_collected": SessionNotCollected,
		"unavailable":   SessionUnavailable,
		"disabled":      SessionDisabled,
	}
	seen := map[SessionCollectionOutcome]string{}
	for name, value := range all {
		if value == "" {
			t.Fatalf("outcome %s is the empty string, which a JSON reader would read as absent", name)
		}
		if other, dup := seen[value]; dup {
			t.Fatalf("outcomes %s and %s share the value %q", other, name, value)
		}
		seen[value] = name
	}

	// And only one of them permits a fork.
	for name, value := range all {
		want := name == "collected"
		if value.CollectsSession() != want {
			t.Errorf("CollectsSession() for %s = %v, want %v", name, !want, want)
		}
	}
}

func TestCollectSessionDoesNotReadThePodWhenThereIsNoSession(t *testing.T) {
	// A pod-killed-early run names no file. Reading anyway would produce a
	// `unavailable` (a read failure) instead of the truthful `not_collected`, so
	// the reader must not be reached at all.
	readCalls := 0
	poster := &fakeSessionPoster{}
	collection := sessionFixture(poster, nil)
	collection.filePath = ""
	collection.read = func(context.Context, string, string, string, string) ([]byte, error) {
		readCalls++
		return nil, errors.New("should not be called")
	}

	got := collectSession(context.Background(), collection)

	if readCalls != 0 {
		t.Errorf("touched the pod %d times for a run with no session", readCalls)
	}
	if got != SessionNotCollected {
		t.Fatalf("outcome = %q, want %q", got, SessionNotCollected)
	}
}

func TestCollectSessionDowngradesACollectedSessionWhenTheReportFails(t *testing.T) {
	// The honest answer, and the reason this is a test rather than a comment: if
	// the API never received the artifact then from the API's side no session was
	// collected, so reporting `collected` locally would be a claim only this
	// process knows to be false.
	poster := &fakeSessionPoster{err: errors.New("API returned status 502")}

	got := collectSession(context.Background(), sessionFixture(poster, []byte("jsonl-bytes")))

	if got != SessionUnavailable {
		t.Fatalf("outcome = %q, want %q", got, SessionUnavailable)
	}
	if got.CollectsSession() {
		t.Error("a session whose report failed must not claim it can be forked from")
	}
}

func TestCollectSessionKeepsAFailingOutcomeWhenTheReportAlsoFails(t *testing.T) {
	// Both are "no artifact" and the API has no record either way, so there is no
	// distinction left to lose — and inventing `unavailable` here would lose the
	// one the run actually produced.
	poster := &fakeSessionPoster{err: errors.New("API returned status 502")}
	collection := sessionFixture(poster, nil)
	collection.filePath = ""

	got := collectSession(context.Background(), collection)

	if got != SessionNotCollected {
		t.Fatalf("outcome = %q, want %q", got, SessionNotCollected)
	}
}

func TestCollectSessionAcceptsAnArtifactExactlyAtTheCap(t *testing.T) {
	// The bound is inclusive, matching collectRecording and the API's own
	// `exceedsSizeCap`, so the two sides cannot disagree by one byte.
	poster := &fakeSessionPoster{}
	collection := sessionFixture(poster, []byte("1234"))
	collection.maxBytes = 4

	got := collectSession(context.Background(), collection)

	if got != SessionCollected {
		t.Fatalf("outcome = %q, want %q for an artifact at the cap", got, SessionCollected)
	}
}

func TestCollectSessionDoesNotPanicWithoutItsDependencies(t *testing.T) {
	// Guards the partial-collection case, the same way the recording equivalent
	// does: a worker configured without session collection must not nil-panic on
	// the path every agent job takes.
	got := collectSession(context.Background(), sessionCollection{jobID: "job-1", maxBytes: 1000})
	if got != SessionDisabled {
		t.Fatalf("outcome = %q, want %q", got, SessionDisabled)
	}

	poster := &fakeSessionPoster{}
	got = collectSession(context.Background(), sessionCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return []byte("x"), nil
		},
		jobID: "job-1", maxBytes: 1000,
	})
	if got != SessionDisabled {
		t.Fatalf("outcome = %q, want %q without a poster", got, SessionDisabled)
	}
	if poster.calls != 0 {
		t.Errorf("reported %d times without a poster", poster.calls)
	}
}

func TestSessionArtifactForCarriesNoIdentityForAFailingOutcome(t *testing.T) {
	// Reporting an id or a size for a session that was not read would suggest the
	// artifact exists — the failure mode item 5 is about.
	for _, outcome := range []SessionCollectionOutcome{
		SessionNotCollected, SessionUnavailable, SessionDisabled,
	} {
		artifact := sessionArtifactFor("job-1", outcome)
		if artifact.SessionID != "" || artifact.PodFilePath != "" {
			t.Errorf("outcome %q carried identity fields: %+v", outcome, artifact)
		}
	}
}

// The mutation this guards is the one the field was added for: treating an
// unanswered ask as "no session". Both leave an empty filePath, so nothing else in
// the struct can tell them apart — and the two need different words in front of a
// user (ADR 032 item 5).
func TestCollectSessionDistinguishesAnUnansweredAskFromNoSession(t *testing.T) {
	unanswered := &fakeSessionPoster{}
	collection := sessionFixture(unanswered, nil)
	collection.filePath = ""
	collection.fileUnknown = true
	got := collectSession(context.Background(), collection)
	if got != SessionUnavailable {
		t.Fatalf("an unanswered terminal read reported %q, want %q", got, SessionUnavailable)
	}

	noSession := &fakeSessionPoster{}
	collection = sessionFixture(noSession, nil)
	collection.filePath = ""
	collection.fileUnknown = false
	got = collectSession(context.Background(), collection)
	if got != SessionNotCollected {
		t.Fatalf("a session Pi answered about reported %q, want %q", got, SessionNotCollected)
	}

	// And they are not the same value, which is the whole point.
	if SessionUnavailable == SessionNotCollected {
		t.Fatal("the two outcomes are indistinguishable")
	}
}

// A session id Pi reported before the read failed is still worth carrying, so the
// zeroing above is about the *path*, not the whole struct.
func TestSessionFileAskedIsSetOnlyByAnAnswer(t *testing.T) {
	ev := rawEvent(t, `{"type":"response","command":"get_state","success":true,"data":{"sessionFile":"/s.jsonl","sessionId":"abc"}}`)
	session, ok := rpc.ParseSessionFile(ev)
	if !ok || !session.Asked {
		t.Fatalf("a parsed answer must record that it was asked: %+v ok=%v", session, ok)
	}

	// A failed response is not an answer, so it must not parse at all — the
	// caller's zero value then means "not asked", which is what makes the
	// distinction above reachable.
	if _, ok := rpc.ParseSessionFile(rawEvent(t, `{"type":"response","command":"get_state","success":false}`)); ok {
		t.Fatal("a failed response must not parse as an answer")
	}
}
