package worker

import (
	"context"
	"log"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// sessionReadLimit bounds the raw read of a session JSONL, independently of the
// policy cap in Config — the same two-bound arrangement recordingReadLimit has,
// and for the same reason: the policy check is what produces the "skipped, too
// large" log line, while this bound is only a safety net against a mis-reported
// path (a device file, a runaway writer) exhausting the Orchestrator's memory.
//
// Larger than the recording's 64 MiB on purpose. A session is text and a long
// grill is still only megabytes, but Pi appends tool results verbatim and a build
// can cat a large file, so the ceiling is set where a *real* session cannot reach
// it rather than where a typical one does.
const sessionReadLimit = 256 << 20 // 256 MiB

// sessionCollectTimeout bounds the read-and-upload of a finished job's session
// file. Same order as recordingCollectTimeout: this runs after the job's outcome
// is already decided, and must never hold a worker slot open.
const sessionCollectTimeout = 2 * time.Minute

// DefaultSessionMaxBytes is ADR 032 item 4's default cap on a collected session.
//
// Deliberately **not** inherited from DefaultRecordingMaxBytes, which the ADR
// calls out explicitly: a session is text and far smaller than a video, so
// borrowing the recording's 25 MB would advertise a ceiling no session reaches
// and make the "skipped, too large" path untestable in practice.
//
// Exported for the same reason DefaultRecordingMaxBytes is: cmd/server/main.go's
// env resolver returns it for an unset or unparseable value, so the default
// cannot disagree with itself.
const DefaultSessionMaxBytes int64 = 5_000_000

// sessionPoster is the subset of *apiclient.Client this needs. An interface so
// the collection logic is exercisable against a fake without an HTTP server —
// the same reason recordingPoster exists.
type sessionPoster interface {
	PostJobSession(ctx context.Context, artifact apiclient.SessionArtifact, data []byte) error
}

// sessionCollection captures everything needed to fetch one job's Pi session
// file after its session has ended but before its pod is deleted (ADR 032 item
// 1). Shaped after recordingCollection, deliberately: ADR 032's claim is that
// this is the third artifact type through one layer, and a struct that does not
// match the first two would be evidence against it.
type sessionCollection struct {
	read      podFileReader
	api       sessionPoster
	jobID     string
	namespace string
	podName   string
	filePath  string
	// sessionID is Pi's own id for this session, carried so the artifact can
	// report it without a second round trip. Empty when Pi reported none.
	sessionID string
	maxBytes  int64
}

// SessionCollectionOutcome is what became of one job's session file, and it is
// ADR 032 item 5's honesty requirement made concrete.
//
// **Why this exists rather than a bare error.** Item 5 requires three outcomes
// to be distinguishable, and two of them look identical from inside this
// function: a pod killed before Pi wrote a session, and a session the Orchestrator
// tried to read and could not. Both leave nothing stored. The API half arrives
// next wave and *cannot invent this distinction* — so it is recorded here, in the
// one place that knows which happened.
//
// The values are strings rather than an iota so that the API's field can carry
// them verbatim without a mapping table on either side, and so a log line is
// readable without a lookup.
type SessionCollectionOutcome string

const (
	// SessionCollected — the JSONL was read and stored. The only outcome that
	// means a fork can be offered.
	SessionCollected SessionCollectionOutcome = "collected"

	// SessionNotCollected — Pi never produced a session file for this run, so
	// there was nothing to store and nothing was attempted.
	//
	// ADR 032's trade-offs name this case: "a session only exists if the pod
	// reached a point where one was written. A pod killed early leaves nothing."
	// It is the routine shape of a *failed* run here, not an anomaly.
	SessionNotCollected SessionCollectionOutcome = "not_collected"

	// SessionUnavailable — a session file existed (or may have) but this run
	// could not obtain it: the read failed, the artifact was over the cap, or the
	// upload was refused.
	//
	// Distinct from SessionNotCollected because the two need different words in
	// front of a user. "This run did not save a session" is a fact about the run;
	// "this run's session could not be retrieved" is a fact about the retrieval,
	// and only the second is worth an operator looking at. Collapsing them would
	// make a transient upload failure indistinguishable from a run that never got
	// far enough to have a session — which is precisely the "looks finished, does
	// nothing" failure this burn-down keeps finding.
	SessionUnavailable SessionCollectionOutcome = "unavailable"

	// SessionDisabled — collection is switched off by configuration
	// (SessionMaxBytes <= 0), or this process has no poster to report with.
	// Nothing was attempted.
	//
	// **The one outcome that is never reported to the API**, and the reason is that
	// it is a fact about the *installation* rather than about the run: a deployment
	// with collection off would otherwise post a "disabled" record for every job it
	// ever runs, which is noise that says nothing about any of them. The API can
	// still tell it apart from the other three — they all produce a record and this
	// one produces none — so nothing item 5 needs is lost.
	//
	// A distinct constant rather than folding into SessionNotCollected, because the
	// two have opposite remediations: one is "the run did not produce a session",
	// the other is "this installation turned collection off". The recording path
	// returns early and silently in this case; here it is named, since the outcome
	// type is what the rest of this file branches on.
	SessionDisabled SessionCollectionOutcome = "disabled"
)

// CollectsSession reports whether this outcome means the caller can attempt a
// fork against the stored artifact.
//
// A method rather than a comparison at each call site so that the one outcome
// that enables a fork is stated once. The API half will want the same predicate.
func (o SessionCollectionOutcome) CollectsSession() bool {
	return o == SessionCollected
}

// sessionArtifactFrom builds the report for a session the Orchestrator *did*
// collect, so the path, size and id travel with the outcome.
//
// Extracted from the collection function so the mapping is unit-testable without
// a cluster — the same reason usageReportFrom exists.
func sessionArtifactFrom(jobID string, session rpc.SessionFile, byteSize int64) apiclient.SessionArtifact {
	return apiclient.SessionArtifact{
		JobID:       jobID,
		Outcome:     string(SessionCollected),
		SessionID:   session.SessionID,
		ByteSize:    byteSize,
		PodFilePath: session.FilePath,
	}
}

// sessionArtifactFor builds the report for a session the Orchestrator did *not*
// collect, which is the half ADR 032 item 5 exists for.
//
// The fields other than the outcome are left zero on purpose: a session that was
// not read has no size, and its id and path are exactly the facts a reader must
// not be given, since reporting them would suggest the artifact exists.
func sessionArtifactFor(jobID string, outcome SessionCollectionOutcome) apiclient.SessionArtifact {
	return apiclient.SessionArtifact{JobID: jobID, Outcome: string(outcome)}
}

// collectSession reads a finished job's Pi session file out of its pod, reports
// it to the API (ADR 032 item 1), and returns what became of it.
//
// The return is the worker's own `SessionCollectionOutcome` rather than the wire
// artifact, because the outcome is what every caller here branches on and the
// artifact's identity fields are the API's to read back. Tests assert on the
// artifact through the poster they substituted.
//
// **The posture is collectRecording's, deliberately and in detail**: the same
// seam (after the session ends, before the deferred DeleteJob), the same
// "read it out of the pod before the pod dies", the same read bound plus policy
// cap, the same best-effort failure handling. ADR 032's claim is that this is the
// third artifact type through one layer rather than a new mechanism, and following
// the first two is what makes that true rather than merely asserted.
//
// **One deliberate difference from collectRecording, and it is the whole of item
// 5.** collectRecording returns nothing: a recording is a diagnostic extra whose
// absence is uninteresting, so "logged and dropped" is complete. A session is
// *load-bearing* — ADR 032 item 3 makes it what a non-destructive "Resume from
// here" needs — so its absence is a fact the user is shown. Hence an outcome
// rather than silence, and hence the failing cases are named separately.
//
// **It still never fails the job.** The distinction is between reporting and
// failing: the artifact travels to the API as a side channel, and a run that did
// its real work is never reported as failed because an artifact could not be
// stored. That is ADR 029's rule and item 5 does not change it.
//
// Every outcome *about the run* is reported, including the failing ones — see
// PostJobSession's comment for why a route that is only called on success cannot
// express item 5's distinction. `SessionDisabled` is the exception and returns
// before reporting, because it is a fact about the installation (see its own
// comment). A failure to report at all is logged and dropped, like every other
// side channel here: it is the one case where the outcome is lost, and there is
// nothing left to record it with.
func collectSession(ctx context.Context, c sessionCollection) SessionCollectionOutcome {
	if c.read == nil || c.api == nil {
		// A worker configured without session collection, or a partially-built
		// collection in a test. Reported as disabled rather than not_collected:
		// nothing about the *run* says there was no session.
		return SessionDisabled
	}
	// A non-positive cap means collection is switched off (see
	// Config.SessionMaxBytes). Stated as an explicit early return rather than
	// folded into the size comparison below, for the reason collectRecording
	// gives: because the comparison is `>`, a cap of 0 would refuse every artifact
	// anyway, but only *after* reading the file out of the pod.
	if c.maxBytes <= 0 {
		return SessionDisabled
	}
	// Pi never reported a session file for this run, so there is nothing to read
	// and no point touching the pod. This is the pod-killed-early case, and
	// distinguishing it here rather than letting the read fail is what makes
	// `not_collected` honest rather than a guess.
	if c.filePath == "" {
		return c.report(ctx, sessionArtifactFor(c.jobID, SessionNotCollected), nil)
	}

	data, err := c.read(ctx, c.namespace, c.podName, recordingContainer, c.filePath)
	if err != nil {
		// A read failure is `unavailable`, not `not_collected`: the file was
		// named, so the run *had* a session, and this run could not obtain it.
		log.Printf("worker: could not read session %s for job %s: %v", c.filePath, c.jobID, err)
		return c.report(ctx, sessionArtifactFor(c.jobID, SessionUnavailable), nil)
	}
	if int64(len(data)) > c.maxBytes {
		log.Printf(
			"worker: skipping session for job %s: %d bytes exceeds the %d byte limit",
			c.jobID, len(data), c.maxBytes,
		)
		return c.report(ctx, sessionArtifactFor(c.jobID, SessionUnavailable), nil)
	}

	// The bytes ride with the artifact in one call, so a reader never sees an
	// outcome saying `collected` without the object beside it (or the reverse).
	return c.report(ctx, sessionArtifactFrom(c.jobID, rpc.SessionFile{
		FilePath: c.filePath, SessionID: c.sessionID,
	}, int64(len(data))), data)
}

// report posts one artifact and returns the outcome that was actually reported.
//
// A *successful* read whose post fails is downgraded to `unavailable`, and that
// is the honest answer rather than a nicety: if the API never received the
// artifact then from the API's side no session was collected, so returning
// `collected` would be a claim only this process knows to be false.
//
// A failing outcome stays as it was when its post also fails: `not_collected` and
// `unavailable` are both "no artifact" and the API holds no record either way, so
// there is no distinction left to lose — and overwriting the run's real outcome
// would lose the one fact the API would have wanted.
func (c sessionCollection) report(
	ctx context.Context,
	artifact apiclient.SessionArtifact,
	data []byte,
) SessionCollectionOutcome {
	outcome := SessionCollectionOutcome(artifact.Outcome)
	if err := c.api.PostJobSession(ctx, artifact, data); err != nil {
		log.Printf("worker: failed to report session for job %s (outcome %s): %v", artifact.JobID, artifact.Outcome, err)
		if outcome == SessionCollected {
			return SessionUnavailable
		}
	}
	return outcome
}
