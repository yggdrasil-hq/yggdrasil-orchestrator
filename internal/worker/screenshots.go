package worker

import (
	"context"
	"log"
	"path"
	"strings"
	"sync"
	"time"
)

/*
Issue #22's collection half: read each reported screenshot out of a finished job's
pod and upload it, before the deferred DeleteJob destroys the pod that holds it.

**The defect this closes.** `test_run_steps.screenshot_path` has been a pointer
into a job pod since it was written, and the pod is deleted at job end (ADR 006
item 11) — so every screenshot path ever reported referred to a file that no
longer existed. It is exactly the bug ADR 029 fixed for recordings, and it went
unnoticed for the same reason: nothing ever tried to read the bytes. The API half
landed first (`job_screenshots`, retention, an upload endpoint and read
endpoints), so the storage exists and no bytes arrived; this is the transport.

**Shaped after `recordings.go` deliberately**, because the constraints are the
same: the read must happen after the session ends but before the pod is deleted,
it must never be able to fail the job, and it must not depend on the session's own
context surviving teardown. Where it differs, it differs for a reason:

1. **One screenshot per step, not one per job.** A recording is a single artifact
   the run produces at its end; a screenshot is reported per `report_test_step`,
   so a run has as many as it has `##` headings. That is what makes this module a
   *collector* rather than a single read — the paths arrive over the session and
   are read in a batch once it is over.

2. **They are read in a batch after the session, not as they stream past.** The
   obvious alternative — read and upload inside the event sink the moment a step
   reports — would block the RPC read loop for the duration of a pod exec and an
   HTTP round trip, *per step*, adding that latency to the agent's own turn. Issue
   #23 went to some trouble to stop the read loop being the bottleneck for deltas;
   doing per-step I/O there would reintroduce the same shape from a different
   direction. Accumulating a path during the session is free, so the I/O is moved
   to where nothing is waiting on it.

3. **The formats are the API's set, and an unknown one is skipped rather than
   guessed.** `recordingContentType` defaults to WebM for an unrecognised
   extension on the reasoning that a recording which plays beats one refused over
   its filename. That reasoning does not transfer: the API accepts exactly PNG,
   JPEG and WebP, so an unrecognised extension has no valid Content-Type to send
   at all — and guessing PNG for a `.gif` would store GIF bytes labelled
   `image/png`, which renders as a broken image and mislabels the artifact in the
   database. Skipping is the only honest option, and it is logged.
*/

// screenshotContainer is the container name a job pod runs its Pi session in,
// and therefore the one the skill wrote the screenshot inside. Aliased to the
// same constant recordings use rather than re-spelled, because it is the k8s
// package's name for the container it creates.
const screenshotContainer = recordingContainer

// screenshotReadLimit bounds the raw read, independently of the policy cap in
// Config — the same relationship `recordingReadLimit` has to RecordingMaxBytes,
// and it exists for the same reason: the policy check is what produces the
// "skipped, too large" line, and a read bound set exactly at the cap would turn
// an artifact one byte over it into an opaque stream error instead.
//
// Unlike the recording bound this is not sized to hold a legitimate artifact
// (screenshots are capped at a couple of MB); it only has to be low enough that a
// mis-reported path — a device file, a runaway writer — cannot exhaust the
// Orchestrator's memory. 64 MiB matches the recording bound because there is no
// value in the two differing: neither is a policy, and one number is one thing to
// reason about.
const screenshotReadLimit = recordingReadLimit

// DefaultScreenshotMaxBytes is the default cap on one collected screenshot.
//
// Exported for the same two-callers reason DefaultRecordingMaxBytes is: this
// package applies it when a deployment leaves the value unset, and
// cmd/server/main.go's env resolver returns it for an unset or unparseable value.
//
// 2 MB is the API's own SCREENSHOT_MAX_BYTES default, and the two **must** agree —
// otherwise the Orchestrator would either ship bytes only to be declined, or
// refuse artifacts the API would have kept. A full-page PNG of a dense UI at a
// desktop viewport is a few hundred KB; 2 MB leaves generous headroom while
// still refusing something that is not a screenshot.
const DefaultScreenshotMaxBytes int64 = 2_000_000

// screenshotCollectTimeout bounds the whole batch read for one job.
//
// One budget for all of a run's screenshots rather than one per artifact,
// because this runs on the teardown path: the pod is alive only until
// `runAgentRPCJob` returns, so an unbounded tail of per-screenshot waits would
// hold teardown open. Each read gets its own slice of this budget (see
// `screenshotReadTimeout`), so one wedged exec cannot consume it all and abandon
// the rest.
//
// Two minutes is generous against artifacts of a few hundred KB read over the API
// server's exec stream, and it is the same order as recordingCollectTimeout —
// which moves a far larger artifact through the same path.
const screenshotCollectTimeout = 2 * time.Minute

// screenshotReadTimeout bounds one screenshot's read inside the batch budget.
//
// Per-read rather than shared so that a single hung `cat` costs its own timeout
// and not the whole batch: with one shared deadline, the first wedged read would
// silently discard every screenshot after it, which on a run with a dozen steps
// is a dozen artifacts lost to one broken exec.
const screenshotReadTimeout = 15 * time.Second

// screenshotPoster is the subset of *apiclient.Client this needs — the same
// narrow-interface treatment `recordingPoster` gets, so this module's logic can
// be exercised without an API.
type screenshotPoster interface {
	PostJobScreenshot(ctx context.Context, jobID, stepName, contentType string, data []byte) error
}

// screenshotArtifact is one step's reported screenshot, as the agent's own event
// named it: the step it belongs to, and the in-pod path to read.
type screenshotArtifact struct {
	stepName string
	path     string
}

// screenshotCollector accumulates the screenshots a run reports while its event
// stream is live, to be read and uploaded once the session is over.
//
// **Why it de-duplicates on the step name.** The API's identity for a screenshot
// is (job, stepName) and it upserts, so the same step reported twice can only
// overwrite itself. Keeping both would be harmless to the stored result but not
// to anything else: the count of uploads would inflate, the newest path is the
// only one worth reading anyway, and a step that re-reports could crowd a
// distinct step out of a bounded structure.
//
// **Why the mutex is here at all.** Every event this cares about
// (`report_test_step`) reaches the sink from the RPC read loop, which has
// returned by the time the batch runs — so the lock is not load-bearing for the
// paths taken today. It is held anyway because the sink *can* be invoked from
// the delta coalescer's timer goroutine (issue #23), and a reader should not have
// to derive which event types take which path to trust this type. `-race` sees
// the same thing a reviewer would.
//
// **No count cap.** The API owns the per-job budget (`SCREENSHOT_MAX_PER_JOB`)
// and is authoritative about what it stores. A second bound here would have to
// agree with a configurable number on the other side, and the failure mode of
// getting that wrong is *silent* — screenshots the API would have accepted never
// uploaded at all. Exceeding the API's budget instead produces a logged decline
// per extra artifact, which is noisy but visible and loses nothing that would
// otherwise have been stored. The overall batch timeout is what keeps the tail
// finite.
type screenshotCollector struct {
	mu        sync.Mutex
	artifacts []screenshotArtifact
}

func newScreenshotCollector() *screenshotCollector {
	return &screenshotCollector{}
}

// add records one reported screenshot. Cheap and I/O-free by design: it runs
// inside the session's event sink, so anything slow here would land on the
// agent's own turn.
func (c *screenshotCollector) add(stepName, screenshotPath string) {
	if c == nil || stepName == "" || screenshotPath == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// A linear scan, because the collection is small by construction (one entry
	// per `##` heading in the project's test markdown) and a map would add a
	// second structure to keep consistent for no measurable gain.
	for i := range c.artifacts {
		if c.artifacts[i].stepName == stepName {
			c.artifacts[i].path = screenshotPath
			return
		}
	}
	c.artifacts = append(c.artifacts, screenshotArtifact{
		stepName: stepName,
		path:     screenshotPath,
	})
}

// pending returns what is waiting to be collected, in the order the steps were
// first reported.
func (c *screenshotCollector) pending() []screenshotArtifact {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]screenshotArtifact(nil), c.artifacts...)
}

// screenshotCollection captures everything needed to fetch one job's screenshots
// after its session has ended but before its pod is deleted.
type screenshotCollection struct {
	read      podFileReader
	api       screenshotPoster
	jobID     string
	namespace string
	podName   string
	maxBytes  int64
}

// screenshotContentType maps a reported screenshot's file extension to the MIME
// type the API accepts, or "" when the extension is not one of them.
//
// The empty string is a real answer rather than a fallback, and the reason is in
// this module's header: the API accepts PNG, JPEG and WebP only, so an
// unrecognised extension has no valid type to send. `.jpeg` and `.jpg` are both
// accepted because both are what JPEG files are called in practice, and a
// screenshot tool is free to write either.
func screenshotContentType(filePath string) string {
	switch strings.ToLower(path.Ext(filePath)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	default:
		return ""
	}
}

// collectScreenshots reads each reported screenshot out of the pod and uploads
// it to the API.
//
// Best-effort by contract, and that contract is the point: a screenshot is a
// diagnostic extra attached to a run that has already decided its own outcome, so
// **every** failure here — a missing file, an unreadable format, an oversized
// artifact, an API that is down — is logged and dropped. It must never turn a
// passing test run into a failed job. The same rule as recordings, and ADR 029
// item 5's rule for the API's side of the same pipeline.
//
// One artifact's failure never stops the others: each is read, validated and
// uploaded in its own iteration, so a single bad path costs that one screenshot
// rather than the rest of the batch. That matters more here than for recordings
// because there are many — a run with twelve steps should not lose all twelve to
// one unreadable file.
func collectScreenshots(ctx context.Context, c screenshotCollection, artifacts []screenshotArtifact) {
	if c.read == nil || c.api == nil {
		return
	}
	// A non-positive cap means collection is switched off (see
	// Config.ScreenshotMaxBytes), so this returns before touching the pod at all
	// — stated explicitly rather than left to the size comparison below, for the
	// reason collectRecording gives: the comparison would refuse every artifact
	// anyway, but only after reading it out of the pod and logging a misleading
	// "too large". Disabled should mean "do nothing", not "do the work and then
	// discard it".
	if c.maxBytes <= 0 {
		return
	}

	uploaded := 0
	for _, artifact := range artifacts {
		contentType := screenshotContentType(artifact.path)
		if contentType == "" {
			log.Printf(
				"worker: skipping screenshot %s for step %q on job %s: it is not a PNG, JPEG or WebP, which are the only formats the API stores",
				artifact.path, artifact.stepName, c.jobID,
			)
			continue
		}

		// Its own slice of the batch budget, so a wedged exec costs one timeout
		// rather than every remaining screenshot.
		readCtx, cancelRead := context.WithTimeout(ctx, screenshotReadTimeout)
		data, err := c.read(readCtx, c.namespace, c.podName, screenshotContainer, artifact.path)
		cancelRead()
		if err != nil {
			log.Printf(
				"worker: could not read screenshot %s for step %q on job %s: %v",
				artifact.path, artifact.stepName, c.jobID, err,
			)
			continue
		}

		// Checked here as well as in the reader because the reader's own bound is
		// a safety net against a lying size, while this is the policy cap the API
		// will enforce anyway — refusing locally avoids shipping bytes only to
		// have them declined. The bound is inclusive, matching the API's own
		// rejection rule, so the two sides cannot disagree by one byte.
		if int64(len(data)) > c.maxBytes {
			log.Printf(
				"worker: skipping screenshot for step %q on job %s: %d bytes exceeds the %d byte limit",
				artifact.stepName, c.jobID, len(data), c.maxBytes,
			)
			continue
		}

		// The batch context, not the per-read one: the read's timeout has already
		// fired by now if it was the slow part, and the upload is bounded by the
		// budget the caller set.
		if err := c.api.PostJobScreenshot(ctx, c.jobID, artifact.stepName, contentType, data); err != nil {
			log.Printf(
				"worker: failed to upload screenshot for step %q on job %s: %v",
				artifact.stepName, c.jobID, err,
			)
			continue
		}
		uploaded++
	}

	if uploaded > 0 {
		log.Printf("worker: uploaded %d screenshot(s) for job %s", uploaded, c.jobID)
	}
}
