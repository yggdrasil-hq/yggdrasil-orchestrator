package worker

import (
	"context"
	"log"
	"path"
	"strings"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
)

// recordingContainer is the container name every agent job pod runs its Pi
// session in (see runTurn's attach call) — the same container the skill wrote
// the recording inside. Aliased rather than re-spelled: the k8s package owns
// the name because it is what creates the container.
const recordingContainer = k8s.RunContainerName

// podFileReader reads one file out of a running pod. A function type rather than
// the k8s package's signature directly, so this module's logic can be exercised
// against a fake reader without a cluster — the same reason replyWaiter and
// cancelWatcher exist.
type podFileReader func(ctx context.Context, namespace, podName, container, filePath string) ([]byte, error)

// recordingPoster is the subset of *apiclient.Client this needs.
type recordingPoster interface {
	PostJobRecording(ctx context.Context, jobID, contentType string, data []byte) error
}

// recordingCollection captures everything needed to fetch one job's recording
// after its session has ended but before its pod is deleted.
type recordingCollection struct {
	read      podFileReader
	api       recordingPoster
	jobID     string
	namespace string
	podName   string
	maxBytes  int64
}

// podFileReaderFrom adapts the k8s package's ReadPodFile to the function type
// this module takes, so the two callers that need it (production, and a test
// that wants the real thing against a fake pod) read the same way.
func podFileReaderFrom(client *k8s.Client) podFileReader {
	return func(ctx context.Context, namespace, podName, container, filePath string) ([]byte, error) {
		return k8s.ReadPodFile(
			ctx, client.Interface, client.Config,
			namespace, podName, container, filePath, recordingReadLimit,
		)
	}
}

// recordingReadLimit bounds the raw read itself, independently of the policy cap
// in Config. It is deliberately a little above that cap: the policy check is
// what produces the "skipped, too large" log line, and a read bound set exactly
// at the cap would turn an artifact one byte over it into an opaque stream
// error instead. This only has to be low enough that a mis-reported path (a
// device file, a runaway writer) cannot exhaust the Orchestrator's memory.
const recordingReadLimit = 64 << 20 // 64 MiB

// DefaultRecordingMaxBytes is ADR 029's default cap on a collected recording.
//
// Exported because the two callers that care live in different packages: this
// one applies it when a deployment leaves the value unset, and
// cmd/server/main.go's env resolver returns it for an unset or unparseable
// value. One spelling, so the default cannot disagree with itself.
//
// 25 MB comfortably covers a several-minute Playwright session of a UI test at a
// sane viewport, and is the same number the API's RECORDING_MAX_BYTES defaults
// to — the two sides must agree or the Orchestrator would either ship megabytes
// only to be refused, or refuse artifacts the API would have kept.
const DefaultRecordingMaxBytes int64 = 25_000_000

// recordingContentType picks the MIME type to store from the reported file's
// extension.
//
// Playwright's video writer only ever produces WebM, so that is the default and
// the answer for every real run. The extension is still consulted rather than
// assumed, because a project is free to post-process its recording, and storing
// an MP4 as `video/webm` would make the browser's player fail on a file that is
// perfectly playable. An unrecognised extension falls back to WebM rather than
// refusing: a recording that plays is more useful than one rejected over its
// filename, and the API's own content-type check is the backstop.
func recordingContentType(filePath string) string {
	switch strings.ToLower(path.Ext(filePath)) {
	case ".mp4", ".m4v":
		return "video/mp4"
	default:
		return "video/webm"
	}
}

// collectRecording reads a finished job's recording out of its pod and uploads
// it to the API (ADR 029).
//
// Best-effort by contract, and that contract is the point: a recording is a
// diagnostic extra attached to a run that has already decided its own outcome,
// so **every** failure here — no file, an oversized artifact, an API that is
// down — is logged and dropped. It must never turn a passing test run into a
// failed job, and it must never prevent the report from being stored (the report
// went over the curated-event channel before this ran, so the ordering is
// already correct rather than merely intended).
//
// Called with a context that outlives the session deliberately: a run that just
// finished successfully should still get its artifact collected even if the
// worker is winding down, the same reasoning reportUsage uses for its post.
func collectRecording(ctx context.Context, c recordingCollection, filePath string) {
	if c.read == nil || c.api == nil {
		return
	}
	// A non-positive cap means collection is switched off (see
	// Config.RecordingMaxBytes), so this returns before touching the pod at all.
	//
	// Stated as an explicit early return rather than folded into the size
	// comparison below: because the comparison is `>` , a cap of 0 would refuse
	// every artifact anyway, but it would do so *after* reading the file out of
	// the pod and logging a misleading "too large" line. Disabled should mean
	// "do nothing", not "do the work and then discard it".
	if c.maxBytes <= 0 {
		return
	}

	data, err := c.read(ctx, c.namespace, c.podName, recordingContainer, filePath)
	if err != nil {
		log.Printf("worker: could not read recording %s for job %s: %v", filePath, c.jobID, err)
		return
	}
	// Checked here as well as in the reader because the reader's own bound is a
	// safety net against a lying size, while this is the policy cap the API will
	// enforce anyway — refusing locally avoids shipping megabytes only to have
	// them declined. The bound is inclusive, matching the API's own
	// `exceedsSizeCap`, so the two sides cannot disagree by one byte.
	if int64(len(data)) > c.maxBytes {
		log.Printf(
			"worker: skipping recording for job %s: %d bytes exceeds the %d byte limit",
			c.jobID, len(data), c.maxBytes,
		)
		return
	}

	contentType := recordingContentType(filePath)
	if err := c.api.PostJobRecording(ctx, c.jobID, contentType, data); err != nil {
		log.Printf("worker: failed to upload recording for job %s: %v", c.jobID, err)
		return
	}
	log.Printf("worker: uploaded a %d byte recording for job %s", len(data), c.jobID)
}
