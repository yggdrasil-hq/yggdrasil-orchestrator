package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
)

/*
Issue #22's collection half. The API side (storage, retention, endpoints) landed
first, so what these tests cover is the transport: read each reported screenshot
out of the pod before it is deleted, and upload it — without any failure in that
path being able to change a run's outcome.
*/

type fakeScreenshotPoster struct {
	calls     int
	jobID     string
	stepNames []string
	mediaType string
	data      []byte
	err       error
}

func (f *fakeScreenshotPoster) PostJobScreenshot(
	_ context.Context,
	jobID, stepName, contentType string,
	data []byte,
) error {
	f.calls++
	f.jobID = jobID
	f.stepNames = append(f.stepNames, stepName)
	f.mediaType = contentType
	f.data = data
	return f.err
}

// readablePod answers every read with the bytes for the path it was asked for, so
// a multi-artifact test can tell one upload from another.
func readablePod() podFileReader {
	return func(_ context.Context, namespace, podName, container, filePath string) ([]byte, error) {
		if namespace != "proj-1" || podName != "job-pod" || container != "run" {
			return nil, errors.New("reader got an unexpected target: " + namespace + "/" + podName + "/" + container)
		}
		return []byte("bytes:" + filePath), nil
	}
}

func TestScreenshotContentType(t *testing.T) {
	cases := map[string]string{
		"/workspace/.yggdrasil/screenshots/step-1.png":  "image/png",
		"/workspace/.yggdrasil/screenshots/step-1.PNG":  "image/png",
		"/workspace/.yggdrasil/screenshots/step-1.jpg":  "image/jpeg",
		"/workspace/.yggdrasil/screenshots/step-1.jpeg": "image/jpeg",
		"/workspace/.yggdrasil/screenshots/step-1.JPEG": "image/jpeg",
		"/workspace/.yggdrasil/screenshots/step-1.webp": "image/webp",
		// Not accepted by the API, so there is no valid Content-Type to send.
		// Asserted explicitly because skipping these is a decision (see the
		// module header), not an oversight.
		"/workspace/shot.gif":   "",
		"/workspace/shot.bmp":   "",
		"/workspace/shot.svg":   "",
		"/workspace/shot.tiff":  "",
		"/workspace/shot":       "",
		"":                      "",
		"/workspace/shot.txt":   "",
		"/workspace/shot.png.x": "",
	}
	for path, want := range cases {
		if got := screenshotContentType(path); got != want {
			t.Errorf("screenshotContentType(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestCollectScreenshotsUploadsEachStepWithItsOwnName(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	collectScreenshots(context.Background(), screenshotCollection{
		read:      readablePod(),
		api:       poster,
		jobID:     "job-1",
		namespace: "proj-1",
		podName:   "job-pod",
		maxBytes:  1000,
	}, []screenshotArtifact{
		{stepName: "opens checkout", path: "/workspace/.yggdrasil/shot-1.png"},
		{stepName: "pays", path: "/workspace/.yggdrasil/shot-2.jpg"},
	})

	if poster.calls != 2 {
		t.Fatalf("expected two uploads, got %d", poster.calls)
	}
	// The step name is the API's identity for a screenshot, so it has to arrive
	// intact and per-artifact rather than once for the batch.
	if poster.stepNames[0] != "opens checkout" || poster.stepNames[1] != "pays" {
		t.Fatalf("uploaded step names %v, want the reported ones in order", poster.stepNames)
	}
	if poster.jobID != "job-1" {
		t.Errorf("uploaded for job %q, want job-1", poster.jobID)
	}
	if poster.mediaType != "image/jpeg" {
		t.Errorf("last upload used %q, want image/jpeg for the .jpg path", poster.mediaType)
	}
	if string(poster.data) != "bytes:/workspace/.yggdrasil/shot-2.jpg" {
		t.Errorf("uploaded %q", poster.data)
	}
}

// The bytes must be the file's, not the previous artifact's — an off-by-one in
// the loop would pair each step with the wrong image, which would be worse than
// uploading nothing.
func TestCollectScreenshotsPairsEachStepWithItsOwnBytes(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	collectScreenshots(context.Background(), screenshotCollection{
		read: readablePod(), api: poster, jobID: "job-1",
		namespace: "proj-1", podName: "job-pod", maxBytes: 1000,
	}, []screenshotArtifact{
		{stepName: "first", path: "/a.png"},
		{stepName: "second", path: "/b.png"},
	})

	// The fake returns "bytes:<path>", so the pairing is checkable by reading the
	// last upload back against the last step.
	if string(poster.data) != "bytes:/b.png" {
		t.Fatalf("last upload carried %q, which does not pair with the second step", poster.data)
	}
	if poster.stepNames[len(poster.stepNames)-1] != "second" {
		t.Fatalf("last upload named step %q, want second", poster.stepNames[len(poster.stepNames)-1])
	}
}

// A format the API does not accept has no valid Content-Type to send, so the
// artifact is skipped — and crucially the pod is not touched, since there is
// nothing to do with the bytes.
func TestCollectScreenshotsSkipsAnUnsupportedFormatWithoutReading(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	readCalls := 0
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			readCalls++
			return []byte("gif-bytes"), nil
		},
		api: poster, jobID: "job-1", maxBytes: 1000,
	}, []screenshotArtifact{{stepName: "animates", path: "/shot.gif"}})

	if readCalls != 0 {
		t.Fatalf("expected no read for an unsupported format, got %d", readCalls)
	}
	if poster.calls != 0 {
		t.Fatalf("expected no upload for an unsupported format, got %d", poster.calls)
	}
}

// The property that matters most here: a run with many steps must not lose all of
// them to one bad artifact.
func TestCollectScreenshotsKeepsGoingAfterAFailure(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(_ context.Context, _, _, _, filePath string) ([]byte, error) {
			if filePath == "/missing.png" {
				return nil, errors.New("cat: no such file")
			}
			return []byte("ok:" + filePath), nil
		},
		api: poster, jobID: "job-1", maxBytes: 1000,
	}, []screenshotArtifact{
		{stepName: "first", path: "/first.png"},
		{stepName: "gone", path: "/missing.png"},
		{stepName: "third", path: "/third.png"},
	})

	if poster.calls != 2 {
		t.Fatalf("expected the two readable steps to upload, got %d uploads", poster.calls)
	}
	if poster.stepNames[0] != "first" || poster.stepNames[1] != "third" {
		t.Fatalf("uploaded %v, want the two steps around the failure", poster.stepNames)
	}
}

// An oversized artifact costs its own upload and nothing else.
func TestCollectScreenshotsSkipsAnOversizedArtifactWithoutUploading(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(_ context.Context, _, _, _, filePath string) ([]byte, error) {
			if filePath == "/big.png" {
				return make([]byte, 2001), nil
			}
			return []byte("small"), nil
		},
		api: poster, jobID: "job-1", maxBytes: 2000,
	}, []screenshotArtifact{
		{stepName: "big", path: "/big.png"},
		{stepName: "small", path: "/small.png"},
	})

	if poster.calls != 1 {
		t.Fatalf("expected only the in-cap artifact to upload, got %d", poster.calls)
	}
	if poster.stepNames[0] != "small" {
		t.Fatalf("uploaded %v, want only the small step", poster.stepNames)
	}
}

func TestCollectScreenshotsAcceptsAnArtifactExactlyAtTheCap(t *testing.T) {
	// The cap reads as "the largest screenshot you may store", matching the API's
	// own inclusive bound — the two must not disagree by one byte.
	poster := &fakeScreenshotPoster{}
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return make([]byte, 2000), nil
		},
		api: poster, jobID: "job-1", maxBytes: 2000,
	}, []screenshotArtifact{{stepName: "at-cap", path: "/x.png"}})

	if poster.calls != 1 {
		t.Fatalf("expected the artifact at the cap to be uploaded, got %d uploads", poster.calls)
	}
}

func TestCollectScreenshotsIsBestEffort(t *testing.T) {
	// Every failure mode must be swallowed: a screenshot is a diagnostic extra on
	// a run that already decided its own outcome, so none of these may surface as
	// a job error.
	cases := []struct {
		name   string
		read   podFileReader
		poster *fakeScreenshotPoster
	}{
		{
			name: "the read fails (file missing, pod gone, exec refused)",
			read: func(context.Context, string, string, string, string) ([]byte, error) {
				return nil, errors.New("cat: no such file")
			},
			poster: &fakeScreenshotPoster{},
		},
		{
			name: "the upload fails",
			read: func(context.Context, string, string, string, string) ([]byte, error) {
				return []byte("x"), nil
			},
			poster: &fakeScreenshotPoster{err: errors.New("API returned status 502")},
		},
		{
			name: "the API declines the artifact",
			read: func(context.Context, string, string, string, string) ([]byte, error) {
				return []byte("x"), nil
			},
			poster: &fakeScreenshotPoster{
				err: errors.New("API declined the screenshot for step \"x\" on job job-1: Screenshot exceeds the 2000000 byte limit"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collectScreenshots(context.Background(), screenshotCollection{
				read: tc.read, api: tc.poster, jobID: "job-1", maxBytes: 1000,
			}, []screenshotArtifact{{stepName: "step", path: "/x.png"}})
			// Reaching here at all is the assertion: collectScreenshots returns
			// nothing and must not panic.
		})
	}
}

func TestCollectScreenshotsIsDisabledByANonPositiveCap(t *testing.T) {
	poster := &fakeScreenshotPoster{}
	readCalls := 0
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			readCalls++
			return []byte("x"), nil
		},
		api: poster, jobID: "job-1", maxBytes: 0,
	}, []screenshotArtifact{{stepName: "step", path: "/x.png"}})

	// "Off" must mean do nothing, not read everything and discard it — which is
	// why the guard is an explicit early return rather than the size comparison.
	if readCalls != 0 {
		t.Fatalf("expected no read when collection is disabled, got %d", readCalls)
	}
	if poster.calls != 0 {
		t.Fatalf("expected no upload when collection is disabled, got %d", poster.calls)
	}
}

func TestCollectScreenshotsDoesNothingWithoutItsDependencies(t *testing.T) {
	// Guards the nil-callback case: a worker configured without an API client (or
	// a test constructing a partial collection) must not nil-panic on the path
	// every completed test_run takes.
	artifacts := []screenshotArtifact{{stepName: "step", path: "/x.png"}}
	collectScreenshots(context.Background(), screenshotCollection{jobID: "job-1", maxBytes: 1000}, artifacts)
	collectScreenshots(context.Background(), screenshotCollection{
		read: readablePod(), jobID: "job-1", maxBytes: 1000,
	}, artifacts)
}

func TestCollectScreenshotsWithNoArtifactsDoesNothing(t *testing.T) {
	// A run that reported no screenshots must not touch the pod at all — this is
	// the common case for a feature_build, which reports no steps.
	readCalls := 0
	collectScreenshots(context.Background(), screenshotCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			readCalls++
			return nil, nil
		},
		api: &fakeScreenshotPoster{}, jobID: "job-1", maxBytes: 1000,
	}, nil)

	if readCalls != 0 {
		t.Fatalf("expected no read with nothing pending, got %d", readCalls)
	}
}

func TestScreenshotCollectorDeduplicatesOnTheStepName(t *testing.T) {
	// The API's identity for a screenshot is (job, stepName) and it upserts, so a
	// step reported twice can only overwrite itself — and counting it twice would
	// let a re-reported step crowd a distinct one out. The newest path wins
	// because it is the one that reflects the step's final state.
	c := newScreenshotCollector()
	c.add("first", "/first-v1.png")
	c.add("second", "/second.png")
	c.add("first", "/first-v2.png")

	got := c.pending()
	if len(got) != 2 {
		t.Fatalf("expected two artifacts after dedupe, got %d: %v", len(got), got)
	}
	if got[0].stepName != "first" || got[0].path != "/first-v2.png" {
		t.Errorf("expected the newest path for the re-reported step, got %+v", got[0])
	}
	if got[1].stepName != "second" {
		t.Errorf("expected the second step to keep its position, got %+v", got[1])
	}
}

func TestScreenshotCollectorKeepsReportOrder(t *testing.T) {
	c := newScreenshotCollector()
	for _, name := range []string{"a", "b", "c", "d"} {
		c.add(name, "/"+name+".png")
	}

	got := c.pending()
	if len(got) != 4 {
		t.Fatalf("expected four artifacts, got %d", len(got))
	}
	for i, want := range []string{"a", "b", "c", "d"} {
		if got[i].stepName != want {
			t.Errorf("artifact %d is %q, want %q", i, got[i].stepName, want)
		}
	}
}

func TestScreenshotCollectorIgnoresAnIncompleteReport(t *testing.T) {
	// `report_test_step` requires a test name, and a step with no path reported
	// nothing to collect — so either being absent must be a no-op rather than an
	// artifact that would be read from an empty path.
	c := newScreenshotCollector()
	c.add("", "/x.png")
	c.add("step", "")
	c.add("", "")
	c.add("real", "/real.png")

	got := c.pending()
	if len(got) != 1 || got[0].stepName != "real" {
		t.Fatalf("expected only the complete report, got %+v", got)
	}
}

func TestScreenshotCollectorPendingCopiesRatherThanAliasing(t *testing.T) {
	// The batch runs after the session, but the collector is still reachable; a
	// caller that mutated what it was handed must not be able to corrupt the
	// collector's own view, and vice versa.
	c := newScreenshotCollector()
	c.add("first", "/first.png")

	got := c.pending()
	got[0].path = "/mutated.png"
	c.add("second", "/second.png")

	again := c.pending()
	if again[0].path != "/first.png" {
		t.Fatalf("a caller's mutation reached the collector: %+v", again[0])
	}
}

func TestScreenshotCollectorIsNilSafe(t *testing.T) {
	// The collector is constructed on every RPC job's path, but a partial
	// construction (or a future caller that only sometimes needs one) must not
	// panic inside an event sink, where a panic would take down the run.
	var c *screenshotCollector
	c.add("step", "/x.png")
	if got := c.pending(); got != nil {
		t.Fatalf("expected nil from a nil collector, got %+v", got)
	}
}

func TestScreenshotCollectTimeoutIsPerReadNotShared(t *testing.T) {
	// A guard on the design, not on a value: if the per-read bound were ever
	// raised to the batch bound, one wedged exec would consume the whole budget
	// and silently discard every screenshot after it.
	if screenshotReadTimeout >= screenshotCollectTimeout {
		t.Fatalf(
			"per-read timeout (%s) must stay well under the batch timeout (%s), or one slow read abandons the batch",
			screenshotReadTimeout, screenshotCollectTimeout,
		)
	}
}

func TestScreenshotDefaultCapMatchesTheAPIsDefault(t *testing.T) {
	// The two sides must agree or the Orchestrator would either ship bytes only to
	// have them declined, or refuse artifacts the API would have kept. This pins
	// the number so a change on one side is a visible test failure rather than a
	// silent divergence.
	if DefaultScreenshotMaxBytes != 2_000_000 {
		t.Fatalf("expected the 2 MB default the API's SCREENSHOT_MAX_BYTES uses, got %d", DefaultScreenshotMaxBytes)
	}
}

func TestScreenshotReadLimitExceedsThePolicyCap(t *testing.T) {
	// The read bound is a safety net, not the policy: if it were at or below the
	// policy cap, an artifact one byte over would become an opaque stream error
	// instead of the "skipped, too large" line the policy check produces.
	if screenshotReadLimit <= DefaultScreenshotMaxBytes {
		t.Fatalf("read limit %d must exceed the policy cap %d", screenshotReadLimit, DefaultScreenshotMaxBytes)
	}
}

func TestScreenshotContainerMatchesThePodTheAgentWroteIn(t *testing.T) {
	// The screenshots are written by the skill inside the container the Pi session
	// runs in; reading from any other name would fail every collection.
	if screenshotContainer != "run" {
		t.Fatalf("expected the run container, got %q", screenshotContainer)
	}
	if screenshotContainer != recordingContainer {
		t.Fatalf("expected the same container recordings read from, got %q", screenshotContainer)
	}
	if !strings.Contains(screenshotContainer, "run") {
		t.Fatalf("unexpected container name %q", screenshotContainer)
	}
}
