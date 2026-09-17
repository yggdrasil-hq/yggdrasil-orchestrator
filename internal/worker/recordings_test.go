package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRecorder struct {
	calls     int
	jobID     string
	mediaType string
	data      []byte
	err       error
}

func (f *fakeRecorder) PostJobRecording(_ context.Context, jobID, contentType string, data []byte) error {
	f.calls++
	f.jobID = jobID
	f.mediaType = contentType
	f.data = data
	return f.err
}

func TestRecordingContentType(t *testing.T) {
	cases := map[string]string{
		"/workspace/.yggdrasil/recordings/run.webm":     "video/webm",
		"/workspace/.yggdrasil/recordings/run.WEBM":     "video/webm",
		"/workspace/.yggdrasil/recordings/run.mp4":      "video/mp4",
		"/workspace/.yggdrasil/recordings/run.m4v":      "video/mp4",
		"/workspace/.yggdrasil/recordings/no-extension": "video/webm",
		"": "video/webm",
	}
	for path, want := range cases {
		if got := recordingContentType(path); got != want {
			t.Errorf("recordingContentType(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestCollectRecordingUploadsTheBytes(t *testing.T) {
	poster := &fakeRecorder{}
	c := recordingCollection{
		read: func(_ context.Context, namespace, podName, container, path string) ([]byte, error) {
			if namespace != "proj-1" || podName != "job-pod" || container != "run" {
				t.Fatalf("reader got unexpected target: %s/%s/%s", namespace, podName, container)
			}
			if !strings.HasSuffix(path, ".webm") {
				t.Fatalf("reader got unexpected path %q", path)
			}
			return []byte("recording-bytes"), nil
		},
		api:       poster,
		jobID:     "job-1",
		namespace: "proj-1",
		podName:   "job-pod",
		maxBytes:  1000,
	}

	collectRecording(context.Background(), c, "/workspace/.yggdrasil/run.webm")

	if poster.calls != 1 {
		t.Fatalf("expected one upload, got %d", poster.calls)
	}
	if poster.jobID != "job-1" {
		t.Errorf("uploaded for job %q, want job-1", poster.jobID)
	}
	if poster.mediaType != "video/webm" {
		t.Errorf("uploaded as %q, want video/webm", poster.mediaType)
	}
	if string(poster.data) != "recording-bytes" {
		t.Errorf("uploaded %q", poster.data)
	}
}

func TestCollectRecordingIsBestEffort(t *testing.T) {
	// Every failure mode must be swallowed: a recording is a diagnostic extra on
	// a run that already decided its own outcome, so none of these may surface
	// as a job error.
	cases := []struct {
		name   string
		read   podFileReader
		poster *fakeRecorder
	}{
		{
			name: "the read fails (file missing, pod gone, exec refused)",
			read: func(context.Context, string, string, string, string) ([]byte, error) {
				return nil, errors.New("cat: no such file")
			},
			poster: &fakeRecorder{},
		},
		{
			name: "the upload fails",
			read: func(context.Context, string, string, string, string) ([]byte, error) {
				return []byte("x"), nil
			},
			poster: &fakeRecorder{err: errors.New("API returned status 502")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collectRecording(context.Background(), recordingCollection{
				read: tc.read, api: tc.poster, jobID: "job-1", maxBytes: 1000,
			}, "/workspace/run.webm")
			// Reaching here at all is the assertion: collectRecording returns
			// nothing and must not panic.
		})
	}
}

func TestCollectRecordingSkipsAnOversizedArtifactWithoutUploading(t *testing.T) {
	poster := &fakeRecorder{}
	collectRecording(context.Background(), recordingCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return make([]byte, 2001), nil
		},
		api: poster, jobID: "job-1", maxBytes: 2000,
	}, "/workspace/run.webm")

	if poster.calls != 0 {
		t.Fatalf("expected no upload for an oversized artifact, got %d", poster.calls)
	}
}

func TestCollectRecordingAcceptsAnArtifactExactlyAtTheCap(t *testing.T) {
	// The cap reads as "the largest recording you may store", matching the API's
	// own inclusive bound — the two must not disagree by one byte.
	poster := &fakeRecorder{}
	collectRecording(context.Background(), recordingCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return make([]byte, 2000), nil
		},
		api: poster, jobID: "job-1", maxBytes: 2000,
	}, "/workspace/run.webm")

	if poster.calls != 1 {
		t.Fatalf("expected the artifact at the cap to be uploaded, got %d uploads", poster.calls)
	}
}

func TestCollectRecordingIsDisabledByANonPositiveCap(t *testing.T) {
	// 0 means "off" (see Config.RecordingMaxBytes), so the caller must not even
	// build the collection — but if it does, no upload happens for a non-empty
	// artifact.
	poster := &fakeRecorder{}
	collectRecording(context.Background(), recordingCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return []byte("x"), nil
		},
		api: poster, jobID: "job-1", maxBytes: 0,
	}, "/workspace/run.webm")

	// maxBytes 0 with a non-positive-cap guard inside collectRecording: the
	// artifact exceeds the cap, so nothing is uploaded.
	if poster.calls != 0 {
		t.Fatalf("expected no upload when collection is disabled, got %d", poster.calls)
	}
}

func TestCollectRecordingDoesNothingWithoutItsDependencies(t *testing.T) {
	// Guards the nil-callback case: a worker configured without recordings (or a
	// test constructing a partial collection) must not nil-panic on the path
	// every completed test_run takes.
	collectRecording(context.Background(), recordingCollection{jobID: "job-1", maxBytes: 1000}, "/x.webm")
	collectRecording(context.Background(), recordingCollection{
		read: func(context.Context, string, string, string, string) ([]byte, error) {
			return []byte("x"), nil
		},
		jobID: "job-1", maxBytes: 1000,
	}, "/x.webm")
}
