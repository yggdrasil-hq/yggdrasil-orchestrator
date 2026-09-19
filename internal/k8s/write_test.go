package k8s

import (
	"context"
	"testing"
)

// WritePodFile's guards are the only part testable without a cluster, and they
// are the part that decides what the caller is told — which is ADR 032 item 3's
// "the same sentinel discipline" as the read.
//
// A nil clientset is passed deliberately: every case below must be decided
// *before* anything is sent to a cluster, so a test that accidentally reached the
// API server would panic here rather than pass quietly.

func TestWritePodFileRefusesAnOversizedPayloadAsTooLarge(t *testing.T) {
	// The distinction this asserts is the whole reason the sentinel is shared with
	// the read: an oversized artifact is *expected* and handled by the caller
	// (report the refusal, finish the run), whereas an unreachable pod is not. A
	// bare error would make the two indistinguishable.
	err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run",
		"/tmp/session.jsonl", []byte("0123456789"), 5,
	)
	if err == nil {
		t.Fatal("expected an oversized write to be refused")
	}
	if !ErrPodFileTooLarge(err) {
		t.Fatalf("expected ErrPodFileTooLarge to report true, got %v", err)
	}
}

func TestWritePodFileAcceptsAPayloadExactlyAtTheBound(t *testing.T) {
	// The bound is inclusive, matching exceedsSizeCap's `>` rather than `>=`: a
	// session of exactly the cap is storable, and an off-by-one here would refuse
	// the largest session the API itself accepted.
	//
	// Reaching the exec means the guard passed, so the error (if any) must not be
	// the size sentinel — a nil clientset panics or errors inside the exec, which
	// is fine; what matters is that it got past validation.
	defer func() {
		if r := recover(); r != nil {
			// A nil clientset can panic inside client-go; that is not this test's
			// subject. Recovering keeps the assertion about validation only.
			return
		}
	}()
	err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run",
		"/tmp/session.jsonl", []byte("0123456789"), 10,
	)
	if ErrPodFileTooLarge(err) {
		t.Fatalf("a payload at the bound must not be reported as too large: %v", err)
	}
}

func TestWritePodFileRefusesAnEmptyPayload(t *testing.T) {
	// Not an error path the caller hits today — the API refuses a `collected`
	// session with no bytes — but writing an empty file would make the path exist,
	// and Pi's switch-verify would then be reasoning about a file this function
	// invented rather than about a stored artifact.
	err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run",
		"/tmp/session.jsonl", nil, 100,
	)
	if err == nil {
		t.Fatal("expected an empty write to be refused")
	}
	if ErrPodFileTooLarge(err) {
		t.Fatal("an empty payload is not a size problem and must not read as one")
	}
}

func TestWritePodFileRequiresAPathAndAPositiveBound(t *testing.T) {
	// A zero bound would otherwise mean "write anything", which is the one reading
	// this convention must not have: throughout this codebase zero means "no
	// ceiling" only where the value is documented that way, and here it is a
	// caller's bug.
	if err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run", "", []byte("x"), 10,
	); err == nil {
		t.Fatal("expected a write with no path to be refused")
	}
	if err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run", "/tmp/x", []byte("x"), 0,
	); err == nil {
		t.Fatal("expected a write with a zero bound to be refused")
	}
}

func TestErrPodFileTooLargeFindsTheSentinelThroughWrapping(t *testing.T) {
	// The write wraps the sentinel exactly as the read does, so the exported
	// predicate has to see through that wrapping — otherwise the caller's
	// `ErrPodFileTooLarge(err)` branch is dead and every oversize is reported as a
	// broken pod.
	err := WritePodFile(
		context.Background(), nil, nil, "ns", "pod", "run",
		"/tmp/session.jsonl", []byte("0123456789"), 1,
	)
	if err == nil || !ErrPodFileTooLarge(err) {
		t.Fatalf("expected a wrapped sentinel, got %v", err)
	}
}
