package k8s

import (
	"context"
	"errors"
	"testing"
)

// The bound on ReadPodFile's stdout is the one thing about it that can be
// tested without a cluster, and it is also the part that most needs testing: it
// is what stops a path the agent reported (a device file, a runaway writer, an
// artifact far larger than the cap) from consuming the Orchestrator's memory.

func TestLimitedBufferAcceptsExactlyUpToItsLimit(t *testing.T) {
	buf := &limitedBuffer{limit: 10}
	n, err := buf.Write([]byte("0123456789"))
	if err != nil {
		t.Fatalf("expected a write at the limit to succeed, got %v", err)
	}
	if n != 10 || buf.buf.Len() != 10 {
		t.Fatalf("expected 10 bytes stored, got n=%d len=%d", n, buf.buf.Len())
	}
}

func TestLimitedBufferRejectsTheByteThatOverruns(t *testing.T) {
	buf := &limitedBuffer{limit: 10}
	if _, err := buf.Write([]byte("0123456789")); err != nil {
		t.Fatalf("first write should fit: %v", err)
	}
	if _, err := buf.Write([]byte("x")); err == nil {
		t.Fatal("expected the overrunning write to fail")
	}
}

func TestLimitedBufferDoesNotStoreAPartialOversizeChunk(t *testing.T) {
	// A chunk that would overrun must be refused entirely rather than partially
	// buffered: the caller treats a failure as "too large to collect", and a
	// half-stored artifact that looked successful would be worse than none.
	buf := &limitedBuffer{limit: 5}
	if _, err := buf.Write([]byte("0123456789")); err == nil {
		t.Fatal("expected an oversized first chunk to fail")
	}
	if buf.buf.Len() != 0 {
		t.Fatalf("expected nothing buffered, got %d bytes", buf.buf.Len())
	}
}

func TestLimitedBufferWorksAcrossManySmallChunks(t *testing.T) {
	// The real stream arrives in arbitrarily-sized chunks (SPDY frames), not one
	// write, so the running total is what has to be enforced.
	buf := &limitedBuffer{limit: 6}
	for i := 0; i < 6; i++ {
		if _, err := buf.Write([]byte("a")); err != nil {
			t.Fatalf("chunk %d should fit: %v", i, err)
		}
	}
	if _, err := buf.Write([]byte("a")); err == nil {
		t.Fatal("expected the 7th byte to be refused")
	}
	if buf.buf.Len() != 6 {
		t.Fatalf("expected 6 bytes retained, got %d", buf.buf.Len())
	}
}

func TestErrPodFileTooLargeRecognisesWrappedAndUnwrappedForms(t *testing.T) {
	if !ErrPodFileTooLarge(errTooLarge) {
		t.Error("expected the bare sentinel to be recognised")
	}
	if !ErrPodFileTooLarge(ReadPodFileError{errTooLarge}) {
		t.Error("expected a wrapped sentinel to be recognised")
	}
	if ErrPodFileTooLarge(nil) {
		t.Error("nil is not an over-limit error")
	}
	if ErrPodFileTooLarge(errors.New("some other failure")) {
		t.Error("an unrelated error must not be mistaken for the sentinel")
	}
}

// ReadPodFileError is a minimal wrapper used only to exercise the unwrap walk.
type ReadPodFileError struct{ inner error }

func (e ReadPodFileError) Error() string { return "wrapped: " + e.inner.Error() }
func (e ReadPodFileError) Unwrap() error { return e.inner }

func TestReadPodFileRejectsAnEmptyPathWithoutACluster(t *testing.T) {
	// Guards the argument checks that run before any cluster call, so a
	// misconfigured caller fails fast instead of opening a stream.
	if _, err := ReadPodFile(context.Background(), nil, nil, "ns", "pod", "run", "", 10); err == nil {
		t.Fatal("expected an empty path to be refused")
	}
	if _, err := ReadPodFile(context.Background(), nil, nil, "ns", "pod", "run", "/x", 0); err == nil {
		t.Fatal("expected a non-positive bound to be refused")
	}
}
