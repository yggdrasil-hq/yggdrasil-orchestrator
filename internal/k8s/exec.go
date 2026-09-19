package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// errTooLarge is returned by ReadPodFile when a file overruns its bound. It
// exists as a sentinel so a caller can tell "this artifact is too big to
// collect" (expected, and handled by skipping the artifact) apart from "the
// pod could not be read" (unexpected, and worth logging differently).
var errTooLarge = fmt.Errorf("file exceeds the read limit")

// ErrPodFileTooLarge reports whether err is the over-limit sentinel.
func ErrPodFileTooLarge(err error) bool {
	for err != nil {
		if err == errTooLarge {
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

// limitedBuffer accumulates at most limit bytes, then fails every subsequent
// write.
//
// This is what makes reading an artifact out of a pod safe: the size of the
// file is a claim made by the agent inside the pod, not a fact the
// Orchestrator can verify before it starts reading. Without a bound, a
// mis-reported path (or a genuinely enormous recording) would stream until the
// Orchestrator's own memory ran out — a remote process deciding how much of
// the control plane's heap to consume. Failing the write instead aborts the
// exec stream, so the read stops at the bound and the caller gets a clean
// "too large" rather than an OOM.
type limitedBuffer struct {
	buf   bytes.Buffer
	limit int64
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	if int64(w.buf.Len())+int64(len(p)) > w.limit {
		return 0, errTooLarge
	}
	return w.buf.Write(p)
}

// ReadPodFile reads one file out of a running pod's container and returns its
// bytes, failing if the file exceeds maxBytes.
//
// This is the transport ADR 029 needs and that the suite previously lacked: a
// job pod is ephemeral and deleted the moment its run ends (ADR 006 item 11), so
// an artifact the agent wrote to `/workspace` is unreachable afterwards — which
// is why `recordingPath`, `screenshotPath` and `.yggdrasil/test-report.json`'s
// siblings have all been dead pointers until now.
//
// Deliberately NOT `kubectl cp`. That requires `tar` inside the container and a
// round trip through the CLI; this uses the same client-go `remotecommand`
// machinery `Attach` already uses, so it adds no dependency and no subprocess —
// and, unlike a tar stream, a single `cat` cannot be tricked into walking
// outside the path it was given.
//
// The command is an argv slice, never a shell string, so a path containing shell
// metacharacters is passed to `cat` verbatim rather than interpreted. The path
// comes from the agent's own tool call; it runs inside a container the job
// already fully controls, but there is no reason to hand it a shell inside the
// Orchestrator's exec request as well.
//
// Bounded by maxBytes (see limitedBuffer). A missing file, a directory, or a
// container without `cat` all surface as an error from the exec itself.
func ReadPodFile(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	namespace, podName, containerName, path string,
	maxBytes int64,
) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("no path given")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be positive (got %d)", maxBytes)
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   []string{"cat", "--", path},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to build exec executor for pod %s/%s: %w", namespace, podName, err)
	}

	// stdout is read through the bound; stderr is captured so a failure carries
	// the container's own message ("cat: ...: No such file or directory") rather
	// than a bare stream error.
	var stdout limitedBuffer
	stdout.limit = maxBytes
	var stderr bytes.Buffer

	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		if ErrPodFileTooLarge(err) {
			return nil, fmt.Errorf("read %s: %w (limit %d bytes)", path, errTooLarge, maxBytes)
		}
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("failed to read %s from pod %s/%s: %w: %s",
				path, namespace, podName, err, bytes.TrimSpace(stderr.Bytes()))
		}
		return nil, fmt.Errorf("failed to read %s from pod %s/%s: %w", path, namespace, podName, err)
	}

	// The exec can also report success while the writer refused the last chunk,
	// depending on how the stream unwinds — so the bound is re-checked on the
	// collected bytes rather than trusted to have surfaced as an error.
	if int64(stdout.buf.Len()) > maxBytes {
		return nil, fmt.Errorf("read %s: %w (limit %d bytes)", path, errTooLarge, maxBytes)
	}
	if stdout.buf.Len() == 0 {
		return nil, fmt.Errorf("read %s: file is empty", path)
	}
	return stdout.buf.Bytes(), nil
}

// WritePodFile writes data to one path inside a running pod's container.
//
// **This is the mirror of ReadPodFile, and the same three rules apply.** Its
// comment explains why the read is an argv slice rather than a shell string, and
// those reasons are not read-specific:
//
//   - **`tee`, never `sh -c 'cat > …'`.** The path comes from a stored artifact's
//     own report rather than from an agent's tool call, but a path is a path: an
//     argv slice hands it to `tee` verbatim, whereas a shell string would let a
//     path containing a quote or a `;` become a *command*. The agent images do
//     contain a shell (`node:22-slim` is Debian-based), which is exactly why the
//     shell must not be used — the capability being present is not a reason to
//     route through it.
//   - **A bound, checked before the exec.** ReadPodFile bounds what the *pod*
//     claims a file is, because that size is a claim the Orchestrator cannot
//     verify. Here the bytes are the Orchestrator's own, so the bound is a
//     caller-supplied policy cap (ADR 032 item 3 draws it from the API's
//     `SESSION_MAX_BYTES`) — but it is still enforced, because the alternative is
//     an unbounded write into a container.
//   - **The same sentinel split.** A payload over the bound is *expected* and
//     handled by the caller (skip the fork, report the reason); a pod that cannot
//     be reached is not. Reusing `errTooLarge`/`ErrPodFileTooLarge` is what keeps
//     that distinction identical on both sides of the mirror rather than
//     re-invented per direction.
//
// **Why `tee` and not a redirect.** A redirect needs a shell; `tee -- <path>`
// takes the path as its own argument and copies stdin to it, so the same
// single-command posture as `cat` is available in the write direction. `tee`
// writes the file with the container's own permissions, which is what the pod's
// process needs to read it back.
//
// A missing `tee` (a container built `FROM scratch`, which no agent image is)
// surfaces as an error from the exec itself, exactly as a missing `cat` does for
// the read.
func WritePodFile(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	namespace, podName, containerName, path string,
	data []byte,
	maxBytes int64,
) error {
	if path == "" {
		return fmt.Errorf("no path given")
	}
	if maxBytes <= 0 {
		return fmt.Errorf("maxBytes must be positive (got %d)", maxBytes)
	}
	// Checked before the exec rather than streamed and aborted, so an oversized
	// payload is reported as `errTooLarge` by the same predicate a truncated read
	// uses — one meaning for "this artifact does not fit", on both sides.
	if int64(len(data)) > maxBytes {
		return fmt.Errorf("write %s: %w (limit %d bytes)", path, errTooLarge, maxBytes)
	}
	if len(data) == 0 {
		// Not an error, but refused: writing a zero-byte session file would make
		// the path exist and Pi's switch-verify would then be deciding about a
		// file this function invented rather than about a stored artifact. The
		// caller already refuses empty sessions (`collected` requires bytes), so
		// this is a belt-and-braces check against a future caller.
		return fmt.Errorf("write %s: refusing to write an empty session", path)
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   []string{"tee", "--", path},
			Stdin:     true,
			// `tee` echoes what it writes to stdout. Discarded rather than
			// captured: the payload is already known to the caller, and holding a
			// copy of a session-sized blob in memory to throw it away is the kind
			// of cost that only shows up on the runs that matter.
			Stdout: true,
			Stderr: true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to build exec executor for pod %s/%s: %w", namespace, podName, err)
	}

	var stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  bytes.NewReader(data),
		Stdout: io.Discard,
		Stderr: &stderr,
	})
	if err != nil {
		if stderr.Len() > 0 {
			return fmt.Errorf("failed to write %s into pod %s/%s: %w: %s",
				path, namespace, podName, err, bytes.TrimSpace(stderr.Bytes()))
		}
		return fmt.Errorf("failed to write %s into pod %s/%s: %w", path, namespace, podName, err)
	}
	return nil
}
