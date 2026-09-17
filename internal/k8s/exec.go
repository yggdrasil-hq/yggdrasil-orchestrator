package k8s

import (
	"bytes"
	"context"
	"fmt"

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
