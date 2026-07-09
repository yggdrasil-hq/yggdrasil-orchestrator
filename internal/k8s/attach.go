package k8s

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Attach opens a bidirectional stream to a running pod's container —
// stdin/stdout/stderr — the same Kubernetes API `kubectl attach -i` uses.
// It blocks until the stream ends (the container's process exits, ctx is
// cancelled, the connection drops, or stdin reaches EOF — see below),
// or returns immediately if the attach request itself can't be
// established (e.g. pod not found).
//
// One call = one turn, not a whole multi-turn session. Verified against a
// real attached pod: client-go's remotecommand (both the SPDY and
// WebSocket executors — they share the same copyStdin implementation,
// tools/remotecommand/v2.go) does not reliably deliver a *second* write to
// a container's stdin within one continuous attach: the write call itself
// reports success, but the container never receives it, and stdout stops
// being read too. copyStdin's own two racing goroutines (one forwarding
// stdin, one discarding whatever the server sends back on that same
// stream, both closing it via a shared sync.Once) are the likely culprit,
// though not fully root-caused here.
//
// The fix isn't a different executor — it's a different call pattern: end
// each Attach call by letting stdin reach EOF once a turn's response has
// been read (not immediately after writing), then call Attach again for
// the next turn. This works because the container was created with
// StdinOnce: false (the k8s default) — its stdin channel stays open across
// separate attach sessions, so reattaching doesn't require restarting the
// process. ADR 006's RPC client (internal/rpc) exposes BeginTurn/EndTurn
// for exactly this: internal/worker's driveSpecGrillSession calls Attach
// once per prompt/response turn, not once for the whole spec_grill run.
//
// The container must have been created with Stdin: true (JobSpec.Stdin) —
// otherwise Kubernetes has nothing to attach to. stdout/stderr are plain
// io.Writers the caller drains as bytes arrive; ADR 006's RPC client
// (internal/rpc) implements io.Writer itself so it can be passed directly
// as stdout to parse Pi's JSONL event stream as it streams in.
func Attach(
	ctx context.Context,
	clientset kubernetes.Interface,
	restConfig *rest.Config,
	namespace, podName, containerName string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("attach").
		VersionedParams(&corev1.PodAttachOptions{
			Container: containerName,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to build attach executor for pod %s/%s: %w", namespace, podName, err)
	}

	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}); err != nil {
		return fmt.Errorf("attach stream to pod %s/%s ended with error: %w", namespace, podName, err)
	}
	return nil
}
