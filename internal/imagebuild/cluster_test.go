package imagebuild

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
)

/*
Opt-in verification against a real cluster (issue #69).

Everything else in this package asserts what the Job *asks the cluster to run*,
which cannot tell you whether the cluster can run it at all — and issue #69 is
precisely a case where valid YAML described a pod that runc refused to create, so
the suite was green while every preview build failed. The other ways a build is
un-runnable are equally invisible to those tests: an image that will not pull, a
resource request the cluster cannot satisfy, a registry the pod cannot reach.

So the real-cluster check is opt-in rather than part of the default suite, which
has no cluster:

	YGGDRASIL_IMAGEBUILD_CLUSTER_TEST=1 \
	YGGDRASIL_IMAGEBUILD_KUBECONFIG=/path/to/kubeconfig \
	YGGDRASIL_IMAGEBUILD_NAMESPACE=imgbuild-verify \
	YGGDRASIL_IMAGEBUILD_REGISTRY=registry.imgbuild-verify.svc.cluster.local:5000 \
	go test ./internal/imagebuild/ -run TestBuild_AgainstARealCluster -v

`YGGDRASIL_IMAGEBUILD_CLONE_URL` and `..._GIT_REF` default to a small public
repository with a root Dockerfile (the ADR 003 §12 contract), so a default run
needs nothing beyond a cluster and a registry.

Verified this way on 2026-09-19 against k3s v1.36.4+k3s1, and it is what found
the distroless-shell bug in the first place.
*/

const clusterTestEnvVar = "YGGDRASIL_IMAGEBUILD_CLUSTER_TEST"

// defaultClusterCloneURL is deliberately tiny and public: the build has to pull a
// base image and push a result, and a large application would make this check
// slow enough that nobody would run it.
const defaultClusterCloneURL = "https://github.com/docker/welcome-to-docker"

func clusterTestConfig(t *testing.T) Config {
	t.Helper()

	if os.Getenv(clusterTestEnvVar) == "" {
		t.Skipf("set %s=1 to run the real-cluster build verification", clusterTestEnvVar)
	}

	kubeconfigPath := os.Getenv("YGGDRASIL_IMAGEBUILD_KUBECONFIG")
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("KUBECONFIG")
	}
	if kubeconfigPath == "" {
		t.Skip("set YGGDRASIL_IMAGEBUILD_KUBECONFIG (or KUBECONFIG) to the cluster to verify against")
	}

	kubeconfig, err := os.ReadFile(kubeconfigPath)
	if err != nil {
		t.Fatalf("failed to read the kubeconfig at %s: %v", kubeconfigPath, err)
	}

	namespace := os.Getenv("YGGDRASIL_IMAGEBUILD_NAMESPACE")
	if namespace == "" {
		t.Skip("set YGGDRASIL_IMAGEBUILD_NAMESPACE to a scratch namespace the registry is reachable from")
	}
	registry := os.Getenv("YGGDRASIL_IMAGEBUILD_REGISTRY")
	if registry == "" {
		t.Skip("set YGGDRASIL_IMAGEBUILD_REGISTRY to a registry the cluster can push to")
	}

	client, err := k8s.NewClientFromKubeconfig(kubeconfig)
	if err != nil {
		t.Fatalf("failed to build a cluster client: %v", err)
	}

	cloneURL := os.Getenv("YGGDRASIL_IMAGEBUILD_CLONE_URL")
	if cloneURL == "" {
		cloneURL = defaultClusterCloneURL
	}
	ref := os.Getenv("YGGDRASIL_IMAGEBUILD_GIT_REF")
	if ref == "" {
		ref = "main"
	}

	return Config{
		Clientset: client.Interface,
		Namespace: namespace,

		RepoName: "imagebuild-verify",
		CloneURL: cloneURL,
		GitRef:   ref,
		// A short, label-safe id. The Job's *name* is sanitised and trimmed by
		// buildJobName, but its `yggdrasil.io/job` label carries the id verbatim —
		// which is fine in production, where the id is the job row's uuid, and was
		// not fine for `"verify-" + t.Name()`: the API server rejected the label
		// (over 63 bytes) rather than the build, which is how this harness first
		// failed.
		JobID:       "verify69",
		Destination: Destination(registry, "11111111-1111-4111-8111-111111111111", "imagebuild-verify", ref),

		// Five minutes is enough for this repository and short enough that a wedged
		// build fails the check rather than hanging it.
		Timeout: 5 * time.Minute,
	}
}

// A real build, all the way to a push.
//
// This is the assertion issue #69 needed and did not have: the build container
// starts, Kaniko runs through its own CLI, it clones, it builds, and it pushes.
func TestBuild_AgainstARealCluster(t *testing.T) {
	cfg := clusterTestConfig(t)

	result, err := Build(context.Background(), cfg)
	if err != nil {
		t.Fatalf("the build failed: %v", err)
	}
	if !result.Built {
		t.Fatalf("expected a build, got a skip: %s", result.Reason)
	}
	if result.Image != cfg.Destination {
		t.Fatalf("expected the image to be %q, got %q", cfg.Destination, result.Image)
	}
}

// The Dockerfile guard, exercised through a real pod.
//
// This is the half of issue #69 that moved containers: the check needs a shell,
// so it runs in the context container rather than the build container. Asserting
// it here means the *real* exit code reaches awaitResult and is translated into a
// skip — which is what a project without a Dockerfile depends on, and which no
// unit test can confirm (they assert the script text, not that the pod runs it).
func TestBuild_AgainstARealCluster_SkipsWhenThereIsNoDockerfile(t *testing.T) {
	cfg := clusterTestConfig(t)
	// A path the repository certainly does not have. Equivalent to a repository
	// with no Dockerfile: the guard is what decides, and it is the same guard.
	cfg.DockerfilePath = "this-does-not-exist/Dockerfile"

	result, err := Build(context.Background(), cfg)
	if err != nil {
		t.Fatalf("expected a skip rather than a failure, got: %v", err)
	}
	if result.Built {
		t.Fatalf("expected a skip, got a built image at %q", result.Image)
	}
	if result.Reason == "" {
		t.Fatal("expected a reason for the skip")
	}
}

// The other skip, which is the common one: a preview is created when a job starts,
// and a feature branch is not pushed until the agent finishes.
func TestBuild_AgainstARealCluster_SkipsWhenTheRefIsNotOnTheRemote(t *testing.T) {
	cfg := clusterTestConfig(t)
	cfg.GitRef = "yggdrasil/this-ref-does-not-exist-69"

	result, err := Build(context.Background(), cfg)
	if err != nil {
		t.Fatalf("expected a skip rather than a failure, got: %v", err)
	}
	if result.Built {
		t.Fatal("expected a skip, got a built image")
	}
	if result.Reason == "" {
		t.Fatal("expected a reason for the skip")
	}
}
