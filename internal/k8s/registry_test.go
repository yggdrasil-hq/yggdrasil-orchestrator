package k8s_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
)

/*
Issue #29's pre-emptive half. The issue assumed every agent-image package is
private on GHCR; checking the registry showed that five of the six answer an
anonymous pull while `test_run` does not. These tests therefore pin the *probe's*
accuracy rather than a host-based guess: a warning that fired for the five public
packages would be noise on almost every install, which is how a useful warning
gets ignored.

`fakeRegistry` stands in for GHCR's token endpoint, so the decision is testable
without a network.
*/

// fakeRegistry serves GHCR's token endpoint shape and counts requests, so the
// caching can be asserted rather than assumed.
type fakeRegistry struct {
	server   *httptest.Server
	requests atomic.Int64
	lastPath atomic.Value // string
}

func newFakeRegistry(t *testing.T, status int, body string) *fakeRegistry {
	t.Helper()
	registry := &fakeRegistry{}
	registry.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registry.requests.Add(1)
		registry.lastPath.Store(r.URL.RequestURI())
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(registry.server.Close)
	return registry
}

func publicRegistry(t *testing.T) *fakeRegistry {
	return newFakeRegistry(t, http.StatusOK, `{"token":"anonymous-token"}`)
}

func privateRegistry(t *testing.T) *fakeRegistry {
	return newFakeRegistry(t, http.StatusUnauthorized, `{"errors":[{"code":"UNAUTHORIZED"}]}`)
}

func preflightAgainst(registry *fakeRegistry) *k8s.ImagePullPreflight {
	preflight := k8s.NewImagePullPreflight()
	if registry != nil {
		preflight.TokenEndpoint = registry.server.URL
	}
	return preflight
}

func bareNamespace() string {
	return "proj-" + rand.String(8)
}

// A namespace whose default service account references a credential.
func credentialedClientset(t *testing.T, namespace string) *fake.Clientset {
	t.Helper()
	return fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: namespace},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "ghcr-pull"}},
	})
}

// A namespace whose default service account exists and references nothing —
// the state a fresh install is in.
func bareClientset(t *testing.T, namespace string) *fake.Clientset {
	t.Helper()
	return fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
	})
}

// The case that matters most for this to be trustworthy: five of the six
// published packages are anonymously pullable, so the preflight must stay quiet
// for them even though nothing in the namespace can authenticate.
func TestImagePullPreflight_IsSilentForAPubliclyPullableImage(t *testing.T) {
	registry := publicRegistry(t)
	namespace := bareNamespace()

	finding := preflightAgainst(registry).Check(
		context.Background(), bareClientset(t, namespace), namespace, ghcrImage, "",
	)

	if finding != nil {
		t.Fatalf("expected no finding for an anonymously pullable image, got: %v", finding)
	}
	if registry.requests.Load() != 1 {
		t.Fatalf("expected exactly one probe, got %d", registry.requests.Load())
	}
	if scope := registry.lastPath.Load().(string); !strings.Contains(scope, "repository%3Ayggdrasil-hq%2Fyggdrasil-agent-images%2Fspec_grill%3Apull") {
		t.Fatalf("expected the probe to name the image's repository, got %q", scope)
	}
}

// The case the issue is about, and the one still true on the dev cluster: the
// registry refuses an anonymous pull for the package and nothing here can
// authenticate.
func TestImagePullPreflight_ReportsAPrivateImageWithNoCredential(t *testing.T) {
	registry := privateRegistry(t)
	namespace := bareNamespace()

	finding := preflightAgainst(registry).Check(
		context.Background(), bareClientset(t, namespace), namespace, ghcrImage, "",
	)
	if finding == nil {
		t.Fatal("expected a finding for a private image with no credential")
	}

	rendered := finding.Error()
	for _, want := range []string{
		"setup error",
		ghcrImage,
		"namespace " + namespace,
		"JOB_IMAGE_PULL_SECRET",
		"docker-registry",
		"make the package public",
		"deployment configuration gap, not a problem with the project's code",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected the finding to mention %q, got:\n%s", want, rendered)
		}
	}
}

func TestImagePullPreflight_CredentialFromEitherRouteSilencesIt(t *testing.T) {
	ctx := context.Background()

	// Route one: the Orchestrator references a secret in the pod spec. The
	// registry is never asked, because the answer could not change anything.
	registry := privateRegistry(t)
	namespace := bareNamespace()
	preflight := preflightAgainst(registry)
	if finding := preflight.Check(ctx, bareClientset(t, namespace), namespace, ghcrImage, "ghcr-pull"); finding != nil {
		t.Fatalf("expected no finding when a pull secret is configured, got: %v", finding)
	}
	if registry.requests.Load() != 0 {
		t.Fatalf("expected no probe when a secret is already configured, got %d", registry.requests.Load())
	}

	// Route two: the namespace's default service account references one, which
	// Kubernetes applies without the Orchestrator being told anything.
	registry = privateRegistry(t)
	namespace = bareNamespace()
	preflight = preflightAgainst(registry)
	if finding := preflight.Check(ctx, credentialedClientset(t, namespace), namespace, ghcrImage, ""); finding != nil {
		t.Fatalf("expected no finding when the service account references a credential, got: %v", finding)
	}
}

// An inconclusive probe is not evidence of anything: a 5xx, an empty token, or an
// unreachable registry must all stay quiet, because the kubelet's own answer
// (ImagePullFailure) is the one that can be trusted.
func TestImagePullPreflight_SaysNothingWhenTheProbeIsInconclusive(t *testing.T) {
	ctx := context.Background()

	cases := map[string]func(t *testing.T) *fakeRegistry{
		"server error":     func(t *testing.T) *fakeRegistry { return newFakeRegistry(t, http.StatusInternalServerError, "") },
		"rate limited":     func(t *testing.T) *fakeRegistry { return newFakeRegistry(t, http.StatusTooManyRequests, "") },
		"empty body":       func(t *testing.T) *fakeRegistry { return newFakeRegistry(t, http.StatusOK, `{}`) },
		"not json":         func(t *testing.T) *fakeRegistry { return newFakeRegistry(t, http.StatusOK, "<html>") },
		"endpoint missing": func(t *testing.T) *fakeRegistry { return nil },
	}

	for name, makeRegistry := range cases {
		registry := makeRegistry(t)
		namespace := bareNamespace()
		preflight := preflightAgainst(registry)
		if registry == nil {
			// Nothing is listening on the real GHCR endpoint in a test sandbox,
			// so this is also the "registry unreachable" case. Point it at a
			// closed port so the failure is immediate rather than a timeout.
			preflight.TokenEndpoint = "http://127.0.0.1:1/token"
		}

		if finding := preflight.Check(ctx, bareClientset(t, namespace), namespace, ghcrImage, ""); finding != nil {
			t.Fatalf("%s: expected no finding from an inconclusive probe, got: %v", name, finding)
		}
	}
}

// Only GHCR is probed. The placeholder default and any other registry must not
// turn the Orchestrator into a client that fetches whatever an env var names.
func TestImagePullPreflight_DoesNotProbeOtherRegistries(t *testing.T) {
	registry := privateRegistry(t)
	preflight := preflightAgainst(registry)

	for _, image := range []string{
		"busybox:1.36",
		"docker.io/library/busybox:1.36",
		"localhost:5000/project/app:v1",
		"registry.internal.example/app:v1",
		"yggdrasil-agent-images/spec_grill:tag",
	} {
		namespace := bareNamespace()
		if finding := preflight.Check(context.Background(), bareClientset(t, namespace), namespace, image, ""); finding != nil {
			t.Fatalf("%s: expected no finding, got: %v", image, finding)
		}
	}
	if registry.requests.Load() != 0 {
		t.Fatalf("expected no probes for non-GHCR images, got %d", registry.requests.Load())
	}
}

// The warning is once per namespace; the probe is once per repository. Without
// both, an unconfigured install would repeat itself on every job — a spec_grill
// run plus two `script_test_run` probes per feature — and a repeated warning is a
// filtered warning.
func TestImagePullPreflight_WarnsOncePerNamespaceAndProbesOncePerRepository(t *testing.T) {
	ctx := context.Background()
	registry := privateRegistry(t)
	preflight := preflightAgainst(registry)

	namespace := bareNamespace()
	clientset := bareClientset(t, namespace)
	if finding := preflight.Check(ctx, clientset, namespace, ghcrImage, ""); finding == nil {
		t.Fatal("expected the first check to report")
	}
	if finding := preflight.Check(ctx, clientset, namespace, ghcrImage, ""); finding != nil {
		t.Fatalf("expected the second check in the same namespace to be suppressed, got: %v", finding)
	}

	// A different project is a different namespace — its own first warning, and
	// still only one probe, because the repository's visibility has not changed.
	other := bareNamespace()
	if finding := preflight.Check(ctx, bareClientset(t, other), other, ghcrImage, ""); finding == nil {
		t.Fatal("expected a warning for a different namespace")
	}

	// A different image in the same repository (a pinned tag) also reuses the
	// cached answer.
	pinned := strings.TrimSuffix(ghcrImage, ":latest") + ":sha-12345678"
	third := bareNamespace()
	if finding := preflight.Check(ctx, bareClientset(t, third), third, pinned, ""); finding == nil {
		t.Fatal("expected a warning for a different namespace with a pinned tag")
	}

	if registry.requests.Load() != 1 {
		t.Fatalf("expected exactly one probe for the repository, got %d", registry.requests.Load())
	}
}

// The other image family in the same GHCR repository is a different repository
// for the token scope, so it must be probed separately — which is exactly how
// `test_run` differs from `spec_grill` in practice.
func TestImagePullPreflight_ProbesEachPackageSeparately(t *testing.T) {
	ctx := context.Background()
	registry := privateRegistry(t)
	preflight := preflightAgainst(registry)

	for _, image := range []string{
		"ghcr.io/yggdrasil-hq/yggdrasil-agent-images/spec_grill:latest",
		"ghcr.io/yggdrasil-hq/yggdrasil-agent-images/test_run:latest",
	} {
		namespace := bareNamespace()
		if finding := preflight.Check(ctx, bareClientset(t, namespace), namespace, image, ""); finding == nil {
			t.Fatalf("%s: expected a warning", image)
		}
	}

	if registry.requests.Load() != 2 {
		t.Fatalf("expected one probe per package, got %d", registry.requests.Load())
	}
}

// A digest-pinned image names the same repository as a tag-pinned one, so the
// probe's scope has to drop the digest rather than include it in the path.
func TestImagePullPreflight_StripsTheDigestFromTheScope(t *testing.T) {
	registry := publicRegistry(t)
	namespace := bareNamespace()
	image := "ghcr.io/yggdrasil-hq/yggdrasil-agent-images/base@sha256:0000000000000000000000000000000000000000000000000000000000000000"

	if finding := preflightAgainst(registry).Check(
		context.Background(), bareClientset(t, namespace), namespace, image, "",
	); finding != nil {
		t.Fatalf("expected no finding, got: %v", finding)
	}

	scope := registry.lastPath.Load().(string)
	if strings.Contains(scope, "sha256") {
		t.Fatalf("expected the digest to be stripped from the scope, got %q", scope)
	}
	if !strings.Contains(scope, "repository%3Ayggdrasil-hq%2Fyggdrasil-agent-images%2Fbase%3Apull") {
		t.Fatalf("expected the scope to name the repository, got %q", scope)
	}
}

// The success path has to actually read the token rather than treating any 200
// as "public": an empty 200 is what a proxy in front of a registry returns, and
// it means nothing about the package. Both outcomes are silent, which is the
// point — the difference only shows in whether a *warning* could ever be raised,
// and an inconclusive one must never raise it.
func TestImagePullPreflight_TreatsATokenlessResponseAsInconclusive(t *testing.T) {
	namespace := bareNamespace()
	registry := newFakeRegistry(t, http.StatusOK, `{}`)

	if finding := preflightAgainst(registry).Check(
		context.Background(), bareClientset(t, namespace), namespace, ghcrImage, "",
	); finding != nil {
		t.Fatalf("expected no finding from a tokenless 200, got: %v", finding)
	}
	// And the probe really did run — otherwise this would pass for the wrong
	// reason (no request at all).
	if registry.requests.Load() != 1 {
		t.Fatalf("expected the registry to have been asked, got %d requests", registry.requests.Load())
	}
}
