package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
)

/*
The pre-emptive half of issue #29: say something *before* the pod exists, rather
than only once the kubelet has failed to pull.

**What the issue assumed, and what is actually true.** #29 says the agent images
are private by default on GHCR. That was the state when it was filed, and it is
still true for `test_run` — verified against the cluster this was developed on,
which fails with `401 Unauthorized` from GHCR's token endpoint — but the other
five packages (`spec_grill`, `feature_build`, `script_test_run`, `agentic_review`,
`design_grill`) answer an anonymous pull request successfully. So a check that
inferred "private" from the registry host would be wrong five times out of six,
and a warning that is wrong that often is worse than no warning: it is how
operators learn to ignore the one that matters.

So this does not guess from the host. It asks the registry the same question the
kubelet asks — "will you issue an anonymous pull token for this repository?" — and
reports only when the answer is no *and* nothing in the namespace can authenticate
instead. Same accuracy as the failure it predicts, without waiting for it.
*/

// defaultProbeTimeout bounds one visibility request. Short because it sits on the
// job path: a slow registry must not hold up a job that is going to fail anyway a
// few seconds later at the pod.
const defaultProbeTimeout = 5 * time.Second

// ghcrTokenEndpoint is where GHCR issues pull tokens. Overridable on
// ImagePullPreflight so the probe can be tested against a fake registry.
const ghcrTokenEndpoint = "https://ghcr.io/token"

// pullVisibility is the answer to "can this image be pulled without a credential?"
//
// Unexported on purpose: the verdict is an internal step, and only one of its
// three values is ever observable from outside (`CredentialRequired`, when a
// finding is produced). The other two are indistinguishable to a caller and
// deliberately behave the same way, so exporting the type would invite a caller
// to branch on a distinction that does not exist.
type pullVisibility int

const (
	// pullVisibilityUnknown: the registry was not asked, could not be reached, or
	// answered something this does not recognise. Never produces a warning — an
	// unanswered question is not evidence of a misconfiguration.
	pullVisibilityUnknown pullVisibility = iota
	// pullVisibilityAnonymous: the registry issued an anonymous pull token, so no
	// credential is needed.
	pullVisibilityAnonymous
	// pullVisibilityCredentialRequired: the registry refused an anonymous token,
	// so the package is private (or has never been published). Both are setup
	// gaps with the same remedy, and the pod would fail either way.
	pullVisibilityCredentialRequired
)

// ImagePullPreflight answers, at most once per namespace and once per repository
// per process, whether a job's image can be pulled and either does not need a
// credential or has one available.
//
// Both memos are one-shot rather than time-boxed. The visibility memo is the
// expensive one (a network call); the namespace memo is what keeps the warning
// from repeating on every job in an unconfigured install — a spec_grill run plus
// two `script_test_run` probes per feature would otherwise log it three times per
// feature. Nothing depends on either for correctness: the evidence-based
// `ImagePullFailure` path is what fails a job, and it is never suppressed.
type ImagePullPreflight struct {
	// TokenEndpoint overrides GHCR's token endpoint. Empty means the real one;
	// setting it to a test server is how the probe's decision is exercised without
	// a network. The zero value of this struct is usable — the maps below are
	// created lazily — because a struct whose zero value panics is a trap.
	TokenEndpoint string

	mu sync.Mutex
	// warned records namespaces already reported, so a single install does not
	// repeat itself.
	warned map[string]bool
	// visibility caches the probe per repository, so N jobs for the same image
	// cost one request.
	visibility map[string]pullVisibility
}

func NewImagePullPreflight() *ImagePullPreflight {
	return &ImagePullPreflight{
		warned:     map[string]bool{},
		visibility: map[string]pullVisibility{},
	}
}

// Check reports the preflight finding for one job's image, or nil.
//
// `configuredSecret` is the name the Orchestrator will put in the pod spec, so a
// non-empty value means the credential will be presented and there is nothing to
// warn about. Otherwise the namespace's `default` service account is asked, which
// is the route that needs no Orchestrator configuration at all.
//
// The credential checks come *before* the registry probe, so a correctly
// configured install never pays for the network call — and so the probe's result
// is only ever consulted when it could change the answer.
//
// Returns non-nil at most once per namespace.
func (p *ImagePullPreflight) Check(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, image, configuredSecret string,
) *SetupError {
	if configuredSecret != "" {
		return nil
	}

	p.mu.Lock()
	if p.warned == nil {
		p.warned = map[string]bool{}
	}
	alreadyWarned := p.warned[namespace]
	p.mu.Unlock()
	if alreadyWarned {
		return nil
	}
	// A *missing* service account is reported as "no reference": the read
	// succeeding is not what makes the account usable, and a namespace without
	// one has nothing for a pod to inherit. Any other read error is "has one" —
	// a permissions gap must not produce a warning.
	if serviceAccountHasPullSecret(ctx, clientset, namespace) {
		return nil
	}

	if p.probeVisibility(ctx, image) != pullVisibilityCredentialRequired {
		return nil
	}

	// Mark only now, so a probe that was inconclusive does not suppress a real
	// warning later in the same namespace.
	p.mu.Lock()
	p.warned[namespace] = true
	p.mu.Unlock()

	return &SetupError{
		Summary: fmt.Sprintf("the agent image %q could not be pulled without credentials — the registry refused an "+
			"anonymous pull token for that package, and nothing this job's pod can use authenticates to it "+
			"(namespace %s)", image, namespace),
		Detail: "GHCR did not issue an anonymous pull token for that repository, which is how a private package " +
			"(or one that has never been published) answers — so the pull will fail with ImagePullBackOff",
		Remedy: "create a registry credential and reference it — `kubectl -n " + namespace +
			" create secret docker-registry <name> --docker-server=ghcr.io --docker-username=<github user> " +
			"--docker-password=<token with read:packages>`, then either set JOB_IMAGE_PULL_SECRET=<name> on the " +
			"Orchestrator or attach it to the `default` service account. Alternatively, make the package public. " +
			"See orchestrator/docs/overview/setup.md. This is a deployment configuration gap, not a problem with " +
			"the project's code.",
	}
}

// probeVisibility returns the cached visibility of an image's repository,
// probing the registry the first time it is asked.
func (p *ImagePullPreflight) probeVisibility(ctx context.Context, image string) pullVisibility {
	repository, host := imageRepository(image)
	if repository == "" || host != "ghcr.io" {
		// Only GHCR is probed. It is the single host ADR 004 publishes agent
		// images to; probing an arbitrary registry would mean the Orchestrator
		// making outbound requests to whatever an env var happens to name, which
		// is a surprising thing for it to do and would not help.
		return pullVisibilityUnknown
	}

	p.mu.Lock()
	if cached, ok := p.visibility[repository]; ok {
		p.mu.Unlock()
		return cached
	}
	p.mu.Unlock()

	visibility := probeAnonymousPull(ctx, p.tokenEndpoint(), repository)

	p.mu.Lock()
	if p.visibility == nil {
		p.visibility = map[string]pullVisibility{}
	}
	p.visibility[repository] = visibility
	p.mu.Unlock()
	return visibility
}

func (p *ImagePullPreflight) tokenEndpoint() string {
	if p.TokenEndpoint != "" {
		return p.TokenEndpoint
	}
	return ghcrTokenEndpoint
}

// probeAnonymousPull asks the registry to issue an anonymous pull token for one
// repository.
//
// That request is the registry's own definition of "can this be pulled without
// credentials": GHCR answers 200 with a token for a public package and 401 for a
// private one. Reporting the answer as unknown for anything else (a 5xx, a
// timeout, an unexpected body) is deliberate — this must never claim a problem
// the registry did not confirm.
func probeAnonymousPull(ctx context.Context, endpoint, repository string) pullVisibility {
	probeCtx, cancel := context.WithTimeout(ctx, defaultProbeTimeout)
	defer cancel()

	requestURL := fmt.Sprintf(
		"%s?scope=%s&service=ghcr.io",
		endpoint,
		url.QueryEscape("repository:"+repository+":pull"),
	)
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, requestURL, nil)
	if err != nil {
		return pullVisibilityUnknown
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return pullVisibilityUnknown
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK:
		// A token in the body is what makes this conclusive rather than merely
		// "not refused" — an empty 200 is what a proxy in front of a registry
		// returns, and it says nothing about the package.
		var body struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil || body.Token == "" {
			return pullVisibilityUnknown
		}
		return pullVisibilityAnonymous
	case http.StatusUnauthorized, http.StatusForbidden:
		return pullVisibilityCredentialRequired
	default:
		return pullVisibilityUnknown
	}
}

// imageRepository splits an image reference into the repository path GHCR wants
// in a token scope and its registry host.
//
// The repository is the path with the tag or digest removed — GHCR's scope is
// `repository:<path>:pull`, so `ghcr.io/org/name:tag` and
// `ghcr.io/org/name@sha256:…` both mean the repository `org/name`.
func imageRepository(image string) (repository string, host string) {
	base := image
	if at := strings.IndexByte(base, '@'); at >= 0 {
		base = base[:at]
	}
	lastSlash := strings.LastIndexByte(base, '/')
	if lastColon := strings.LastIndexByte(base, ':'); lastColon > lastSlash {
		base = base[:lastColon]
	}

	host = imageRegistryHost(image)
	if host == "" {
		return base, ""
	}
	repository = strings.TrimPrefix(base, host+"/")
	return repository, host
}
