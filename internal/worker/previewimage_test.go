package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/imagebuild"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

/*
Issue #19. The wiring, tested without a cluster: what values a preview is given,
which ref it builds, and which repository it builds. The build itself is covered
in internal/imagebuild; what is checked here is the decision the worker makes
before calling it.
*/

// --- the Helm values a preview is given ------------------------------------

func TestPreviewImageValues_SplitsIntoTheChartsTwoKeys(t *testing.T) {
	got := previewImageValues("registry.local:5000/proj-1/luffy:main")

	image, ok := got["image"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected an image map, got %#v", got)
	}
	// The chart interpolates `repository:tag`, so both keys are required — and
	// the registry's port has to stay with the repository, not be cut off.
	if image["repository"] != "registry.local:5000/proj-1/luffy" {
		t.Fatalf("unexpected repository: %#v", image["repository"])
	}
	if image["tag"] != "main" {
		t.Fatalf("unexpected tag: %#v", image["tag"])
	}
}

// No image means no override at all, so the chart's declared image still applies.
// An empty `image:` map would blank the chart's defaults instead.
func TestPreviewImageValues_IsNilWithoutAnImage(t *testing.T) {
	if got := previewImageValues(""); got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

// A digest-pinned reference cannot be expressed by a chart that interpolates
// `repository:tag`. Refusing it falls back to the chart's image and logs; the
// alternative deploys something visibly broken.
func TestPreviewImageValues_RefusesADigestPinnedReference(t *testing.T) {
	got := previewImageValues("registry.local/proj-1/luffy@sha256:abc123")

	if got != nil {
		t.Fatalf("expected no override for a digest, got %#v", got)
	}
}

// --- which repository is built ---------------------------------------------

func TestPrimaryRepo_PicksTheFlaggedOne(t *testing.T) {
	repos := []apiclient.FeatureSpecRepo{
		{CloneURL: "https://github.com/acme/docs"},
		{CloneURL: "https://github.com/acme/web", IsPrimary: true},
	}

	got, ok := primaryRepo(repos)

	if !ok {
		t.Fatal("expected to find the primary repository")
	}
	if got.CloneURL != "https://github.com/acme/web" {
		t.Fatalf("expected the flagged repo, got %q", got.CloneURL)
	}
}

// A project whose init has not finished, or whose repo was unlinked after the
// job was queued, has no primary — the preview must still come up.
func TestPrimaryRepo_ReportsNoPrimaryRatherThanGuessing(t *testing.T) {
	if _, ok := primaryRepo([]apiclient.FeatureSpecRepo{{CloneURL: "https://github.com/acme/docs"}}); ok {
		t.Fatal("expected no primary to be found")
	}
	if _, ok := primaryRepo(nil); ok {
		t.Fatal("expected no primary for no repos")
	}
}

// --- naming the pushed image ----------------------------------------------

func TestRepoNameFromCloneURL(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"https://github.com/acme/web.git", "web"},
		{"https://github.com/acme/web", "web"},
		{"https://github.com/acme/web/", "web"},
		{"git@github.com:acme/web.git", "web"},
		{"https://github.com/acme/luffy-portfolio.git", "luffy-portfolio"},
		// A URL with nothing usable still has to produce a legal name rather
		// than an empty path segment.
		{"", "app"},
		{"https://github.com/", "app"},
	} {
		if got := repoNameFromCloneURL(tc.url); got != tc.want {
			t.Errorf("repoNameFromCloneURL(%q): expected %q, got %q", tc.url, tc.want, got)
		}
	}
}

// --- which ref is built ---------------------------------------------------

// A feature job previews the feature's branch: that is the whole point, since
// the chart's declared image is what a preview showed before this.
func TestPreviewBuildRef_PrefersTheFeatureBranch(t *testing.T) {
	ref := "main"
	job := &queue.Job{Ref: &ref}

	got := previewBuildRef(job, apiclient.FeatureSpec{Branch: "yggdrasil/feature-abc"})

	if got != "yggdrasil/feature-abc" {
		t.Fatalf("expected the feature branch, got %q", got)
	}
}

// A scheduled test_run has no feature branch, so it previews the ref it was
// dispatched against — the same rule the API uses when it resolves the job's
// spec, so the image and the agent's checkout cannot disagree.
func TestPreviewBuildRef_FallsBackToTheJobsRef(t *testing.T) {
	ref := "main"
	job := &queue.Job{Ref: &ref}

	if got := previewBuildRef(job, apiclient.FeatureSpec{}); got != "main" {
		t.Fatalf("expected the job's ref, got %q", got)
	}
}

// A job with neither still previews *something* rather than an empty ref, which
// would build a Dockerfile from whatever the clone happened to land on.
func TestPreviewBuildRef_DefaultsToMain(t *testing.T) {
	if got := previewBuildRef(&queue.Job{}, apiclient.FeatureSpec{}); got != "main" {
		t.Fatalf("expected main, got %q", got)
	}
	if got := previewBuildRef(&queue.Job{Ref: new(string)}, apiclient.FeatureSpec{}); got != "main" {
		t.Fatalf("expected main for an empty ref, got %q", got)
	}
}

// --- the build is opt-in --------------------------------------------------

// The property that makes this safe to land: with no registry configured the
// build is skipped entirely, so a preview keeps the chart's declared image
// exactly as it did before this work. That is asserted through the wiring rather
// than the constant, because "empty means off" is the whole guarantee.
func TestBuildPreviewImage_IsSkippedWithoutARegistry(t *testing.T) {
	var requested bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = true
		_ = json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer server.Close()

	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")} // no ImageRegistry

	got := buildPreviewImage(context.Background(), nil, &queue.Job{ID: "job-1", ProjectID: "p1"}, "ns", cfg)

	if got != "" {
		t.Fatalf("expected no image without a registry, got %q", got)
	}
	// And it does not even ask the API: an install that has not opted in should
	// cost nothing per preview.
	if requested {
		t.Fatal("expected no API call when the registry is unconfigured")
	}
}

// A registry with no cluster client is a misconfiguration, and it must degrade
// rather than panic — this runs on the job's own path.
func TestBuildPreviewImage_DegradesWithoutAClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "T"})
	}))
	defer server.Close()

	cfg := Config{
		APIClient:     apiclient.New(server.URL, "test-token"),
		ImageRegistry: "registry.local",
	}

	got := buildPreviewImage(context.Background(), nil, &queue.Job{ID: "job-1", ProjectID: "p1"}, "ns", cfg)

	if got != "" {
		t.Fatalf("expected no image, got %q", got)
	}
}

// An API that cannot be asked for the job's spec must not fail the preview: the
// whole preview path treats the build as additive.
func TestBuildPreviewImage_SurvivesASpecFetchFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := Config{
		APIClient:     apiclient.New(server.URL, "test-token"),
		ImageRegistry: "registry.local",
	}

	got := buildPreviewImage(context.Background(), unreachableClient(), featureBuildJob(), "ns", cfg)

	if got != "" {
		t.Fatalf("expected no image, got %q", got)
	}
}

// featureBuildJob is a feature-scoped job, which is what makes fetchJobSpec take
// the feature path. A job with no feature id short-circuits that call, and three
// of these tests would then pass without ever reaching the code they are about —
// the kind of green that proves nothing.
func featureBuildJob() *queue.Job {
	featureID := "0cd9a850-e4b2-4ae3-b8ec-699f8d37392d"
	return &queue.Job{
		ID:        "job-1",
		ProjectID: "p1",
		Kind:      queue.KindFeatureBuild,
		FeatureID: &featureID,
	}
}

// A project with no primary repository has nothing to build from; the preview
// still applies the chart.
func TestBuildPreviewImage_SkipsWithoutAPrimaryRepository(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "T",
			"repos": []map[string]any{{"cloneUrl": "https://github.com/acme/docs", "isPrimary": false}},
		})
	}))
	defer server.Close()

	cfg := Config{
		APIClient:     apiclient.New(server.URL, "test-token"),
		ImageRegistry: "registry.local",
	}
	job := featureBuildJob()

	got := buildPreviewImage(context.Background(), unreachableClient(), job, "ns", cfg)

	if got != "" {
		t.Fatalf("expected no image without a primary repo, got %q", got)
	}
}

// With a registry configured and a primary repository present, the build is
// genuinely attempted and its outcome still yields no image — so the preview
// falls back to the chart's declared one.
//
// The context is bounded on purpose. A fake clientset accepts the build Job and
// then never produces a pod for it, so the real path would poll until the build's
// own 20-minute timeout; a short deadline exercises the same code (create, poll,
// give up) in milliseconds and keeps the suite honest about what it covers: this
// asserts the *wiring's* error handling, not that Kaniko works.
func TestBuildPreviewImage_YieldsNoImageWhenTheBuildDoesNotComplete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "T",
			"repos": []map[string]any{
				{"cloneUrl": "https://github.com/acme/web.git", "isPrimary": true},
			},
			"githubToken": "ghs_test",
		})
	}))
	defer server.Close()

	cfg := Config{
		APIClient:     apiclient.New(server.URL, "test-token"),
		ImageRegistry: "registry.local:5000",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	got := buildPreviewImage(ctx, unreachableClient(), featureBuildJob(), "ns", cfg)

	if got != "" {
		t.Fatalf("expected no image after a build that did not complete, got %q", got)
	}
}

// The destination the worker asks for, composed exactly as buildPreviewImage
// composes it — asserted here so the naming is pinned without a cluster, and so a
// change to how the repo name or ref are derived is visible end to end.
func TestPreviewImageDestinationNaming(t *testing.T) {
	got := imagebuild.Destination(
		"registry.local:5000",
		"b51e1313-d315-47bf-be25-038acc16d6a4",
		repoNameFromCloneURL("https://github.com/acme/web.git"),
		"yggdrasil/feature-abc",
	)

	if !strings.HasPrefix(got, "registry.local:5000/proj-b51e1313-d315-47bf-be25-038acc16d6a4/web:") {
		t.Fatalf("unexpected destination %q", got)
	}
	// A branch with a slash must not reach the tag, and the registry's port must
	// survive the split.
	if !strings.HasSuffix(got, ":yggdrasil-feature-abc") {
		t.Fatalf("expected a slash-free tag, got %q", got)
	}
}
