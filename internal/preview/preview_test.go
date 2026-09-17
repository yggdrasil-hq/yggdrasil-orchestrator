package preview

import (
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

// The hostname format is a cross-service contract: the API stores whatever
// this package reports and the Web app links to it, so it is pinned
// literally here rather than assembled the way the implementation does.
func TestHost_MatchesADR003URLScheme(t *testing.T) {
	cases := []struct {
		kind queue.JobKind
		want string
	}{
		{queue.KindTestRun, "acme-web-test-run-job-1.preview.yggdrasil.local"},
		{queue.KindFeatureBuild, "acme-web-feature-build-job-1.preview.yggdrasil.local"},
		{queue.KindSpecGrill, "acme-web-spec-grill-job-1.preview.yggdrasil.local"},
	}
	for _, tc := range cases {
		if got := Host("acme-web", tc.kind, "job-1", "yggdrasil.local"); got != tc.want {
			t.Fatalf("Host(%s) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// A `test_run` pod already received a PREVIEW_URL before this package existed
// (worker.buildAgentEnv), built by an inline Sprintf. That env var is read by
// the shipped test_run image, so the host it produces must not change when the
// format moves here.
func TestHost_PreservesExistingTestRunPreviewURL(t *testing.T) {
	const slug = "acme-web"
	const jobID = "2d88c75e-7ad0-458c-8da5-ce8684ce6fa6"
	const domain = "yggdrasil.local"

	legacy := "https://" + slug + "-test-run-" + jobID + ".preview." + domain
	if got := "https://" + Host(slug, queue.KindTestRun, jobID, domain); got != legacy {
		t.Fatalf("test_run preview URL changed: got %q, want the shipped format %q", got, legacy)
	}
}

// A DNS label is capped at 63 characters. Kind token plus job id is fixed
// length, so only the slug can vary — and a truncated label must not end in the
// hyphen the truncation can leave behind.
func TestHost_TruncatesLongSlugToALegalLabel(t *testing.T) {
	longSlug := strings.Repeat("a", 80)
	host := Host(longSlug, queue.KindFeatureBuild, "job-1", "yggdrasil.local")

	label := strings.SplitN(host, ".", 2)[0]
	if len(label) > 63 {
		t.Fatalf("label %q is %d characters, over the DNS limit of 63", label, len(label))
	}
	if strings.HasSuffix(label, "-") {
		t.Fatalf("label %q ends in a hyphen, which is not a legal DNS label", label)
	}
	// Still recognisably a preview for the right job.
	if !strings.HasSuffix(label, "-feature-build-job-1") {
		t.Fatalf("expected the label to keep the kind and job id, got %q", label)
	}
}

func TestHost_DistinctPerJob(t *testing.T) {
	a := Host("acme", queue.KindTestRun, "job-1", "d.local")
	b := Host("acme", queue.KindTestRun, "job-2", "d.local")
	if a == b {
		t.Fatalf("two jobs produced the same preview host %q", a)
	}
}

// A retried job is a new job row (ADR 012), so each attempt gets its own
// release and cannot collide with the attempt it replaced.
func TestReleaseName_KeyedOnJobID(t *testing.T) {
	name := ReleaseName("job-1")
	if name != "preview-job-1" {
		t.Fatalf("ReleaseName = %q", name)
	}

	jobID, ok := jobIDFromReleaseName(name)
	if !ok || jobID != "job-1" {
		t.Fatalf("jobIDFromReleaseName(%q) = %q, %v", name, jobID, ok)
	}

	// The project's always-on release is not a preview and must never be
	// swept as one — deleting it would take production down.
	if _, ok := jobIDFromReleaseName("primary"); ok {
		t.Fatal("the primary release must not be treated as a preview")
	}
	if _, ok := jobIDFromReleaseName(releasePrefix); ok {
		t.Fatal("a bare prefix with no job id is not a valid preview release")
	}
}

func TestKindToken_UnderscoreBecomesHyphen(t *testing.T) {
	if got := KindToken(queue.KindTestRun); got != "test-run" {
		t.Fatalf("KindToken(test_run) = %q", got)
	}
	if got := KindToken(queue.KindSpecGrill); got != "spec-grill" {
		t.Fatalf("KindToken(spec_grill) = %q", got)
	}
}

// ADR 003 §10 names exactly these three. A `deploy`, `rollback`,
// `agentic_review`, `design_grill` or `script_test_run` job must never occupy
// a preview slot — the first two manage the always-on release, and the rest
// produce no deployable artifact.
func TestEligible_OnlyTheADR003Kinds(t *testing.T) {
	eligible := []queue.JobKind{queue.KindSpecGrill, queue.KindFeatureBuild, queue.KindTestRun}
	for _, kind := range eligible {
		if !Eligible(kind) {
			t.Fatalf("expected %s to be preview-eligible (ADR 003 §10)", kind)
		}
	}

	notEligible := []queue.JobKind{
		queue.KindDeploy,
		queue.KindRollback,
		queue.KindAgenticReview,
		queue.KindDesignGrill,
		queue.KindScriptTestRun,
	}
	for _, kind := range notEligible {
		if Eligible(kind) {
			t.Fatalf("expected %s NOT to be preview-eligible", kind)
		}
	}
}

// EligibleKinds feeds the queue's admission clause, so it must agree with
// Eligible exactly — a kind present here but not in Eligible (or vice versa)
// would let the cap count against a slot nothing ever occupies, or stop
// enforcing the cap for a kind that does.
func TestEligibleKinds_AgreesWithEligible(t *testing.T) {
	all := []queue.JobKind{
		queue.KindSpecGrill, queue.KindFeatureBuild, queue.KindTestRun,
		queue.KindDeploy, queue.KindRollback, queue.KindAgenticReview,
		queue.KindDesignGrill, queue.KindScriptTestRun,
	}

	listed := make(map[string]bool)
	for _, kind := range EligibleKinds() {
		listed[kind] = true
	}

	for _, kind := range all {
		if Eligible(kind) != listed[string(kind)] {
			t.Fatalf("EligibleKinds disagrees with Eligible for %s", kind)
		}
	}
}
