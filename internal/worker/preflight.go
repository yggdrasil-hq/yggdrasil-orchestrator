package worker

import (
	"context"
	"log"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
Issues #29 and #44: the two ways a job kind can fail before any of its own code
runs, and what the operator is told about each.

Both are *setup* problems — the installation is incomplete, not the project's
code — and both used to surface as something else.

- #29: the agent images come from GHCR, where packages are private by default
  (ADR 004), so an install with no registry credential cannot pull them. The pod
  sat in `ImagePullBackOff` and the job died on its context deadline reporting
  "context deadline exceeded". `k8s.CheckImagePullPrerequisites` warns before the
  pod exists and `k8s.ImagePullFailure` proves it once it does.
- #44: the API dispatches a `script_test_run` probe for unit and integration on
  every feature, unconditionally, because the *presence of a script* in the
  project's repo is what turns a group on (ADR 015 item 10) — and the API
  deliberately never reads the repo. The check that implements that toggle lives
  in the image, so when `SCRIPT_TEST_RUN_IMAGE` is unset the probes fail before
  they can look, and every feature on that install fails at Testing for a reason
  that has nothing to do with the feature.

This file holds the worker-side half of both. The API-side half of #44 — whether
an unconfigured install should be dispatching those probes at all, or reading the
resulting skip as a hard error rather than as a pass — is not here; see the issue.
*/

// reportUnrunnableTestGroup submits the canonical report for a `script_test_run`
// group whose image is not configured on this installation (#44).
//
// **Why a report rather than an error.** A missing `test-unit.sh` in the
// project's repo is a legitimate, documented state meaning "this group is
// disabled" (ADR 015 item 10), and the runner reports it as an empty, skipped
// group. An unconfigured image is a different cause of the same observable
// outcome — the group did not run — and reporting it the same way is what keeps
// one broken setting from failing every feature in the install. That is the
// trade-off #44 names explicitly, and this is the shape it recommends.
//
// **The report says which cause it was.** `summary` is the only field in the
// canonical schema that can carry the distinction, and the Testing tab renders
// it against the run, so an operator looking at a skipped group is told that the
// installation is missing an image rather than left to infer that a project
// chose not to have unit tests. `skipped: 1, total: 1` says "one group was there
// to run and was skipped", which is more accurate than the all-zero report a
// missing script produces ("there was nothing to run") and still satisfies the
// schema's `total >= passed+failed+skipped` rule.
//
// A failed post is logged and not fatal: the caller's job outcome is decided by
// the report the API already has or does not have, and turning a reporting
// failure into a different failure would replace an accurate record with a
// misleading one.
func reportUnrunnableTestGroup(
	ctx context.Context,
	job *queue.Job,
	cfg Config,
	cause string,
) error {
	skipped := 1
	total := 1
	passed := 0
	failed := 0

	summary := "Skipped: " + cause + ". No verification was performed for this group."
	if job.TestGroup != nil && *job.TestGroup != "" {
		summary = "Skipped (" + *job.TestGroup + "): " + cause + ". No verification was performed for this group."
	}

	if err := cfg.APIClient.PostJobEvent(ctx, job.ID, rpc.CuratedEvent{
		Type:    rpc.EventSubmitTestReport,
		Summary: summary,
		Passed:  &passed,
		Failed:  &failed,
		Skipped: &skipped,
		Total:   &total,
		// `FailingTests` is deliberately not set, and cannot be: the relay's
		// request struct tags it `omitempty`, so an empty slice is dropped on the
		// wire. That is the right shape here anyway — the API's schema treats the
		// field as optional, and "no failures to list" is exactly what a skipped
		// group has to say.
	}); err != nil {
		log.Printf("worker: failed to report the skipped test group for job %s: %v", job.ID, err)
		return err
	}
	return nil
}

/*
skipWhenScriptImageUnconfigured decides whether a `script_test_run` job can run
at all, and reports the group as skipped when it cannot.

**Why this is not #29's pull-secret case.** These are different gaps with
different fixes, and conflating them would send an operator to the wrong one:

  - no `SCRIPT_TEST_RUN_IMAGE` configured → the installation never asked for this
    image, so there is nothing to pull and no credential would help;
  - an image configured but unpullable → a credential is missing (#29), which
    `k8s.CheckImagePullPrerequisites` and `k8s.ImagePullFailure` report.

The first is handled here, before any pod exists. The second is handled by the
shared preflight and by the wait-path detection, because failing it with the
setup message is the correct outcome: an operator who *configured* the image
expects it to work, and silently skipping their tests would hide a real mistake.

Returns `handled` true when the caller must not run the job at all — either the
group was recorded as skipped (`err` nil, so the job completes) or the report
itself could not be posted (`err` non-nil, so the job fails; see
`reportUnrunnableTestGroup` for why a lost report is the one case that still
fails). Returns `handled` false for every other job kind, and for a
`script_test_run` whose image *is* configured.
*/
func skipWhenScriptImageUnconfigured(
	ctx context.Context,
	job *queue.Job,
	cfg Config,
) (handled bool, err error) {
	if job.Kind != queue.KindScriptTestRun {
		return false, nil
	}
	if configuredImage(cfg, job.Kind) != "" {
		return false, nil
	}

	cause := "this installation has no script_test_run image configured " +
		"(set SCRIPT_TEST_RUN_IMAGE on the Orchestrator — see orchestrator/docs/overview/setup.md)"

	log.Printf(
		"worker: job %s (kind=%s group=%s): reporting the group as skipped — %s",
		job.ID, job.Kind, testGroupName(job), cause,
	)
	return true, reportUnrunnableTestGroup(ctx, job, cfg, cause)
}

// testGroupName renders a job's unit/integration group for a log line.
func testGroupName(job *queue.Job) string {
	if job.TestGroup == nil || *job.TestGroup == "" {
		return "unknown"
	}
	return *job.TestGroup
}

// preflightImagePull is #29's pre-emptive half: it warns, before the pod exists,
// when the image this job is about to run cannot plausibly be pulled.
//
// Only warns. The evidence-based `k8s.ImagePullFailure` path is what actually
// fails a job, for the reason given in `k8s/imagepull.go`: the check here is an
// inference from the registry host and the absence of a credential, and blocking
// real work on an inference would be a worse bug than the one being fixed. A
// wrong guess costs one log line, once per namespace per process (see
// `k8s.PullWarningOnce`).
//
// Called after the namespace exists (the check reads it) and after the image is
// resolved, but before anything expensive: no pod has been created and no GitHub
// token has been minted, so an operator reading the log sees this before the
// confusing failure rather than after it.
func preflightImagePull(ctx context.Context, client *k8s.Client, job *queue.Job, namespace string, cfg Config) {
	image, _ := resolveAgentImage(cfg, job.Kind)
	if image == "" {
		return
	}
	finding := cfg.PullPreflight.Check(ctx, client.Interface, namespace, image, cfg.ImagePullSecret)
	if finding == nil {
		return
	}
	log.Printf(
		"worker: WARNING this job's image cannot be pulled as configured (job=%s kind=%s image=%s): %v",
		job.ID, job.Kind, image, finding,
	)
}
