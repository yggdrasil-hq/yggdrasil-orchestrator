package worker

import (
	"context"
	"log"
	"net/url"
	"strings"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/imagebuild"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/queue"
)

/*
Issue #19 / ADR 003 §12/§14: a preview should serve the branch under
construction, not the image the chart happens to declare.

Before this, a preview applied the project's chart with no image override, so it
showed `nginxdemos/hello` — the scaffolded template's placeholder — regardless of
what the branch contained. That is the one thing a preview exists not to do
("look at my in-progress build").

**Scope, and what this deliberately does not do.** ADR 003 §12 says each *linked
repository* provides its own Dockerfile, so a complete implementation builds every
linked repo. This builds the **primary** repository only: a project whose app is
the primary repo gets a correct preview, and one that splits its runtime across
sub-repositories does not yet. Building all of them is the same call in a loop,
but the multi-image half needs a chart convention for which values key carries
which repository's image, and that convention does not exist — inventing it here
would be guessed API surface rather than implementation. Reported on the issue
rather than implied to work.

**Failure is never fatal to the job.** The preview is additive to job kinds that
worked before it existed, so a registry that is down, a Dockerfile that does not
compile, or a branch that has not been pushed must all degrade to the chart's
declared image with the reason logged. Same posture `startJobPreview` already
takes for a preview that cannot be created at all.
*/

// previewImageValues returns the Helm values that make a preview release run a
// specific image, or nil when there is nothing to override.
//
// The keys are the scaffolded chart's own (`image.repository` / `image.tag` — see
// internal/helm/charts/placeholder/values.yaml), which is why this returns a
// values map rather than an image string: the chart interpolates the two parts
// separately, so the contract belongs to the chart rather than to this function.
//
// Nil for a digest-pinned reference. A chart that interpolates `repository:tag`
// cannot express a digest, and rendering one as `repo@sha256:...:` would deploy
// something visibly broken; leaving the override out falls back to the chart's
// image, which is a preview that works and is honest about what it shows.
func previewImageValues(image string) map[string]interface{} {
	if image == "" {
		return nil
	}
	repository, tag := imagebuild.SplitImage(image)
	if repository == "" || tag == "" {
		log.Printf("worker: not using %q for a preview: the chart needs a repository and a tag", image)
		return nil
	}
	return map[string]interface{}{
		"image": map[string]interface{}{
			"repository": repository,
			"tag":        tag,
		},
	}
}

// buildPreviewImage builds and pushes the image a preview should run and returns
// its reference, or "" when there is nothing to build or nowhere to push.
//
// Every "nothing to build" outcome is a skip rather than an error, because each
// is a state the operator either expects or cannot fix mid-run: no registry
// configured, no repository linked, no Dockerfile yet, and — the common one,
// because a preview is created when a job *starts* — a ref that is not on the
// remote because the agent has not pushed it.
func buildPreviewImage(
	ctx context.Context,
	client *k8s.Client,
	job *queue.Job,
	namespace string,
	cfg Config,
) string {
	if cfg.ImageRegistry == "" {
		// Not logged: an install with no registry would otherwise log this for
		// every preview, and "a feature you have not configured is off" is not
		// news. See Config.ImageRegistry for why empty is the default.
		return ""
	}
	if client == nil {
		log.Printf("worker: no Kubernetes client available to build a preview image for job %s", job.ID)
		return ""
	}

	spec, err := fetchJobSpec(ctx, cfg, job)
	if err != nil {
		log.Printf("worker: cannot build a preview image for job %s: %v", job.ID, err)
		return ""
	}

	primary, ok := primaryRepo(spec.Repos)
	if !ok {
		log.Printf("worker: job %s has no primary repository to build a preview image from", job.ID)
		return ""
	}

	repoName := repoNameFromCloneURL(primary.CloneURL)
	ref := previewBuildRef(job, spec)
	destination := imagebuild.Destination(cfg.ImageRegistry, job.ProjectID, repoName, ref)

	result, err := imagebuild.Build(ctx, imagebuild.Config{
		Clientset:          client.Interface,
		Namespace:          namespace,
		RepoName:           repoName,
		CloneURL:           primary.CloneURL,
		GitRef:             ref,
		GithubToken:        spec.GithubToken,
		Destination:        destination,
		RegistryAuthSecret: cfg.ImageBuildAuthSecret,
		JobID:              job.ID,
	})
	if err != nil {
		// Logged, not returned: the preview still comes up on the chart's image
		// and the job is unaffected. The operator sees the reason here rather
		// than a preview that silently serves the wrong app.
		log.Printf("worker: WARNING could not build a preview image for job %s: %v", job.ID, err)
		return ""
	}
	if !result.Built {
		log.Printf("worker: skipped the preview image build for job %s: %s", job.ID, result.Reason)
		return ""
	}

	log.Printf("worker: built the preview image %s for job %s", result.Image, job.ID)
	return result.Image
}

// previewBuildRef picks the git ref a preview should build.
//
// The feature's branch when the job has one; otherwise the ref the job was
// dispatched against, defaulting to main. This mirrors `agentRepoEnv`'s rule and
// the API's dispatch, so the image a preview runs and the code an agent is
// working on cannot disagree about which commit is being looked at.
//
// `spec.Branch` is preferred over `job.Ref` when both are present because it is
// what the API resolved for this job *and* this feature, while `job.Ref` is the
// dispatch-time value that a scheduled run leaves null.
func previewBuildRef(job *queue.Job, spec apiclient.FeatureSpec) string {
	if spec.Branch != "" {
		return spec.Branch
	}
	if job.Ref != nil && *job.Ref != "" {
		return *job.Ref
	}
	return "main"
}

// primaryRepo returns the project's primary repository, or ok=false when none is
// linked (an init that has not finished, or a repo unlinked after the job was
// queued).
func primaryRepo(repos []apiclient.FeatureSpecRepo) (apiclient.FeatureSpecRepo, bool) {
	for _, repo := range repos {
		if repo.IsPrimary {
			return repo, true
		}
	}
	return apiclient.FeatureSpecRepo{}, false
}

// repoNameFromCloneURL derives a repository's name from its clone URL, for the
// image's path segment and the build Job's name.
//
// Derived rather than read, because the API's FeatureSpecRepo carries only the
// URL and whether it is primary — there is no name field. The last path segment
// with a `.git` suffix trimmed is the name GitHub itself uses, which is what
// makes the pushed image recognisable in a registry listing.
//
// Three URL shapes have to work, because all three are real clone URLs: the
// https form, a trailing-slash variant of it, and the scp-like `git@host:owner/
// repo` form. The host must never end up as the answer — a URL with no path is a
// malformed input, and pushing to `.../github.com` would be a silent
// misdirection rather than a failure.
func repoNameFromCloneURL(cloneURL string) string {
	trimmed := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(cloneURL), "/"), ".git")
	if trimmed == "" {
		return "app"
	}

	switch {
	case strings.Contains(trimmed, "://"):
		parsed, err := url.Parse(trimmed)
		if err != nil {
			return "app"
		}
		trimmed = strings.Trim(parsed.Path, "/")
	case strings.Contains(trimmed, "@") && strings.Contains(trimmed, ":"):
		// scp-like: git@github.com:owner/repo. The path is whatever follows the
		// host-colon, which is the only colon before the path starts.
		trimmed = strings.Trim(strings.SplitN(trimmed, ":", 2)[1], "/")
	}

	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	name := parts[len(parts)-1]
	if name == "" {
		return "app"
	}
	return name
}
