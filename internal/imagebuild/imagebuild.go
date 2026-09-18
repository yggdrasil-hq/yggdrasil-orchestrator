// Package imagebuild builds and pushes an image for one repository at one git
// ref, inside the target cluster.
//
// ADR 003 §12/§14 describe this contract — each repository provides its own
// Dockerfile, images live in a per-project registry namespace — and until now
// nothing implemented it. A preview deployment therefore showed the project's
// app as its *chart* declares it (`nginxdemos/hello` in the scaffolded
// template) rather than the branch under construction, which is the one thing a
// preview exists to show (issue #19).
//
// **Why a Job rather than a Docker daemon.** ADR 003 §6 makes the cluster the
// sandbox boundary, and the Orchestrator targets Kubernetes rather than a
// container runtime socket. Mounting a daemon socket into a build would hand
// whatever runs there control of the node, so the build runs as an ordinary
// in-cluster Job instead: a git clone into an emptyDir, then a rootless
// userspace build of that context against the project's registry.
//
// **Why the clone is its own container.** The obvious shortcut is a build tool
// that fetches a git context itself, which means learning that tool's own
// credential mechanism. Cloning with `git` first reuses the arrangement the
// agent images already use and which is already proven against this project's
// private repositories (the token in a URL rewrite, never in a committed remote),
// and it makes the "this repository has no Dockerfile" case a plain filesystem
// check rather than a tool-specific failure to decode.
//
// **The two containers are not interchangeable** (issue #69). The context
// container runs `alpine/git`, which has a shell; the build container runs
// Kaniko's executor image, which is distroless and has none. So every guard and
// every conditional belongs in the context container, and the build container is
// invoked through Kaniko's own CLI and nothing else. Treating that split as
// arbitrary — putting a shell script in the build container, say — produces a pod
// that fails at container start rather than a build that fails, which is a much
// less obvious failure to read. `TestBuildJob_NoContainerInvokesAShellItsImageLacks`
// is what keeps that honest.
package imagebuild

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultExecutorImage is the rootless image builder. Kaniko runs as an
	// ordinary user with no daemon and no privileged mode, which is what makes it
	// usable here at all — the alternative (a Buildah/dockerd sidecar) needs a
	// privilege the job namespace does not grant.
	DefaultExecutorImage = "gcr.io/kaniko-project/executor:v1.23.2"

	// DefaultCloneImage is used only to fetch the build context. `alpine/git` is a
	// few MB and does nothing but clone.
	DefaultCloneImage = "alpine/git:2.45.2"

	// DefaultDockerfilePath is the Dockerfile the contract assumes at the
	// repository root (ADR 003 §12).
	DefaultDockerfilePath = "Dockerfile"

	// buildContextDir is where the clone lands. Both containers share it through
	// one emptyDir, which is the only handoff between them.
	buildContextDir = "/workspace"

	// prepareContainerName and buildContainerName are the two containers in a
	// build Job, named so that the code which resolves a pod's outcome by
	// matching on them (buildOutcome, build.go) cannot drift from the code that
	// creates them. A rename in one place only would silently misattribute a
	// failure to the wrong step — the operator would be told the build failed
	// when the context was never prepared, or the reverse.
	prepareContainerName = "prepare"
	buildContainerName   = "build"

	// kanikoExecutorPath is the executor binary inside the executor image,
	// which the build container invokes directly (see buildJob). Named rather
	// than inlined so the test that guards that invocation looks for the same
	// string the code uses.
	kanikoExecutorPath = "/kaniko/executor"

	// NoDockerfileExitCode is what the build container exits with when the
	// checkout has no Dockerfile at the contract's path. A distinct code rather
	// than a message, because the difference between "this project has not added
	// a Dockerfile yet" and "the build failed" decides whether a preview falls
	// back to the chart's image or the job is reported as broken — and a log line
	// is not something the Orchestrator can act on. Chosen above the range an
	// ordinary command failure uses by accident.
	NoDockerfileExitCode = 42

	// RefUnavailableExitCode is what the clone step exits with when the requested
	// ref is not a branch or tag on the remote yet.
	//
	// This is the **common** case rather than an edge one, and it is why the code
	// exists: a preview is created when a job *starts*, and a `feature_build`'s
	// branch is not pushed until the agent finishes. So on a feature's first
	// build the branch genuinely does not exist yet, and on every build of a
	// feature whose ref was never pushed it never will. Treating that as a build
	// failure would replace a working preview with a failed-looking one, for a
	// reason the operator can do nothing about mid-run.
	RefUnavailableExitCode = 43

	// defaultBuildTimeout bounds one build. A first build of a real application
	// compiles dependencies and can legitimately take minutes; this is generous
	// enough for that and still bounds a wedged build, which is what matters —
	// a preview whose build never finishes would hold the job open forever.
	defaultBuildTimeout = 20 * time.Minute
)

// buildJobName builds the deterministic name of a build Job for one job id and
// repository. Deterministic so a retry replaces its own Job rather than piling
// up, and prefixed so it is identifiable in a namespace full of job pods.
func buildJobName(jobID, repoName string) string {
	return sanitizeName("imgbuild-" + repoName + "-" + jobID)
}

// sanitizeName lowercases and replaces anything Kubernetes does not accept in a
// name, then trims to the length limit. Repository names are usually already
// valid; the branch-in-a-tag case below is where this earns its keep.
func sanitizeName(value string) string {
	cleaned := strings.ToLower(value)
	cleaned = regexp.MustCompile(`[^a-z0-9.-]+`).ReplaceAllString(cleaned, "-")
	cleaned = strings.Trim(cleaned, "-.")
	if len(cleaned) > 63 {
		cleaned = strings.Trim(cleaned[:63], "-.")
	}
	return cleaned
}

// sanitizeTag makes a git ref usable as an image tag: a tag may not contain `/`
// or `:`, so `yggdrasil/feature-abc` has to become something else. Deterministic
// (the same ref always yields the same tag) so repeated builds of one branch
// reuse the tag rather than filling the registry with a new one per run.
//
// A purely sanitising transform, not a hash: the tag is meant to be readable in
// a registry listing so an operator can tell which branch an image is, which a
// digest would defeat.
func sanitizeTag(ref string) string {
	tag := regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(ref, "-")
	tag = strings.TrimLeft(tag, "-_.")
	if len(tag) > 128 {
		tag = tag[:128]
	}
	if tag == "" {
		return "latest"
	}
	return tag
}

// Destination computes the image reference a build of `ref` should push to and
// the chart values that would make a deployment use it.
//
// The registry namespace is derived from the project id rather than its slug,
// matching ADR 003 §14's "namespaced per project" and this suite's habit of
// keying cluster-side objects by the immutable id (`proj-<id>` namespaces,
// `job-<id>` pods). A slug can be renamed.
//
// The repository name is the project's own repository name, so an install
// pushing several projects into one registry can still tell them apart in a
// listing, and two linked repositories of one project produce two images rather
// than colliding on one.
func Destination(registry, projectID, repoName, ref string) (image string) {
	return fmt.Sprintf(
		"%s/proj-%s/%s:%s",
		strings.TrimSuffix(registry, "/"),
		sanitizeName(projectID),
		sanitizeName(repoName),
		sanitizeTag(ref),
	)
}

// SplitImage separates an image reference into the repository and tag parts the
// scaffolded chart's values expect (`image.repository` and `image.tag`). The
// chart is fixed and takes them separately, so the caller cannot pass the whole
// reference as one string.
func SplitImage(image string) (repository, tag string) {
	// A digest-pinned reference has no tag to split; leave it whole rather than
	// silently dropping the digest, and let the tag stay empty so a chart that
	// interpolates `repo:tag` produces something visibly wrong instead of the
	// wrong image.
	if strings.Contains(image, "@") {
		return strings.TrimSuffix(image, "/"), ""
	}
	if idx := strings.LastIndex(image, ":"); idx > strings.LastIndex(image, "/") {
		return image[:idx], image[idx+1:]
	}
	return image, ""
}

// Config is one build.
type Config struct {
	Clientset kubernetes.Interface
	Namespace string

	// RepoName is the repository being built — the primary repo's name. Used for
	// the Job's name and the pushed repository's name.
	RepoName string
	// CloneURL is the repository to clone, without the token embedded.
	CloneURL string
	// GitRef is the branch or tag the build is of. Not a commit: the clone is
	// shallow and single-ref (`git clone --depth 1 --branch`), which a bare SHA
	// cannot express without fetching history first. Narrowed deliberately rather
	// than left as the WIP's "branch, tag or commit" — every caller in this suite
	// passes a branch, and a ref that is not a branch or tag is reported as
	// RefUnavailableExitCode rather than failing opaquely.
	GitRef string
	// GithubToken authenticates the clone. Required for a private repository.
	GithubToken string

	// Destination is the full image reference to push.
	Destination string
	// RegistryAuthSecret names a docker-config object in Namespace that carries
	// push credentials, or "" for a registry that needs none (the bundled
	// `registry:2` of ADR 003 §14 takes unauthenticated pushes inside the
	// cluster).
	RegistryAuthSecret string

	ExecutorImage  string
	CloneImage     string
	DockerfilePath string

	// JobID is the Orchestrator job this build belongs to, used for the build
	// Job's name and for logging.
	JobID string

	Timeout time.Duration
}

func (c Config) dockerfilePath() string {
	if c.DockerfilePath == "" {
		return DefaultDockerfilePath
	}
	return c.DockerfilePath
}

func (c Config) executorImage() string {
	if c.ExecutorImage == "" {
		return DefaultExecutorImage
	}
	return c.ExecutorImage
}

func (c Config) cloneImage() string {
	if c.CloneImage == "" {
		return DefaultCloneImage
	}
	return c.CloneImage
}

func (c Config) timeout() time.Duration {
	if c.Timeout <= 0 {
		return defaultBuildTimeout
	}
	return c.Timeout
}

// buildJob constructs the build Job. Pure, so what the cluster is actually asked
// to run can be asserted without a cluster.
func buildJob(cfg Config) *batchv1.Job {
	var authVolume *corev1.Volume
	var authMount *corev1.VolumeMount
	if cfg.RegistryAuthSecret != "" {
		// Kaniko reads its push credentials from the standard docker config path.
		authVolume = &corev1.Volume{
			Name: "registry-auth",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: cfg.RegistryAuthSecret},
			},
		}
		authMount = &corev1.VolumeMount{Name: "registry-auth", MountPath: "/kaniko/.docker"}
	}

	// The context step is an init container rather than a second app container: the
	// build must not start against a half-populated context, and `initContainers`
	// is Kubernetes' own expression of that ordering — no polling, no readiness
	// signal to invent.
	//
	// It carries **both** reasons a build is skipped rather than failing, which is
	// why it is named `prepare` rather than `clone`: it fetches the ref and then
	// decides whether there is anything buildable at the other end. The two
	// belong together, and issue #69 is why they are here — see below.
	//
	// The token goes in through git's URL rewrite, exactly as the agent images do
	// it: nothing is written to a remote, and the command line of this container
	// is the only place it appears (a Job's container command is readable by
	// anyone who can read Jobs in the namespace, which is the same audience that
	// can read the project's secrets).
	//
	// `git ls-remote` runs first, before the clone, so "this ref does not exist on
	// the remote" is a definite answer rather than a message to pattern-match out
	// of git's stderr — and so a missing ref costs one cheap round trip instead of
	// a failed clone. See RefUnavailableExitCode for why it matters.
	//
	// The Dockerfile check runs last, against the context just materialised.
	// **It has to live here rather than in the build container** (issue #69): that
	// container is Kaniko's executor image, which is distroless, so the
	// `if [ ! -f ... ]` guard it used to carry could not survive there — the
	// container could not start at all (`StartError: exec: "sh": executable file
	// not found in $PATH`). A container that cannot run a guard cannot report what
	// the guard would have said. Here it costs nothing: this container already has
	// a shell, and the emptyDir means it already sees the files it is checking.
	prepareScript := fmt.Sprintf(`
set -eu
git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"
if ! git ls-remote --exit-code --heads --tags "${CLONE_URL}" "${GIT_REF}" >/dev/null 2>&1; then
  echo "ref ${GIT_REF} is not a branch or tag on the remote yet"
  exit %d
fi
git clone --depth 1 --branch "${GIT_REF}" "${CLONE_URL}" %s
git -C %s log -1 --format=%%H > %s/.yggdrasil-commit
if [ ! -f "%s/%s" ]; then
  echo "no %s at the repository root; nothing to build"
  exit %d
fi
`, RefUnavailableExitCode, buildContextDir, buildContextDir, buildContextDir,
		buildContextDir, cfg.dockerfilePath(), cfg.dockerfilePath(), NoDockerfileExitCode)

	// The build container runs Kaniko through its own CLI, with no shell involved
	// anywhere.
	//
	// That is not a style preference — it is the only thing that works (issue
	// #69). Kaniko's executor image is distroless: it contains the executor and
	// nothing else, *including no shell*. Configuring this container as
	// `sh -c <script>` therefore fails at container start, before any build logic
	// runs, for every repository regardless of its Dockerfile. Distroless is
	// Kaniko's choice and a good one for a container whose whole job is to execute
	// a Dockerfile, so the fix is to stop asking it for something it does not have
	// rather than to swap in a builder that ships a shell.
	//
	// `Command` names the binary rather than relying on the image's entrypoint.
	// The entrypoint *is* the executor today, so `Args` alone would work — but an
	// explicit `Command` stays honest if `ExecutorImage` is ever pointed at a
	// variant, and it is what the tests assert. (`gcr.io/kaniko-project/executor:debug`
	// is the shell-bearing variant; nothing here uses it, and using it would
	// reintroduce exactly the thing this container no longer depends on.)
	//
	// The cache repository is derived with SplitImage rather than by cutting the
	// destination at its first colon: a registry that carries a port (`reg:5000`)
	// would otherwise yield a cache repository of just `reg`, pushing cache layers
	// to a registry that is not the one configured.
	destinationRepo, _ := SplitImage(cfg.Destination)

	backoffLimit := int32(0)
	ttl := int32(300)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildJobName(cfg.JobID, cfg.RepoName),
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				// Lets an operator find a build for a job, and lets a future sweep
				// collect builds left by a crashed replica.
				"yggdrasil.io/job":  cfg.JobID,
				"yggdrasil.io/kind": "image-build",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					InitContainers: []corev1.Container{{
						Name:    prepareContainerName,
						Image:   cfg.cloneImage(),
						Command: []string{"sh", "-c", prepareScript},
						Env: []corev1.EnvVar{
							{Name: "CLONE_URL", Value: cfg.CloneURL},
							{Name: "GIT_REF", Value: cfg.GitRef},
							{Name: "GITHUB_TOKEN", Value: cfg.GithubToken},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "build-context", MountPath: buildContextDir}},
						Resources:    cloneResources(),
					}},
					Containers: []corev1.Container{{
						Name:    buildContainerName,
						Image:   cfg.executorImage(),
						Command: []string{kanikoExecutorPath},
						Args: []string{
							"--context=dir://" + buildContextDir,
							"--dockerfile=" + buildContextDir + "/" + cfg.dockerfilePath(),
							"--destination=" + cfg.Destination,
							"--cache=true",
							"--cache-repo=" + destinationRepo + "-cache",
						},
						VolumeMounts: append(
							[]corev1.VolumeMount{{Name: "build-context", MountPath: buildContextDir}},
							optionalMount(authMount)...,
						),
						Resources: buildResources(),
					}},
					Volumes: append(
						[]corev1.Volume{{
							Name: "build-context",
							// An emptyDir is the whole handoff between the two
							// containers and disappears with the pod: a build's
							// context is not state worth keeping, and the pushed
							// image is the artifact.
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
						}},
						optionalVolume(authVolume)...,
					),
				},
			},
		},
	}
}

func optionalMount(mount *corev1.VolumeMount) []corev1.VolumeMount {
	if mount == nil {
		return nil
	}
	return []corev1.VolumeMount{*mount}
}

func optionalVolume(volume *corev1.Volume) []corev1.Volume {
	if volume == nil {
		return nil
	}
	return []corev1.Volume{*volume}
}

// buildResources sizes the build container. A Dockerfile build is CPU- and
// memory-hungry in a way the agent pods are not (it may compile a whole
// dependency tree), so it gets its own numbers rather than the package defaults
// — and a limit low enough to OOM a real build would surface as a build failure
// with no useful message, which is exactly the kind of thing that wastes an
// afternoon.
func buildResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("300m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("3Gi"),
		},
	}
}

func cloneResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}
