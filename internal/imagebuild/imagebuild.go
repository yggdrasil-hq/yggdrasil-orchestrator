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
package imagebuild

import (
	"context"
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

	// NoDockerfileExitCode is what the build container exits with when the
	// checkout has no Dockerfile at the contract's path. A distinct code rather
	// than a message, because the difference between "this project has not added
	// a Dockerfile yet" and "the build failed" decides whether a preview falls
	// back to the chart's image or the job is reported as broken — and a log line
	// is not something the Orchestrator can act on. Chosen above the range an
	// ordinary command failure uses by accident.
	NoDockerfileExitCode = 42

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
// or `:` and must not begin with a separator, so `yggdrasil/feature-abc` has to
// become something else. Deterministic (the same ref always yields the same tag)
// so repeated builds of one branch reuse the tag rather than filling the
// registry with a new one per run.
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
	// GitRef is the branch, tag or commit the build is of.
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

	// The clone is an init container rather than a second app container: the
	// build must not start against a half-populated context, and `initContainers`
	// is Kubernetes' own expression of that ordering — no polling, no readiness
	// signal to invent.
	//
	// The token goes in through git's URL rewrite, exactly as the agent images do
	// it: nothing is written to a remote, and the command line of this container
	// is the only place it appears (a Job's container command is readable by
	// anyone who can read Jobs in the namespace, which is the same audience that
	// can read the project's secrets).
	cloneScript := fmt.Sprintf(`
set -eu
git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"
git clone --depth 1 --branch "${GIT_REF}" "${CLONE_URL}" %s
git -C %s log -1 --format=%%H > %s/.yggdrasil-commit
`, buildContextDir, buildContextDir, buildContextDir)

	// The build container decides for itself whether there is anything to build.
	// Exiting with NoDockerfileExitCode is how it says "this repository has no
	// Dockerfile at the contract's path" — a state a freshly scaffolded project
	// is in, and one the caller must handle by falling back to the chart's image
	// rather than by failing the job.
	buildScript := fmt.Sprintf(`
set -eu
if [ ! -f "%s/%s" ]; then
  echo "no %s at the repository root; nothing to build"
  exit %d
fi
exec /kaniko/executor \
  --context=dir://%s \
  --dockerfile=%s/%s \
  --destination=%s \
  --cache=true \
  --cache-repo=%s-cache
`, buildContextDir, cfg.dockerfilePath(), cfg.dockerfilePath(), NoDockerfileExitCode,
		buildContextDir, buildContextDir, cfg.dockerfilePath(), cfg.Destination,
		strings.Split(cfg.Destination, ":")[0])

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
						Name:    "clone",
						Image:   cfg.cloneImage(),
						Command: []string{"sh", "-c", cloneScript},
						Env: []corev1.EnvVar{
							{Name: "CLONE_URL", Value: cfg.CloneURL},
							{Name: "GIT_REF", Value: cfg.GitRef},
							{Name: "GITHUB_TOKEN", Value: cfg.GithubToken},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "build-context", MountPath: buildContextDir}},
						Resources:    cloneResources(),
					}},
					Containers: []corev1.Container{{
						Name:         "build",
						Image:        cfg.executorImage(),
						Command:      []string{"sh", "-c", buildScript},
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
