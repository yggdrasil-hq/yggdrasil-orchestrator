package imagebuild

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"
)

/*
Issue #19. The parts of an in-cluster image build that are worth testing are the
ones that decide *what the cluster is asked to run* and *what a build's outcome
means* — both of which are pure here, so they are asserted without a cluster.

The parts that genuinely need one (does Kaniko push, does the registry accept the
push) cannot be covered by these, and are not pretended to be.
*/

// --- naming and references -------------------------------------------------

func TestDestination_NamespacesPerProjectAndTagsByRef(t *testing.T) {
	got := Destination("registry.local:5000", "b51e1313-d315-47bf-be25-038acc16d6a4", "luffy-portfolio", "main")

	if want := "registry.local:5000/proj-b51e1313-d315-47bf-be25-038acc16d6a4/luffy-portfolio:main"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

// ADR 003 §14 puts images in a per-project namespace, and this suite keys
// cluster-side objects by immutable id everywhere else (`proj-<id>` namespaces,
// `job-<id>` pods). A slug can be renamed; an id cannot.
func TestDestination_KeysOnProjectIDNotSlug(t *testing.T) {
	got := Destination("registry.local", "b51e1313-d315-47bf-be25-038acc16d6a4", "repo", "main")

	if strings.Contains(got, "luffy") {
		t.Fatalf("expected the id and not a slug in %q", got)
	}
	if !strings.Contains(got, "proj-b51e1313") {
		t.Fatalf("expected the project id in the path, got %q", got)
	}
}

// The same ref must always produce the same reference, so repeated builds of one
// branch overwrite one tag instead of filling the registry with a new tag per
// run — which is what makes a preview's image findable after the fact.
func TestDestination_IsDeterministic(t *testing.T) {
	first := Destination("r", "p", "repo", "yggdrasil/feature-abc")
	second := Destination("r", "p", "repo", "yggdrasil/feature-abc")

	if first != second {
		t.Fatalf("expected the same reference twice, got %q then %q", first, second)
	}
}

func TestDestination_ToleratesATrailingSlashOnTheRegistry(t *testing.T) {
	withSlash := Destination("registry.local/", "p", "repo", "main")
	without := Destination("registry.local", "p", "repo", "main")

	if withSlash != without {
		t.Fatalf("expected %q and %q to agree", withSlash, without)
	}
	if strings.Contains(withSlash, "//proj") {
		t.Fatalf("expected no doubled separator, got %q", withSlash)
	}
}

// A branch name contains `/`, which an image tag may not.
func TestDestination_SanitisesABranchRefIntoATag(t *testing.T) {
	got := Destination("r", "p", "repo", "yggdrasil/feature-c20fc193-f4b9")

	if !strings.HasSuffix(got, ":yggdrasil-feature-c20fc193-f4b9") {
		t.Fatalf("expected a slash-free tag, got %q", got)
	}
}

func TestSanitizeTag(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want string
	}{
		{"main", "main"},
		{"yggdrasil/feature-abc", "yggdrasil-feature-abc"},
		{"/leading-slash", "leading-slash"},
		{"trailing/", "trailing-"},
		{"a b c", "a-b-c"},
		{"", "latest"},
		{"///", "latest"},
	} {
		if got := sanitizeTag(tc.ref); got != tc.want {
			t.Errorf("sanitizeTag(%q): expected %q, got %q", tc.ref, tc.want, got)
		}
	}
}

// A tag longer than 128 characters is rejected by the registry, so the ref has
// to be trimmed rather than the push failing at the far end.
func TestSanitizeTag_TrimsToTheRegistryLimit(t *testing.T) {
	got := sanitizeTag(strings.Repeat("a", 200))

	if len(got) != 128 {
		t.Fatalf("expected 128 characters, got %d", len(got))
	}
}

func TestSanitizeName_LowercasesAndReplacesIllegalCharacters(t *testing.T) {
	if got := sanitizeName("My_Repo.Name"); got != "my-repo.name" {
		t.Fatalf("expected \"my-repo.name\", got %q", got)
	}
	if got := sanitizeName("--weird__name--"); got != "weird-name" {
		t.Fatalf("expected the leading/trailing separators trimmed, got %q", got)
	}
}

// Kubernetes rejects a label longer than 63 characters, so a long repository
// name has to be trimmed rather than rejected by the API server.
func TestSanitizeName_TrimsToTheKubernetesLabelLimit(t *testing.T) {
	got := sanitizeName(strings.Repeat("a", 100))

	if len(got) > 63 {
		t.Fatalf("expected at most 63 characters, got %d (%q)", len(got), got)
	}
}

func TestBuildJobName_IsDeterministicAndPrefixed(t *testing.T) {
	first := buildJobName("job-1", "luffy-portfolio")
	second := buildJobName("job-1", "luffy-portfolio")

	if first != second {
		t.Fatalf("expected a stable name, got %q then %q", first, second)
	}
	// Deterministic so a retry replaces its own Job rather than piling up.
	if !strings.HasPrefix(first, "imgbuild-") {
		t.Fatalf("expected the imgbuild prefix, got %q", first)
	}
	// And distinct per repository, since one project builds several.
	if buildJobName("job-1", "api") == buildJobName("job-1", "web") {
		t.Fatal("expected different repositories to produce different job names")
	}
}

func TestSplitImage(t *testing.T) {
	for _, tc := range []struct {
		image string
		repo  string
		tag   string
	}{
		{"registry.local/proj-p/repo:main", "registry.local/proj-p/repo", "main"},
		// The case that distinguishes a real split from cutting at the first
		// colon: a registry with a port. Getting this wrong pushes cache layers
		// to a registry that is not the one configured.
		{"registry.local:5000/proj-p/repo:main", "registry.local:5000/proj-p/repo", "main"},
		{"repo-without-registry:tag", "repo-without-registry", "tag"},
		{"repo-without-tag", "repo-without-tag", ""},
	} {
		repo, tag := SplitImage(tc.image)
		if repo != tc.repo || tag != tc.tag {
			t.Errorf("SplitImage(%q): expected (%q, %q), got (%q, %q)", tc.image, tc.repo, tc.tag, repo, tag)
		}
	}
}

// The chart's Deployment interpolates `image.repository:image.tag`, so a
// digest-pinned reference cannot be represented. Returning it whole with an
// empty tag makes that visibly wrong rather than silently the wrong image — the
// alternative (dropping the digest) would ship something nobody asked for.
func TestSplitImage_LeavesADigestPinnedReferenceVisiblyWrong(t *testing.T) {
	repo, tag := SplitImage("registry.local/proj-p/repo@sha256:abc123")

	if !strings.Contains(repo, "@sha256:") {
		t.Fatalf("expected the digest kept in the repository part, got %q", repo)
	}
	if tag != "" {
		t.Fatalf("expected no tag, got %q", tag)
	}
}

// --- the Job the cluster is asked to run ----------------------------------

func baseConfig() Config {
	return Config{
		Clientset:   fake.NewSimpleClientset(),
		Namespace:   "proj-1",
		RepoName:    "luffy-portfolio",
		CloneURL:    "https://github.com/acme/web",
		GitRef:      "yggdrasil/feature-abc",
		GithubToken: "ghs_test",
		Destination: "registry.local/proj-1/luffy-portfolio:yggdrasil-feature-abc",
		JobID:       "job-1",
	}
}

func TestBuildJob_ClonesBeforeBuilding(t *testing.T) {
	job := buildJob(baseConfig())

	spec := job.Spec.Template.Spec
	if len(spec.InitContainers) != 1 {
		t.Fatalf("expected exactly one init container, got %d", len(spec.InitContainers))
	}
	if spec.InitContainers[0].Name != "clone" {
		t.Fatalf("expected the clone init container, got %q", spec.InitContainers[0].Name)
	}
	if len(spec.Containers) != 1 || spec.Containers[0].Name != "build" {
		t.Fatalf("expected one build container, got %v", spec.Containers)
	}
	// The build must not start against a half-populated context; an init
	// container is Kubernetes' own expression of that ordering, so asserting the
	// separation is asserting the ordering.
	if !strings.Contains(spec.InitContainers[0].Command[2], "git clone") {
		t.Fatalf("expected the init container to clone, got %q", spec.InitContainers[0].Command[2])
	}
}

// The ref check runs before the clone so a missing ref costs one cheap round trip
// and is reported as a skip rather than a build failure — the common case,
// because a preview is created before a feature branch is pushed.
func TestBuildJob_ChecksTheRefExistsBeforeCloning(t *testing.T) {
	job := buildJob(baseConfig())
	script := job.Spec.Template.Spec.InitContainers[0].Command[2]

	if !strings.Contains(script, "git ls-remote --exit-code --heads --tags") {
		t.Fatalf("expected an ls-remote guard before the clone, got %q", script)
	}
	if !strings.Contains(script, "exit 43") {
		t.Fatalf("expected the ref-unavailable exit code, got %q", script)
	}
	// The guard must come first, or it is not guarding anything.
	if strings.Index(script, "ls-remote") > strings.Index(script, "git clone") {
		t.Fatalf("expected ls-remote before the clone, got %q", script)
	}
}

// A private repository is the normal case, so the token has to reach the clone —
// and it must do so through git's URL rewrite (nothing written to a remote)
// rather than being embedded in the stored origin URL.
func TestBuildJob_PassesTheTokenThroughGitURLRewrite(t *testing.T) {
	job := buildJob(baseConfig())
	init := job.Spec.Template.Spec.InitContainers[0]

	if !strings.Contains(init.Command[2], "insteadOf") {
		t.Fatalf("expected a URL rewrite, got %q", init.Command[2])
	}
	// The token is passed as an env var, not interpolated into the script text:
	// a Job's pod spec is readable by anyone who can read Jobs in the namespace.
	var found bool
	for _, env := range init.Env {
		if env.Name == "GITHUB_TOKEN" && env.Value == "ghs_test" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the token as an env var, got %v", init.Env)
	}
	if strings.Contains(init.Command[2], "ghs_test") {
		t.Fatal("expected the token not to be inlined into the command")
	}
}

func TestBuildJob_BuildsFromTheContractDockerfilePath(t *testing.T) {
	job := buildJob(baseConfig())
	script := job.Spec.Template.Spec.Containers[0].Command[2]

	if !strings.Contains(script, "/workspace/Dockerfile") {
		t.Fatalf("expected the contract's Dockerfile path, got %q", script)
	}
	if !strings.Contains(script, "--destination=registry.local/proj-1/luffy-portfolio:yggdrasil-feature-abc") {
		t.Fatalf("expected the push destination, got %q", script)
	}
}

// The cache repository has to be derived from the destination's *repository*
// part. Deriving it by cutting at the first colon sends cache pushes to
// `registry.local` instead of `registry.local:5000` — a different registry.
func TestBuildJob_DerivesTheCacheRepoWithoutBreakingARegistryPort(t *testing.T) {
	cfg := baseConfig()
	cfg.Destination = "registry.local:5000/proj-1/repo:main"

	script := buildJob(cfg).Spec.Template.Spec.Containers[0].Command[2]

	if !strings.Contains(script, "--cache-repo=registry.local:5000/proj-1/repo-cache") {
		t.Fatalf("expected the cache repo to keep the registry port, got %q", script)
	}
}

// A missing Dockerfile is a skip, not a failure: a freshly scaffolded project
// has none, and its preview should still work.
func TestBuildJob_ReportsAMissingDockerfileWithItsOwnExitCode(t *testing.T) {
	script := buildJob(baseConfig()).Spec.Template.Spec.Containers[0].Command[2]

	if !strings.Contains(script, "exit 42") {
		t.Fatalf("expected the no-Dockerfile exit code, got %q", script)
	}
}

func TestBuildJob_HonoursAConfiguredDockerfilePath(t *testing.T) {
	cfg := baseConfig()
	cfg.DockerfilePath = "build/Dockerfile"

	script := buildJob(cfg).Spec.Template.Spec.Containers[0].Command[2]

	if !strings.Contains(script, "/workspace/build/Dockerfile") {
		t.Fatalf("expected the configured path, got %q", script)
	}
}

// The bundled registry of ADR 003 §14 takes unauthenticated pushes, so the auth
// volume must be absent rather than an empty mount that would make Kaniko look
// for credentials it does not have.
func TestBuildJob_OmitsRegistryAuthWhenNoSecretIsConfigured(t *testing.T) {
	job := buildJob(baseConfig())

	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "registry-auth" {
			t.Fatalf("expected no registry-auth volume, got %v", job.Spec.Template.Spec.Volumes)
		}
	}
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "registry-auth" {
			t.Fatal("expected no registry-auth mount")
		}
	}
}

func TestBuildJob_MountsRegistryAuthAtTheDockerConfigPath(t *testing.T) {
	cfg := baseConfig()
	cfg.RegistryAuthSecret = "registry-push"

	job := buildJob(cfg)

	var volumeFound bool
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "registry-auth" {
			volumeFound = true
			if volume.Secret == nil || volume.Secret.SecretName != "registry-push" {
				t.Fatalf("expected the configured secret, got %+v", volume)
			}
		}
	}
	if !volumeFound {
		t.Fatal("expected a registry-auth volume")
	}
	// Kaniko reads push credentials from the standard docker config path, so a
	// mount anywhere else would be a build that silently cannot push.
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "registry-auth" && mount.MountPath != "/kaniko/.docker" {
			t.Fatalf("expected /kaniko/.docker, got %q", mount.MountPath)
		}
	}
}

// A retry must not hit AlreadyExists, and a crashed replica's leftover Job must
// not block the next attempt — so no retries at the Job level either, since a
// failed Dockerfile build will fail again identically and BackoffLimit would
// triple the cost of finding that out.
func TestBuildJob_DoesNotRetryItself(t *testing.T) {
	job := buildJob(baseConfig())

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("expected a zero backoff limit, got %v", job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("expected a TTL so finished builds are collected")
	}
}

func TestBuildJob_LabelsIdentifyTheJobAndTheKind(t *testing.T) {
	labels := buildJob(baseConfig()).Labels

	if labels["yggdrasil.io/job"] != "job-1" {
		t.Fatalf("expected the job label, got %v", labels)
	}
	if labels["yggdrasil.io/kind"] != "image-build" {
		t.Fatalf("expected the kind label, got %v", labels)
	}
}

// --- outcome translation ---------------------------------------------------

func terminatedPod(container string, exitCode int32) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "build-pod"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: container,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Reason: "Completed"},
				},
			}},
		},
	}
}

func TestBuildOutcome_ReportsARunningPodAsNotDone(t *testing.T) {
	pod := corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}

	if _, done := buildOutcome(&pod); done {
		t.Fatal("expected a running pod to report as not done")
	}
}

func TestBuildOutcome_ReportsSuccess(t *testing.T) {
	pod := corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}

	outcome, done := buildOutcome(&pod)
	if !done || outcome.exitCode != 0 {
		t.Fatalf("expected a terminal success, got %+v done=%v", outcome, done)
	}
}

func TestBuildOutcome_ReportsTheBuildContainersExitCode(t *testing.T) {
	pod := terminatedPod("build", NoDockerfileExitCode)

	outcome, done := buildOutcome(&pod)
	if !done {
		t.Fatal("expected a terminal outcome")
	}
	if outcome.exitCode != NoDockerfileExitCode {
		t.Fatalf("expected exit code %d, got %d", NoDockerfileExitCode, outcome.exitCode)
	}
}

// A clone failure is the most likely reason a real build does not start (a token
// that cannot read the repository, or a ref that does not exist), so the
// outcome has to name the clone rather than reporting a bare build failure.
func TestBuildOutcome_AttributesAFailureToTheCloneStep(t *testing.T) {
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "clone",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 128, Reason: "Error"},
				},
			}},
		},
	}

	outcome, done := buildOutcome(&pod)
	if !done {
		t.Fatal("expected a terminal outcome")
	}
	if !strings.Contains(outcome.reason, "clone") {
		t.Fatalf("expected the clone named as the cause, got %q", outcome.reason)
	}
}

// A clone that succeeded is not an outcome — something else has to decide the
// build's fate, so the init status must not be mistaken for the build's.
func TestBuildOutcome_IgnoresASuccessfulClone(t *testing.T) {
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "clone",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"},
				},
			}},
		},
	}

	if _, done := buildOutcome(&pod); done {
		t.Fatal("expected a pod still building to report as not done")
	}
}

func TestPodOutcome_DescribeNamesTheCodeAndReason(t *testing.T) {
	got := podOutcome{exitCode: 1, reason: "Error", message: "no such file"}.describe()

	for _, want := range []string{"1", "Error", "no such file"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q to contain %q", got, want)
		}
	}
}

// --- configuration defaults ------------------------------------------------

func TestConfig_AppliesDefaults(t *testing.T) {
	cfg := Config{}

	if got := cfg.dockerfilePath(); got != DefaultDockerfilePath {
		t.Fatalf("expected %q, got %q", DefaultDockerfilePath, got)
	}
	if got := cfg.executorImage(); got != DefaultExecutorImage {
		t.Fatalf("expected %q, got %q", DefaultExecutorImage, got)
	}
	if got := cfg.cloneImage(); got != DefaultCloneImage {
		t.Fatalf("expected %q, got %q", DefaultCloneImage, got)
	}
	if got := cfg.timeout(); got != defaultBuildTimeout {
		t.Fatalf("expected the default timeout, got %s", got)
	}
}

func TestConfig_KeepsExplicitValues(t *testing.T) {
	cfg := Config{
		DockerfilePath: "docker/Dockerfile",
		ExecutorImage:  "custom/kaniko:v1",
		CloneImage:     "custom/git:v1",
		Timeout:        time.Minute,
	}

	if got := cfg.dockerfilePath(); got != "docker/Dockerfile" {
		t.Fatalf("got %q", got)
	}
	if got := cfg.executorImage(); got != "custom/kaniko:v1" {
		t.Fatalf("got %q", got)
	}
	if got := cfg.cloneImage(); got != "custom/git:v1" {
		t.Fatalf("got %q", got)
	}
	if got := cfg.timeout(); got != time.Minute {
		t.Fatalf("got %s", got)
	}
}

// --- the Job survives serialization ---------------------------------------

// The tests above assert *strings* inside the Job's command. This asserts the
// object itself is a well-formed `batch/v1` Job that survives being written and
// read back — which is what the API server does with it, and the closest thing to
// cluster validation reachable from here (the sandbox has neither a registry nor
// permission to create Jobs; see the issue).
//
// Deliberately not a substitute for a real build: it cannot say whether Kaniko
// can push, whether the registry accepts the push, or whether the pod is
// schedulable.
func TestBuildJob_RoundTripsThroughSerialization(t *testing.T) {
	cfg := baseConfig()
	cfg.RegistryAuthSecret = "registry-push"

	original := buildJob(cfg)

	encoded, err := yaml.Marshal(original)
	if err != nil {
		t.Fatalf("failed to serialize the Job: %v", err)
	}

	var decoded batchv1.Job
	if err := yaml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("failed to parse the Job back: %v", err)
	}

	// TypeMeta is deliberately empty on a Job built here, and that is correct
	// rather than an omission: this object is created through the typed client
	// (`Clientset.BatchV1().Jobs().Create`), which addresses
	// `/apis/batch/v1/namespaces/<ns>/jobs` and takes the group/version from the
	// scheme. Setting it here would be the thing that is redundant. Asserted so
	// the fact is recorded where someone wondering about it will look.
	if original.APIVersion != "" || original.Kind != "" {
		t.Fatalf("expected empty TypeMeta for a typed-client create, got %q %q",
			original.APIVersion, original.Kind)
	}

	if decoded.Name != original.Name || decoded.Namespace != original.Namespace {
		t.Fatalf("identity did not survive: %s/%s became %s/%s",
			original.Namespace, original.Name, decoded.Namespace, decoded.Name)
	}
	if decoded.Spec.BackoffLimit == nil || *decoded.Spec.BackoffLimit != 0 {
		t.Fatalf("the backoff limit did not survive: %v", decoded.Spec.BackoffLimit)
	}
	if decoded.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("the TTL did not survive")
	}
	if len(decoded.Spec.Template.Spec.InitContainers) != 1 || len(decoded.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected one init container and one container, got %d and %d",
			len(decoded.Spec.Template.Spec.InitContainers), len(decoded.Spec.Template.Spec.Containers))
	}

	// The scripts are multi-line with backslash continuations, so they are the
	// part most likely to be mangled by encoding — and a mangled Kaniko
	// invocation would surface only as a failed build in a cluster.
	if decoded.Spec.Template.Spec.Containers[0].Command[2] != original.Spec.Template.Spec.Containers[0].Command[2] {
		t.Fatal("the build script did not survive serialization")
	}
	if !strings.Contains(decoded.Spec.Template.Spec.Containers[0].Command[2], "--destination=") {
		t.Fatalf("expected the Kaniko invocation intact, got %q", decoded.Spec.Template.Spec.Containers[0].Command[2])
	}

	// The auth volume is what would silently break a push if it were dropped:
	// Kaniko would find no credentials and fail at the far end.
	var found bool
	for _, volume := range decoded.Spec.Template.Spec.Volumes {
		if volume.Name == "registry-auth" && volume.Secret != nil && volume.Secret.SecretName == "registry-push" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the auth volume did not survive: %v", decoded.Spec.Template.Spec.Volumes)
	}
}
