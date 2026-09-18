package worker

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/k8s"
	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
ADR 021 follow-up 1 / issue #27: a build that resolved merge conflicts is
invisible to the reviewer.

`feature_build` syncs the feature branch onto the latest base before the agent
starts, and resolves conflicts itself (ADR 021: merge both sides, never abort,
never revert the other side). `agent-images/base/entrypoint.sh` records that it
did so — `.yggdrasil/merge-conflicts.md` plus `YGGDRASIL_MERGE_CONFLICTS=1` — but
nothing in the product ever said so. The build-progress panel did not show it and
a reviewer opening the PR saw the resolution interleaved with ordinary work.

That matters because a conflict resolution is the highest-risk part of a build's
diff: it is where the agent guessed at *how two changes should coexist*, which is
exactly the judgement ADR 021 §9 says cannot be automated. A reviewer should be
told to look at it.

**Why the Orchestrator posts this rather than the pod.** The issue suggested
emitting it from the pod and mapping it in `rpc.Translate`. Two reasons not to:

1. `Translate` translates *Pi's* vocabulary. This is not something Pi knows about
   — the entrypoint computed it before Pi was even exec'd — so a case there would
   be a category error. Events the system synthesizes about itself already have a
   home here (`run_started`, `run_failed`, `run_cancelled`), and this joins them.
2. The alternative — having the entrypoint POST to `/internal/jobs/:id/events`
   the way `script_test_run` submits its report — would mean handing every agent
   pod the shared internal bearer token, which can reach every `/internal/*`
   route. Widening that to pods that hold a write-scoped GitHub token is a much
   bigger change than this feature needs.

So the Orchestrator reads the marker out of the running pod with the exec helper
ADR 029 already introduced for artifacts, and forwards it through the same
`handle` every other curated event uses. No new channel, no new credential.

**Timing.** The read happens once, lazily, on the first curated event of the
session rather than right after the pod reports Running. The pod is Running from
the moment its container starts — which is *before* the entrypoint finishes
cloning and merging — so a read at that point would race the merge. Pi's first
event cannot arrive until after the entrypoint exec'd it, which is strictly after
the merge, so the lazy read is ordered correctly by construction rather than by a
timeout. It also costs one exec on a run that has conflicts and one on a run that
does not, instead of a poll loop on every run.
*/

// mergeConflictMarkerPath is where `agent-images/base/entrypoint.sh` writes the
// note it generated when it resolved conflicts with the base. Kept in sync with
// that script by name and by this comment; a rename there without a rename here
// degrades to "no event", not to a wrong one.
const mergeConflictMarkerPath = "/workspace/.yggdrasil/merge-conflicts.md"

// mergeConflictReadLimit bounds the read. The marker is a short generated note —
// a heading, the conflicted file list, four instruction lines — so a few KiB is
// generous, and a bigger file is a bug rather than a legitimate input.
const mergeConflictReadLimit = 64 * 1024

// mergeConflictReporter reads the marker at most once and forwards one event if
// it exists.
type mergeConflictReporter struct {
	once     sync.Once
	read     podFileReader
	handle   func(rpc.CuratedEvent)
	jobID    string
	jobKind  string
	readOnce func() ([]byte, bool)
}

func newMergeConflictReporter(
	client *k8s.Client,
	namespace, podName string,
	jobID, jobKind string,
	handle func(rpc.CuratedEvent),
) *mergeConflictReporter {
	reader := podFileReaderFrom(client)
	return &mergeConflictReporter{
		read:    reader,
		handle:  handle,
		jobID:   jobID,
		jobKind: jobKind,
		readOnce: func() ([]byte, bool) {
			content, err := reader(
				context.Background(), namespace, podName, "run", mergeConflictMarkerPath,
			)
			if err != nil {
				// Absent is the ordinary case: most builds have nothing to
				// resolve, and `cat` on a missing file is an error. Logged at a
				// level that does not read as a failure, because it is not one.
				log.Printf(
					"worker: no merge-conflict marker for job %s (nothing to report): %v",
					jobID, err,
				)
				return nil, false
			}
			return content, true
		},
	}
}

// observe is what the session's event sink calls: it checks once, then forwards
// the event it was given. Called before the forward so the conflict notice lands
// at the top of the run's stream, ahead of any agent text.
func (m *mergeConflictReporter) observe(ev rpc.CuratedEvent) {
	m.once.Do(m.report)
	m.handle(ev)
}

func (m *mergeConflictReporter) report() {
	content, ok := m.readOnce()
	if !ok {
		return
	}
	message := describeMergeConflicts(string(content))
	log.Printf("worker: job %s resolved merge conflicts with the base: %s", m.jobID, message)
	m.handle(rpc.CuratedEvent{Type: rpc.EventMergeConflicts, Message: message})
}

// describeMergeConflicts turns the entrypoint's note into one line naming what
// conflicted.
//
// The note is written for the *agent* — a heading, the file list, and four
// numbered instructions — and the event stream wants the reviewer's half of it:
// which files. Everything after `## Conflicted files` up to the next heading is
// that list; anything unparseable falls back to a fixed sentence rather than an
// empty message, because "conflicts were resolved, files unknown" is still the
// thing the reviewer needs to know.
func describeMergeConflicts(note string) string {
	lines := strings.Split(note, "\n")
	collecting := false
	var files []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			// Starts the list, or ends it — the next heading after it is "How to
			// resolve".
			if collecting {
				break
			}
			collecting = strings.Contains(strings.ToLower(trimmed), "conflicted")
			continue
		}
		if !collecting || trimmed == "" {
			continue
		}
		files = append(files, trimmed)
	}

	if len(files) == 0 {
		return "This build resolved merge conflicts with the base branch; the conflicted files were not recorded."
	}
	return "This build resolved merge conflicts with the base branch: " + strings.Join(files, ", ")
}
