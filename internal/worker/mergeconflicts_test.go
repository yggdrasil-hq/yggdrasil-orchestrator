package worker

import (
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

/*
Issue #27: the entrypoint's note is written for the *agent* — a heading, the
conflicted file list, and four numbered instructions — while the event stream
wants the reviewer's half of it: which files. These pin that translation, and the
fallback, because an empty message would be worse than a vague one.
*/

// The exact shape agent-images/base/entrypoint.sh writes.
const sampleNote = `# Merge conflicts with origin/main

This workspace was left mid-merge on purpose (ADR 021). origin/main advanced
after this feature branch was last built, and the two sets of changes
conflict. Resolve them before implementing anything.

## Conflicted files

src/app/page.tsx
src/lib/api.ts

## How to resolve

1. Edit each file above, keeping both intents wherever they are compatible.
2. ` + "`git add`" + ` every resolved file.
3. ` + "`git commit`" + ` to complete the merge (` + "`--no-edit`" + ` is fine).
4. Then start on the ADR in ` + "`.yggdrasil/adr.md`" + `.
`

func TestDescribeMergeConflicts_NamesTheConflictedFiles(t *testing.T) {
	got := describeMergeConflicts(sampleNote)

	if !strings.Contains(got, "src/app/page.tsx") || !strings.Contains(got, "src/lib/api.ts") {
		t.Fatalf("expected both conflicted files named, got %q", got)
	}
	// The agent's own instructions must not leak into the reviewer's line.
	for _, unwanted := range []string{"git add", "How to resolve", "keeping both intents"} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("expected %q to be left out of the event, got %q", unwanted, got)
		}
	}
	if !strings.HasPrefix(got, "This build resolved merge conflicts") {
		t.Fatalf("expected the event to say what happened, got %q", got)
	}
}

// A vague message beats an empty one: the fact that conflicts were resolved is
// the part a reviewer must not miss, and it survives a note we cannot parse.
func TestDescribeMergeConflicts_FallsBackWhenTheListIsUnreadable(t *testing.T) {
	for name, note := range map[string]string{
		"empty":            "",
		"no heading":       "# Merge conflicts with origin/main\n\nsomething happened\n",
		"heading no files": "## Conflicted files\n\n## How to resolve\n\n1. do it\n",
	} {
		got := describeMergeConflicts(note)
		if !strings.Contains(got, "resolved merge conflicts with the base branch") {
			t.Fatalf("%s: expected the fact to survive, got %q", name, got)
		}
		if strings.TrimSpace(got) == "" {
			t.Fatalf("%s: expected a non-empty message", name)
		}
	}
}

func TestDescribeMergeConflicts_StopsAtTheNextHeading(t *testing.T) {
	note := "## Conflicted files\n\na.ts\n\n## How to resolve\n\nb.ts\n"
	got := describeMergeConflicts(note)

	if !strings.Contains(got, "a.ts") {
		t.Fatalf("expected a.ts, got %q", got)
	}
	if strings.Contains(got, "b.ts") {
		t.Fatalf("expected the instructions heading to end the list, got %q", got)
	}
}

// The event must not end the turn or the run: it is context, not a result.
func TestMergeConflictsEventIsNotTerminal(t *testing.T) {
	if (rpc.CuratedEvent{Type: rpc.EventMergeConflicts}).Terminal() {
		t.Fatal("expected merge_conflicts to be non-terminal")
	}
}
