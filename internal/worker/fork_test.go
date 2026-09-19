package worker

import (
	"strings"
	"testing"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/rpc"
)

// verifySwitch is the step ADR 032 item 3 exists for, so its rejection cases are
// the point of this file. The trigger is a real Pi 0.84.4: pointed at a session
// path that does not exist it answers success:true, creates nothing, and then
// reports no sessionFile — so a caller trusting switch_session's own flag cannot
// tell a loaded session from an empty one.

func TestVerifySwitchAcceptsALoadedSession(t *testing.T) {
	refusal := verifySwitch(rpc.SessionFile{
		FilePath:     "/tmp/yggdrasil-session-a.jsonl",
		MessageCount: 6,
		Asked:        true,
	}, "/tmp/yggdrasil-session-a.jsonl")
	if refusal != nil {
		t.Fatalf("expected the loaded session to verify, got %+v", refusal)
	}
}

func TestVerifySwitchRefusesTheSilentEmptyLoad(t *testing.T) {
	// The exact shape the finding describes: switch_session said success, and this
	// is what the state says afterwards. Without this branch the job would fork on
	// an empty conversation and the user would see the agent answer with no memory
	// of the interview — the failure ADR 032 item 5 forbids reporting as success.
	refusal := verifySwitch(rpc.SessionFile{Asked: true}, "/tmp/yggdrasil-session-a.jsonl")
	if refusal == nil {
		t.Fatal("expected an empty load to be refused")
	}
	if refusal.Stage != ForkStageSwitch {
		t.Fatalf("expected the switch stage, got %q", refusal.Stage)
	}
}

func TestVerifySwitchRefusesAnUnansweredStateQuestion(t *testing.T) {
	// Asked=false is "nobody found out", which must not read as "it loaded" — the
	// same distinction SessionFile.Asked carries for collection.
	refusal := verifySwitch(rpc.SessionFile{FilePath: "/tmp/x.jsonl", MessageCount: 3}, "/tmp/x.jsonl")
	if refusal == nil || refusal.Stage != ForkStageSwitch {
		t.Fatalf("expected an unanswered state question to be refused, got %+v", refusal)
	}
}

func TestVerifySwitchRefusesASessionWithNoMessages(t *testing.T) {
	// The path matched but there is nothing in it. A file of zero messages is one
	// Pi would happily accept and fork from, producing a run that silently lost the
	// conversation — so the count is checked even though the path already agreed.
	refusal := verifySwitch(rpc.SessionFile{
		FilePath:     "/tmp/yggdrasil-session-a.jsonl",
		MessageCount: 0,
		Asked:        true,
	}, "/tmp/yggdrasil-session-a.jsonl")
	if refusal == nil || refusal.Stage != ForkStageSwitch {
		t.Fatalf("expected an empty session to be refused, got %+v", refusal)
	}
	if !strings.Contains(refusal.Reason, "no messages") {
		t.Fatalf("the reason must say why, got %q", refusal.Reason)
	}
}

func TestVerifySwitchRefusesADifferentSessionThanTheOneAsked(t *testing.T) {
	// The failure a path check alone would miss in the other direction: something
	// loaded, with content, but not what this run restored. Forking here would
	// branch the wrong conversation.
	refusal := verifySwitch(rpc.SessionFile{
		FilePath:     "/root/.pi/agent/sessions/other.jsonl",
		MessageCount: 4,
		Asked:        true,
	}, "/tmp/yggdrasil-session-a.jsonl")
	if refusal == nil || refusal.Stage != ForkStageSwitch {
		t.Fatalf("expected a mismatched session to be refused, got %+v", refusal)
	}
	if !strings.Contains(refusal.Reason, "other.jsonl") {
		t.Fatalf("the reason must name what was loaded instead, got %q", refusal.Reason)
	}
}

func TestVerifySwitchNamesAnAbsentPathInWords(t *testing.T) {
	// describeSessionPath exists so a refusal reads as a sentence rather than as a
	// dangling colon, and this is the case it was written for.
	refusal := verifySwitch(rpc.SessionFile{FilePath: "", MessageCount: 2, Asked: true}, "/tmp/x")
	if refusal == nil {
		t.Fatal("expected a refusal")
	}
	if !strings.Contains(refusal.Reason, "no session at all") {
		t.Fatalf("expected words rather than an empty fragment, got %q", refusal.Reason)
	}
}

func TestForkRejectionReasonKeepsPisOwnWording(t *testing.T) {
	// Pi's text names the rejected input but not the situation, so both are kept:
	// without the first half a user cannot tell which point failed, and without the
	// second they do not know that the list they are looking at is stale.
	reason := forkRejectionReason("c3d4e5f6", "Invalid entry ID for forking")
	if !strings.Contains(reason, "c3d4e5f6") {
		t.Fatalf("expected the entry id in the reason, got %q", reason)
	}
	if !strings.Contains(reason, "Invalid entry ID for forking") {
		t.Fatalf("expected Pi's own wording preserved, got %q", reason)
	}
}

func TestForkRejectionReasonStandsAloneWithoutPiWording(t *testing.T) {
	// A failure with no `error` field must still produce a usable sentence rather
	// than one ending in empty parentheses.
	reason := forkRejectionReason("c3d4e5f6", "")
	if strings.Contains(reason, "()") {
		t.Fatalf("expected no empty parenthetical, got %q", reason)
	}
	if !strings.Contains(reason, "c3d4e5f6") {
		t.Fatalf("expected the entry id in the reason, got %q", reason)
	}
}

func TestDescribeSessionFetchSeparatesReclaimedFromMissing(t *testing.T) {
	// 410 is a *finished* artifact and 404 is an absent one. The read route makes
	// the same split, and it is what stops an expired session being reported to a
	// user as a broken one.
	reclaimed := describeSessionFetch(410)
	missing := describeSessionFetch(404)
	if reclaimed == missing {
		t.Fatal("a reclaimed session and a missing one must not read the same")
	}
	if !strings.Contains(reclaimed, "retention") {
		t.Fatalf("expected the retention window named, got %q", reclaimed)
	}
	if !strings.Contains(missing, "no usable session") {
		t.Fatalf("expected the missing case named, got %q", missing)
	}
}

func TestDescribeSessionFetchNamesAnUnreachableAPIDifferently(t *testing.T) {
	// Status 0 is "the request never completed", which is an operational problem
	// rather than a fact about the session — an operator acting on it would look in
	// a different place.
	if describeSessionFetch(0) == describeSessionFetch(404) {
		t.Fatal("an unreachable API must not read as a missing session")
	}
}

func TestForkRestorePathIsStableAndUnderSlashTmp(t *testing.T) {
	// Stable because the path is sent to switch_session and compared against what
	// get_state reports; under /tmp because the write is an argv-slice `tee` with
	// nothing to create a parent directory, so any other location would fail on a
	// container that does not happen to have it.
	path := forkRestorePath("b352cb0e-0000-4000-8000-000000000000")
	if !strings.HasPrefix(path, forkRestoreDir+"/") {
		t.Fatalf("expected a path under %s, got %q", forkRestoreDir, path)
	}
	if path != forkRestorePath("b352cb0e-0000-4000-8000-000000000000") {
		t.Fatal("the restore path must be deterministic")
	}
	if !strings.Contains(path, "b352cb0e-0000-4000-8000-000000000000") {
		t.Fatalf("expected the source job id in the name, got %q", path)
	}
}

func TestForkStagesAreDistinctSoTheReasonsCannotCollapse(t *testing.T) {
	// The stage is what an operator acts on, so no two of the three may be the same
	// value — the collapse ADR 032 item 5 forbids, applied to dispatch.
	seen := map[ForkStage]bool{}
	for _, stage := range []ForkStage{ForkStageWrite, ForkStageSwitch, ForkStageFork} {
		if seen[stage] {
			t.Fatalf("fork stage %q is used twice", stage)
		}
		seen[stage] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected three distinct stages, got %d", len(seen))
	}
}
