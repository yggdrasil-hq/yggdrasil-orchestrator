package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yggdrasil-hq/yggdrasil-orchestrator/internal/apiclient"
)

// ADR 022 §5 / issue #26: a lost deploy report silently removes a rollback
// target, because the revision did apply but nothing recorded it. These tests
// pin the retry that narrows that window, and the fact that it stays bounded —
// an unbounded retry would hold the job open forever waiting for an API that is
// not coming back.

// deployAPI is a fake API's deploy-result endpoint that fails the first
// `failures` attempts (500) and succeeds afterwards; `failures` can be large
// to stand in for an API that is simply down. `status` of 0 means never fail.
func deployAPI(t *testing.T, failures int, status int) (*httptest.Server, *int, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		this := attempts
		mu.Unlock()

		if status != 0 && this <= failures {
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{})
	}))
	t.Cleanup(server.Close)
	return server, &attempts, &mu
}

// shrinkDeployBackoff removes the real 1s/2s/4s waits for the duration of a
// test. Nothing in production reassigns the variable.
func shrinkDeployBackoff(t *testing.T) {
	t.Helper()
	original := deployReportBackoff
	deployReportBackoff = []time.Duration{0, 0, 0}
	t.Cleanup(func() { deployReportBackoff = original })
}

func TestReportDeployResult_RetriesATransientFailure(t *testing.T) {
	shrinkDeployBackoff(t)
	// Two 500s then a 201 — an API restarting mid-report.
	server, attempts, mu := deployAPI(t, 2, http.StatusInternalServerError)
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	reportDeployResult(context.Background(), cfg, "job-1", apiclient.DeployResultInput{Revision: 7})

	mu.Lock()
	defer mu.Unlock()
	if *attempts != 3 {
		t.Fatalf("expected the report to be retried until it landed (3 attempts), got %d", *attempts)
	}
}

func TestReportDeployResult_StopsAfterTheBackoffIsExhausted(t *testing.T) {
	shrinkDeployBackoff(t)
	server, attempts, mu := deployAPI(t, 100, http.StatusBadGateway)
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	reportDeployResult(context.Background(), cfg, "job-2", apiclient.DeployResultInput{Revision: 8})

	mu.Lock()
	defer mu.Unlock()
	// One attempt plus one per backoff entry. The point is that it is bounded:
	// the caller then fails the job and the missing revision is logged.
	if want := len(deployReportBackoff) + 1; *attempts != want {
		t.Fatalf("expected %d attempts then give up, got %d", want, *attempts)
	}
}

func TestReportDeployResult_DoesNotRetryASuccess(t *testing.T) {
	shrinkDeployBackoff(t)
	server, attempts, mu := deployAPI(t, 0, 0)
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	reportDeployResult(context.Background(), cfg, "job-3", apiclient.DeployResultInput{Revision: 9})

	mu.Lock()
	defer mu.Unlock()
	if *attempts != 1 {
		t.Fatalf("expected exactly one report on success, got %d", *attempts)
	}
}

func TestReportDeployResult_GivesUpWhenTheContextIsCancelled(t *testing.T) {
	// Not shrunk on purpose: the cancellation has to be what stops the wait, so
	// a zero backoff would make the test pass for the wrong reason.
	original := deployReportBackoff
	deployReportBackoff = []time.Duration{time.Hour}
	t.Cleanup(func() { deployReportBackoff = original })

	server, attempts, mu := deployAPI(t, 100, http.StatusInternalServerError)
	cfg := Config{APIClient: apiclient.New(server.URL, "test-token")}

	// The first attempt fails, then the one-hour wait has to be interrupted by
	// the cancellation — otherwise this test would hang.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		reportDeployResult(ctx, cfg, "job-4", apiclient.DeployResultInput{Revision: 10})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reportDeployResult did not return after the context was cancelled")
	}

	mu.Lock()
	defer mu.Unlock()
	if *attempts != 1 {
		t.Fatalf("expected the cancelled context to stop further attempts, got %d", *attempts)
	}
}
