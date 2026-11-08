package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/observability"
)

func enableMetrics(h *harness) {
	h.metrics = observability.New(h.engine.Live.Stats)
	h.engine.Metrics = h.metrics
}

func scrapeMetrics(t *testing.T, h *harness) string {
	t.Helper()
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("metrics: HTTP %d %s", w.Code, w.Body.String())
	}
	return w.Body.String()
}

func expectMetric(t *testing.T, text string, samples ...string) {
	t.Helper()
	for _, sample := range samples {
		if !strings.Contains(text, "\n"+sample+"\n") {
			t.Errorf("missing metric sample %s", sample)
		}
	}
}

func TestMetricsPreserveStreamingAndCountInStreamFailures(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	enableMetrics(h)
	input := sendInput(key)
	input["content"] = "private prompt sentinel"
	rows.refuse = func(row *domain.StoredRow) error {
		if chat := mustRead(domain.ReadThreadRow(row)); chat.ActiveRun == nil {
			return errors.New("private upstream error sentinel")
		}
		return nil
	}
	w := postRoute(h, "/v1/threads/turn", input, "alice")
	frames := eventsIn(t, w)
	if w.Code != http.StatusOK || !w.Flushed || len(frames) == 0 || frames[len(frames)-1].Type != "RUN_ERROR" {
		t.Fatalf("stream lost its terminal failure: HTTP %d %s", w.Code, w.Body.String())
	}
	text := scrapeMetrics(t, h)
	expectMetric(t, text,
		`harness_http_requests_total{code="200",method="post",route="POST /v1/threads/turn"} 1`,
		`harness_http_requests_in_flight{route="POST /v1/threads/turn"} 0`,
		`harness_runs_admitted_total 1`,
		`harness_run_duration_seconds_count{outcome="failed"} 1`,
		`harness_run_duration_seconds_count{outcome="completed"} 0`,
	)
	for _, private := range []string{api.Str(input["content"]), api.Str(input["threadId"]), api.Str(input["clientRequestId"]), keyBase64(key), "clerk-alice", "private upstream error sentinel", frames[0].RunID} {
		if private != "" && strings.Contains(text, private) {
			t.Fatalf("metrics contain private request or error data")
		}
	}
}

func TestMetricsTrackQueueOutcomesWithoutCountingRetries(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	enableMetrics(h)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	accept := func(input api.Object) *engine.Run {
		t.Helper()
		run, err := h.engine.AcceptTurn(t.Context(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](input))
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	input := sendInput(key)
	first := accept(input)
	awaitFrame(t, first.Log, "TEXT_MESSAGE_CHUNK")
	if retry := accept(input); retry != first {
		t.Fatal("retry started another run")
	}
	broken := continueInput(key, first.Thread.ID)
	broken["kind"], broken["messageId"] = "edit", "missing-message"
	failure := accept(broken)
	canceled := accept(continueInput(key, first.Thread.ID))
	expectMetric(t, scrapeMetrics(t, h), "harness_runs_admitted_total 3", "harness_runs_active 1", "harness_runs_queued 2")
	_, err := h.engine.RemoveQueued(t.Context(), p, key, requestOf[engine.RunRequest](api.Object{"key": keyBase64(key), "threadId": first.Thread.ID, "queueId": canceled.Log.ID}))
	if err != nil {
		t.Fatal(err)
	}
	release()
	<-first.Finished
	<-failure.Finished
	<-canceled.Finished
	expectMetric(t, scrapeMetrics(t, h),
		"harness_runs_admitted_total 3", "harness_runs_active 0", "harness_runs_queued 0",
		`harness_run_duration_seconds_count{outcome="completed"} 1`,
		`harness_run_duration_seconds_count{outcome="canceled"} 1`,
		`harness_run_duration_seconds_count{outcome="failed"} 1`,
	)
}

func TestMetricsTrackCanceledAndRetainedDeviceRuns(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	enableMetrics(h)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](localInput("0000000000007_local", priorThread())))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.Log, "TEXT_MESSAGE_CHUNK")
	_, err = h.engine.CancelThread(t.Context(), p, key, requestOf[engine.RunRequest](api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID}))
	if err != nil {
		t.Fatal(err)
	}
	<-run.Finished
	expectMetric(t, scrapeMetrics(t, h), "harness_runs_active 0", "harness_runs_retained 1", `harness_run_duration_seconds_count{outcome="canceled"} 1`)
	h.engine.StopDeviceThread(p, run.Thread.ID)
	expectMetric(t, scrapeMetrics(t, h), "harness_runs_retained 0")
}
