package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

func cancelRun(t *testing.T, e *Executor, runID string) RunView {
	t.Helper()
	rv, err := e.CancelRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("cancel %s: %v", runID, err)
	}
	return rv
}

func TestCancelAPendingRun(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	got := cancelRun(t, e, rv.ID)
	if got.Status != RunCancelled || got.FinishedAt == nil {
		t.Errorf("run %s, finished_at %v; want cancelled with a finished_at", got.Status, got.FinishedAt)
	}
	for _, s := range got.Steps {
		if s.Status != StepCancelled {
			t.Errorf("step %s is %s, want cancelled", s.Name, s.Status)
		}
	}
	if again, _ := e.StartRun(context.Background(), rv.ID); again.Status != RunCancelled {
		t.Errorf("start after cancel: run is %s, want cancelled", again.Status)
	}
	if second := cancelRun(t, e, rv.ID); second.Status != RunCancelled || !second.FinishedAt.Equal(*got.FinishedAt) {
		t.Errorf("second cancel changed the run: %+v", second)
	}
}

func TestCancelAnUnknownRun(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	if _, err := e.CancelRun(context.Background(), "run-nope"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("err = %v, want ErrRunNotFound", err)
	}
}

func TestCancelAFinishedRunChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.drivers()
	rv := h.startRun(sendWorkflow)
	done := h.waitRun(rv.ID, RunCompleted, 10*time.Second)
	if got := cancelRun(t, h.e, rv.ID); got.Status != RunCompleted || !got.FinishedAt.Equal(*done.FinishedAt) {
		t.Errorf("cancel of a completed run: %s at %v, want it unchanged", got.Status, got.FinishedAt)
	}
	var n int
	h.e.db.QueryRow(`SELECT count(*) FROM steps WHERE run_id = $1 AND (lifecycle->>'Cancelled')::boolean`, rv.ID).Scan(&n)
	if n != 0 {
		t.Errorf("%d steps of a finished run were marked cancelled", n)
	}
}

// A cancel that comes too late to stop anything: the only step is already
// running and completes. Every step completed, so the run did (K42, rule 1).
func TestACancelTooLateToStopAnything(t *testing.T) {
	h := newHarness(t)
	lh, _ := h.drivers()
	lh.takes = time.Second
	rv := h.startRun([]StepTemplate{{Name: "fill", DeviceID: "liquid-handler-1"}})
	for deadline := time.Now().Add(5 * time.Second); phaseOf(t, h.e, rv.ID, "fill") != PhaseRunning; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("fill never started")
		}
	}
	cancelRun(t, h.e, rv.ID)
	h.waitRun(rv.ID, RunCompleted, 10*time.Second)
}

// The step already running is not stopped: it completes, with its ledger times.
// Nothing after it is sent. The run ends cancelled once nothing is in flight,
// with finished_at the last ledger time.
func TestCancelWhileAStepRuns(t *testing.T) {
	h := newHarness(t)
	lh, inc := h.drivers()
	lh.takes = time.Second
	rv := h.startRun(sendWorkflow)
	h.waitStep(rv.ID, "fill", StepRunning)
	for deadline := time.Now().Add(5 * time.Second); phaseOf(t, h.e, rv.ID, "fill") != PhaseRunning; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("fill never started")
		}
	}
	if mid := cancelRun(t, h.e, rv.ID); mid.Status != RunRunning {
		t.Errorf("right after the cancel the run is %s, want running: fill is still in flight", mid.Status)
	}

	done := h.waitRun(rv.ID, RunCancelled, 10*time.Second)
	fill := stepView(t, h.e, rv.ID, "fill")
	if fill.Status != StepCompleted || fill.FinishedAt == nil {
		t.Errorf("fill: %s, finished %v; want completed with its ledger time", fill.Status, fill.FinishedAt)
	}
	for _, name := range []string{"incubate", "wash"} {
		if s := stepView(t, h.e, rv.ID, name); s.Status != StepCancelled {
			t.Errorf("%s is %s, want cancelled", name, s.Status)
		}
	}
	if n := inc.commands(protocol.JobID(rv.ID, "incubate", 1)); n != 0 {
		t.Errorf("incubate was commanded %d times after the cancel", n)
	}
	if done.FinishedAt == nil || !done.FinishedAt.Equal(*fill.FinishedAt) {
		t.Errorf("run finished_at %v, want fill's ledger finished_at %v", done.FinishedAt, fill.FinishedAt)
	}
}

// No new attempt after a cancel: a retry sent but not claimed is cancelled and
// never runs. The attempt that ran and failed still shows, and the run is
// cancelled, not failed: the cancel is why it was not repeated (K42, rule 1).
func TestCancelStopsARetry(t *testing.T) {
	h := newHarness(t)
	_, inc := h.drivers()
	rv := h.startRun(sendWorkflow)
	first, second := protocol.JobID(rv.ID, "incubate", 1), protocol.JobID(rv.ID, "incubate", 2)
	inc.mu.Lock()
	inc.outcome[first], inc.retryable["incubate"], inc.busyFor[second] = "failed", true, true
	inc.mu.Unlock()
	for deadline := time.Now().Add(10 * time.Second); inc.commands(second) == 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("no retry was sent; incubate is %s", phaseOf(t, h.e, rv.ID, "incubate"))
		}
	}
	cancelRun(t, h.e, rv.ID)
	h.waitRun(rv.ID, RunCancelled, 10*time.Second)
	sv := stepView(t, h.e, rv.ID, "incubate")
	if sv.Status != StepFailed || sv.Error == nil || *sv.Error != "instrument error; not retried: cancelled" {
		t.Errorf("incubate: %s, %q; want failed, instrument error; not retried: cancelled", sv.Status, deref(sv.Error))
	}
	if inc.started(second) != 0 {
		t.Errorf("the retry ran after the cancel")
	}
}

// A step that failed on its own, with no cancel involved, still fails its run
// when a cancel arrives for the rest (K42, rule 1).
func TestAFailureIsNotHiddenByACancel(t *testing.T) {
	h := newHarness(t)
	newFakeDriver(t, h.nc, "liquid-handler-1").serve()
	inc := newFakeDriver(t, h.nc, "incubator-1").serve()
	slow := newFakeDriver(t, h.nc, "incubator-2").serve()
	slow.takes = 2 * time.Second
	inc.outcome["incubate"] = "failed" // not retryable: the policy gives up
	rv := h.startRun([]StepTemplate{
		{Name: "fill", DeviceID: "liquid-handler-1"},
		{Name: "incubate", DeviceID: "incubator-1", DependsOn: []string{"fill"}},
		{Name: "read", DeviceID: "incubator-2", DependsOn: []string{"fill"}},
		{Name: "report", DeviceID: "liquid-handler-1", DependsOn: []string{"incubate", "read"}},
	})
	h.waitStep(rv.ID, "incubate", StepFailed)
	if s := stepView(t, h.e, rv.ID, "read"); s.Status != StepRunning {
		t.Fatalf("read is %s; the cancel must arrive while it runs", s.Status)
	}
	cancelRun(t, h.e, rv.ID)
	h.waitRun(rv.ID, RunFailed, 10*time.Second)
}

// K54: the driver died after its claim was granted, before the job started. The
// run is cancelled; the restarted driver, asked again (S7), claims the job and
// is told abort. Nothing runs, and the step and the run end cancelled.
func TestCancelAfterAClaimThatNeverStarted(t *testing.T) {
	h := newHarness(t)
	crashed := newFakeDriver(t, h.nc, "liquid-handler-1").serve()
	newFakeDriver(t, h.nc, "incubator-1").serve()
	rv := h.startRun(sendWorkflow)
	fill := protocol.JobID(rv.ID, "fill", 1)
	crashed.mu.Lock()
	crashed.stall[fill] = true
	crashed.mu.Unlock()
	for deadline := time.Now().Add(5 * time.Second); phaseOf(t, h.e, rv.ID, "fill") != PhaseClaimed; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("fill never claimed")
		}
	}
	crashed.sub.Unsubscribe()
	cancelRun(t, h.e, rv.ID)
	restarted := newFakeDriver(t, h.nc, "liquid-handler-1").serve()

	h.waitRun(rv.ID, RunCancelled, 15*time.Second)
	if s := stepView(t, h.e, rv.ID, "fill"); s.Status != StepCancelled {
		t.Errorf("fill is %s, want cancelled: it never started", s.Status)
	}
	if n := restarted.started(fill) + crashed.started(fill); n != 0 {
		t.Errorf("fill started %d times after the cancel", n)
	}
	if restarted.commands(fill) == 0 {
		t.Errorf("the restarted driver was never asked; the withdrawal was not proven by a claim")
	}
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
