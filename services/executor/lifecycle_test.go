package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	t0 = time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	t1 = t0.Add(2 * time.Second)
	t2 = t0.Add(3 * time.Second)
	t3 = t0.Add(4 * time.Second)
)

const job = "run-1/read_plate/1"

func applyAll(s Step, facts ...Fact) Step {
	for _, f := range facts {
		s, _ = Apply(s, f)
	}
	return s
}

func ran(outcome Outcome, sha string) Finished {
	return Finished{JobID: job, Outcome: outcome, StartedAt: t0, FinishedAt: t1, SHA: sha}
}

func stored(sha string, at time.Time) DataStored {
	return DataStored{JobID: job, StorageCheck: StorageCheck{Ref: "s3://lab-data/runs/" + job + ".csv", SHA: sha, At: at}}
}

var lost = DataLost{JobID: job}

var (
	dispatched = Dispatched{JobID: job, Attempt: 1}
	claimed    = Claimed{JobID: job}
	started    = Started{JobID: job, At: t0}
	reader     = Step{ProducesData: true}
)

func TestStepLifecycle(t *testing.T) {
	cases := []struct {
		name       string
		start      Step
		facts      []Fact
		phase      Phase
		reasonHas  string
		hasDataRef bool
	}{
		{"new step is pending", Step{}, nil, PhasePending, "", false},
		{"dispatched", Step{}, []Fact{dispatched}, PhaseDispatched, "", false},
		{"claimed", Step{}, []Fact{dispatched, claimed}, PhaseClaimed, "", false},
		{"started", Step{}, []Fact{dispatched, claimed, started}, PhaseRunning, "", false},
		{"step without data completes on the ledger's word", Step{},
			[]Fact{dispatched, claimed, started, ran(OutcomeCompleted, "")}, PhaseCompleted, "", false},

		{"a completed read is not completed until its data is verified", reader,
			[]Fact{dispatched, claimed, started, ran(OutcomeCompleted, "X")}, PhaseStoring, "", false},
		{"a read completes when stored data matches the ledger", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("X", t2)}, PhaseCompleted, "", true},
		{"data stored before finished waits for the ledger", reader,
			[]Fact{dispatched, claimed, started, stored("X", t2)}, PhaseRunning, "", false},
		{"data stored before finished completes once finished arrives", reader,
			[]Fact{dispatched, claimed, stored("X", t2), ran(OutcomeCompleted, "X")}, PhaseCompleted, "", true},

		{"hash mismatch is unresolved, and claims no data_ref", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("Y", t2)}, PhaseUnresolved, "does not match", false},
		{"a newer matching check resolves a mismatch", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("Y", t2), stored("X", t3)}, PhaseCompleted, "", true},
		{"corruption found after verification keeps completed and data_ref", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("X", t2), stored("Y", t3)}, PhaseCompleted, "", true},
		{"the same checks arriving in reverse order give the same record", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("Y", t3), stored("X", t2)}, PhaseCompleted, "", true},

		// A lost report is not proof: an upload from before the driver restarted
		// may still land (D3). Only storage, or a decision, can settle it.
		{"data lost before storage leaves the read unresolved", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), lost}, PhaseUnresolved, "may still", false},
		{"data lost before finished arrives: unresolved once finished arrives", reader,
			[]Fact{dispatched, claimed, started, lost, ran(OutcomeCompleted, "X")}, PhaseUnresolved, "may still", false},
		{"data lost, but storage has a matching copy: completed", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), lost, stored("X", t2)}, PhaseCompleted, "", true},
		{"data lost and the stored copy does not match: unresolved", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("Y", t2), lost}, PhaseUnresolved, "does not match", false},
		{"data lost for another job is ignored", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), DataLost{JobID: "run-1/read_plate/0"}}, PhaseStoring, "", false},
		{"a completed read with no ledger digest is unresolved", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "")}, PhaseUnresolved, "digest", false},

		{"hung waits for the retry decision, not failed", Step{},
			[]Fact{dispatched, claimed, started, ran(OutcomeHung, "")}, PhaseEnded, "hung", false},
		{"instrument failure, not repeated, is failed", Step{},
			[]Fact{dispatched, claimed, started, Finished{JobID: job, Outcome: OutcomeFailed, StartedAt: t0, FinishedAt: t1, Error: "instrument error"}, gaveUp},
			PhaseFailed, "", false},
		{"two different outcomes for one job are unresolved", Step{},
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, ""), ran(OutcomeFailed, "")}, PhaseUnresolved, "conflicting", false},

		{"evidence for another job is ignored", Step{},
			[]Fact{dispatched, claimed, Finished{JobID: "run-1/read_plate/0", Outcome: OutcomeCompleted, StartedAt: t0, FinishedAt: t1}},
			PhaseClaimed, "", false},
		{"started after finished changes nothing", Step{},
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, ""), started}, PhaseCompleted, "", false},

		{"cancel before claim cancels", Step{}, []Fact{dispatched, Cancel{}}, PhaseCancelled, "", false},
		{"cancel before dispatch cancels", Step{}, []Fact{Cancel{}}, PhaseCancelled, "", false},
		{"claim after cancel is refused", Step{}, []Fact{dispatched, Cancel{}, claimed}, PhaseCancelled, "", false},
		{"cancel after claim changes nothing", Step{}, []Fact{dispatched, claimed, Cancel{}}, PhaseClaimed, "", false},
		{"late facts win: a running step still records its real outcome after a cancel", Step{},
			[]Fact{dispatched, claimed, started, Cancel{}, ran(OutcomeCompleted, "")}, PhaseCompleted, "", false},
		{"physical evidence wins over a cancel it raced", Step{},
			[]Fact{dispatched, Cancel{}, started}, PhaseRunning, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := applyAll(c.start, c.facts...)
			if got := s.Phase(); got != c.phase {
				t.Fatalf("phase = %s, want %s (step %+v)", got, c.phase, s)
			}
			if e := s.APIError(); c.reasonHas != "" && (e == nil || !strings.Contains(*e, c.reasonHas)) {
				t.Errorf("error = %v, want it to mention %q", e, c.reasonHas)
			}
			if got := s.APIDataRef() != nil; got != c.hasDataRef {
				t.Errorf("data_ref present = %v, want %v", got, c.hasDataRef)
			}
		})
	}
}

// A verified completion is never undone: later read-backs can only add a warning.
func TestVerifiedCompletionStaysCompleted(t *testing.T) {
	s := applyAll(reader, dispatched, claimed, ran(OutcomeCompleted, "X"), stored("X", t2))
	for _, f := range []Fact{stored("Y", t3), lost, Cancel{}, stored("Z", t3.Add(time.Second))} {
		s, _ = Apply(s, f)
		if s.Phase() != PhaseCompleted || s.Phase().APIStatus() != StepCompleted {
			t.Fatalf("after %T the step is %s; a verified completion must stay completed", f, s.Phase())
		}
	}
	if e := s.APIError(); e == nil || !strings.Contains(*e, "no longer matches") {
		t.Errorf("a later mismatch must be reported in error, got %v", e)
	}
	if s.APIDataRef() == nil {
		t.Error("data_ref must stay set so the stored object can still be found")
	}
	healed, _ := Apply(s, stored("X", t3.Add(2*time.Second)))
	if e := healed.APIError(); e != nil {
		t.Errorf("a newer matching read-back clears the warning, got %q", *e)
	}
}

func TestDataLostIsExplained(t *testing.T) {
	s := applyAll(reader, dispatched, claimed, ran(OutcomeCompleted, "X"), lost)
	if e := s.APIError(); e == nil || !strings.Contains(*e, "lost") || s.Phase().APIStatus() != StepRunning {
		t.Errorf("a read reported lost must say so and stay running (unresolved), got %s, %v", s.Phase().APIStatus(), e)
	}
	if s.Attempts[0].Outcome != OutcomeCompleted {
		t.Errorf("the instrument's completed outcome must be kept, got %q", s.Attempts[0].Outcome)
	}
}

func TestTimesComeFromTheLedger(t *testing.T) {
	start, end := applyAll(Step{}, dispatched, claimed, ran(OutcomeCompleted, "")).Times()
	if start == nil || !start.Equal(t0) || end == nil || !end.Equal(t1) {
		t.Fatalf("times = %v, %v; want the ledger's %v, %v", start, end, t0, t1)
	}
}

func TestDuplicateFactsChangeNothing(t *testing.T) {
	facts := []Fact{dispatched, claimed, started, ran(OutcomeCompleted, "X"), stored("X", t2), lost, Cancel{}}
	s := reader
	for _, f := range facts {
		s, _ = Apply(s, f)
		if _, changed := Apply(s, f); changed {
			t.Errorf("applying %T twice changed the step the second time", f)
		}
	}
}

// Evidence can arrive in any order: every ordering must end in the same record.
func TestEvidenceOrderDoesNotMatter(t *testing.T) {
	evidence := []Fact{claimed, started, ran(OutcomeCompleted, "X"), stored("X", t2)}
	want := applyAll(reader, append([]Fact{dispatched}, evidence...)...)
	if want.Phase() != PhaseCompleted {
		t.Fatalf("baseline phase = %s, want completed", want.Phase())
	}
	permute(evidence, func(order []Fact) {
		got := applyAll(reader, append([]Fact{dispatched}, order...)...)
		gs, ge := got.Times()
		ws, we := want.Times()
		if got.Phase() != want.Phase() || *got.APIDataRef() != *want.APIDataRef() || !gs.Equal(*ws) || !ge.Equal(*we) {
			t.Errorf("order %v ended as %+v, want %+v", typeNames(order), got, want)
		}
	})

	// Same for the mixed cases: data lost with or without a matching copy, and a
	// mismatch found after verification.
	for _, c := range []struct {
		name  string
		facts []Fact
		phase Phase
	}{
		{"lost, no copy", []Fact{claimed, ran(OutcomeCompleted, "X"), lost}, PhaseUnresolved},
		{"lost, matching copy", []Fact{claimed, ran(OutcomeCompleted, "X"), lost, stored("X", t2)}, PhaseCompleted},
		{"verified then corrupted", []Fact{claimed, ran(OutcomeCompleted, "X"), stored("X", t2), stored("Y", t3)}, PhaseCompleted},
	} {
		permute(c.facts, func(order []Fact) {
			if got := applyAll(reader, append([]Fact{dispatched}, order...)...).Phase(); got != c.phase {
				t.Errorf("%s: order %v ended %s, want %s", c.name, typeNames(order), got, c.phase)
			}
		})
	}
}

func TestAPIView(t *testing.T) {
	want := map[Phase]string{
		PhasePending: StepPending, PhaseDispatched: StepPending,
		PhaseRetrying: StepRunning, PhaseEnded: StepRunning,
		PhaseClaimed: StepRunning, PhaseRunning: StepRunning, PhaseStoring: StepRunning, PhaseUnresolved: StepRunning,
		PhaseCompleted: StepCompleted, PhaseFailed: StepFailed, PhaseCancelled: StepCancelled,
	}
	for p, status := range want {
		if got := p.APIStatus(); got != status {
			t.Errorf("%s.APIStatus() = %s, want %s", p, got, status)
		}
	}

	hung := applyAll(Step{}, dispatched, claimed, ran(OutcomeHung, ""))
	if e := hung.APIError(); e == nil || !strings.Contains(*e, "hung") {
		t.Errorf("unresolved step must say why in error, got %v", e)
	}
	failed := applyAll(Step{}, dispatched, claimed, Finished{JobID: job, Outcome: OutcomeFailed, StartedAt: t0, FinishedAt: t1, Error: "instrument error"}, gaveUp)
	if e := failed.APIError(); e == nil || *e != "instrument error; not repeated" {
		t.Errorf("failed step must carry the instrument's error and why it was not repeated, got %v", e)
	}
	if e := applyAll(Step{}, dispatched, claimed, started).APIError(); e != nil {
		t.Errorf("running step has no error, got %q", *e)
	}
}

func TestDispatchedStepOccupiesItsDevice(t *testing.T) {
	steps := []StepState{
		{Name: "a", DeviceID: "lh", Status: PhaseDispatched.SchedulerStatus()},
		{Name: "b", DeviceID: "lh", Status: PhasePending.SchedulerStatus()},
	}
	if got := NextSteps(steps); len(got) != 0 {
		t.Fatalf("NextSteps = %v; a dispatched step must keep its device busy", got)
	}
}

func permute(fs []Fact, visit func([]Fact)) {
	var rec func(int)
	rec = func(k int) {
		if k == len(fs) {
			visit(append([]Fact(nil), fs...))
			return
		}
		for i := k; i < len(fs); i++ {
			fs[k], fs[i] = fs[i], fs[k]
			rec(k + 1)
			fs[k], fs[i] = fs[i], fs[k]
		}
	}
	rec(0)
}

func typeNames(fs []Fact) []string {
	var out []string
	for _, f := range fs {
		out = append(out, strings.TrimPrefix(fmt.Sprintf("%T", f), "main."))
	}
	return out
}

// Retryable is the instrument's word about a failure, kept from the first
// report; a replay without it does not take it back, and it means nothing on
// another outcome (D4).
func TestRetryableBelongsToAFailure(t *testing.T) {
	failed := ran(OutcomeFailed, "")
	failed.Retryable = true
	replayed := ran(OutcomeFailed, "")
	if s := applyAll(Step{}, dispatched, claimed, failed, replayed); !s.Attempts[0].Retryable {
		t.Errorf("a retryable failure, then the same failure replayed: Retryable = false")
	}
	if s := applyAll(Step{}, dispatched, claimed, replayed, failed); s.Attempts[0].Retryable {
		t.Errorf("first report said not retryable, a later one cannot change it: Retryable = true")
	}
	odd := ran(OutcomeHung, "")
	odd.Retryable = true
	if s := applyAll(Step{}, dispatched, claimed, odd); s.Attempts[0].Retryable {
		t.Errorf("a hang marked retryable: Retryable = true, want false")
	}
}

// Facts about attempt n of the read in `job` (which is attempt 1).
func jobN(n int) string          { return fmt.Sprintf("run-1/read_plate/%d", n) }
func dispatchN(n int) Dispatched { return Dispatched{JobID: jobN(n), Attempt: n} }
func claimN(n int) Claimed       { return Claimed{JobID: jobN(n)} }
func ranN(n int, outcome Outcome, sha string) Finished {
	return Finished{JobID: jobN(n), Outcome: outcome, StartedAt: t0.Add(time.Duration(n) * time.Minute),
		FinishedAt: t1.Add(time.Duration(n) * time.Minute), SHA: sha, Error: errorFor(outcome)}
}
func storedN(n int, sha string, at time.Time) DataStored {
	return DataStored{JobID: jobN(n), StorageCheck: StorageCheck{Ref: "s3://lab-data/runs/" + jobN(n) + ".csv", SHA: sha, At: at}}
}
func lostN(n int) DataLost { return DataLost{JobID: jobN(n)} }
func errorFor(o Outcome) string {
	if o == OutcomeFailed {
		return "instrument error"
	}
	return ""
}

var gaveUp = GaveUp{Reason: "not repeated"}

// D4: a step is decided by all its attempts. Completed once any attempt is
// verified (the first to verify supplies data_ref, D2); failed only when every
// attempt has ended and no further one will be made; otherwise running.
func TestAStepIsDecidedByAllItsAttempts(t *testing.T) {
	attempt1Failed := []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeFailed, "")}
	attempt1Lost := []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, "X"), lostN(1)}
	then := func(a []Fact, more ...Fact) []Fact { return append(slices.Clone(a), more...) }
	cases := []struct {
		name     string
		start    Step
		facts    []Fact
		phase    Phase
		errorHas string // "" means no error
		dataRef  string // "" means none
		attempts int
	}{
		{"an ended attempt waits for the retry decision", Step{}, attempt1Failed,
			PhaseEnded, "instrument error", "", 1},
		{"given up after a failure: failed, and says why", Step{}, then(attempt1Failed, gaveUp),
			PhaseFailed, "instrument error; not repeated", "", 1},
		{"a hang waits for the retry decision", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeHung, "")},
			PhaseEnded, "instrument hung; outcome unknown", "", 1},
		{"given up after a hang: failed, outcome unknown", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeHung, ""), gaveUp},
			PhaseFailed, "instrument hung; outcome unknown; not repeated", "", 1},
		{"a new attempt is retrying until claimed, never back to pending", Step{}, then(attempt1Failed, dispatchN(2)),
			PhaseRetrying, "", "", 2},
		{"a second attempt that completes completes the step", Step{}, then(attempt1Failed, dispatchN(2), claimN(2), ranN(2, OutcomeCompleted, "")),
			PhaseCompleted, "", "", 2},
		{"a second attempt that fails waits for the decision again", Step{}, then(attempt1Failed, dispatchN(2), claimN(2), ranN(2, OutcomeFailed, "")),
			PhaseEnded, "instrument error", "", 2},
		{"no second attempt while the first is in flight", Step{}, []Fact{dispatchN(1), claimN(1), dispatchN(2)},
			PhaseClaimed, "", "", 1},
		{"attempts are numbered in order", Step{}, then(attempt1Failed, dispatchN(3)),
			PhaseEnded, "instrument error", "", 1},
		{"no attempt after giving up", Step{}, then(attempt1Failed, gaveUp, dispatchN(2)),
			PhaseFailed, "not repeated", "", 1},
		{"giving up while an attempt is in flight is ignored", Step{}, []Fact{dispatchN(1), claimN(1), gaveUp},
			PhaseClaimed, "", "", 1},
		{"a give-up ignored in flight stays ignored once the attempt ends", Step{}, []Fact{dispatchN(1), claimN(1), gaveUp, ranN(1, OutcomeFailed, "")},
			PhaseEnded, "instrument error", "", 1},
		{"a conflicting report after giving up reopens the step: unresolved, not failed", Step{},
			then(attempt1Failed, gaveUp, ranN(1, OutcomeCompleted, "")),
			PhaseUnresolved, "conflicting", "", 1},

		// Lost data: attempt 1 stays open alongside its re-read (D3, D4).
		{"a lost read may be re-read alongside it", reader, then(attempt1Lost, dispatchN(2)),
			PhaseRetrying, "", "", 2},
		{"a re-read that fails leaves the lost read open: not failed", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), ranN(2, OutcomeFailed, "")),
			PhaseUnresolved, "may still appear", "", 2},
		{"giving up cannot fail a step whose lost read is still open", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), ranN(2, OutcomeFailed, ""), gaveUp),
			PhaseUnresolved, "may still appear", "", 2},
		{"a verified re-read completes the step", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), ranN(2, OutcomeCompleted, "Y"), storedN(2, "Y", t2)),
			PhaseCompleted, "", jobN(2), 2},
		{"a late upload after the re-read decided does not replace it", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), ranN(2, OutcomeCompleted, "Y"), storedN(2, "Y", t2), storedN(1, "X", t3)),
			PhaseCompleted, "", jobN(2), 2},
		{"a late upload before the re-read finishes decides, and stays", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), storedN(1, "X", t2), ranN(2, OutcomeCompleted, "Y"), storedN(2, "Y", t3)),
			PhaseCompleted, "", jobN(1), 2},
		{"a re-read that fails after the late upload decided changes nothing", reader,
			then(attempt1Lost, dispatchN(2), claimN(2), storedN(1, "X", t2), ranN(2, OutcomeFailed, "")),
			PhaseCompleted, "", jobN(1), 2},
		{"a conflict on the deciding read is unresolved and claims no data_ref", reader,
			[]Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, "X"), storedN(1, "X", t2), ranN(1, OutcomeFailed, "")},
			PhaseUnresolved, "conflicting", "", 1},
		{"no attempt after the step completed", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, ""), dispatchN(2)},
			PhaseCompleted, "", "", 1},

		// Cancel, as D1 has it, across attempts.
		// After a cancel the policy still says why there is no other attempt (D5,
		// TestACancelDoesNotRelabelAFinalFailure); until it has, the step is ended.
		{"cancel after a failed attempt: ended until the policy decides", Step{}, then(attempt1Failed, Cancel{}),
			PhaseEnded, "instrument error", "", 1},
		{"cancelled, and the cancel is why: failed, not retried", Step{}, then(attempt1Failed, Cancel{}, GaveUp{Reason: CancelledReason}),
			PhaseFailed, "instrument error; not retried: cancelled", "", 1},
		{"cancel wins over the retry's claim; the attempt that ran still shows", Step{}, then(attempt1Failed, dispatchN(2), Cancel{}, claimN(2)),
			PhaseEnded, "instrument error", "", 2},
		{"no new attempt after a cancel", Step{}, []Fact{dispatchN(1), claimN(1), Cancel{}, ranN(1, OutcomeFailed, ""), dispatchN(2)},
			PhaseEnded, "instrument error", "", 1},
		{"a cancel never undoes a completion", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, ""), Cancel{}},
			PhaseCompleted, "", "", 1},
		{"a lost read is not re-read after a cancel, and stays open", reader, then(attempt1Lost, Cancel{}, dispatchN(2)),
			PhaseUnresolved, "may still appear", "", 1},
		{"cancelled only if no attempt ever ran", Step{}, []Fact{dispatchN(1), Cancel{}},
			PhaseCancelled, "", "", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := applyAll(c.start, c.facts...)
			if got := s.Phase(); got != c.phase {
				t.Fatalf("phase = %s, want %s (step %+v)", got, c.phase, s)
			}
			e := s.APIError()
			switch {
			case c.errorHas == "" && e != nil:
				t.Errorf("error = %q, want none", *e)
			case c.errorHas != "" && (e == nil || !strings.Contains(*e, c.errorHas)):
				t.Errorf("error = %v, want it to contain %q", e, c.errorHas)
			}
			ref := s.APIDataRef()
			switch {
			case c.dataRef == "" && ref != nil:
				t.Errorf("data_ref = %s, want none", *ref)
			case c.dataRef != "" && (ref == nil || !strings.Contains(*ref, c.dataRef)):
				t.Errorf("data_ref = %v, want the object of %s", ref, c.dataRef)
			}
			if len(s.Attempts) != c.attempts {
				t.Errorf("%d attempts kept, want %d", len(s.Attempts), c.attempts)
			}
		})
	}
}

// The step's times are those of the attempt that decided it, or of the latest
// attempt that ran.
func TestStepTimesComeFromTheDecidingAttempt(t *testing.T) {
	s := applyAll(Step{}, dispatchN(1), claimN(1), ranN(1, OutcomeFailed, ""), dispatchN(2), claimN(2), ranN(2, OutcomeCompleted, ""))
	if start, end := s.Times(); !start.Equal(t0.Add(2*time.Minute)) || !end.Equal(t1.Add(2*time.Minute)) {
		t.Errorf("times %v..%v, want attempt 2's", start, end)
	}
	s = applyAll(Step{}, dispatchN(1), claimN(1), ranN(1, OutcomeFailed, ""), dispatchN(2), Cancel{})
	if start, _ := s.Times(); start == nil || !start.Equal(t0.Add(time.Minute)) {
		t.Errorf("start %v, want attempt 1's: attempt 2 never ran", start)
	}
}

// K54: a claim already granted is asked for again after a cancel. A driver asks
// for a claim only before it runs a job, and only for one its ledger does not
// have, so this proves the job never started: the claim is withdrawn and the
// driver is told abort. Without a cancel, or once there is evidence, the claim
// stands (D1).
func TestAClaimIsWithdrawnOnlyWhenCancelledAndNeverStarted(t *testing.T) {
	retryable := func(n int) Fact { f := ranN(n, OutcomeFailed, ""); f.Retryable = true; return f }
	cases := []struct {
		name    string
		facts   []Fact
		phase   Phase
		claimed bool // what the driver of attempt 1 (or of the last attempt) is told
		job     string
		errHas  string
	}{
		{"re-claim without a cancel: go", []Fact{dispatchN(1), claimN(1), claimN(1)}, PhaseClaimed, true, jobN(1), ""},
		{"re-claim after a cancel, nothing started: withdrawn, cancelled", []Fact{dispatchN(1), claimN(1), Cancel{}, claimN(1)}, PhaseCancelled, false, jobN(1), ""},
		{"re-claim after a cancel, but it started: go", []Fact{dispatchN(1), claimN(1), started, Cancel{}, claimN(1)}, PhaseRunning, true, jobN(1), ""},
		{"evidence after a withdrawal still wins", []Fact{dispatchN(1), claimN(1), Cancel{}, claimN(1), started}, PhaseRunning, false, jobN(1), ""},
		{"a withdrawn retry: the attempt that ran still shows", []Fact{dispatchN(1), claimN(1), retryable(1), dispatchN(2), claimN(2), Cancel{}, claimN(2)},
			PhaseEnded, false, jobN(2), "instrument error"},
		{"a cancel with the claim not yet granted: the claim is refused", []Fact{dispatchN(1), Cancel{}, claimN(1)}, PhaseCancelled, false, jobN(1), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := applyAll(Step{}, c.facts...)
			if got := s.Phase(); got != c.phase {
				t.Fatalf("phase = %s, want %s (%+v)", got, c.phase, s)
			}
			if got := s.claimed(c.job); got != c.claimed {
				t.Errorf("claimed(%s) = %v, want %v", c.job, got, c.claimed)
			}
			if e := s.APIError(); c.errHas != "" && (e == nil || !strings.Contains(*e, c.errHas)) {
				t.Errorf("error = %v, want it to contain %q", e, c.errHas)
			}
		})
	}
}
