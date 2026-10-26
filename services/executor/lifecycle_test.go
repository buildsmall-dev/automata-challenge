package main

import (
	"fmt"
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

		{"hung is unresolved, not failed", Step{},
			[]Fact{dispatched, claimed, started, ran(OutcomeHung, "")}, PhaseUnresolved, "hung", false},
		{"instrument failure is failed", Step{},
			[]Fact{dispatched, claimed, started, Finished{JobID: job, Outcome: OutcomeFailed, StartedAt: t0, FinishedAt: t1, Error: "instrument error"}},
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
			if c.reasonHas != "" && !strings.Contains(s.Reason(), c.reasonHas) {
				t.Errorf("reason = %q, want it to mention %q", s.Reason(), c.reasonHas)
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
	if s.Outcome != OutcomeCompleted {
		t.Errorf("the instrument's completed outcome must be kept, got %q", s.Outcome)
	}
}

func TestTimesComeFromTheLedger(t *testing.T) {
	s := applyAll(Step{}, dispatched, claimed, ran(OutcomeCompleted, ""))
	if s.StartedAt == nil || !s.StartedAt.Equal(t0) || s.FinishedAt == nil || !s.FinishedAt.Equal(t1) {
		t.Fatalf("times = %v, %v; want the ledger's %v, %v", s.StartedAt, s.FinishedAt, t0, t1)
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
		if got.Phase() != want.Phase() || *got.APIDataRef() != *want.APIDataRef() ||
			!got.StartedAt.Equal(*want.StartedAt) || !got.FinishedAt.Equal(*want.FinishedAt) {
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
	failed := applyAll(Step{}, dispatched, claimed, Finished{JobID: job, Outcome: OutcomeFailed, StartedAt: t0, FinishedAt: t1, Error: "instrument error"})
	if e := failed.APIError(); e == nil || *e != "instrument error" {
		t.Errorf("failed step must carry the instrument's error, got %v", e)
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
