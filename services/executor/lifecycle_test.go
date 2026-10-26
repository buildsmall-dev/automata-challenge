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
	return DataStored{JobID: job, Ref: "s3://lab-data/runs/run-1/read_plate/" + job + ".csv", SHA: sha, At: at}
}

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
		{"an older check arriving late does not override a newer one", reader,
			[]Fact{dispatched, claimed, ran(OutcomeCompleted, "X"), stored("Y", t3), stored("X", t2)}, PhaseUnresolved, "does not match", false},
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

func TestTimesComeFromTheLedger(t *testing.T) {
	s := applyAll(Step{}, dispatched, claimed, ran(OutcomeCompleted, ""))
	if s.StartedAt == nil || !s.StartedAt.Equal(t0) || s.FinishedAt == nil || !s.FinishedAt.Equal(t1) {
		t.Fatalf("times = %v, %v; want the ledger's %v, %v", s.StartedAt, s.FinishedAt, t0, t1)
	}
}

func TestDuplicateFactsChangeNothing(t *testing.T) {
	facts := []Fact{dispatched, claimed, started, ran(OutcomeCompleted, "X"), stored("X", t2), Cancel{}}
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
