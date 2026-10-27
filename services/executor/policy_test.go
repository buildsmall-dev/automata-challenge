package main

import (
	"reflect"
	"testing"
)

func TestPolicyFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	if p, err := PolicyFromEnv(env(nil)); err != nil || !reflect.DeepEqual(p, defaultPolicy) {
		t.Errorf("unset: %+v, %v; want D4's defaults %+v", p, err, defaultPolicy)
	}
	p, err := PolicyFromEnv(env(map[string]string{"REPEAT_SAFE": "plate-reader, incubator", "REREAD_ON_DATA_LOSS": "", "MAX_ATTEMPTS": "2"}))
	want := Policy{RepeatSafe: []string{"plate-reader", "incubator"}, MaxAttempts: 2}
	if err != nil || !reflect.DeepEqual(p, want) {
		t.Errorf("set: %+v, %v; want %+v (empty means none)", p, err, want)
	}
	if p, _ := PolicyFromEnv(env(map[string]string{"REPEAT_SAFE": ""})); len(p.RepeatSafe) != 0 {
		t.Errorf("REPEAT_SAFE empty: %v, want no device", p.RepeatSafe)
	}
	for _, bad := range []string{"0", "x", "-1"} {
		if _, err := PolicyFromEnv(env(map[string]string{"MAX_ATTEMPTS": bad})); err == nil {
			t.Errorf("MAX_ATTEMPTS=%q accepted", bad)
		}
	}
}

// What D4 does with a step none of whose attempts is in flight. The step is
// run-1's read_plate; the device decides what is safe.
func TestNextAttempt(t *testing.T) {
	failedRetryable := ranN(1, OutcomeFailed, "")
	failedRetryable.Retryable = true
	lost1 := []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, "X"), lostN(1)}
	again := func(n int) Fact { return Dispatched{JobID: jobN(n), Attempt: n} }
	giveUp := func(why string) Fact { return GaveUp{Reason: "not repeated: " + why} }
	retryableN := func(n int) Fact { f := ranN(n, OutcomeFailed, ""); f.Retryable = true; return f }
	cases := []struct {
		name   string
		device string
		start  Step
		facts  []Fact
		want   Fact
	}{
		{"retryable failure: again", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1), failedRetryable}, again(2)},
		{"failure not marked retryable: give up", "liquid-handler-1", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeFailed, "")},
			giveUp("the instrument did not mark the failure retryable")},
		{"hang on a repeat-safe device: again", "plate-reader-1", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeHung, "")}, again(2)},
		{"hang elsewhere: give up", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeHung, "")},
			giveUp("incubator-1 is not safe to repeat")},
		{"lost read on a device that re-reads: again", "plate-reader-1", reader, lost1, again(2)},
		{"lost read elsewhere: nothing, it stays open", "spectrometer-1", reader, lost1, nil},
		{"third attempt failed: give up", "incubator-1", Step{},
			[]Fact{dispatchN(1), claimN(1), retryableN(1), dispatchN(2), claimN(2), retryableN(2), dispatchN(3), claimN(3), retryableN(3)},
			giveUp("3 attempts made")},
		{"lost read at the limit: nothing, it stays open", "plate-reader-1", reader,
			append(lost1, dispatchN(2), claimN(2), ranN(2, OutcomeCompleted, "Y"), lostN(2), dispatchN(3), claimN(3), ranN(3, OutcomeCompleted, "Z"), lostN(3)), nil},
		{"a re-read that failed retryably is retried, the lost read still open", "plate-reader-1", reader,
			append(lost1, dispatchN(2), claimN(2), retryableN(2)), again(3)},
		{"a re-read that failed for good: nothing to give up while the lost read is open", "plate-reader-1", reader,
			append(lost1, dispatchN(2), claimN(2), ranN(2, OutcomeFailed, "")), nil},
		{"cancelled: nothing", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1), failedRetryable, Cancel{}}, nil},
		{"in flight: nothing", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1)}, nil},
		{"completed: nothing", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeCompleted, "")}, nil},
		{"two outcomes reported: nothing", "incubator-1", Step{}, []Fact{dispatchN(1), claimN(1), failedRetryable, ranN(1, OutcomeCompleted, "")}, nil},
		{"given up already: nothing", "liquid-handler-1", Step{}, []Fact{dispatchN(1), claimN(1), ranN(1, OutcomeFailed, ""), gaveUp}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultPolicy.next("run-1", "read_plate", c.device, applyAll(c.start, c.facts...))
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("next = %#v, want %#v", got, c.want)
			}
		})
	}
}
