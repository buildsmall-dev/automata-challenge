package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var devices = []string{"liquid-handler-1", "incubator-1", "plate-reader-1"}

func TestSubjects(t *testing.T) {
	if got := RunSubject("incubator-1"); got != "lab.incubator-1.cmd.run" {
		t.Errorf("RunSubject = %q", got)
	}
	if got := EventSubject("plate-reader-1", KindFinished); got != "lab.plate-reader-1.evt.finished" {
		t.Errorf("EventSubject = %q", got)
	}
	if ClaimSubject != "lab.executor.claim" {
		t.Errorf("ClaimSubject = %q", ClaimSubject)
	}
	// Every event lands in the stream; commands and claims stay out of it.
	for _, d := range devices {
		for _, k := range []Kind{KindStarted, KindFinished, KindStored, KindDataLost} {
			if s := EventSubject(d, k); !matches(StreamSubjects, s) {
				t.Errorf("event subject %q is not captured by stream subjects %q", s, StreamSubjects)
			}
		}
		if matches(StreamSubjects, RunSubject(d)) {
			t.Errorf("command subject %q must not be stored in the event stream", RunSubject(d))
		}
	}
	if matches(StreamSubjects, ClaimSubject) {
		t.Error("claims must not be stored in the event stream")
	}
	if StreamName == "" {
		t.Error("StreamName is empty")
	}
}

func TestIDs(t *testing.T) {
	a1, a2 := JobID("run-1", "combine", 1), JobID("run-1", "combine", 2)
	if a1 != "run-1/combine/1" || a1 == a2 {
		t.Errorf("JobID = %q, %q; want run/step/attempt, distinct per attempt", a1, a2)
	}
	if JobID("run-1", "combine", 1) == JobID("run-2", "combine", 1) || JobID("run-1", "fill", 1) == a1 {
		t.Error("JobID must differ by run and by step")
	}
	if MsgID(a1, KindStarted) == MsgID(a1, KindFinished) || MsgID(a1, KindFinished) == MsgID(a2, KindFinished) {
		t.Error("MsgID must differ by job and by kind, so dedupe only drops true repeats")
	}
	if MsgID(a1, KindFinished) != MsgID(a1, KindFinished) {
		t.Error("MsgID must be the same for a repeat of the same event")
	}
	if got := DataKey("run-1/read_plate/1"); got != "runs/run-1/read_plate/1.csv" {
		t.Errorf("DataKey = %q", got)
	}
}

// The wire shape is the contract: a renamed field must fail here, not in production.
func TestWireShape(t *testing.T) {
	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		msg  any
		want string
	}{
		{NewRunCommand("run-1", "fill", 1),
			`{"v":1,"job_id":"run-1/fill/1","run_id":"run-1","step":"fill","attempt":1}`},
		{RunReply{V: Version, Result: RunAccepted},
			`{"v":1,"result":"accepted"}`},
		{ClaimRequest{V: Version, JobID: "run-1/fill/1", Device: "liquid-handler-1"},
			`{"v":1,"job_id":"run-1/fill/1","device":"liquid-handler-1"}`},
		{ClaimReply{V: Version, Result: ClaimAbort, Reason: "cancelled"},
			`{"v":1,"result":"abort","reason":"cancelled"}`},
		{Event{V: Version, Kind: KindFinished, Device: "plate-reader-1", JobID: "run-1/read_plate/1",
			StartedAt: &at, FinishedAt: &at, Outcome: "completed", SHA: "abc"},
			`{"v":1,"kind":"finished","device":"plate-reader-1","job_id":"run-1/read_plate/1","started_at":"2026-10-26T09:00:00Z","finished_at":"2026-10-26T09:00:00Z","outcome":"completed","sha256":"abc"}`},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.msg)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Errorf("%T\n got %s\nwant %s", c.msg, got, c.want)
		}
	}
}

func TestDecodeRejectsOtherVersions(t *testing.T) {
	var cmd RunCommand
	if err := Decode([]byte(`{"v":1,"job_id":"run-1/fill/1","run_id":"run-1","step":"fill","attempt":1}`), &cmd); err != nil || cmd.Step != "fill" {
		t.Fatalf("decode v1: %v (%+v)", err, cmd)
	}
	for _, raw := range []string{`{"v":2,"job_id":"x"}`, `{"job_id":"x"}`, `not json`} {
		var c RunCommand
		if err := Decode([]byte(raw), &c); err == nil {
			t.Errorf("Decode(%s) accepted it", raw)
		}
	}
}

func TestEventValidate(t *testing.T) {
	at := time.Now()
	ok := map[string]Event{
		"started":   {Kind: KindStarted, StartedAt: &at},
		"finished":  {Kind: KindFinished, Outcome: "hung", StartedAt: &at, FinishedAt: &at},
		"stored":    {Kind: KindStored, Ref: "s3://lab-data/runs/run-1/read_plate/1.csv"},
		"data_lost": {Kind: KindDataLost},
	}
	bad := map[string]Event{
		"no kind":                  {},
		"unknown kind":             {Kind: "exploded"},
		"started without a time":   {Kind: KindStarted},
		"finished without times":   {Kind: KindFinished, Outcome: "completed"},
		"finished, odd outcome":    {Kind: KindFinished, Outcome: "maybe", StartedAt: &at, FinishedAt: &at},
		"stored without a ref":     {Kind: KindStored},
		"stored, ref is not s3://": {Kind: KindStored, Ref: "/tmp/x.csv"},
	}
	for name, e := range ok {
		e.V, e.Device, e.JobID = Version, "plate-reader-1", "run-1/read_plate/1"
		if err := e.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, e := range bad {
		e.V, e.Device, e.JobID = Version, "plate-reader-1", "run-1/read_plate/1"
		if err := e.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := (Event{V: Version, Kind: KindDataLost, Device: "plate-reader-1"}).Validate(); err == nil {
		t.Error("event without job_id accepted")
	}
}

// matches is NATS subject matching: * is one token, > is one or more at the end.
func matches(pattern, subject string) bool {
	if pattern == "" {
		return false
	}
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) || tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

func TestParseJobID(t *testing.T) {
	for _, c := range []struct {
		job, run, step string
		attempt        int
		ok             bool
	}{
		{JobID("run-1", "fill", 1), "run-1", "fill", 1, true},
		{JobID("run-1", "read/plate", 12), "run-1", "read/plate", 12, true}, // the run id has no slash; the step may
		{"run-1/fill", "", "", 0, false},
		{"run-1/fill/x", "", "", 0, false},
		{"run-1/fill/0", "", "", 0, false},
		{"/fill/1", "", "", 0, false},
		{"run-1//1", "", "", 0, false},
	} {
		run, step, attempt, ok := ParseJobID(c.job)
		if run != c.run || step != c.step || attempt != c.attempt || ok != c.ok {
			t.Errorf("ParseJobID(%q) = %q, %q, %d, %v; want %q, %q, %d, %v", c.job, run, step, attempt, ok, c.run, c.step, c.attempt, c.ok)
		}
	}
}
