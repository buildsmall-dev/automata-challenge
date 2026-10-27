package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
)

func testNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

type harness struct {
	t  *testing.T
	nc *nats.Conn
	js jetstream.JetStream
	e  *Executor
}

// st is optional: only checks of plate data need storage.
func newHarness(t *testing.T, st ...*storage.Storage) *harness {
	db, nc := testDB(t), testNATS(t)
	js, _ := jetstream.New(nc)
	var store *storage.Storage
	if len(st) > 0 {
		store = st[0]
	}
	e := NewExecutor(db, nc, store)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return &harness{t: t, nc: nc, js: js, e: e}
}

// dispatch puts a step of a new run in the dispatched phase and returns its job.
func (h *harness) dispatch(rv RunView, step string) string {
	h.t.Helper()
	jobID := protocol.JobID(rv.ID, step, 1)
	if _, _, err := h.e.applyToStep(context.Background(), rv.ID, step, Dispatched{JobID: jobID, Attempt: 1}); err != nil {
		h.t.Fatal(err)
	}
	return jobID
}

func (h *harness) claim(jobID string) protocol.ClaimReply {
	h.t.Helper()
	return h.claimAs(deviceOf(jobID), jobID)
}

func (h *harness) claimAs(device, jobID string) protocol.ClaimReply {
	h.t.Helper()
	b, _ := json.Marshal(protocol.ClaimRequest{V: protocol.Version, JobID: jobID, Device: device})
	msg, err := h.nc.Request(protocol.ClaimSubject, b, 2*time.Second)
	if err != nil {
		h.t.Fatalf("claim %s: %v", jobID, err)
	}
	var reply protocol.ClaimReply
	if err := protocol.Decode(msg.Data, &reply); err != nil {
		h.t.Fatal(err)
	}
	return reply
}

func (h *harness) publish(subject string, payload []byte, msgID string) {
	h.t.Helper()
	if _, err := h.js.Publish(context.Background(), subject, payload, jetstream.WithMsgID(msgID)); err != nil {
		h.t.Fatalf("publish %s: %v", subject, err)
	}
}

func (h *harness) event(device string, ev protocol.Event) {
	h.t.Helper()
	ev.V, ev.Device = protocol.Version, device
	b, _ := json.Marshal(ev)
	h.publish(protocol.EventSubject(device, ev.Kind), b, protocol.MsgID(ev.JobID, ev.Kind))
}

func (h *harness) waitStep(runID, name, status string) StepView {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sv := stepView(h.t, h.e, runID, name)
		if sv.Status == status || time.Now().After(deadline) {
			if sv.Status != status {
				h.t.Fatalf("step %s is %s, want %s", name, sv.Status, status)
			}
			return sv
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitDrained: the executor's consumer has nothing pending or unacknowledged.
func (h *harness) waitDrained() *jetstream.ConsumerInfo {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cons, err := h.js.Consumer(context.Background(), protocol.StreamName, consumerName)
		if err != nil {
			h.t.Fatal(err)
		}
		info, err := cons.Info(context.Background())
		if err != nil {
			h.t.Fatal(err)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			return info
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("consumer not drained: pending %d, ack pending %d, redelivered %d", info.NumPending, info.NumAckPending, info.NumRedelivered)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func finished(jobID string, at time.Time) protocol.Event {
	end := at.Add(2 * time.Second)
	return protocol.Event{Kind: protocol.KindFinished, JobID: jobID, Outcome: "completed", StartedAt: &at, FinishedAt: &end}
}

func TestStartCreatesTheEventStream(t *testing.T) {
	h := newHarness(t)
	s, err := h.js.Stream(context.Background(), protocol.StreamName)
	if err != nil {
		t.Fatalf("stream %s: %v", protocol.StreamName, err)
	}
	info, _ := s.Info(context.Background())
	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != protocol.StreamSubjects || info.Config.Storage != jetstream.FileStorage {
		t.Errorf("stream config = %+v", info.Config)
	}
	// Starting again (a restart) must keep the same stream, not fail.
	again := NewExecutor(h.e.db, h.nc, nil)
	if err := again.Start(context.Background()); err != nil {
		t.Errorf("second Start: %v", err)
	}
	again.Close()
}

func TestClaimRepliesFromTheRecord(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)

	fill := h.dispatch(rv, "fill")
	// Misrouted: only the assigned instrument may run a step.
	if r := h.claimAs("incubator-1", fill); r.Result != protocol.ClaimAbort {
		t.Fatalf("claim by another device = %+v, want abort", r)
	}
	if r := h.claim(fill); r.Result != protocol.ClaimGo {
		t.Fatalf("claim of a dispatched step = %+v, want go", r)
	}
	if got := stepView(t, h.e, rv.ID, "fill").Status; got != StepRunning {
		t.Errorf("after the claim the step is %s, want running", got)
	}
	if r := h.claim(fill); r.Result != protocol.ClaimGo {
		t.Errorf("repeated claim = %+v, want go again", r)
	}

	read := h.dispatch(rv, "read_plate")
	if _, _, err := h.e.applyToStep(context.Background(), rv.ID, "read_plate", Cancel{}); err != nil {
		t.Fatal(err)
	}
	if r := h.claim(read); r.Result != protocol.ClaimAbort || r.Reason == "" {
		t.Errorf("claim of a cancelled step = %+v, want abort with a reason", r)
	}
	if r := h.claim("run-nope/fill/1"); r.Result != protocol.ClaimAbort {
		t.Errorf("claim of an unknown job = %+v, want abort", r)
	}
}

func TestEventsBecomeFacts(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)
	job := h.dispatch(rv, "fill")
	h.claim(job)

	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	h.event("liquid-handler-1", protocol.Event{Kind: protocol.KindStarted, JobID: job, StartedAt: &at})
	h.waitStep(rv.ID, "fill", StepRunning)
	h.event("liquid-handler-1", finished(job, at))
	sv := h.waitStep(rv.ID, "fill", StepCompleted)
	if !sv.StartedAt.Equal(at) || !sv.FinishedAt.Equal(at.Add(2*time.Second)) {
		t.Errorf("times %v..%v, want the event's ledger times", sv.StartedAt, sv.FinishedAt)
	}
	h.waitDrained()
}

// Bad messages are dropped, not redelivered forever, and do not hold up good ones.
func TestBadEventsAreDroppedAndGoodOnesStillApply(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)
	job := h.dispatch(rv, "fill")
	h.claim(job)
	at := time.Now().UTC()

	h.publish("lab.liquid-handler-1.evt.finished", []byte("not json"), "bad-1")
	missing, _ := json.Marshal(protocol.Event{V: protocol.Version, Kind: protocol.KindFinished, Device: "liquid-handler-1", JobID: job})
	h.publish("lab.liquid-handler-1.evt.finished", missing, "bad-2")
	// Claims to be the liquid handler, but published on the incubator's subject.
	// Says "failed": if it were applied, the step could not end completed.
	wrong, _ := json.Marshal(func() protocol.Event {
		e := finished(job, at)
		e.V, e.Device, e.Outcome = protocol.Version, "liquid-handler-1", "failed"
		return e
	}())
	h.publish("lab.incubator-1.evt.finished", wrong, "bad-3")
	// Consistent with its subject, but from a device the step is not assigned to.
	other, _ := json.Marshal(func() protocol.Event {
		e := finished(job, at)
		e.V, e.Device, e.Outcome = protocol.Version, "incubator-1", "failed"
		return e
	}())
	h.publish(protocol.EventSubject("incubator-1", protocol.KindFinished), other, "bad-4")
	unknown := finished("run-nope/fill/1", at)
	h.event("liquid-handler-1", unknown)

	if got := stepView(t, h.e, rv.ID, "fill").Status; got != StepRunning {
		t.Fatalf("bad events changed the step to %s", got)
	}
	h.event("liquid-handler-1", finished(job, at))
	h.waitStep(rv.ID, "fill", StepCompleted)
	if info := h.waitDrained(); info.NumRedelivered != 0 {
		t.Errorf("%d messages were redelivered; bad and unknown events must be settled, not retried", info.NumRedelivered)
	}
}

// A repeat that got past the dedupe window (another msg id) changes nothing.
func TestRepeatedEventIsHarmless(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)
	job := h.dispatch(rv, "fill")
	h.claim(job)
	at := time.Now().UTC()
	h.event("liquid-handler-1", finished(job, at))
	h.waitStep(rv.ID, "fill", StepCompleted)
	b, _ := json.Marshal(func() protocol.Event {
		e := finished(job, at)
		e.V, e.Device = protocol.Version, "liquid-handler-1"
		return e
	}())
	h.publish(protocol.EventSubject("liquid-handler-1", protocol.KindFinished), b, "replayed-later")
	h.waitDrained()
	if got := stepView(t, h.e, rv.ID, "fill"); got.Status != StepCompleted || got.Error != nil {
		t.Errorf("after a repeat: %s, error %v", got.Status, got.Error)
	}
}

// Events published while the executor is down wait in the stream.
func TestEventsWaitForTheExecutor(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)
	job := h.dispatch(rv, "fill")
	h.claim(job)
	h.e.Close()

	h.event("liquid-handler-1", finished(job, time.Now().UTC()))
	time.Sleep(300 * time.Millisecond)
	if got := stepView(t, h.e, rv.ID, "fill").Status; got != StepRunning {
		t.Fatalf("applied while the executor was down: %s", got)
	}
	again := NewExecutor(h.e.db, h.nc, nil)
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Close)
	h.e = again
	h.waitStep(rv.ID, "fill", StepCompleted)
}

// The step keeps what the instrument said about repeating a failure (D4).
func TestFinishedKeepsRetryable(t *testing.T) {
	h := newHarness(t)
	rv := newRun(t, h.e)
	job := h.dispatch(rv, "fill")
	h.claim(job)
	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	ev := finished(job, at)
	ev.Outcome, ev.Error, ev.Retryable = "failed", "instrument error during fill", true
	h.event("liquid-handler-1", ev)
	// The run was never started, so no decision follows: the step stays ended.
	for deadline := time.Now().Add(5 * time.Second); phaseOf(t, h.e, rv.ID, "fill") != PhaseEnded; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("fill is %s, want ended", phaseOf(t, h.e, rv.ID, "fill"))
		}
	}

	var s Step
	var raw []byte
	if err := h.e.db.QueryRow(`SELECT lifecycle FROM steps WHERE job_id = $1`, job).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Attempts) != 1 || !s.Attempts[0].Retryable {
		t.Errorf("step after a retryable failure = %+v, want Retryable", s)
	}
}
