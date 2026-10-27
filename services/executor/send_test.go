package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// No plate reader, so a run can complete before S6 stores plate data.
var sendWorkflow = []StepTemplate{
	{Name: "fill", DeviceID: "liquid-handler-1"},
	{Name: "incubate", DeviceID: "incubator-1", DependsOn: []string{"fill"}},
	{Name: "wash", DeviceID: "liquid-handler-1", DependsOn: []string{"incubate"}},
}

// fakeDriver keeps the driver's side of D1 for one device: answer at once,
// claim before running, report from its own ledger, never run a job twice.
type fakeDriver struct {
	t      *testing.T
	nc     *nats.Conn
	js     jetstream.JetStream
	device string

	mu      sync.Mutex
	busy    int                  // reply busy to this many commands first
	drop    int                  // accept but send no reply to this many commands
	outcome map[string]string    // step -> ledger outcome; default completed
	ledger  map[string]int       // job -> times it physically ran
	cmds    map[string]int       // job -> commands received
	running bool                 // one job at a time, like the instrument
	at      map[string]time.Time // job -> ledger finished_at
	sha     map[string]string    // step -> ledger data_sha256
	lose    map[string]bool      // step -> report its data lost after finishing
	sub     *nats.Subscription
}

func newFakeDriver(t *testing.T, nc *nats.Conn, device string) *fakeDriver {
	js, _ := jetstream.New(nc)
	d := &fakeDriver{t: t, nc: nc, js: js, device: device,
		outcome: map[string]string{}, ledger: map[string]int{}, cmds: map[string]int{}, at: map[string]time.Time{},
		sha: map[string]string{}, lose: map[string]bool{}}
	return d
}

func (d *fakeDriver) serve() *fakeDriver {
	var err error
	d.sub, err = d.nc.Subscribe(protocol.RunSubject(d.device), d.onRun)
	if err != nil {
		d.t.Fatal(err)
	}
	d.t.Cleanup(func() { d.sub.Unsubscribe() })
	return d
}

func (d *fakeDriver) onRun(m *nats.Msg) {
	var cmd protocol.RunCommand
	if err := protocol.Decode(m.Data, &cmd); err != nil {
		d.t.Errorf("%s: bad command: %v", d.device, err)
		return
	}
	d.mu.Lock()
	d.cmds[cmd.JobID]++
	result, reply := protocol.RunAccepted, true
	switch {
	case d.ledger[cmd.JobID] > 0:
		result = protocol.RunKnown
	case d.busy > 0:
		d.busy--
		result = protocol.RunBusy
	case d.running:
		result = protocol.RunBusy
	default:
		d.running = true
		d.ledger[cmd.JobID]++ // reserved before the reply, as the real driver's accept does
		if d.drop > 0 {
			d.drop--
			reply = false
		}
	}
	d.mu.Unlock()
	if reply {
		b, _ := json.Marshal(protocol.RunReply{V: protocol.Version, Result: result})
		m.Respond(b)
	}
	if result == protocol.RunAccepted {
		go d.execute(cmd)
	}
}

func (d *fakeDriver) execute(cmd protocol.RunCommand) {
	defer func() { d.mu.Lock(); d.running = false; d.mu.Unlock() }()
	b, _ := json.Marshal(protocol.ClaimRequest{V: protocol.Version, JobID: cmd.JobID, Device: d.device})
	msg, err := d.nc.Request(protocol.ClaimSubject, b, 2*time.Second)
	if err != nil {
		d.t.Errorf("%s: claim %s: %v", d.device, cmd.JobID, err)
		return
	}
	var claim protocol.ClaimReply
	if protocol.Decode(msg.Data, &claim); claim.Result != protocol.ClaimGo {
		return
	}
	start := time.Now().UTC()
	end := start.Add(10 * time.Millisecond)
	d.mu.Lock()
	outcome, sha, lose := d.outcome[cmd.Step], d.sha[cmd.Step], d.lose[cmd.Step]
	d.at[cmd.JobID] = end
	d.mu.Unlock()
	if outcome == "" {
		outcome = "completed"
	}
	d.publish(protocol.Event{Kind: protocol.KindStarted, JobID: cmd.JobID, StartedAt: &start})
	d.publish(protocol.Event{Kind: protocol.KindFinished, JobID: cmd.JobID, Outcome: outcome, StartedAt: &start, FinishedAt: &end, SHA: sha})
	if lose {
		d.publish(protocol.Event{Kind: protocol.KindDataLost, JobID: cmd.JobID})
	}
}

func (d *fakeDriver) publish(ev protocol.Event) {
	ev.V, ev.Device = protocol.Version, d.device
	b, _ := json.Marshal(ev)
	if _, err := d.js.Publish(context.Background(), protocol.EventSubject(d.device, ev.Kind), b,
		jetstream.WithMsgID(protocol.MsgID(ev.JobID, ev.Kind))); err != nil {
		d.t.Errorf("%s: publish: %v", d.device, err)
	}
}

func (d *fakeDriver) ran(jobID string) int { d.mu.Lock(); defer d.mu.Unlock(); return d.ledger[jobID] }
func (d *fakeDriver) commands(jobID string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cmds[jobID]
}

func (h *harness) drivers() (lh, inc *fakeDriver) {
	return newFakeDriver(h.t, h.nc, "liquid-handler-1").serve(), newFakeDriver(h.t, h.nc, "incubator-1").serve()
}

func (h *harness) startRun(steps []StepTemplate) RunView {
	h.t.Helper()
	rv, err := h.e.CreateRun(context.Background(), "test", steps)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.e.StartRun(context.Background(), rv.ID); err != nil {
		h.t.Fatal(err)
	}
	return rv
}

func (h *harness) waitRun(runID, status string, within time.Duration) RunView {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for {
		rv, err := h.e.GetRun(context.Background(), runID)
		if err != nil {
			h.t.Fatal(err)
		}
		if rv.Status == status {
			return rv
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("run is %s after %s, want %s; steps %+v", rv.Status, within, status, rv.Steps)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRunCompletesEndToEnd(t *testing.T) {
	h := newHarness(t)
	lh, inc := h.drivers()
	rv := h.startRun(sendWorkflow)

	done := h.waitRun(rv.ID, RunCompleted, 10*time.Second)
	var last time.Time
	for _, s := range done.Steps {
		if s.Status != StepCompleted {
			t.Errorf("step %s is %s", s.Name, s.Status)
		}
		if s.FinishedAt != nil && s.FinishedAt.After(last) {
			last = *s.FinishedAt
		}
	}
	if done.FinishedAt == nil || !done.FinishedAt.Equal(last) {
		t.Errorf("run finished_at %v, want the last step's ledger finished_at %v", done.FinishedAt, last)
	}
	for drv, steps := range map[*fakeDriver][]string{lh: {"fill", "wash"}, inc: {"incubate"}} {
		for _, step := range steps {
			if n := drv.ran(protocol.JobID(rv.ID, step, 1)); n != 1 {
				t.Errorf("%s ran %d times, want 1", step, n)
			}
		}
	}
}

func TestBusyDeviceIsAskedAgain(t *testing.T) {
	h := newHarness(t)
	lh, _ := h.drivers()
	lh.busy = 3
	rv := h.startRun(sendWorkflow)
	h.waitRun(rv.ID, RunCompleted, 10*time.Second)
	fill := protocol.JobID(rv.ID, "fill", 1)
	if lh.commands(fill) < 4 || lh.ran(fill) != 1 {
		t.Errorf("fill: %d commands, ran %d times; want at least 4 commands and one run", lh.commands(fill), lh.ran(fill))
	}
}

// The command arrives but its reply is lost: the executor cannot tell it ran,
// asks again, and the driver's "known" keeps it from running twice.
func TestLostReplyDoesNotRunTwice(t *testing.T) {
	h := newHarness(t)
	lh, _ := h.drivers()
	lh.drop = 1
	rv := h.startRun(sendWorkflow)
	h.waitRun(rv.ID, RunCompleted, 15*time.Second)
	if fill := protocol.JobID(rv.ID, "fill", 1); lh.ran(fill) != 1 {
		t.Errorf("fill ran %d times, want 1", lh.ran(fill))
	}
}

func TestFailedStepFailsTheRun(t *testing.T) {
	h := newHarness(t)
	lh, inc := h.drivers()
	inc.outcome["incubate"] = "failed"
	rv := h.startRun(sendWorkflow)
	done := h.waitRun(rv.ID, RunFailed, 10*time.Second)
	if done.FinishedAt == nil {
		t.Error("failed run has no finished_at")
	}
	if got := stepView(t, h.e, rv.ID, "wash").Status; got != StepPending {
		t.Errorf("wash is %s; a step after a failure was never sent, so it stays pending", got)
	}
	if n := lh.commands(protocol.JobID(rv.ID, "wash", 1)); n != 0 {
		t.Errorf("wash was commanded %d times after its dependency failed", n)
	}
}

// What was dispatched is in Postgres, so a new executor process finishes the
// job of the old one: it resends to the driver that was not there before.
func TestDispatchSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	rv := h.startRun(sendWorkflow)
	h.waitStep(rv.ID, "fill", StepPending) // dispatched maps to pending in the API
	deadline := time.Now().Add(5 * time.Second)
	for phaseOf(t, h.e, rv.ID, "fill") != PhaseDispatched {
		if time.Now().After(deadline) {
			t.Fatal("fill was never dispatched")
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.e.Close()

	again := NewExecutor(h.e.db, h.nc, nil)
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Close)
	h.e = again
	h.drivers()
	h.waitRun(rv.ID, RunCompleted, 10*time.Second)
}

// A run's steps hold off a plate read: until its data is stored the run stays running.
func TestRunWaitsForEveryStep(t *testing.T) {
	h := newHarness(t)
	h.drivers()
	rv := h.startRun([]StepTemplate{{Name: "fill", DeviceID: "liquid-handler-1"}, {Name: "incubate", DeviceID: "incubator-1"}})
	h.waitRun(rv.ID, RunCompleted, 10*time.Second)

	pr := h.startRun(testWorkflow) // fill -> read_plate; nobody serves the plate reader
	h.waitStep(pr.ID, "fill", StepCompleted)
	time.Sleep(time.Second)
	if got, _ := h.e.GetRun(context.Background(), pr.ID); got.Status != RunRunning {
		t.Errorf("run with an unsent read is %s, want running", got.Status)
	}
}

func phaseOf(t *testing.T, e *Executor, runID, name string) Phase {
	t.Helper()
	var p Phase
	if err := e.db.QueryRow(`SELECT phase FROM steps WHERE run_id = $1 AND name = $2`, runID, name).Scan(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

// Nothing repeats a hang yet, so a hung step ends failed with the reason, and
// so does its run (D4). Before D4 it stayed running for ever.
func TestHungStepFailsItsRun(t *testing.T) {
	h := newHarness(t)
	lh, inc := h.drivers()
	inc.outcome["incubate"] = "hung"
	rv := h.startRun(sendWorkflow)
	h.waitRun(rv.ID, RunFailed, 10*time.Second)
	sv := stepView(t, h.e, rv.ID, "incubate")
	if sv.Status != StepFailed || sv.Error == nil || *sv.Error != "instrument hung; outcome unknown; not repeated" {
		t.Errorf("incubate: %s, %v; want failed, instrument hung; outcome unknown; not repeated", sv.Status, sv.Error)
	}
	if n := lh.commands(protocol.JobID(rv.ID, "wash", 1)); n != 0 {
		t.Errorf("wash was commanded %d times after incubate failed", n)
	}
}
