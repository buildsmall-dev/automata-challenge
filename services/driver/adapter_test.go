package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// Each test gets its own in-process NATS server with JetStream, so the
// LAB_EVENTS stream never clashes with the compose stack's.
type rig struct {
	t      *testing.T
	nc     *nats.Conn
	js     jetstream.JetStream
	inst   *instrument.Instrument
	device string
}

// st is optional: only the plate reader needs storage.
func newRig(t *testing.T, cfg instrument.Config, withStream bool, st ...*storage.Storage) *rig {
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
	js, _ := jetstream.New(nc)

	cfg.DeviceID = "test-device"
	if cfg.LedgerDir == "" {
		cfg.LedgerDir = t.TempDir()
	}
	cfg.Seed = "0"
	if cfg.Duration == 0 {
		cfg.Duration = 50 * time.Millisecond
	}
	cfg.HangDuration = 300 * time.Millisecond
	inst, err := instrument.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, nc: nc, js: js, inst: inst, device: cfg.DeviceID}
	if withStream {
		r.createStream()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var store *storage.Storage
	if len(st) > 0 {
		store = st[0]
	}
	go func() { defer close(done); Serve(ctx, inst, nc, store) }()
	t.Cleanup(func() { cancel(); <-done })
	return r
}

func (r *rig) createStream() {
	if _, err := r.js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name: protocol.StreamName, Subjects: []string{protocol.StreamSubjects}}); err != nil {
		r.t.Fatal(err)
	}
}

// claimer stands in for the executor's claim handler.
func (r *rig) claimer(answer protocol.ClaimResult) *[]string {
	var mu sync.Mutex
	var asked []string
	sub, err := r.nc.Subscribe(protocol.ClaimSubject, func(m *nats.Msg) {
		var req protocol.ClaimRequest
		if err := protocol.Decode(m.Data, &req); err != nil {
			r.t.Errorf("bad claim: %v", err)
			return
		}
		mu.Lock()
		asked = append(asked, req.JobID)
		mu.Unlock()
		b, _ := json.Marshal(protocol.ClaimReply{V: protocol.Version, Result: answer})
		m.Respond(b)
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { sub.Unsubscribe() })
	return &asked
}

func (r *rig) send(cmd protocol.RunCommand) protocol.RunResult {
	r.t.Helper()
	b, _ := json.Marshal(cmd)
	deadline := time.Now().Add(3 * time.Second)
	for {
		msg, err := r.nc.Request(protocol.RunSubject(r.device), b, time.Second)
		if errors.Is(err, nats.ErrNoResponders) && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond) // the driver is still subscribing
			continue
		}
		if err != nil {
			r.t.Fatalf("run command: %v", err)
		}
		var reply protocol.RunReply
		if err := protocol.Decode(msg.Data, &reply); err != nil {
			r.t.Fatalf("run reply: %v", err)
		}
		return reply.Result
	}
}

type event struct {
	protocol.Event
	msgID string
}

// events waits until the stream holds n events for this device, then returns them.
func (r *rig) events(n int, within time.Duration) []event {
	r.t.Helper()
	deadline := time.Now().Add(within)
	for {
		var out []event
		cons, err := r.js.OrderedConsumer(context.Background(), protocol.StreamName,
			jetstream.OrderedConsumerConfig{FilterSubjects: []string{"lab." + r.device + ".evt.*"}})
		if err == nil {
			batch, _ := cons.Fetch(100, jetstream.FetchMaxWait(100*time.Millisecond))
			for m := range batch.Messages() {
				var e protocol.Event
				if err := protocol.Decode(m.Data(), &e); err != nil {
					r.t.Fatalf("event: %v", err)
				}
				if err := e.Validate(); err != nil {
					r.t.Errorf("invalid event %+v: %v", e, err)
				}
				if m.Subject() != protocol.EventSubject(e.Device, e.Kind) {
					r.t.Errorf("event %s published on %s", e.Kind, m.Subject())
				}
				out = append(out, event{e, m.Headers().Get(jetstream.MsgIDHeader)})
			}
		}
		if len(out) >= n || time.Now().After(deadline) {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *rig) executions(jobID string) []instrument.Execution {
	var out []instrument.Execution
	for _, e := range r.inst.Status().Executions {
		if e.JobID == jobID {
			out = append(out, e)
		}
	}
	return out
}

func kinds(es []event) []protocol.Kind {
	var out []protocol.Kind
	for _, e := range es {
		out = append(out, e.Kind)
	}
	return out
}

func TestRunsAClaimedJobOnceAndReportsIt(t *testing.T) {
	r := newRig(t, instrument.Config{}, true)
	asked := r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand("run-1", "fill", 1)

	if got := r.send(cmd); got != protocol.RunAccepted {
		t.Fatalf("reply = %s, want accepted", got)
	}
	es := r.events(2, 3*time.Second)
	if len(es) != 2 || es[0].Kind != protocol.KindStarted || es[1].Kind != protocol.KindFinished {
		t.Fatalf("events = %v, want [started finished]", kinds(es))
	}
	fin := es[1]
	if fin.Outcome != instrument.OutcomeCompleted || fin.JobID != cmd.JobID || fin.StartedAt == nil || fin.FinishedAt == nil {
		t.Errorf("finished = %+v", fin.Event)
	}
	for _, e := range es {
		if e.msgID != protocol.MsgID(cmd.JobID, e.Kind) {
			t.Errorf("%s has Nats-Msg-Id %q, want %q", e.Kind, e.msgID, protocol.MsgID(cmd.JobID, e.Kind))
		}
	}
	ex := r.executions(cmd.JobID)
	if len(ex) != 1 {
		t.Fatalf("ledger has %d executions of %s, want 1", len(ex), cmd.JobID)
	}
	if want := time.Unix(0, int64(ex[0].StartedAt*1e9)); fin.StartedAt.Sub(want).Abs() > time.Millisecond {
		t.Errorf("started_at %v, want the ledger's %v", fin.StartedAt, want)
	}
	if len(*asked) != 1 || (*asked)[0] != cmd.JobID {
		t.Errorf("claims = %v, want one for %s", *asked, cmd.JobID)
	}

	// The same command again, once done: recognised from the ledger, not rerun.
	if got := r.send(cmd); got != protocol.RunKnown {
		t.Errorf("repeat reply = %s, want known", got)
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(r.executions(cmd.JobID)); n != 1 {
		t.Errorf("after a repeat the ledger has %d executions, want 1", n)
	}
}

func TestRepeatWhileRunningIsKnown(t *testing.T) {
	r := newRig(t, instrument.Config{Duration: 400 * time.Millisecond}, true)
	r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand("run-1", "fill", 1)
	r.send(cmd)
	time.Sleep(100 * time.Millisecond)
	if got := r.send(cmd); got != protocol.RunKnown {
		t.Errorf("repeat while running = %s, want known", got)
	}
	r.events(2, 3*time.Second)
	if n := len(r.executions(cmd.JobID)); n != 1 {
		t.Errorf("ledger has %d executions, want 1", n)
	}
}

func TestOtherJobWhileRunningIsBusy(t *testing.T) {
	r := newRig(t, instrument.Config{Duration: 400 * time.Millisecond}, true)
	r.claimer(protocol.ClaimGo)
	a, b := protocol.NewRunCommand("run-1", "fill", 1), protocol.NewRunCommand("run-2", "fill", 1)
	r.send(a)
	time.Sleep(100 * time.Millisecond)
	if got := r.send(b); got != protocol.RunBusy {
		t.Errorf("second job while busy = %s, want busy", got)
	}
	r.events(2, 3*time.Second)
	if n := len(r.executions(b.JobID)); n != 0 {
		t.Errorf("busy job ran %d times, want 0", n)
	}
}

func TestAbortedClaimRunsNothing(t *testing.T) {
	r := newRig(t, instrument.Config{}, true)
	r.claimer(protocol.ClaimAbort)
	cmd := protocol.NewRunCommand("run-1", "fill", 1)
	if got := r.send(cmd); got != protocol.RunAccepted {
		t.Fatalf("reply = %s, want accepted", got)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(r.executions(cmd.JobID)); n != 0 {
		t.Errorf("aborted job ran %d times, want 0", n)
	}
	if es := r.events(1, 200*time.Millisecond); len(es) != 0 {
		t.Errorf("aborted job reported %v", kinds(es))
	}
	if got := r.send(protocol.NewRunCommand("run-2", "fill", 1)); got != protocol.RunAccepted {
		t.Errorf("after an abort the device must be free, got %s", got)
	}
}

// No answer to a claim is not permission: wait and ask again.
func TestUnansweredClaimDoesNotRun(t *testing.T) {
	r := newRig(t, instrument.Config{}, true)
	cmd := protocol.NewRunCommand("run-1", "fill", 1)
	r.send(cmd)
	time.Sleep(1500 * time.Millisecond)
	if n := len(r.executions(cmd.JobID)); n != 0 {
		t.Fatalf("ran %d times without a claim answer, want 0", n)
	}
	r.claimer(protocol.ClaimGo)
	if es := r.events(2, 4*time.Second); len(es) != 2 {
		t.Fatalf("once the claim is answered the job must run, events %v", kinds(es))
	}
	if n := len(r.executions(cmd.JobID)); n != 1 {
		t.Errorf("ledger has %d executions, want 1", n)
	}
}

// A hung job never returns from Run; the ledger still says how it ended.
func TestHungJobIsReportedFromTheLedger(t *testing.T) {
	r := newRig(t, instrument.Config{HangPct: 100}, true)
	r.claimer(protocol.ClaimGo)
	r.send(protocol.NewRunCommand("run-1", "fill", 1))
	es := r.events(2, 3*time.Second)
	if len(es) != 2 || es[1].Outcome != instrument.OutcomeHung {
		t.Fatalf("events = %v (%+v), want started then finished hung", kinds(es), es)
	}
}

func TestFailedJobCarriesTheInstrumentError(t *testing.T) {
	r := newRig(t, instrument.Config{FailPct: 100}, true)
	r.claimer(protocol.ClaimGo)
	r.send(protocol.NewRunCommand("run-1", "fill", 1))
	es := r.events(2, 3*time.Second)
	if len(es) != 2 || es[1].Outcome != instrument.OutcomeFailed || es[1].Error == "" {
		t.Fatalf("events = %+v, want finished failed with an error", es)
	}
}

// Until JetStream acknowledges an event it is retried, so an event published
// while the stream is unavailable still arrives.
func TestEventsAreRetriedUntilStored(t *testing.T) {
	r := newRig(t, instrument.Config{}, false)
	r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand("run-1", "fill", 1)
	r.send(cmd)
	time.Sleep(800 * time.Millisecond)
	r.createStream()
	es := r.events(2, 4*time.Second)
	if len(es) != 2 || es[0].Kind != protocol.KindStarted || es[1].Kind != protocol.KindFinished {
		t.Fatalf("events = %v, want [started finished] once the stream exists", kinds(es))
	}
}

// testStorage is the compose object store (docker compose up -d storage), in a
// bucket of its own.
func testStorage(t *testing.T) *storage.Storage {
	t.Helper()
	st, err := storage.New("http://localhost:8343", "lab-data-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureBucket(context.Background()); err != nil {
		t.Skipf("storage not reachable (%v); run: docker compose up -d storage", err)
	}
	return st
}

func uniqueRun() string {
	b := make([]byte, 4)
	rand.Read(b)
	return "run-" + hex.EncodeToString(b)
}

// A read is reported finished, then its bytes are stored under the job's key,
// then stored is reported with the ref. The object is what the ledger measured.
func TestPlateDataIsStoredThenReported(t *testing.T) {
	st := testStorage(t)
	r := newRig(t, instrument.Config{ProducesData: true}, true, st)
	r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand(uniqueRun(), "read_plate", 1)
	if got := r.send(cmd); got != protocol.RunAccepted {
		t.Fatalf("reply = %s", got)
	}
	es := r.events(3, 5*time.Second)
	if got := kinds(es); len(got) != 3 || got[1] != protocol.KindFinished || got[2] != protocol.KindStored {
		t.Fatalf("events = %v, want [started finished stored]", got)
	}
	key := protocol.DataKey(cmd.JobID)
	if es[2].Ref != st.Ref(key) || es[2].msgID != protocol.MsgID(cmd.JobID, protocol.KindStored) {
		t.Errorf("stored = %+v (msg id %s), want ref %s", es[2].Event, es[2].msgID, st.Ref(key))
	}
	data, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("object %s: %v", key, err)
	}
	sum := sha256.Sum256(data)
	ex := r.executions(cmd.JobID)
	if len(ex) != 1 || ex[0].DataSHA256 == nil || hex.EncodeToString(sum[:]) != *ex[0].DataSHA256 {
		t.Errorf("stored object sha %x does not match the ledger %+v", sum, ex)
	}
}

// Without working storage nothing claims the data is stored, and the device is
// held: the one copy of the bytes is still in memory.
func TestNoStoredEventWithoutStorage(t *testing.T) {
	down, _ := storage.New("http://127.0.0.1:1", "lab-data-test")
	r := newRig(t, instrument.Config{ProducesData: true}, true, down)
	r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand(uniqueRun(), "read_plate", 1)
	r.send(cmd)
	es := r.events(3, 1500*time.Millisecond)
	if got := kinds(es); len(got) != 2 || got[1] != protocol.KindFinished {
		t.Fatalf("events = %v, want [started finished] and no stored", got)
	}
	if got := r.send(protocol.NewRunCommand(uniqueRun(), "read_plate", 1)); got != protocol.RunBusy {
		t.Errorf("another job while the data is unstored = %s, want busy", got)
	}
}

// before builds a ledger the way an earlier process of this driver left it:
// the instrument ran jobs, and the process went away before reporting them.
func before(t *testing.T, cfg instrument.Config, run func(*instrument.Instrument)) instrument.Config {
	t.Helper()
	cfg.DeviceID, cfg.Seed, cfg.LedgerDir = "test-device", "0", t.TempDir()
	if cfg.Duration == 0 {
		cfg.Duration = 50 * time.Millisecond
	}
	cfg.HangDuration = time.Hour
	inst, err := instrument.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	run(inst)
	cfg.HangPct = 0
	return cfg
}

func runJob(t *testing.T, inst *instrument.Instrument, jobID string) instrument.Outcome {
	t.Helper()
	out, err := inst.Run(context.Background(), instrument.Job{JobID: jobID, RunID: "run-x", StepName: "read_plate"})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func eventFor(es []event, job string, kind protocol.Kind) *event {
	for i := range es {
		if es[i].JobID == job && es[i].Kind == kind {
			return &es[i]
		}
	}
	return nil
}

// A restarted driver reports how every job in its ledger ended, from the
// ledger, without being asked and without claiming anything.
func TestReplayReportsWhatTheLedgerSays(t *testing.T) {
	job := uniqueRun() + "/fill/1"
	cfg := before(t, instrument.Config{}, func(in *instrument.Instrument) { runJob(t, in, job) })
	r := newRig(t, cfg, true)
	asked := r.claimer(protocol.ClaimGo)
	es := r.events(2, 3*time.Second)
	fin := eventFor(es, job, protocol.KindFinished)
	if eventFor(es, job, protocol.KindStarted) == nil || fin == nil || fin.Outcome != instrument.OutcomeCompleted {
		t.Fatalf("events after restart = %+v, want started and finished(completed) for %s", es, job)
	}
	if fin.msgID != protocol.MsgID(job, protocol.KindFinished) {
		t.Errorf("replayed finished has msg id %q; the same id as the first report lets JetStream drop a repeat", fin.msgID)
	}
	if len(*asked) != 0 {
		t.Errorf("replay asked to claim %v", *asked)
	}
}

// A job cut off by the restart is in the ledger as hung: report that, so the
// step is unresolved rather than running for ever.
func TestReplayReportsAJobCutOffByTheRestart(t *testing.T) {
	job := uniqueRun() + "/fill/1"
	cfg := before(t, instrument.Config{HangPct: 100}, func(in *instrument.Instrument) {
		go in.Run(context.Background(), instrument.Job{JobID: job, RunID: "run-x", StepName: "fill"})
		for in.Status().CurrentJobID != job {
			time.Sleep(10 * time.Millisecond)
		}
	})
	r := newRig(t, cfg, true)
	es := r.events(2, 3*time.Second)
	if fin := eventFor(es, job, protocol.KindFinished); fin == nil || fin.Outcome != instrument.OutcomeHung {
		t.Fatalf("events = %+v, want finished(hung) for %s", es, job)
	}
}

func TestReplayFindsAReadThatWasStored(t *testing.T) {
	st := testStorage(t)
	job := uniqueRun() + "/read_plate/1"
	cfg := before(t, instrument.Config{ProducesData: true}, func(in *instrument.Instrument) {
		out := runJob(t, in, job)
		if _, err := st.Put(context.Background(), protocol.DataKey(job), out.Data); err != nil {
			t.Fatal(err)
		}
		// ...and the process died before publishing stored.
	})
	r := newRig(t, cfg, true, st)
	es := r.events(3, 3*time.Second)
	if ev := eventFor(es, job, protocol.KindStored); ev == nil || ev.Ref != st.Ref(protocol.DataKey(job)) {
		t.Fatalf("events = %v, want stored with ref %s", kinds(es), st.Ref(protocol.DataKey(job)))
	}
	if eventFor(es, job, protocol.KindDataLost) != nil {
		t.Error("a stored read was reported lost")
	}
}

// The bytes were only in the old process: a completed read with no object is lost.
func TestReplayReportsAReadWhoseDataIsGone(t *testing.T) {
	st := testStorage(t)
	job := uniqueRun() + "/read_plate/1"
	cfg := before(t, instrument.Config{ProducesData: true}, func(in *instrument.Instrument) { runJob(t, in, job) })
	r := newRig(t, cfg, true, st)
	es := r.events(3, 3*time.Second)
	if eventFor(es, job, protocol.KindDataLost) == nil || eventFor(es, job, protocol.KindStored) != nil {
		t.Fatalf("events = %v, want data_lost and no stored", kinds(es))
	}
}

// No answer from storage is not evidence that the data is gone.
func TestReplaySaysNothingAboutDataStorageCannotSee(t *testing.T) {
	down, _ := storage.New("http://127.0.0.1:1", "lab-data-test")
	job := uniqueRun() + "/read_plate/1"
	cfg := before(t, instrument.Config{ProducesData: true}, func(in *instrument.Instrument) { runJob(t, in, job) })
	r := newRig(t, cfg, true, down)
	es := r.events(3, 1500*time.Millisecond)
	if eventFor(es, job, protocol.KindFinished) == nil || eventFor(es, job, protocol.KindDataLost) != nil || eventFor(es, job, protocol.KindStored) != nil {
		t.Fatalf("events = %v, want finished only", kinds(es))
	}
}

// Only the ledger as it was at start is replayed: a read that starts after the
// restart is still being uploaded by its own path and must not be called lost.
func TestReplayLeavesNewJobsAlone(t *testing.T) {
	st := testStorage(t)
	r := newRig(t, instrument.Config{ProducesData: true}, true, st)
	r.claimer(protocol.ClaimGo)
	cmd := protocol.NewRunCommand(uniqueRun(), "read_plate", 1)
	r.send(cmd)
	es := r.events(3, 5*time.Second)
	if eventFor(es, cmd.JobID, protocol.KindDataLost) != nil {
		t.Fatalf("a new read was reported lost: %v", kinds(es))
	}
}
