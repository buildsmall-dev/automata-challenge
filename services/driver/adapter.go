package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

const (
	retryEvery   = 500 * time.Millisecond // claims and event publishes
	requestLimit = 2 * time.Second
	ledgerPoll   = 50 * time.Millisecond
)

// Serve is the driver side of the work. This file is yours.
//
// It connects one instrument to the rest of the system. It is called once when
// the driver process starts, and is expected to run until ctx ends. Keep the
// name and the signature.
//
// The instrument is in instrument/instrument.go. Read it, but do not change it.
//
// A run command is answered at once (accepted, busy or known). An accepted job
// is claimed with the executor before anything physical happens; only "go"
// runs it. How it went is read from the instrument's ledger, not from Run's
// return, so a hung job, which never returns, is still reported. A completed
// read's bytes exist only in Run's return: they are stored, then reported.
func Serve(ctx context.Context, inst *instrument.Instrument, nc *nats.Conn, st *storage.Storage) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	d := &driver{inst: inst, nc: nc, js: js, st: st, device: inst.DeviceID()}
	sub, err := nc.Subscribe(protocol.RunSubject(d.device), func(m *nats.Msg) { d.onRun(ctx, m) })
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	log.Printf("driver %s: listening on %s", d.device, protocol.RunSubject(d.device))
	<-ctx.Done()
	return ctx.Err()
}

type driver struct {
	inst   *instrument.Instrument
	nc     *nats.Conn
	js     jetstream.JetStream
	st     *storage.Storage
	device string

	mu      sync.Mutex
	current string // the job accepted and not yet reported; at most one
}

func (d *driver) onRun(ctx context.Context, m *nats.Msg) {
	var cmd protocol.RunCommand
	if err := protocol.Decode(m.Data, &cmd); err != nil {
		log.Printf("driver %s: ignoring run command: %v", d.device, err)
		return
	}
	result := d.accept(cmd.JobID)
	b, _ := json.Marshal(protocol.RunReply{V: protocol.Version, Result: result})
	if err := m.Respond(b); err != nil {
		log.Printf("driver %s: reply to %s: %v", d.device, cmd.JobID, err)
	}
	if result == protocol.RunAccepted {
		go d.execute(ctx, cmd)
	}
}

// accept decides, under the lock, whether this command may become the one job
// in flight. A job the ledger already has is never accepted again.
func (d *driver) accept(jobID string) protocol.RunResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.inst.Status()
	if d.current == jobID || st.CurrentJobID == jobID || ledgerEntry(st, jobID) != nil {
		return protocol.RunKnown
	}
	if d.current != "" || st.Busy {
		return protocol.RunBusy
	}
	d.current = jobID
	return protocol.RunAccepted
}

func (d *driver) execute(ctx context.Context, cmd protocol.RunCommand) {
	defer func() { d.mu.Lock(); d.current = ""; d.mu.Unlock() }()
	if !d.claim(ctx, cmd.JobID) {
		return
	}
	r := &run{done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.out, r.err = d.inst.Run(ctx, instrument.Job{JobID: cmd.JobID, RunID: cmd.RunID, StepName: cmd.Step})
	}()

	started, ok := d.waitLedger(ctx, cmd.JobID, r, func(instrument.Execution) bool { return true })
	if !ok {
		return
	}
	d.publish(ctx, protocol.Event{Kind: protocol.KindStarted, JobID: cmd.JobID, StartedAt: ledgerTime(started.StartedAt)})

	done, ok := d.waitLedger(ctx, cmd.JobID, nil, func(e instrument.Execution) bool { return e.Outcome != instrument.OutcomeRunning })
	if !ok {
		return
	}
	ev := protocol.Event{Kind: protocol.KindFinished, JobID: cmd.JobID, Outcome: done.Outcome, Error: done.Error,
		StartedAt: ledgerTime(done.StartedAt), FinishedAt: ledgerTime(*done.FinishedAt)}
	if done.DataSHA256 != nil {
		ev.SHA = *done.DataSHA256
	}
	d.publish(ctx, ev)

	if done.Outcome == instrument.OutcomeCompleted && d.inst.ProducesData() {
		select {
		case <-r.done: // Run returns just after the ledger records the end
			d.store(ctx, cmd.JobID, r.out.Data)
		case <-ctx.Done():
		}
	}
}

// run is one call of the instrument's Run; done closes when it returns.
type run struct {
	done chan struct{}
	out  instrument.Outcome
	err  error
}

// store uploads a read's bytes until storage takes them, then reports stored.
// Until then this job is still the driver's current one, so the device takes no
// other job: the only copy of the data is in memory. The key is derived from
// the job, so a retried upload overwrites rather than adds.
func (d *driver) store(ctx context.Context, jobID string, data []byte) {
	if d.st == nil {
		log.Printf("driver %s: no storage; data of %s is not stored", d.device, jobID)
		return
	}
	for ctx.Err() == nil {
		ref, err := d.st.Put(ctx, protocol.DataKey(jobID), data)
		if err == nil {
			d.publish(ctx, protocol.Event{Kind: protocol.KindStored, JobID: jobID, Ref: ref})
			return
		}
		log.Printf("driver %s: store data of %s: %v; retrying", d.device, jobID, err)
		sleep(ctx, retryEvery)
	}
}

// claim asks the executor for permission to start. Only "go" is permission:
// no answer is asked again, never taken as a yes.
func (d *driver) claim(ctx context.Context, jobID string) bool {
	req, _ := json.Marshal(protocol.ClaimRequest{V: protocol.Version, JobID: jobID, Device: d.device})
	for ctx.Err() == nil {
		rctx, cancel := context.WithTimeout(ctx, requestLimit)
		msg, err := d.nc.RequestWithContext(rctx, protocol.ClaimSubject, req)
		cancel()
		var reply protocol.ClaimReply
		if err == nil {
			err = protocol.Decode(msg.Data, &reply)
		}
		if err == nil {
			if reply.Result != protocol.ClaimGo {
				log.Printf("driver %s: claim of %s refused: %s", d.device, jobID, reply.Reason)
			}
			return reply.Result == protocol.ClaimGo
		}
		log.Printf("driver %s: claim of %s: %v; asking again", d.device, jobID, err)
		sleep(ctx, retryEvery)
	}
	return false
}

// waitLedger waits for the job's ledger entry to satisfy cond. If Run fails
// before the entry appears, nothing ran (instrument.go: ErrBusy or a ledger
// write error) and there is nothing to report.
func (d *driver) waitLedger(ctx context.Context, jobID string, r *run, cond func(instrument.Execution) bool) (instrument.Execution, bool) {
	var ran <-chan struct{}
	if r != nil {
		ran = r.done
	}
	for {
		if e := ledgerEntry(d.inst.Status(), jobID); e != nil && cond(*e) {
			return *e, true
		}
		select {
		case <-ran:
			if r.err != nil {
				log.Printf("driver %s: %s did not start: %v", d.device, jobID, r.err)
				return instrument.Execution{}, false
			}
			ran = nil // returned normally: keep reading the ledger
		case <-ctx.Done():
			return instrument.Execution{}, false
		case <-time.After(ledgerPoll):
		}
	}
}

// publish stores an event in the stream, retrying until JetStream acknowledges
// it. A timed-out publish may still have been stored (docs/chaos-log.md C1), so
// every retry carries the same Nats-Msg-Id and the server drops the repeat.
func (d *driver) publish(ctx context.Context, ev protocol.Event) {
	ev.V, ev.Device = protocol.Version, d.device
	b, _ := json.Marshal(ev)
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, requestLimit)
		_, err := d.js.Publish(pctx, protocol.EventSubject(d.device, ev.Kind), b, jetstream.WithMsgID(protocol.MsgID(ev.JobID, ev.Kind)))
		cancel()
		if err == nil {
			return
		}
		log.Printf("driver %s: publish %s of %s: %v; retrying", d.device, ev.Kind, ev.JobID, err)
		sleep(ctx, retryEvery)
	}
}

func ledgerEntry(st instrument.Status, jobID string) *instrument.Execution {
	for i := range st.Executions {
		if st.Executions[i].JobID == jobID {
			return &st.Executions[i]
		}
	}
	return nil
}

func ledgerTime(secs float64) *time.Time {
	t := time.Unix(0, int64(secs*1e9)).UTC()
	return &t
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
