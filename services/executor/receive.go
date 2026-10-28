package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// What the executor hears from the drivers (docs/forengineer/decisions.md D1): claims, by
// request/reply, and events, from the LAB_EVENTS stream. Each becomes a fact
// applied in one transaction; the driver is answered, or the event acked, only
// after that transaction commits (K11).

const (
	consumerName = "executor"
	ackWait      = 30 * time.Second
	retryDB      = time.Second     // an event the database could not take
	retryStorage = 2 * time.Second // stored, but the object could not be read yet
)

// listen provisions the stream and starts the claim handler and the event
// consumer. Both survive a restart: the stream and the durable consumer live
// in NATS, so events published while the executor is down wait there.
func (e *Executor) listen(ctx context.Context) error {
	js, err := jetstream.New(e.nc)
	if err != nil {
		return err
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     protocol.StreamName,
		Subjects: []string{protocol.StreamSubjects},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("stream %s: %w", protocol.StreamName, err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   ackWait,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", consumerName, err)
	}

	// A queue group, so a second executor shares claims instead of answering
	// twice; Postgres orders them either way.
	e.claimSub, err = e.nc.QueueSubscribe(protocol.ClaimSubject, consumerName, e.onClaim)
	if err != nil {
		return err
	}
	e.consuming, err = cons.Consume(e.onEvent)
	if err != nil {
		e.claimSub.Unsubscribe()
		return err
	}
	return nil
}

// stopListening waits for the event in hand to finish, so it is acked rather
// than left to be redelivered after ackWait.
func (e *Executor) stopListening() {
	if e.claimSub != nil {
		e.claimSub.Drain()
	}
	if e.consuming != nil {
		e.consuming.Drain()
		<-e.consuming.Closed()
	}
}

// onClaim: "go" only if the claim is now in the record. Cancel and claim take
// the same row lock, so whichever commits first wins and the other sees it.
// If the database cannot be reached there is no reply: the driver asks again,
// and never runs without a "go".
func (e *Executor) onClaim(m *nats.Msg) {
	var req protocol.ClaimRequest
	if err := protocol.Decode(m.Data, &req); err != nil {
		log.Printf("executor: ignoring claim: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply := protocol.ClaimReply{V: protocol.Version, Result: protocol.ClaimAbort}
	s, _, err := e.applyToJob(ctx, req.Device, req.JobID, Claimed{JobID: req.JobID})
	switch {
	case errors.Is(err, ErrUnknownJob):
		reply.Reason = "unknown job for " + req.Device
	case err != nil:
		log.Printf("executor: claim of %s by %s: %v; not answering", req.JobID, req.Device, err)
		return
	case s.claimed(req.JobID):
		reply.Result = protocol.ClaimGo
	default:
		reply.Reason = "cancelled"
	}
	b, _ := json.Marshal(reply)
	if err := m.Respond(b); err != nil {
		log.Printf("executor: reply to claim of %s: %v", req.JobID, err)
	}
}

// onEvent settles every message one way or another, so one bad event cannot
// block the stream: Term what can never apply, Ack what applied (or what no
// step is waiting for), Nak only what may apply later.
func (e *Executor) onEvent(m jetstream.Msg) {
	ev, err := decodeEvent(m.Subject(), m.Data())
	if err != nil {
		log.Printf("executor: dropping event on %s: %v", m.Subject(), err)
		m.Term()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := eventFact(ev)
	if ev.Kind == protocol.KindStored {
		if e.storage == nil {
			m.NakWithDelay(retryStorage)
			return
		}
		if want := e.storage.Ref(protocol.DataKey(ev.JobID)); ev.Ref != want {
			log.Printf("executor: dropping stored event for %s: ref %s, the data belongs at %s", ev.JobID, ev.Ref, want)
			m.Term()
			return
		}
		ds, err := e.readBack(ctx, ev.JobID)
		if err != nil {
			log.Printf("executor: check stored data of %s: %v; will retry", ev.JobID, err)
			m.NakWithDelay(retryStorage)
			return
		}
		f = ds
	}
	_, changed, err := e.applyToJob(ctx, ev.Device, ev.JobID, f)
	switch {
	case errors.Is(err, ErrUnknownJob):
		log.Printf("executor: %s event from %s for unknown job %s; dropping", ev.Kind, ev.Device, ev.JobID)
		m.Ack()
	case err != nil:
		log.Printf("executor: %s event for %s: %v; will retry", ev.Kind, ev.JobID, err)
		m.NakWithDelay(retryDB)
	default:
		m.Ack()
		if changed {
			e.poke() // a step may have finished: the next may start, or the run end
		}
	}
}

// decodeEvent rejects anything malformed, and any event whose device is not the
// one in its subject (K15): the subject is what a device is allowed to publish on.
func decodeEvent(subject string, data []byte) (protocol.Event, error) {
	var ev protocol.Event
	if err := protocol.Decode(data, &ev); err != nil {
		return ev, err
	}
	if err := ev.Validate(); err != nil {
		return ev, err
	}
	if tok := strings.Split(subject, "."); len(tok) != 4 || tok[1] != ev.Device || tok[3] != string(ev.Kind) {
		return ev, fmt.Errorf("event from %s, kind %s, does not match its subject", ev.Device, ev.Kind)
	}
	return ev, nil
}

// eventFact is the fact an event reports. stored has none of its own: the
// driver's word is not evidence, the object read back is (readBack).
func eventFact(ev protocol.Event) Fact {
	switch ev.Kind {
	case protocol.KindStarted:
		return Started{JobID: ev.JobID, At: *ev.StartedAt}
	case protocol.KindFinished:
		return Finished{JobID: ev.JobID, Outcome: Outcome(ev.Outcome), StartedAt: *ev.StartedAt,
			FinishedAt: *ev.FinishedAt, Error: ev.Error, SHA: ev.SHA, Retryable: ev.Retryable}
	case protocol.KindDataLost:
		return DataLost{JobID: ev.JobID}
	}
	return nil
}

// recheckEvery is how often storage is searched again for reads reported lost.
var recheckEvery = 10 * time.Second

// recheckLoop looks in storage for the data of each read reported lost (D3). An
// upload sent before the driver lost the bytes may land at any time, and no
// driver is left to report it, so the executor looks itself.
func (e *Executor) recheckLoop(ctx context.Context) {
	defer e.wg.Done()
	tick := time.NewTicker(recheckEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			e.recheckLost(ctx)
		}
	}
}

func (e *Executor) recheckLost(ctx context.Context) {
	if e.storage == nil {
		return
	}
	rows, err := e.db.QueryContext(ctx,
		// Every step still running, not only unresolved ones: a lost read stays
		// open while its re-read is in flight, and its upload may land then.
		`SELECT run_id, name, lifecycle FROM steps WHERE status = $1`, StepRunning)
	if err != nil {
		log.Printf("executor: list reads reported lost: %v", err)
		return
	}
	type lost struct {
		run, name string
		attempt   Attempt
	}
	var reads []lost
	for rows.Next() {
		var run, name string
		var raw []byte
		if rows.Scan(&run, &name, &raw) != nil {
			continue
		}
		s, err := decodeStep(raw)
		if err != nil {
			log.Printf("executor: recheck %s/%s: %v", run, name, err)
			continue
		}
		for _, a := range s.Attempts {
			if a.DataLost && !a.verified() {
				reads = append(reads, lost{run, name, a})
			}
		}
	}
	rows.Close()
	for _, l := range reads {
		ds, err := e.readBack(ctx, l.attempt.JobID)
		if err != nil {
			continue // still absent, or storage is not answering: nothing new
		}
		if slices.ContainsFunc(l.attempt.Checks, func(c StorageCheck) bool { return c.SHA == ds.SHA }) {
			continue // the same object as last time
		}
		// By step, not by job: our own read-back may be for an earlier attempt.
		if _, changed, err := e.applyToStep(ctx, l.run, l.name, ds); err != nil {
			log.Printf("executor: recheck of %s: %v", l.attempt.JobID, err)
		} else if changed {
			e.poke()
		}
	}
}

// readBack reads the job's object from storage and records what it holds. The
// lifecycle compares that hash with the ledger's; a missing or unreadable
// object is an error, so the event is checked again later.
func (e *Executor) readBack(ctx context.Context, jobID string) (DataStored, error) {
	key := protocol.DataKey(jobID)
	data, err := e.storage.Get(ctx, key)
	if err != nil {
		return DataStored{}, err
	}
	sum := sha256.Sum256(data)
	return DataStored{JobID: jobID, StorageCheck: StorageCheck{
		Ref: e.storage.Ref(key), SHA: hex.EncodeToString(sum[:]), At: time.Now().UTC()}}, nil
}
