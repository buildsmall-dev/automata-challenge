package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// What the executor hears from the drivers (docs/decisions.md D1): claims, by
// request/reply, and events, from the LAB_EVENTS stream. Each becomes a fact
// applied in one transaction; the driver is answered, or the event acked, only
// after that transaction commits (K11).

const (
	consumerName = "executor"
	ackWait      = 30 * time.Second
	retryDB      = time.Second      // an event the database could not take
	retryLater   = 30 * time.Second // an event this version does not handle yet
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
	s, _, err := e.applyToJob(ctx, req.JobID, Claimed{JobID: req.JobID})
	switch {
	case errors.Is(err, ErrUnknownJob):
		reply.Reason = "unknown job"
	case err != nil:
		log.Printf("executor: claim of %s by %s: %v; not answering", req.JobID, req.Device, err)
		return
	case s.Claimed && !s.Cancelled:
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
	f, ok := eventFact(ev)
	if !ok {
		// stored is checked against storage, which comes with the plate data (S6).
		m.NakWithDelay(retryLater)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = e.applyToJob(ctx, ev.JobID, f)
	switch {
	case errors.Is(err, ErrUnknownJob):
		log.Printf("executor: %s event for unknown job %s; dropping", ev.Kind, ev.JobID)
		m.Ack()
	case err != nil:
		log.Printf("executor: %s event for %s: %v; will retry", ev.Kind, ev.JobID, err)
		m.NakWithDelay(retryDB)
	default:
		m.Ack()
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

func eventFact(ev protocol.Event) (Fact, bool) {
	switch ev.Kind {
	case protocol.KindStarted:
		return Started{JobID: ev.JobID, At: *ev.StartedAt}, true
	case protocol.KindFinished:
		return Finished{JobID: ev.JobID, Outcome: Outcome(ev.Outcome), StartedAt: *ev.StartedAt,
			FinishedAt: *ev.FinishedAt, Error: ev.Error, SHA: ev.SHA}, true
	case protocol.KindDataLost:
		return DataLost{JobID: ev.JobID}, true
	}
	return nil, false
}
