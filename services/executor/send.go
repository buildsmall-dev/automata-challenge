package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// What the executor tells the drivers (docs/decisions.md D1). A loop moves each
// running run forward: dispatch what the scheduler says is ready, send the
// command for every dispatched step until there is evidence it started, and
// finish the run once nothing is left in flight. Everything it acts on is read from
// Postgres each time, so a restarted executor carries on where the last stopped.

const (
	tickEvery      = 500 * time.Millisecond
	commandTimeout = 2 * time.Second
	askAgainBusy   = 500 * time.Millisecond // busy, or no answer: nothing ran
	askAgainLater  = 3 * time.Second        // accepted or known: the claim should follow
)

type stepRow struct {
	name, device string
	dependsOn    []string
	phase        Phase
	jobID        string
	finishedAt   *time.Time
}

func (e *Executor) dispatchLoop(ctx context.Context) {
	defer e.wg.Done()
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
	for {
		e.advanceAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-e.wake:
		}
	}
}

// poke runs the loop now rather than at the next tick.
func (e *Executor) poke() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Executor) advanceAll(ctx context.Context) {
	rows, err := e.db.QueryContext(ctx, `SELECT id FROM runs WHERE status = $1`, RunRunning)
	if err != nil {
		log.Printf("executor: list running runs: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	dispatched := map[string]bool{}
	for _, id := range ids {
		if err := e.advance(ctx, id, dispatched); err != nil && ctx.Err() == nil {
			log.Printf("executor: run %s: %v", id, err)
		}
	}
	e.forgetSends(dispatched)
}

func (e *Executor) advance(ctx context.Context, runID string, dispatched map[string]bool) error {
	steps, err := e.loadSteps(ctx, runID)
	if err != nil {
		return err
	}
	states := make([]StepState, len(steps))
	for i, s := range steps {
		states[i] = StepState{Name: s.name, DeviceID: s.device, DependsOn: s.dependsOn, Status: s.phase.SchedulerStatus()}
	}
	next := NextSteps(states)
	for _, name := range next {
		s, _, err := e.applyToStep(ctx, runID, name, Dispatched{JobID: protocol.JobID(runID, name, 1), Attempt: 1})
		if err != nil {
			return fmt.Errorf("dispatch %s: %w", name, err)
		}
		for i := range steps {
			if steps[i].name == name {
				steps[i].phase, steps[i].jobID = s.Phase(), s.JobID()
			}
		}
	}

	inFlight := false
	for _, s := range steps {
		switch s.phase {
		case PhasePending, PhaseCompleted, PhaseFailed, PhaseCancelled:
		case PhaseDispatched, PhaseRetrying, PhaseClaimed:
			// Claimed is sent too, the same job (D1, S7): a driver that died
			// after its claim, or whose instrument refused the job, has nothing
			// in its ledger and no event will come. A driver that has the job
			// answers known; one that has no record of it claims it again.
			inFlight = true
			dispatched[s.jobID] = true
			e.send(ctx, runID, s)
		case PhaseEnded, PhaseUnresolved:
			// The latest attempt ended without success, or its data was lost:
			// try again or give up, as D4 allows (policy.go).
			inFlight = true
			_, changed, err := e.decideStep(ctx, runID, s.name, func(st Step) Fact {
				return e.policy.next(runID, s.name, s.device, st)
			})
			if err != nil {
				return fmt.Errorf("decide %s: %w", s.name, err)
			}
			if changed {
				e.poke()
			}
		default:
			inFlight = true
		}
	}
	if len(next) == 0 && !inFlight {
		return e.finish(ctx, runID, steps)
	}
	return nil
}

// finish ends a run once no step can move any more: completed if every step
// completed, failed if one failed. Its finished_at is the last ledger time.
func (e *Executor) finish(ctx context.Context, runID string, steps []stepRow) error {
	status, last := RunCompleted, time.Time{}
	for _, s := range steps {
		switch s.phase {
		case PhaseFailed:
			status = RunFailed
		case PhaseCompleted:
		default:
			if status != RunFailed {
				status = "" // pending with nothing failed: not finished
			}
		}
		if s.finishedAt != nil && s.finishedAt.After(last) {
			last = *s.finishedAt
		}
	}
	if status == "" {
		return nil
	}
	if last.IsZero() {
		last = time.Now()
	}
	_, err := e.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, finished_at = $2 WHERE id = $3 AND status = $4`,
		status, last, runID, RunRunning)
	return err
}

func (e *Executor) loadSteps(ctx context.Context, runID string) ([]stepRow, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT name, device_id, depends_on, phase, job_id, finished_at FROM steps WHERE run_id = $1 ORDER BY position`, runID)
	if err != nil {
		return nil, fmt.Errorf("load steps: %w", err)
	}
	defer rows.Close()
	var steps []stepRow
	for rows.Next() {
		var s stepRow
		var deps pq.StringArray
		var job sql.NullString
		if err := rows.Scan(&s.name, &s.device, &deps, &s.phase, &job, &s.finishedAt); err != nil {
			return nil, fmt.Errorf("scan step: %w", err)
		}
		s.dependsOn, s.jobID = deps, job.String
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// send asks the step's driver to run it, at most one request per job at a time
// and no sooner than the last answer allows. A lost, late or repeated command is
// harmless: the driver answers "known" for a job in flight or in its ledger,
// which has it before anything physical happens, and runs only after a claim.
func (e *Executor) send(ctx context.Context, runID string, s stepRow) {
	e.sendMu.Lock()
	if e.sending[s.jobID] || time.Now().Before(e.notBefore[s.jobID]) {
		e.sendMu.Unlock()
		return
	}
	e.sending[s.jobID] = true
	e.sendMu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		wait := e.command(ctx, runID, s)
		e.sendMu.Lock()
		delete(e.sending, s.jobID)
		e.notBefore[s.jobID] = time.Now().Add(wait)
		e.sendMu.Unlock()
	}()
}

func (e *Executor) command(ctx context.Context, runID string, s stepRow) time.Duration {
	_, _, attempt, _ := protocol.ParseJobID(s.jobID)
	b, _ := json.Marshal(protocol.NewRunCommand(runID, s.name, attempt))
	rctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	msg, err := e.nc.RequestWithContext(rctx, protocol.RunSubject(s.device), b)
	var reply protocol.RunReply
	if err == nil {
		err = protocol.Decode(msg.Data, &reply)
	}
	switch {
	case err != nil:
		if ctx.Err() == nil {
			log.Printf("executor: command %s to %s: %v; asking again", s.jobID, s.device, err)
		}
		return askAgainBusy
	case reply.Result == protocol.RunBusy:
		return askAgainBusy
	default:
		return askAgainLater
	}
}

// forgetSends drops the bookkeeping of jobs that are no longer dispatched.
func (e *Executor) forgetSends(dispatched map[string]bool) {
	e.sendMu.Lock()
	defer e.sendMu.Unlock()
	for job := range e.notBefore {
		if !dispatched[job] && !e.sending[job] {
			delete(e.notBefore, job)
		}
	}
}
