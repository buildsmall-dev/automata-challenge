package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// ErrUnknownJob: no step has an attempt with this job id.
var ErrUnknownJob = errors.New("unknown job")

// applyToStep applies a fact to the step of a run named name. For intent we
// issue ourselves (dispatch, cancel) and our own read-backs of storage.
func (e *Executor) applyToStep(ctx context.Context, runID, name string, f Fact) (Step, bool, error) {
	return e.apply(ctx, `run_id = $1 AND name = $2`, []any{runID, name}, "", f)
}

// applyToJob applies a fact to the attempt jobID, if its step is assigned to
// device. For everything a driver tells us (claims and evidence): only the
// assigned instrument's ledger is evidence for a step, so a report from any
// other device is treated as about an unknown job. The step is found from the
// job id, not the job_id column, which holds only the latest attempt: a fact
// for an earlier one still reaches it (D4, K45).
func (e *Executor) applyToJob(ctx context.Context, device, jobID string, f Fact) (Step, bool, error) {
	runID, name, _, ok := protocol.ParseJobID(jobID)
	if !ok {
		return Step{}, false, fmt.Errorf("%w: %s is not a job id", ErrUnknownJob, jobID)
	}
	return e.apply(ctx, `run_id = $1 AND name = $2 AND device_id = $3`, []any{runID, name, device}, jobID, f)
}

// apply is load, Apply, save in one transaction. FOR UPDATE makes a second
// writer to the same step wait until the first commits and then read its
// result, so no fact is lost and claim and cancel are ordered. If job is set,
// the step must have an attempt for it.
func (e *Executor) apply(ctx context.Context, where string, args []any, job string, f Fact) (Step, bool, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return Step{}, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	var runID, name string
	var raw []byte
	err = tx.QueryRowContext(ctx,
		`SELECT run_id, name, lifecycle FROM steps WHERE `+where+` FOR UPDATE`, args...).Scan(&runID, &name, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Step{}, false, fmt.Errorf("%w: %v", ErrUnknownJob, args)
	}
	if err != nil {
		return Step{}, false, fmt.Errorf("load step %v: %w", args, err)
	}
	s, err := decodeStep(raw)
	if err != nil {
		return Step{}, false, fmt.Errorf("step %s/%s: %w", runID, name, err)
	}
	if job != "" && s.attempt(job) == nil {
		return Step{}, false, fmt.Errorf("%w: %s has no attempt %s", ErrUnknownJob, name, job)
	}

	s, changed := Apply(s, f)
	if !changed {
		return s, false, nil
	}
	if err := saveStep(ctx, tx, runID, name, s); err != nil {
		return Step{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Step{}, false, fmt.Errorf("commit: %w", err)
	}
	return s, true, nil
}

func saveStep(ctx context.Context, tx *sql.Tx, runID, name string, s Step) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode step %s/%s: %w", runID, name, err)
	}
	var jobID *string
	if j := s.JobID(); j != "" {
		jobID = &j
	}
	phase := s.Phase()
	started, finished := s.Times()
	if _, err := tx.ExecContext(ctx,
		`UPDATE steps SET lifecycle = $3, phase = $4, job_id = $5,
		        status = $6, started_at = $7, finished_at = $8, error = $9, data_ref = $10
		  WHERE run_id = $1 AND name = $2`,
		runID, name, raw, phase, jobID,
		phase.APIStatus(), started, finished, s.APIError(), s.APIDataRef()); err != nil {
		return fmt.Errorf("save step %s/%s: %w", runID, name, err)
	}
	return nil
}

// decodeStep reads a stored step, refusing any shape but the current one: read
// as the new shape, an older step would lose its attempts and be sent again.
func decodeStep(raw []byte) (Step, error) {
	var s Step
	if err := json.Unmarshal(raw, &s); err != nil {
		return Step{}, fmt.Errorf("decode: %w", err)
	}
	if s.V != lifecycleVersion {
		return Step{}, fmt.Errorf("stored as lifecycle v%d, want v%d; local data from an older build must be reset (docker compose down -v)", s.V, lifecycleVersion)
	}
	return s, nil
}

// producesData: which devices hand back data that must be stored. Only the
// plate reader does (README). Known by device id until drivers advertise it.
func producesData(deviceID string) bool {
	return strings.HasPrefix(deviceID, "plate-reader")
}
