// Package instrument is the physical piece of lab equipment behind a driver.
//
// This file is fixed. Do not change it. It is the only part of the system that
// stands for physical reality, and the checks read its ledger to find out what
// really happened. A submission that changes this file cannot be checked.
//
// What it does:
//
//   - It does one job at a time. Offered a job while busy, Run returns ErrBusy
//     straight away. It does not queue.
//   - A job either completes, fails (FAIL_PCT), or hangs (HANG_PCT). A hung job
//     never returns from Run, unless the caller's context ends. The instrument
//     frees itself after HANG_DURATION, but nothing reports what happened.
//   - Every physical execution is written to the ledger the moment it starts,
//     and updated when it ends. The ledger survives restarts.
//   - Status tells you what it is doing now and what it has done.
//
// Whether a job fails or hangs is decided from CHAOS_SEED, the step name and
// how many times this instrument has run that step for that run before. The
// same seed gives the same faults.
package instrument

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	OutcomeRunning   = "running"
	OutcomeCompleted = "completed"
	OutcomeFailed    = "failed"
	OutcomeHung      = "hung"
)

// ErrBusy means the instrument is already running a job.
var ErrBusy = errors.New("instrument is busy")

// Job is one piece of physical work.
//
// JobID is yours to choose. The instrument records it in the ledger and reports
// it back in Status. RunID and StepName say which step of which run this work
// is for.
type Job struct {
	JobID    string
	RunID    string
	StepName string
}

// Outcome is how a job ended. Error is empty on success.
//
// Retryable is the instrument's own view of whether the same step could
// sensibly be run again. It only means anything when Error is set.
//
// Data is the measurement, for instruments that produce one (the plate reader).
// It is nil otherwise, and nil when the job failed.
type Outcome struct {
	JobID     string
	Error     string
	Retryable bool
	Data      []byte
}

// Execution is one entry in the ledger: the instrument physically started a job.
type Execution struct {
	ExecutionID string   `json:"execution_id"`
	JobID       string   `json:"job_id"`
	RunID       string   `json:"run_id"`
	StepName    string   `json:"step_name"`
	StartedAt   float64  `json:"started_at"`
	Outcome     string   `json:"outcome"`
	FinishedAt  *float64 `json:"finished_at"`
	Error       string   `json:"error"`
	DataSHA256  *string  `json:"data_sha256"`
}

type Status struct {
	DeviceID     string
	Busy         bool
	CurrentJobID string
	Executions   []Execution
}

type Config struct {
	DeviceID          string
	Duration          time.Duration
	FailPct           int
	HangPct           int
	HangDuration      time.Duration
	FailuresRetryable bool
	ProducesData      bool
	Seed              string
	LedgerDir         string
}

type Instrument struct {
	cfg        Config
	ledgerPath string

	mu         sync.Mutex
	executions map[string]*Execution
	order      []string
	busy       bool
	currentJob string
}

func New(cfg Config) (*Instrument, error) {
	in := &Instrument{
		cfg:        cfg,
		ledgerPath: filepath.Join(cfg.LedgerDir, cfg.DeviceID+".jsonl"),
		executions: map[string]*Execution{},
	}
	if err := in.loadLedger(); err != nil {
		return nil, err
	}
	return in, nil
}

func (in *Instrument) DeviceID() string        { return in.cfg.DeviceID }
func (in *Instrument) Duration() time.Duration { return in.cfg.Duration }
func (in *Instrument) FailuresRetryable() bool { return in.cfg.FailuresRetryable }
func (in *Instrument) ProducesData() bool      { return in.cfg.ProducesData }

// Run physically runs one job, and returns when it ends.
//
// It returns ErrBusy straight away if a job is already running, and an error
// if the ledger cannot be written; in both cases nothing ran. A job that
// hangs never returns, unless ctx ends. Ending ctx only stops the waiting: the
// physical work carries on, just as in a lab.
func (in *Instrument) Run(ctx context.Context, job Job) (Outcome, error) {
	in.mu.Lock()
	if in.busy {
		current := in.currentJob
		in.mu.Unlock()
		return Outcome{}, fmt.Errorf("%w: %s is busy with %s", ErrBusy, in.cfg.DeviceID, current)
	}

	attempt := 0
	for _, id := range in.order {
		e := in.executions[id]
		if e.RunID == job.RunID && e.StepName == job.StepName {
			attempt++
		}
	}
	exec := &Execution{
		ExecutionID: newID(),
		JobID:       job.JobID,
		RunID:       job.RunID,
		StepName:    job.StepName,
		StartedAt:   now(),
		Outcome:     OutcomeRunning,
	}
	// Nothing physical happens unless the ledger has it first.
	if err := in.recordLocked(exec); err != nil {
		in.mu.Unlock()
		return Outcome{}, err
	}
	in.busy = true
	in.currentJob = job.JobID
	in.mu.Unlock()
	log.Printf("started %s (job=%s run=%s)", job.StepName, job.JobID, job.RunID)

	done := make(chan Outcome, 1)
	go in.work(job, exec, in.fault(job.StepName, attempt), done)

	select {
	case out := <-done:
		return out, nil
	case <-ctx.Done():
		return Outcome{}, ctx.Err()
	}
}

// Status is what the instrument is doing now, and everything it has ever done.
func (in *Instrument) Status() Status {
	in.mu.Lock()
	defer in.mu.Unlock()
	st := Status{DeviceID: in.cfg.DeviceID, Busy: in.busy, CurrentJobID: in.currentJob}
	for _, id := range in.order {
		st.Executions = append(st.Executions, *in.executions[id])
	}
	return st
}

func (in *Instrument) work(job Job, exec *Execution, fault string, done chan<- Outcome) {
	if fault == OutcomeHung {
		log.Printf("HUNG %s (job=%s) -- no report will follow", job.StepName, job.JobID)
		time.Sleep(in.cfg.HangDuration)
		in.finish(exec, OutcomeHung, "instrument hung", nil)
		log.Printf("recovered from hang on %s (job=%s)", job.StepName, job.JobID)
		// done is never written: whoever is waiting waits for ever.
		return
	}

	time.Sleep(in.cfg.Duration)

	if fault == OutcomeFailed {
		msg := "instrument error during " + job.StepName
		in.finish(exec, OutcomeFailed, msg, nil)
		log.Printf("FAILED %s (job=%s)", job.StepName, job.JobID)
		done <- Outcome{JobID: job.JobID, Error: msg, Retryable: in.cfg.FailuresRetryable}
		return
	}

	var data []byte
	var digest *string
	if in.cfg.ProducesData {
		data = measure(job)
		sum := sha256.Sum256(data)
		h := hex.EncodeToString(sum[:])
		digest = &h
	}
	in.finish(exec, OutcomeCompleted, "", digest)
	log.Printf("completed %s (job=%s)", job.StepName, job.JobID)
	done <- Outcome{JobID: job.JobID, Data: data}
}

func (in *Instrument) finish(exec *Execution, outcome, msg string, digest *string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	t := now()
	exec.Outcome = outcome
	exec.FinishedAt = &t
	exec.Error = msg
	exec.DataSHA256 = digest
	in.busy = false
	in.currentJob = ""
	if err := in.recordLocked(exec); err != nil {
		// The work has ended whatever the disk says. Status still has the end;
		// only the ledger file is behind.
		log.Printf("could not record the end of %s (job=%s, outcome=%s): %v", exec.StepName, exec.JobID, outcome, err)
	}
}

func (in *Instrument) fault(stepName string, attempt int) string {
	r := roll(in.cfg.Seed, in.cfg.DeviceID, stepName, attempt)
	switch {
	case r < in.cfg.FailPct:
		return OutcomeFailed
	case r < in.cfg.FailPct+in.cfg.HangPct:
		return OutcomeHung
	default:
		return OutcomeCompleted
	}
}

func roll(seed, deviceID, stepName string, attempt int) int {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%d", seed, deviceID, stepName, attempt)))
	return int(binary.BigEndian.Uint32(sum[:4]) % 100)
}

// measure is a 96-well absorbance reading.
func measure(job Job) []byte {
	digest := sha256.Sum256([]byte(job.JobID))
	var b strings.Builder
	b.WriteString("well,absorbance\n")
	for i := 0; i < 96; i++ {
		well := fmt.Sprintf("%c%d", "ABCDEFGH"[i/12], i%12+1)
		value := float64(digest[i%len(digest)]) / 255 * 2.5
		fmt.Fprintf(&b, "%s,%.4f\n", well, value)
	}
	return []byte(b.String())
}

// recordLocked writes the execution's current state to the ledger, then keeps
// it in memory. Caller holds mu.
func (in *Instrument) recordLocked(exec *Execution) error {
	if err := in.appendLedger(exec); err != nil {
		return err
	}
	if _, ok := in.executions[exec.ExecutionID]; !ok {
		in.order = append(in.order, exec.ExecutionID)
	}
	in.executions[exec.ExecutionID] = exec
	return nil
}

// appendLedger appends one line to the ledger and syncs it to disk.
func (in *Instrument) appendLedger(exec *Execution) error {
	line, err := json.Marshal(exec)
	if err != nil {
		return fmt.Errorf("ledger: encode %s: %w", exec.ExecutionID, err)
	}
	f, err := os.OpenFile(in.ledgerPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	return nil
}

func (in *Instrument) loadLedger() error {
	f, err := os.Open(in.ledgerPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open ledger %s: %w", in.ledgerPath, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e Execution
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			log.Printf("ledger %s: unreadable line %q: %v", in.ledgerPath, line, err)
			continue
		}
		if _, ok := in.executions[e.ExecutionID]; !ok {
			in.order = append(in.order, e.ExecutionID)
		}
		in.executions[e.ExecutionID] = &e
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read ledger %s: %w", in.ledgerPath, err)
	}
	// An execution still "running" in the ledger was cut off by a restart.
	// Write that down too, so that the ledger and Status agree.
	for _, id := range in.order {
		e := in.executions[id]
		if e.Outcome != OutcomeRunning {
			continue
		}
		t := now()
		e.Outcome = OutcomeHung
		e.FinishedAt = &t
		e.Error = "instrument restarted during the job"
		if err := in.appendLedger(e); err != nil {
			return err
		}
	}
	return nil
}

// ServeStatus is a read-only side channel, used by the checks: GET /status.
// It does not go through NATS, so it does not depend on your protocol.
func ServeStatus(in *Instrument, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st := in.Status()
		executions := st.Executions
		if executions == nil {
			executions = []Execution{}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"device_id":          st.DeviceID,
			"busy":               st.Busy,
			"current_job_id":     st.CurrentJobID,
			"failures_retryable": in.cfg.FailuresRetryable,
			"fail_pct":           in.cfg.FailPct,
			"hang_pct":           in.cfg.HangPct,
			"executions":         executions,
		}); err != nil {
			log.Printf("status side channel: %v", err)
		}
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("status side channel: %v", err)
		}
	}()
	return srv
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b[:])
}
