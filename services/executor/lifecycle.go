package main

import (
	"fmt"
	"reflect"
	"time"
)

// The lifecycle of one step: which facts can change it, and what the record may
// then claim.
//
// Two kinds of fact. Intent (Dispatched, Claimed, Cancel) is ours: Postgres
// orders it, and the order matters, because a claim and a cancel decide each
// other. Evidence (Started, Finished, DataStored) comes from the instrument and
// from storage and can arrive in any order, so it is merged regardless of order
// and the phase is derived from what we know.

type Phase string

const (
	PhasePending    Phase = "pending"    // not sent to a device
	PhaseDispatched Phase = "dispatched" // command sent, nothing physical yet
	PhaseClaimed    Phase = "claimed"    // driver may start it; the cancel point has passed
	PhaseRunning    Phase = "running"    // the instrument has started it
	PhaseStoring    Phase = "storing"    // read completed, its data is not verified in storage yet
	PhaseUnresolved Phase = "unresolved" // evidence is not enough to say how it ended; see Reason
	PhaseCompleted  Phase = "completed"
	PhaseFailed     Phase = "failed"
	PhaseCancelled  Phase = "cancelled"
)

// Outcome is how the instrument's ledger says an execution ended.
type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeFailed    Outcome = "failed"
	OutcomeHung      Outcome = "hung"
)

type Step struct {
	ProducesData bool

	// Intent.
	JobID     string // the current attempt; evidence for any other job is ignored
	Attempt   int
	Claimed   bool
	Cancelled bool

	// Evidence. Times come from the instrument's ledger, not from our clock.
	StartedAt       *time.Time
	FinishedAt      *time.Time
	Outcome         Outcome // empty until the ledger says how it ended
	InstrumentError string
	LedgerSHA       string // what the instrument says it measured
	StoredSHA       string // what we found in storage at StoredAt
	StoredRef       string
	StoredAt        time.Time // when storage was last checked
	Conflict        string    // two different outcomes reported for the same job
}

type Fact interface{ fact() }

type Dispatched struct {
	JobID   string
	Attempt int
}
type Claimed struct{ JobID string }
type Cancel struct{}
type Started struct {
	JobID string
	At    time.Time
}
type Finished struct {
	JobID      string
	Outcome    Outcome
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
	SHA        string // ledger data_sha256, for instruments that produce data
}
type DataStored struct {
	JobID string
	Ref   string    // s3://bucket/key
	SHA   string    // sha256 of the object as read back from storage
	At    time.Time // when it was read back
}

func (Dispatched) fact() {}
func (Claimed) fact()    {}
func (Cancel) fact()     {}
func (Started) fact()    {}
func (Finished) fact()   {}
func (DataStored) fact() {}

// Apply returns the step with the fact merged in, and whether anything changed.
func Apply(s Step, f Fact) (Step, bool) {
	before := s
	switch f := f.(type) {
	case Dispatched:
		if s.JobID == "" && !s.Cancelled {
			s.JobID, s.Attempt = f.JobID, f.Attempt
		}
	case Claimed:
		if f.JobID == s.JobID && !s.Cancelled {
			s.Claimed = true
		}
	case Cancel:
		// A cancel stops what has not started. Once claimed, the driver may be
		// running it, and only evidence can say how it ended.
		if !s.Claimed && !s.hasEvidence() {
			s.Cancelled = true
		}
	case Started:
		if f.JobID == s.JobID && s.StartedAt == nil {
			s.StartedAt = &f.At
		}
	case Finished:
		if f.JobID != s.JobID {
			break
		}
		if s.Outcome == "" {
			s.Outcome, s.InstrumentError, s.LedgerSHA = f.Outcome, f.Error, f.SHA
			s.StartedAt, s.FinishedAt = &f.StartedAt, &f.FinishedAt
		} else if (f.Outcome != s.Outcome || f.SHA != s.LedgerSHA) && s.Conflict == "" {
			s.Conflict = fmt.Sprintf("conflicting outcomes reported for %s: %s, then %s", f.JobID, s.Outcome, f.Outcome)
		}
	case DataStored:
		// The newest check of storage wins, whatever order the checks arrive in.
		if f.JobID == s.JobID && f.At.After(s.StoredAt) {
			s.StoredSHA, s.StoredRef, s.StoredAt = f.SHA, f.Ref, f.At
		}
	}
	return s, !reflect.DeepEqual(before, s)
}

func (s Step) hasEvidence() bool {
	return s.StartedAt != nil || s.Outcome != "" || s.StoredSHA != ""
}

// Phase is what the evidence supports, falling back to intent when there is none.
func (s Step) Phase() Phase {
	if s.Reason() != "" {
		return PhaseUnresolved
	}
	switch s.Outcome {
	case OutcomeFailed:
		return PhaseFailed
	case OutcomeCompleted:
		if s.ProducesData && s.StoredSHA == "" {
			return PhaseStoring
		}
		return PhaseCompleted
	}
	switch {
	case s.hasEvidence():
		return PhaseRunning
	case s.Cancelled:
		return PhaseCancelled
	case s.Claimed:
		return PhaseClaimed
	case s.JobID != "":
		return PhaseDispatched
	}
	return PhasePending
}

// Reason says why the evidence is not enough to resolve the step, or "" if it is.
func (s Step) Reason() string {
	switch {
	case s.Conflict != "":
		return s.Conflict
	case s.Outcome == OutcomeHung:
		return "instrument hung; outcome unknown"
	case s.Outcome == OutcomeCompleted && s.ProducesData && s.LedgerSHA == "":
		return "instrument reported no digest for its data"
	case s.Outcome == OutcomeCompleted && s.ProducesData && s.StoredSHA != "" && s.StoredSHA != s.LedgerSHA:
		return "stored data does not match what the instrument measured"
	}
	return ""
}

func (p Phase) APIStatus() string {
	switch p {
	case PhaseDispatched:
		return StepPending
	case PhaseClaimed, PhaseRunning, PhaseStoring, PhaseUnresolved:
		return StepRunning
	}
	return string(p)
}

// SchedulerStatus is the status NextSteps sees. A dispatched step already
// holds its device.
func (p Phase) SchedulerStatus() string {
	if p == PhaseDispatched {
		return StepRunning
	}
	return p.APIStatus()
}

func (s Step) APIError() *string {
	switch s.Phase() {
	case PhaseUnresolved:
		r := s.Reason()
		return &r
	case PhaseFailed:
		e := s.InstrumentError
		return &e
	}
	return nil
}

// APIDataRef is set only once the stored data is verified against the ledger.
func (s Step) APIDataRef() *string {
	if !s.ProducesData || s.Phase() != PhaseCompleted {
		return nil
	}
	ref := s.StoredRef
	return &ref
}
