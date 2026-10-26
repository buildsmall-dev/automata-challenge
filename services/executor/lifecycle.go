package main

import (
	"fmt"
	"reflect"
	"slices"
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
	LedgerSHA       string         // what the instrument says it measured
	Checks          []StorageCheck // every time we read the data back from storage
	DataLost        bool           // the driver no longer has the bytes to store
	Conflict        string         // two different outcomes reported for the same job
}

type StorageCheck struct {
	Ref string    // s3://bucket/key
	SHA string    // sha256 of the object as read back
	At  time.Time // when it was read back
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
	StorageCheck
}

// DataLost: the driver no longer holds the bytes of a read it never stored,
// for example because it restarted between the read and the upload.
type DataLost struct{ JobID string }

func (Dispatched) fact() {}
func (Claimed) fact()    {}
func (Cancel) fact()     {}
func (Started) fact()    {}
func (Finished) fact()   {}
func (DataStored) fact() {}
func (DataLost) fact()   {}

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
		if f.JobID == s.JobID && !slices.Contains(s.Checks, f.StorageCheck) {
			s.Checks = append(s.Checks, f.StorageCheck)
		}
	case DataLost:
		if f.JobID == s.JobID {
			s.DataLost = true
		}
	}
	return s, !reflect.DeepEqual(before, s)
}

func (s Step) hasEvidence() bool {
	return s.StartedAt != nil || s.Outcome != "" || len(s.Checks) > 0 || s.DataLost
}

// verified: some read-back of storage matched what the instrument measured.
// Once true it stays true, whatever order the checks arrive in.
func (s Step) verified() bool {
	return s.LedgerSHA != "" && slices.ContainsFunc(s.Checks, func(c StorageCheck) bool { return c.SHA == s.LedgerSHA })
}

func (s Step) latestCheck() (StorageCheck, bool) {
	if len(s.Checks) == 0 {
		return StorageCheck{}, false
	}
	return slices.MaxFunc(s.Checks, func(a, b StorageCheck) int { return a.At.Compare(b.At) }), true
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
		switch {
		case !s.ProducesData, s.verified():
			return PhaseCompleted
		case s.DataLost:
			return PhaseFailed
		}
		return PhaseStoring
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
	read := s.Outcome == OutcomeCompleted && s.ProducesData
	switch {
	case s.Conflict != "":
		return s.Conflict
	case s.Outcome == OutcomeHung:
		return "instrument hung; outcome unknown"
	case read && s.LedgerSHA == "":
		return "instrument reported no digest for its data"
	case read && len(s.Checks) > 0 && !s.verified() && !s.DataLost:
		return "stored data does not match what the instrument measured"
	}
	return ""
}

func (s Step) failure() string {
	if s.Outcome == OutcomeFailed {
		return s.InstrumentError
	}
	msg := "instrument completed the read, but its data was lost before storage"
	if len(s.Checks) > 0 {
		msg += "; the stored object does not match what it measured"
	}
	return msg
}

// warning: a verified completion is never undone, but a later read-back that
// no longer matches is reported.
func (s Step) warning() string {
	if c, ok := s.latestCheck(); ok && s.verified() && c.SHA != s.LedgerSHA {
		return fmt.Sprintf("stored data no longer matches what the instrument measured (checked %s)", c.At.Format(time.RFC3339))
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
	var msg string
	switch s.Phase() {
	case PhaseUnresolved:
		msg = s.Reason()
	case PhaseFailed:
		msg = s.failure()
	case PhaseCompleted:
		msg = s.warning()
	}
	if msg == "" {
		return nil
	}
	return &msg
}

// APIDataRef is set once the stored data has been verified against the ledger,
// and stays set: a later mismatch is reported in the error, not by hiding where
// the data is.
func (s Step) APIDataRef() *string {
	if !s.ProducesData || s.Phase() != PhaseCompleted {
		return nil
	}
	c, _ := s.latestCheck()
	return &c.Ref
}
