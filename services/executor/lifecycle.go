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
// Two kinds of fact. Intent (Dispatched, Claimed, Cancel, GaveUp) is ours:
// Postgres orders it, and the order matters, because a claim and a cancel decide
// each other. Evidence (Started, Finished, DataStored, DataLost) comes from the
// instrument and from storage and can arrive in any order, so it is merged
// regardless of order and the phase is derived from what we know.
//
// A step can take several attempts (D4), each a job of its own, and every one is
// kept. The step is decided by all of them: completed once any attempt is
// verified, failed only when every attempt has ended and no further one will be
// made, running otherwise.

type Phase string

const (
	PhasePending    Phase = "pending"    // not sent to a device
	PhaseDispatched Phase = "dispatched" // command sent, nothing physical yet
	PhaseRetrying   Phase = "retrying"   // a later attempt sent, not claimed yet; an earlier one ran
	PhaseClaimed    Phase = "claimed"    // driver may start it; the cancel point has passed
	PhaseRunning    Phase = "running"    // the instrument has started it
	PhaseStoring    Phase = "storing"    // read completed, its data is not verified in storage yet
	PhaseUnresolved Phase = "unresolved" // evidence is not enough to say how it ended; see the error
	PhaseEnded      Phase = "ended"      // every attempt has ended unsuccessfully; retry not decided yet
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

// lifecycleVersion is the shape of Step as stored in steps.lifecycle. A row of
// any other version is refused, not misread (K10).
const lifecycleVersion = 2

type Step struct {
	V            int
	ProducesData bool

	// Intent for the step as a whole.
	Cancelled bool   // no new attempt after a cancel; the current one stops only if not yet claimed
	GaveUp    string // no further attempt will be made, and why
	Decided   int    // the attempt that completed the step, fixed when it first did (D2)

	Attempts []Attempt // attempt n is Attempts[n-1]
}

func NewStep(producesData bool) Step { return Step{V: lifecycleVersion, ProducesData: producesData} }

// Attempt is one job sent to the step's device.
type Attempt struct {
	JobID string
	N     int

	// Intent.
	Claimed   bool
	Cancelled bool // stopped before its claim
	Withdrawn bool // claimed, then shown never to have started after a cancel (K54)

	// Evidence. Times come from the instrument's ledger, not from our clock.
	StartedAt       *time.Time
	FinishedAt      *time.Time
	Outcome         Outcome // empty until the ledger says how it ended
	InstrumentError string
	Retryable       bool           // the instrument said the failure may be repeated (D4)
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

// GaveUp: every attempt has ended and no further one will be made.
type GaveUp struct{ Reason string }
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
	Retryable  bool   // the instrument said this failure may be repeated (D4)
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
func (GaveUp) fact()     {}
func (Started) fact()    {}
func (Finished) fact()   {}
func (DataStored) fact() {}
func (DataLost) fact()   {}

// Apply returns the step with the fact merged in, and whether anything changed.
func Apply(s Step, f Fact) (Step, bool) {
	before := s
	if s.Attempts != nil {
		s.Attempts = slices.Clone(s.Attempts) // before must keep its own attempts
	}
	switch f := f.(type) {
	case Dispatched:
		if s.mayAttempt(f.Attempt) {
			s.Attempts = append(s.Attempts, Attempt{JobID: f.JobID, N: f.Attempt})
		}
	case Cancel:
		// A cancel stops what has not started, and every attempt after it. Once
		// claimed, the driver may be running it, and only evidence can say how
		// it ended (D1).
		s.Cancelled = true
		if a := s.current(); a != nil && !a.ran() {
			a.Cancelled = true
		}
	case GaveUp:
		if s.GaveUp == "" && s.Phase() == PhaseEnded {
			s.GaveUp = f.Reason
		}
	case Claimed:
		a := s.attempt(f.JobID)
		switch {
		case a == nil || a.Cancelled || a.Withdrawn:
		case !a.Claimed:
			a.Claimed = true
		case s.Cancelled && !a.hasEvidence():
			// Asked again after a cancel. A driver asks for a claim only before it
			// runs a job, and only for one its ledger does not have, so this job
			// never started: withdraw the claim, and the driver is told abort.
			a.Withdrawn = true
		}
	case Started:
		if a := s.attempt(f.JobID); a != nil && a.StartedAt == nil {
			a.StartedAt = &f.At
		}
	case Finished:
		a := s.attempt(f.JobID)
		if a == nil {
			break
		}
		if a.Outcome == "" {
			a.Outcome, a.InstrumentError, a.LedgerSHA = f.Outcome, f.Error, f.SHA
			a.StartedAt, a.FinishedAt = &f.StartedAt, &f.FinishedAt
			a.Retryable = f.Retryable && f.Outcome == OutcomeFailed
		} else if (f.Outcome != a.Outcome || f.SHA != a.LedgerSHA) && a.Conflict == "" {
			a.Conflict = fmt.Sprintf("conflicting outcomes reported for %s: %s, then %s", f.JobID, a.Outcome, f.Outcome)
		}
	case DataStored:
		if a := s.attempt(f.JobID); a != nil && !slices.Contains(a.Checks, f.StorageCheck) {
			a.Checks = append(a.Checks, f.StorageCheck)
		}
	case DataLost:
		if a := s.attempt(f.JobID); a != nil {
			a.DataLost = true
		}
	}
	if s.Decided == 0 {
		for _, a := range s.Attempts {
			if a.phase(s.ProducesData) == PhaseCompleted {
				s.Decided = a.N
				break
			}
		}
	}
	return s, !reflect.DeepEqual(before, s)
}

// mayAttempt: attempt n is the next one, the step is still to be decided, and
// no attempt is in flight. A read whose data was lost is not in flight: it is
// re-read alongside it (D4).
func (s Step) mayAttempt(n int) bool {
	if s.Cancelled || s.GaveUp != "" || s.Decided > 0 || n != len(s.Attempts)+1 {
		return false
	}
	if a := s.current(); a != nil {
		switch a.phase(s.ProducesData) {
		case PhaseDispatched, PhaseClaimed, PhaseRunning, PhaseStoring:
			return false
		}
	}
	return true
}

func (s Step) current() *Attempt {
	if len(s.Attempts) == 0 {
		return nil
	}
	return &s.Attempts[len(s.Attempts)-1]
}

func (s Step) attempt(jobID string) *Attempt {
	for i := range s.Attempts {
		if s.Attempts[i].JobID == jobID {
			return &s.Attempts[i]
		}
	}
	return nil
}

// claimed: the driver may run this job. A claim granted stays granted, even
// after a cancel (D1), unless a later claim shows the job never started.
func (s Step) claimed(jobID string) bool {
	a := s.attempt(jobID)
	return a != nil && a.Claimed && !a.Withdrawn
}

// JobID is the latest attempt's job, the one commands are sent for.
func (s Step) JobID() string {
	if a := s.current(); a != nil {
		return a.JobID
	}
	return ""
}

// open is the latest attempt that may still change how the step ends.
func (s Step) open() *Attempt {
	for i := len(s.Attempts) - 1; i >= 0; i-- {
		a := &s.Attempts[i]
		if a.open(s.ProducesData) {
			return a
		}
	}
	return nil
}

// shown is the attempt the step's times and error come from: the one that
// decided it, else the open one, else the latest that ran.
func (s Step) shown() *Attempt {
	if s.Decided > 0 {
		return &s.Attempts[s.Decided-1]
	}
	if a := s.open(); a != nil {
		return a
	}
	for i := len(s.Attempts) - 1; i >= 0; i-- {
		if s.Attempts[i].ran() {
			return &s.Attempts[i]
		}
	}
	return s.current()
}

// Phase is what the evidence of all attempts supports, falling back to intent
// when there is none.
func (s Step) Phase() Phase {
	switch {
	case s.Decided > 0:
		// Two outcomes reported for the job that decided: the evidence
		// contradicts itself, so the completion is not claimed.
		if s.Attempts[s.Decided-1].Conflict != "" {
			return PhaseUnresolved
		}
		return PhaseCompleted
	case !slices.ContainsFunc(s.Attempts, Attempt.ran) && s.Cancelled:
		return PhaseCancelled
	case len(s.Attempts) == 0:
		return PhasePending
	}
	if a := s.open(); a != nil {
		p := a.phase(s.ProducesData)
		if p == PhaseDispatched && a.N > 1 {
			return PhaseRetrying
		}
		return p
	}
	// Every attempt has ended. Only the policy says why there is no other, even
	// after a cancel: it may have been final anyway (D5).
	if s.GaveUp != "" {
		return PhaseFailed
	}
	return PhaseEnded
}

// Times are the ledger's times of the attempt shown.
func (s Step) Times() (started, finished *time.Time) {
	if a := s.shown(); a != nil {
		return a.StartedAt, a.FinishedAt
	}
	return nil, nil
}

// CancelledReason is why a step is not tried again when the cancel is what
// stopped it: the policy would have made another attempt (D5).
const CancelledReason = "not retried: cancelled"

func (a Attempt) ran() bool { return (a.Claimed && !a.Withdrawn) || a.hasEvidence() }

func (a Attempt) hasEvidence() bool {
	return a.StartedAt != nil || a.Outcome != "" || len(a.Checks) > 0 || a.DataLost
}

// ended: the instrument has said how it ended, and the step cannot learn more
// from it. A hang is ended too: after the ledger says hung, nothing follows.
func (a Attempt) ended() bool {
	return a.Conflict == "" && (a.Outcome == OutcomeFailed || a.Outcome == OutcomeHung)
}

func (a Attempt) open(producesData bool) bool {
	switch a.phase(producesData) {
	case PhaseCancelled, PhaseCompleted:
		return false
	}
	return !a.ended()
}

func (a Attempt) phase(producesData bool) Phase {
	if a.reason(producesData) != "" {
		return PhaseUnresolved
	}
	switch a.Outcome {
	case OutcomeFailed:
		return PhaseFailed
	case OutcomeCompleted:
		if !producesData || a.verified() {
			return PhaseCompleted
		}
		return PhaseStoring
	}
	switch {
	case a.hasEvidence():
		return PhaseRunning
	case a.Cancelled || a.Withdrawn:
		return PhaseCancelled
	case a.Claimed:
		return PhaseClaimed
	}
	return PhaseDispatched
}

// verified: some read-back of storage matched what the instrument measured.
// Once true it stays true, whatever order the checks arrive in.
func (a Attempt) verified() bool {
	return a.LedgerSHA != "" && slices.ContainsFunc(a.Checks, func(c StorageCheck) bool { return c.SHA == a.LedgerSHA })
}

func (a Attempt) latestCheck() (StorageCheck, bool) {
	if len(a.Checks) == 0 {
		return StorageCheck{}, false
	}
	return slices.MaxFunc(a.Checks, func(x, y StorageCheck) int { return x.At.Compare(y.At) }), true
}

// reason says why the evidence is not enough to resolve the attempt, or "" if it is.
func (a Attempt) reason(producesData bool) string {
	read := a.Outcome == OutcomeCompleted && producesData
	switch {
	case a.Conflict != "":
		return a.Conflict
	case a.Outcome == OutcomeHung:
		return "instrument hung; outcome unknown"
	case read && a.LedgerSHA == "":
		return "instrument reported no digest for its data"
	case read && a.DataLost && !a.verified():
		// Not failed: an upload sent before the driver lost the bytes can still
		// land, and storage cannot refuse it (D3). Only storage showing the data,
		// or a decision, settles it.
		msg := "driver lost the data before it was confirmed stored; an earlier upload may still appear in storage"
		if len(a.Checks) > 0 {
			msg += "; the stored object does not match what the instrument measured"
		}
		return msg
	case read && len(a.Checks) > 0 && !a.verified():
		return "stored data does not match what the instrument measured"
	}
	return ""
}

// failure is how an ended attempt ended.
func (a Attempt) failure() string {
	if a.Outcome == OutcomeHung {
		return "instrument hung; outcome unknown"
	}
	return a.InstrumentError
}

// warning: a verified completion is never undone, but a later read-back that
// no longer matches is reported.
func (a Attempt) warning() string {
	if c, ok := a.latestCheck(); ok && a.verified() && c.SHA != a.LedgerSHA {
		return fmt.Sprintf("stored data no longer matches what the instrument measured (checked %s)", c.At.Format(time.RFC3339))
	}
	return ""
}

func (p Phase) APIStatus() string {
	switch p {
	case PhaseDispatched:
		return StepPending
	case PhaseRetrying, PhaseClaimed, PhaseRunning, PhaseStoring, PhaseUnresolved, PhaseEnded:
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
	a := s.shown()
	if a == nil {
		return nil
	}
	var msg string
	switch s.Phase() {
	case PhaseUnresolved:
		msg = a.reason(s.ProducesData)
	case PhaseEnded:
		msg = a.failure()
	case PhaseFailed:
		msg = s.GaveUp
		if f := a.failure(); f != "" {
			msg = f + "; " + msg
		}
	case PhaseCompleted:
		msg = a.warning()
	}
	if msg == "" {
		return nil
	}
	return &msg
}

// APIDataRef is set once the deciding attempt's data has been verified against
// the ledger, and stays set: a later mismatch is reported in the error, not by
// hiding where the data is.
func (s Step) APIDataRef() *string {
	if !s.ProducesData || s.Phase() != PhaseCompleted {
		return nil
	}
	c, _ := s.Attempts[s.Decided-1].latestCheck()
	return &c.Ref
}
