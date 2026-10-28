// Package protocol is the wire contract between the executor and the drivers
// (docs/forengineer/decisions.md D1). Both sides build subjects, ids and messages only
// from here.
//
//	executor -> driver   request  lab.<device>.cmd.run   RunCommand  -> RunReply
//	driver   -> executor request  lab.executor.claim     ClaimRequest -> ClaimReply
//	driver   -> stream   publish  lab.<device>.evt.<kind> Event, Nats-Msg-Id = MsgID
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Version is the wire version every message carries. A receiver rejects any
// other, rather than guessing at fields it does not know.
const Version = 1

// Kind is what an event says. Each maps onto one fact of the executor's step
// lifecycle: started -> Started, finished -> Finished (the ledger's outcome, as
// soon as the instrument returns), stored -> the executor reads the data back
// and checks it, data_lost -> DataLost.
type Kind string

const (
	KindStarted  Kind = "started"
	KindFinished Kind = "finished"
	KindStored   Kind = "stored"
	KindDataLost Kind = "data_lost"
)

const (
	StreamName     = "LAB_EVENTS"
	StreamSubjects = "lab.*.evt.*"
	ClaimSubject   = "lab.executor.claim"
)

func RunSubject(device string) string              { return "lab." + device + ".cmd.run" }
func EventSubject(device string, kind Kind) string { return "lab." + device + ".evt." + string(kind) }

// JobID names one attempt at one step of one run. It is what the instrument
// records in its ledger, so a repeated command for the same attempt is
// recognised there instead of being run twice.
func JobID(runID, step string, attempt int) string {
	return fmt.Sprintf("%s/%s/%d", runID, step, attempt)
}

// ParseJobID splits a job id into what JobID made it from. A run id has no
// slash and the attempt is a number, so a step name may contain one.
func ParseJobID(jobID string) (runID, step string, attempt int, ok bool) {
	runID, rest, ok1 := strings.Cut(jobID, "/")
	i := strings.LastIndex(rest, "/")
	if !ok1 || i < 0 {
		return "", "", 0, false
	}
	n, err := strconv.Atoi(rest[i+1:])
	if runID == "" || i == 0 || err != nil || n < 1 {
		return "", "", 0, false
	}
	return runID, rest[:i], n, true
}

// MsgID is the Nats-Msg-Id of an event. JetStream drops a repeat of the same
// event of the same job within its dedupe window; nothing else.
func MsgID(jobID string, kind Kind) string { return jobID + ":" + string(kind) }

// DataKey is where a job's data is stored. Derived from the job, so the
// executor can check storage without being told, and a retried upload
// overwrites the same object instead of leaving a second one.
func DataKey(jobID string) string { return "runs/" + jobID + ".csv" }

type RunCommand struct {
	V       int    `json:"v"`
	JobID   string `json:"job_id"`
	RunID   string `json:"run_id"`
	Step    string `json:"step"`
	Attempt int    `json:"attempt"`
}

func NewRunCommand(runID, step string, attempt int) RunCommand {
	return RunCommand{V: Version, JobID: JobID(runID, step, attempt), RunID: runID, Step: step, Attempt: attempt}
}

type RunResult string

const (
	RunAccepted RunResult = "accepted" // will claim it, then run it
	RunBusy     RunResult = "busy"     // the instrument is running another job; nothing ran
	RunKnown    RunResult = "known"    // this job is already running or done; it will not run again
)

type RunReply struct {
	V      int       `json:"v"`
	Result RunResult `json:"result"`
	Detail string    `json:"detail,omitempty"`
}

type ClaimRequest struct {
	V      int    `json:"v"`
	JobID  string `json:"job_id"`
	Device string `json:"device"`
}

type ClaimResult string

const (
	ClaimGo    ClaimResult = "go"
	ClaimAbort ClaimResult = "abort"
)

type ClaimReply struct {
	V      int         `json:"v"`
	Result ClaimResult `json:"result"`
	Reason string      `json:"reason,omitempty"`
}

// Event is one fact a driver reports. Which fields are set depends on Kind;
// Validate enforces it.
type Event struct {
	V          int        `json:"v"`
	Kind       Kind       `json:"kind"`
	Device     string     `json:"device"`
	JobID      string     `json:"job_id"`
	StartedAt  *time.Time `json:"started_at,omitempty"`  // ledger time
	FinishedAt *time.Time `json:"finished_at,omitempty"` // ledger time
	Outcome    string     `json:"outcome,omitempty"`     // completed | failed | hung, as in the ledger
	Error      string     `json:"error,omitempty"`
	SHA        string     `json:"sha256,omitempty"` // ledger data_sha256
	// Retryable is the instrument's word that a failure may be repeated. Only
	// Run's return has it, not the ledger, so absent means not retryable (D4).
	Retryable bool   `json:"retryable,omitempty"`
	Ref       string `json:"ref,omitempty"` // s3://bucket/key
}

func (e Event) Validate() error {
	if e.V != Version {
		return fmt.Errorf("event version %d, want %d", e.V, Version)
	}
	if e.Device == "" || e.JobID == "" {
		return errors.New("event needs device and job_id")
	}
	switch e.Kind {
	case KindStarted:
		if e.StartedAt == nil {
			return errors.New("started event needs started_at")
		}
	case KindFinished:
		if e.StartedAt == nil || e.FinishedAt == nil {
			return errors.New("finished event needs started_at and finished_at")
		}
		switch e.Outcome {
		case "completed", "failed", "hung":
		default:
			return fmt.Errorf("finished event has outcome %q", e.Outcome)
		}
	case KindStored:
		if !strings.HasPrefix(e.Ref, "s3://") {
			return fmt.Errorf("stored event needs an s3:// ref, got %q", e.Ref)
		}
	case KindDataLost:
	default:
		return fmt.Errorf("unknown event kind %q", e.Kind)
	}
	return nil
}

func (m RunCommand) version() int   { return m.V }
func (m RunReply) version() int     { return m.V }
func (m ClaimRequest) version() int { return m.V }
func (m ClaimReply) version() int   { return m.V }
func (m Event) version() int        { return m.V }

type message interface{ version() int }

// Decode parses a message and rejects any version but this one. Events still
// need Validate.
func Decode[M message](data []byte, m *M) error {
	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("decode %T: %w", *m, err)
	}
	if v := (*m).version(); v != Version {
		return fmt.Errorf("decode %T: version %d, want %d", *m, v, Version)
	}
	return nil
}
