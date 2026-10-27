package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/lib/pq"
)

// GraphQL over the run record (README task 2). It keeps no state of its own:
// a step's fields are the columns REST reads, its attempts are the lifecycle
// stored in the same row, both read in one snapshot, and the mutations are the
// Executor's methods. Tools follow a run by polling run(id).

const schemaSDL = `
schema { query: Query  mutation: Mutation }

scalar Time

type Query {
  "One run, or null if there is none with this id."
  run(id: ID!): Run
  "Every run, newest first; only those with this status if one is given."
  runs(status: RunStatus): [Run!]!
  "What workflows.yaml defines now."
  workflows: [Workflow!]!
}

type Mutation {
  "Create a run of the named workflow, or of the default one."
  createRun(workflowName: String): Run!
  "Start a pending run. A run already started is returned as it is."
  startRun(id: ID!): Run!
  "Stop what has not started; work already claimed or running ends and is recorded (D5)."
  cancelRun(id: ID!): Run!
}

type Workflow { name: String!  isDefault: Boolean!  steps: [String!]! }

enum RunStatus { PENDING RUNNING COMPLETED FAILED CANCELLED }
enum StepStatus { PENDING RUNNING COMPLETED FAILED CANCELLED }

type Run {
  id: ID!
  workflowName: String!
  status: RunStatus!
  "A cancel was asked for. The run may still be running: claimed work is not stopped."
  cancelRequested: Boolean!
  createdAt: Time!
  startedAt: Time
  "The ledger's end of the last step to end."
  finishedAt: Time
  "In workflow order."
  steps: [Step!]!
}

"Where a step is, finer than its status."
enum StepPhase {
  PENDING     # not sent to a device
  DISPATCHED  # command sent, not claimed: a cancel still stops it
  RETRYING    # a later attempt sent, not claimed; an earlier one ran
  CLAIMED     # the driver may start it; a cancel no longer stops it
  RUNNING     # the instrument started it
  STORING     # the read completed; its data is not yet verified in storage
  UNRESOLVED  # the evidence cannot say how it ended; see error
  ENDED       # every attempt ended unsuccessfully; whether to try again is not decided
  COMPLETED
  FAILED
  CANCELLED
}

"Fields as in REST GET /runs/{id}, plus phase and attempts."
type Step {
  name: String!
  device: String!
  dependsOn: [String!]!
  status: StepStatus!
  phase: StepPhase!
  "Ledger times of the attempt that decided the step, or else of the latest that ran."
  startedAt: Time
  finishedAt: Time
  error: String
  "s3://bucket/key, only once the data is verified."
  dataRef: String
  "Every attempt, first first, including those that failed or never started."
  attempts: [Attempt!]!
}

enum AttemptPhase { DISPATCHED CLAIMED RUNNING STORING UNRESOLVED COMPLETED FAILED CANCELLED }
enum Outcome { COMPLETED FAILED HUNG }

"One job sent to the device. Each has its own entry in the instrument's ledger."
type Attempt {
  number: Int!
  jobId: ID!
  phase: AttemptPhase!
  "This attempt completed the step."
  decided: Boolean!
  "The executor let the driver start it. A claim later shown never to have started is phase CANCELLED."
  claimed: Boolean!
  "How the ledger says it ended; null until it says."
  outcome: Outcome
  "Ledger times: when the attempt held the device."
  startedAt: Time
  finishedAt: Time
  instrumentError: String
  "The instrument said the failure may be repeated."
  retryable: Boolean!
  "Why the evidence is not enough to say how it ended, if it is not."
  unresolved: String
  "Only for steps that produce data."
  data: AttemptData
}

type AttemptData {
  "What the instrument says it measured (ledger data_sha256)."
  ledgerSha256: String
  "Some read-back of storage matched ledgerSha256."
  verified: Boolean!
  "The driver lost the bytes before they were confirmed stored."
  dataLost: Boolean!
  "Every read-back of storage, in the order recorded."
  checks: [StorageCheck!]!
}

type StorageCheck {
  ref: String!
  "sha256 of the object as read back."
  sha256: String!
  matches: Boolean!
  "When the executor read it back (the executor's clock, not the ledger's)."
  at: Time!
}
`

func newGraphQL(e *Executor, workflowsPath string) http.Handler {
	schema := graphql.MustParseSchema(schemaSDL, &gqlRoot{e: e, workflowsPath: workflowsPath},
		graphql.UseFieldResolvers(), graphql.MaxDepth(10))
	return &relay.Handler{Schema: schema}
}

type gqlRoot struct {
	e             *Executor
	workflowsPath string
}

type gqlRun struct {
	ID                    graphql.ID
	WorkflowName          string
	Status                string
	CancelRequested       bool
	CreatedAt             graphql.Time
	StartedAt, FinishedAt *graphql.Time
	Steps                 []gqlStep
}

type gqlStep struct {
	Name, Device          string
	DependsOn             []string
	Status, Phase         string
	StartedAt, FinishedAt *graphql.Time
	Error, DataRef        *string
	Attempts              []gqlAttempt
}

type gqlAttempt struct {
	Number                int32
	JobID                 graphql.ID
	Phase                 string
	Decided, Claimed      bool
	Outcome               *string
	StartedAt, FinishedAt *graphql.Time
	InstrumentError       *string
	Retryable             bool
	Unresolved            *string
	Data                  *gqlData
}

type gqlData struct {
	LedgerSha256 *string
	Verified     bool
	DataLost     bool
	Checks       []gqlCheck
}

type gqlCheck struct {
	Ref, Sha256 string
	Matches     bool
	At          graphql.Time
}

type gqlWorkflow struct {
	Name      string
	IsDefault bool
	Steps     []string
}

func (r *gqlRoot) Run(ctx context.Context, args struct{ ID graphql.ID }) (*gqlRun, error) {
	var run *gqlRun
	err := r.e.snapshot(ctx, func(tx *sql.Tx) (err error) {
		run, err = readRun(ctx, tx, string(args.ID))
		return err
	})
	if errors.Is(err, ErrRunNotFound) {
		return nil, nil
	}
	return run, gqlFault(err)
}

func (r *gqlRoot) Runs(ctx context.Context, args struct{ Status *string }) ([]*gqlRun, error) {
	var status *string
	if args.Status != nil {
		s := strings.ToLower(*args.Status)
		status = &s
	}
	runs := []*gqlRun{}
	err := r.e.snapshot(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			`SELECT id FROM runs WHERE $1::text IS NULL OR status = $1 ORDER BY created_at DESC`, status)
		if err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("scan run id: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
		for _, id := range ids {
			run, err := readRun(ctx, tx, id)
			if err != nil {
				return err
			}
			runs = append(runs, run)
		}
		return nil
	})
	return runs, gqlFault(err)
}

func (r *gqlRoot) Workflows() ([]gqlWorkflow, error) {
	set, err := LoadWorkflows(r.workflowsPath)
	if err != nil {
		return nil, gqlError{err.Error(), "WORKFLOWS_INVALID"} // the operator's to fix, as in REST
	}
	out := []gqlWorkflow{}
	for _, name := range set.Names() {
		steps, err := set.Get(name)
		if err != nil {
			return nil, gqlFault(err)
		}
		w := gqlWorkflow{Name: name, IsDefault: name == set.Default(), Steps: []string{}}
		for _, st := range steps {
			w.Steps = append(w.Steps, st.Name)
		}
		out = append(out, w)
	}
	return out, nil
}

func (r *gqlRoot) CreateRun(ctx context.Context, args struct{ WorkflowName *string }) (*gqlRun, error) {
	set, err := LoadWorkflows(r.workflowsPath) // read fresh, as REST does
	if err != nil {
		return nil, gqlError{err.Error(), "WORKFLOWS_INVALID"}
	}
	name := set.Default()
	if args.WorkflowName != nil && *args.WorkflowName != "" {
		name = *args.WorkflowName
	}
	tmpl, err := set.Get(name)
	if err != nil {
		return nil, gqlError{err.Error(), "BAD_USER_INPUT"}
	}
	rv, err := r.e.CreateRun(ctx, name, tmpl)
	if err != nil {
		return nil, gqlFault(err)
	}
	return r.Run(ctx, struct{ ID graphql.ID }{graphql.ID(rv.ID)})
}

func (r *gqlRoot) StartRun(ctx context.Context, args struct{ ID graphql.ID }) (*gqlRun, error) {
	return r.after(ctx, args.ID, r.e.StartRun)
}

func (r *gqlRoot) CancelRun(ctx context.Context, args struct{ ID graphql.ID }) (*gqlRun, error) {
	return r.after(ctx, args.ID, r.e.CancelRun)
}

// after does op, then reads the record afresh: the attempts are not in the
// RunView op returns, and the record may already have moved on.
func (r *gqlRoot) after(ctx context.Context, id graphql.ID, op func(context.Context, string) (RunView, error)) (*gqlRun, error) {
	if _, err := op(ctx, string(id)); err != nil {
		return nil, gqlFault(err)
	}
	return r.Run(ctx, struct{ ID graphql.ID }{id})
}

// snapshot reads in one repeatable-read transaction, so a run and its steps
// are seen as of one moment and never half way through an apply.
func (e *Executor) snapshot(ctx context.Context, read func(*sql.Tx) error) error {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin read: %w", err)
	}
	defer tx.Rollback()
	return read(tx)
}

func readRun(ctx context.Context, tx *sql.Tx, runID string) (*gqlRun, error) {
	var run gqlRun
	var created time.Time
	var started, finished *time.Time
	err := tx.QueryRowContext(ctx,
		`SELECT id, workflow_name, status, created_at, started_at, finished_at FROM runs WHERE id = $1`,
		runID).Scan(&run.ID, &run.WorkflowName, &run.Status, &created, &started, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRunNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get run %s: %w", runID, err)
	}
	run.Status = strings.ToUpper(run.Status)
	run.CreatedAt, run.StartedAt, run.FinishedAt = graphql.Time{Time: created}, gqlTime(started), gqlTime(finished)

	rows, err := tx.QueryContext(ctx,
		`SELECT name, device_id, depends_on, status, phase, started_at, finished_at, error, data_ref, lifecycle
		   FROM steps WHERE run_id = $1 ORDER BY position`, runID)
	if err != nil {
		return nil, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	defer rows.Close()
	run.Steps = []gqlStep{}
	for rows.Next() {
		var st gqlStep
		var deps pq.StringArray
		var raw []byte
		if err := rows.Scan(&st.Name, &st.Device, &deps, &st.Status, &st.Phase,
			&started, &finished, &st.Error, &st.DataRef, &raw); err != nil {
			return nil, fmt.Errorf("scan step of %s: %w", runID, err)
		}
		lc, err := decodeStep(raw)
		if err != nil {
			return nil, fmt.Errorf("step %s/%s: %w", runID, st.Name, err)
		}
		st.DependsOn = append([]string{}, deps...)
		st.Status, st.Phase = strings.ToUpper(st.Status), strings.ToUpper(st.Phase)
		st.StartedAt, st.FinishedAt = gqlTime(started), gqlTime(finished)
		st.Attempts = attemptsOf(lc)
		run.CancelRequested = run.CancelRequested || lc.Cancelled
		run.Steps = append(run.Steps, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	return &run, nil
}

func attemptsOf(s Step) []gqlAttempt {
	out := []gqlAttempt{}
	for _, a := range s.Attempts {
		g := gqlAttempt{
			Number: int32(a.N), JobID: graphql.ID(a.JobID), Phase: strings.ToUpper(string(a.phase(s.ProducesData))),
			Decided: s.Decided == a.N, Claimed: a.Claimed, Outcome: upperOrNil(string(a.Outcome)),
			StartedAt: gqlTime(a.StartedAt), FinishedAt: gqlTime(a.FinishedAt),
			InstrumentError: orNil(a.InstrumentError), Retryable: a.Retryable, Unresolved: orNil(a.reason(s.ProducesData)),
		}
		if s.ProducesData {
			d := &gqlData{LedgerSha256: orNil(a.LedgerSHA), Verified: a.verified(), DataLost: a.DataLost, Checks: []gqlCheck{}}
			for _, c := range a.Checks {
				d.Checks = append(d.Checks, gqlCheck{Ref: c.Ref, Sha256: c.SHA, Matches: a.LedgerSHA != "" && c.SHA == a.LedgerSHA,
					At: graphql.Time{Time: c.At}})
			}
			g.Data = d
		}
		out = append(out, g)
	}
	return out
}

func gqlTime(t *time.Time) *graphql.Time {
	if t == nil {
		return nil
	}
	return &graphql.Time{Time: *t}
}

func orNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func upperOrNil(s string) *string { return orNil(strings.ToUpper(s)) }

// gqlError is an error a client can act on, with a code in its extensions.
type gqlError struct{ msg, code string }

func (e gqlError) Error() string              { return e.msg }
func (e gqlError) Extensions() map[string]any { return map[string]any{"code": e.code} }

// gqlFault maps the Executor's errors as REST's writeError does: a missing run
// is said so, anything else goes to the log and the client sees only that it
// failed.
func gqlFault(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrRunNotFound):
		return gqlError{"run not found", "NOT_FOUND"}
	}
	log.Printf("graphql: %v", err)
	return gqlError{"internal error", "INTERNAL"}
}
