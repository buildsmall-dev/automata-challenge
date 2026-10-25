package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/lib/pq"
	"github.com/nats-io/nats.go"

	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
)

// ErrRunNotFound is returned when there is no run with that id. The API turns
// it into a 404.
var ErrRunNotFound = errors.New("run not found")

// ErrNotImplemented is turned into a 501 by the API.
var ErrNotImplemented = errors.New("not implemented")

// Executor is the executor side of the work. This file is yours.
//
// The API calls the methods below and reports what they return. Keep their
// names and signatures, and keep RunView as it is. Everything else -- how runs
// move forward, what goes over NATS, what is stored where -- is yours.
//
// The drivers' side is services/driver/adapter.go.
type Executor struct {
	db      *sql.DB
	nc      *nats.Conn
	storage *storage.Storage
}

func NewExecutor(db *sql.DB, nc *nats.Conn, st *storage.Storage) *Executor {
	return &Executor{db: db, nc: nc, storage: st}
}

// Start is called once, when the executor process starts.
func (e *Executor) Start(ctx context.Context) error {
	// TODO
	log.Printf("executor: started, but nothing talks to the drivers yet")
	return nil
}

// Close is called once, when the executor process stops.
func (e *Executor) Close() {}

func (e *Executor) CreateRun(ctx context.Context, workflowName string, steps []StepTemplate) (RunView, error) {
	runID := "run-" + randomHex(6)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return RunView{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO runs (id, workflow_name) VALUES ($1, $2)`, runID, workflowName); err != nil {
		return RunView{}, fmt.Errorf("insert run: %w", err)
	}
	for position, st := range steps {
		// A nil slice would be stored as NULL, not as an empty array.
		deps := append([]string{}, st.DependsOn...)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO steps (run_id, name, position, device_id, depends_on) VALUES ($1, $2, $3, $4, $5)`,
			runID, st.Name, position, st.DeviceID, pq.Array(deps)); err != nil {
			return RunView{}, fmt.Errorf("insert step %s: %w", st.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return RunView{}, fmt.Errorf("commit: %w", err)
	}
	return e.GetRun(ctx, runID)
}

func (e *Executor) StartRun(ctx context.Context, runID string) (RunView, error) {
	if _, err := e.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, started_at = now() WHERE id = $2 AND status = $3`,
		RunRunning, runID, RunPending); err != nil {
		return RunView{}, fmt.Errorf("start run %s: %w", runID, err)
	}
	// TODO: get the run going.
	log.Printf("executor: run %s started, but nothing happens yet", runID)
	return e.GetRun(ctx, runID)
}

func (e *Executor) CancelRun(ctx context.Context, runID string) (RunView, error) {
	// TODO
	return RunView{}, fmt.Errorf("cancel: %w", ErrNotImplemented)
}

func (e *Executor) GetRun(ctx context.Context, runID string) (RunView, error) {
	var rv RunView
	err := e.db.QueryRowContext(ctx,
		`SELECT id, workflow_name, status, created_at, started_at, finished_at FROM runs WHERE id = $1`,
		runID).Scan(&rv.ID, &rv.WorkflowName, &rv.Status, &rv.CreatedAt, &rv.StartedAt, &rv.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunView{}, ErrRunNotFound
	}
	if err != nil {
		return RunView{}, fmt.Errorf("get run %s: %w", runID, err)
	}

	rows, err := e.db.QueryContext(ctx,
		`SELECT name, device_id, depends_on, status, started_at, finished_at, error, data_ref
		   FROM steps WHERE run_id = $1 ORDER BY position`, runID)
	if err != nil {
		return RunView{}, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	defer rows.Close()
	rv.Steps = []StepView{}
	for rows.Next() {
		var sv StepView
		var deps pq.StringArray
		if err := rows.Scan(&sv.Name, &sv.DeviceID, &deps, &sv.Status,
			&sv.StartedAt, &sv.FinishedAt, &sv.Error, &sv.DataRef); err != nil {
			return RunView{}, fmt.Errorf("scan step of %s: %w", runID, err)
		}
		sv.DependsOn = []string(deps)
		rv.Steps = append(rv.Steps, sv)
	}
	if err := rows.Err(); err != nil {
		return RunView{}, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	return rv, nil
}

func (e *Executor) ListRuns(ctx context.Context) ([]RunView, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT id FROM runs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan run id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}

	runs := []RunView{}
	for _, id := range ids {
		rv, err := e.GetRun(ctx, id)
		if err != nil {
			return nil, err
		}
		runs = append(runs, rv)
	}
	return runs, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)
}
