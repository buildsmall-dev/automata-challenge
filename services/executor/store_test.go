package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// These tests need the compose Postgres (docker compose up -d postgres). Each
// test gets its own database, built from db/init.sql, and drops it afterwards.
const adminURL = "postgresql://lab_admin:lab_password@localhost:5452/lab_default?sslmode=disable"

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_ADMIN_DATABASE_URL")
	if url == "" {
		url = adminURL
	}
	admin, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(); err != nil {
		t.Skipf("postgres not reachable (%v); run: docker compose up -d postgres", err)
	}
	b := make([]byte, 4)
	rand.Read(b)
	name := "executor_test_" + hex.EncodeToString(b)
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.Exec("DROP DATABASE " + name + " WITH (FORCE)")
		admin.Close()
	})

	db, err := sql.Open("postgres", strings.Replace(url, "/lab_default", "/"+name, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// init.sql is a psql script: run what follows its \c line.
	raw, err := os.ReadFile("../../db/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, schema, ok := strings.Cut(string(raw), "\\c executor_db")
	if !ok {
		t.Fatal("db/init.sql has no \\c executor_db line")
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("db/init.sql: %v", err)
	}
	return db
}

var testWorkflow = []StepTemplate{
	{Name: "fill", DeviceID: "liquid-handler-1"},
	{Name: "read_plate", DeviceID: "plate-reader-1", DependsOn: []string{"fill"}},
}

func newRun(t *testing.T, e *Executor) RunView {
	t.Helper()
	rv, err := e.CreateRun(context.Background(), "test", testWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func stepView(t *testing.T, e *Executor, runID, name string) StepView {
	t.Helper()
	rv, err := e.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range rv.Steps {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no step %s", name)
	return StepView{}
}

// deviceOf is the device testWorkflow assigns to a job's step.
func deviceOf(jobID string) string {
	if strings.Contains(jobID, "/read_plate/") {
		return "plate-reader-1"
	}
	return "liquid-handler-1"
}

func mustApply(t *testing.T, e *Executor, jobID string, f Fact) Step {
	t.Helper()
	s, _, err := e.applyToJob(context.Background(), deviceOf(jobID), jobID, f)
	if err != nil {
		t.Fatalf("apply %T: %v", f, err)
	}
	return s
}

func TestStoreKnowsWhichStepsProduceData(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	ctx := context.Background()
	for name, want := range map[string]bool{"fill": false, "read_plate": true} {
		s, _, err := e.applyToStep(ctx, rv.ID, name, Dispatched{JobID: rv.ID + "/" + name + "/1", Attempt: 1})
		if err != nil {
			t.Fatal(err)
		}
		if s.ProducesData != want {
			t.Errorf("%s: ProducesData = %v, want %v", name, s.ProducesData, want)
		}
	}
}

func TestStoreAppliesFactsAndTheAPIShowsThem(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	ctx := context.Background()
	jobID := rv.ID + "/read_plate/1"
	start := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Second)

	if _, _, err := e.applyToStep(ctx, rv.ID, "read_plate", Dispatched{JobID: jobID, Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	if got := stepView(t, e, rv.ID, "read_plate").Status; got != StepPending {
		t.Errorf("dispatched step status = %s, want pending", got)
	}
	mustApply(t, e, jobID, Claimed{JobID: jobID})
	if got := stepView(t, e, rv.ID, "read_plate").Status; got != StepRunning {
		t.Errorf("claimed step status = %s, want running", got)
	}

	mustApply(t, e, jobID, Finished{JobID: jobID, Outcome: OutcomeCompleted, StartedAt: start, FinishedAt: end, SHA: "abc"})
	sv := stepView(t, e, rv.ID, "read_plate")
	if sv.Status != StepRunning || sv.DataRef != nil {
		t.Errorf("read finished but data unverified: status %s data_ref %v, want running and none", sv.Status, sv.DataRef)
	}

	ref := "s3://lab-data/runs/" + jobID + ".csv"
	mustApply(t, e, jobID, DataStored{JobID: jobID, StorageCheck: StorageCheck{Ref: ref, SHA: "abc", At: end.Add(time.Second)}})
	sv = stepView(t, e, rv.ID, "read_plate")
	if sv.Status != StepCompleted || sv.DataRef == nil || *sv.DataRef != ref {
		t.Fatalf("verified read: status %s data_ref %v, want completed and %s", sv.Status, sv.DataRef, ref)
	}
	if sv.StartedAt == nil || !sv.StartedAt.Equal(start) || sv.FinishedAt == nil || !sv.FinishedAt.Equal(end) {
		t.Errorf("times %v..%v, want the ledger's %v..%v", sv.StartedAt, sv.FinishedAt, start, end)
	}
}

func TestStoreReportsUnresolvedSteps(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	jobID := rv.ID + "/fill/1"
	e.applyToStep(context.Background(), rv.ID, "fill", Dispatched{JobID: jobID, Attempt: 1})
	mustApply(t, e, jobID, Claimed{JobID: jobID})
	mustApply(t, e, jobID, Finished{JobID: jobID, Outcome: OutcomeHung, StartedAt: time.Now(), FinishedAt: time.Now()})
	sv := stepView(t, e, rv.ID, "fill")
	if sv.Status != StepRunning || sv.Error == nil || !strings.Contains(*sv.Error, "hung") {
		t.Errorf("hung step: status %s error %v, want running with a reason", sv.Status, sv.Error)
	}
}

func TestStoreApplyIsIdempotent(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	jobID := rv.ID + "/fill/1"
	e.applyToStep(context.Background(), rv.ID, "fill", Dispatched{JobID: jobID, Attempt: 1})
	f := Finished{JobID: jobID, Outcome: OutcomeCompleted, StartedAt: time.Now(), FinishedAt: time.Now()}
	if _, changed, _ := e.applyToJob(context.Background(), deviceOf(jobID), jobID, f); !changed {
		t.Fatal("first apply reported no change")
	}
	if _, changed, _ := e.applyToJob(context.Background(), deviceOf(jobID), jobID, f); changed {
		t.Error("second apply of the same fact reported a change")
	}
}

func TestStoreUnknownJob(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	newRun(t, e)
	_, _, err := e.applyToJob(context.Background(), "liquid-handler-1", "run-nope/fill/1", Claimed{JobID: "run-nope/fill/1"})
	if !errors.Is(err, ErrUnknownJob) {
		t.Errorf("err = %v, want ErrUnknownJob", err)
	}
}

// Claim and cancel race on the same step. Postgres must order them: exactly one
// wins, and what each caller was told matches what was stored.
func TestStoreClaimAndCancelRace(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		rv := newRun(t, e)
		jobID := rv.ID + "/fill/1"
		if _, _, err := e.applyToStep(ctx, rv.ID, "fill", Dispatched{JobID: jobID, Attempt: 1}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var claimSaw, cancelSaw Step
		var claimErr, cancelErr error
		ready := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-ready
			claimSaw, _, claimErr = e.applyToJob(ctx, deviceOf(jobID), jobID, Claimed{JobID: jobID})
		}()
		go func() {
			defer wg.Done()
			<-ready
			cancelSaw, _, cancelErr = e.applyToStep(ctx, rv.ID, "fill", Cancel{})
		}()
		close(ready)
		wg.Wait()
		if claimErr != nil || cancelErr != nil {
			t.Fatalf("claim err %v, cancel err %v", claimErr, cancelErr)
		}

		final := stepView(t, e, rv.ID, "fill")
		granted := claimSaw.Phase() == PhaseClaimed
		cancelled := cancelSaw.Phase() == PhaseCancelled
		if granted == cancelled {
			t.Fatalf("run %d: claim granted=%v and cancel took effect=%v; exactly one must win", i, granted, cancelled)
		}
		if granted && final.Status != StepRunning || cancelled && final.Status != StepCancelled {
			t.Fatalf("run %d: callers saw claim=%v cancel=%v but the record says %s", i, granted, cancelled, final.Status)
		}
	}
}

// The HTTP API keeps its shape: the checks read these exact keys.
func TestAPIShapeUnchanged(t *testing.T) {
	e := NewExecutor(testDB(t), nil, nil)
	rv := newRun(t, e)
	jobID := rv.ID + "/fill/1"
	e.applyToStep(context.Background(), rv.ID, "fill", Dispatched{JobID: jobID, Attempt: 1})
	mustApply(t, e, jobID, Finished{JobID: jobID, Outcome: OutcomeCompleted, StartedAt: time.Now(), FinishedAt: time.Now()})

	srv := httptest.NewServer((&API{executor: e}).Routes())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/runs/" + rv.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if got, want := keys(body), []string{"created_at", "finished_at", "id", "started_at", "status", "steps", "workflow_name"}; !slices.Equal(got, want) {
		t.Errorf("run keys = %v, want %v", got, want)
	}
	step := body["steps"].([]any)[0].(map[string]any)
	if got, want := keys(step), []string{"data_ref", "depends_on", "device_id", "error", "finished_at", "name", "started_at", "status"}; !slices.Equal(got, want) {
		t.Errorf("step keys = %v, want %v", got, want)
	}
	if step["status"] != StepCompleted {
		t.Errorf("fill status = %v, want completed", step["status"])
	}
}

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
