package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

const testWorkflowsYAML = `default: Fill and incubate
workflows:
  - name: Fill and incubate
    steps:
      - {name: fill, device: liquid-handler-1}
      - {name: incubate, device: incubator-1, depends_on: [fill]}
`

func (h *harness) graphql() http.Handler {
	h.t.Helper()
	path := filepath.Join(h.t.TempDir(), "workflows.yaml")
	if err := os.WriteFile(path, []byte(testWorkflowsYAML), 0o644); err != nil {
		h.t.Fatal(err)
	}
	return newGraphQL(h.e, path)
}

type gqlResponse struct {
	Data   json.RawMessage
	Errors []struct {
		Message    string
		Path       []any
		Extensions map[string]any
	}
}

// ask posts one query and decodes data into out. It fails the test on any
// GraphQL error unless the caller wants the errors back.
func ask(t *testing.T, gql http.Handler, query string, vars map[string]any, out any) gqlResponse {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body)))
	var resp gqlResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON (%d): %s", rec.Code, rec.Body)
	}
	if out != nil && len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, out); err != nil {
			t.Fatalf("decode %s: %v", resp.Data, err)
		}
	}
	return resp
}

type seenAttempt struct {
	Number          int
	JobID           string
	Phase           string
	Decided         bool
	Claimed         bool
	Outcome         *string
	StartedAt       *time.Time
	FinishedAt      *time.Time
	InstrumentError *string
	Retryable       bool
	Unresolved      *string
	Data            *struct {
		LedgerSha256 *string
		Verified     bool
		DataLost     bool
		Checks       []struct {
			Ref, Sha256 string
			Matches     bool
		}
	}
}

type seenStep struct {
	Name, Device, Status, Phase string
	DependsOn                   []string
	StartedAt, FinishedAt       *time.Time
	Error, DataRef              *string
	Attempts                    []seenAttempt
}

type seenRun struct {
	ID, WorkflowName, Status string
	CancelRequested          bool
	CreatedAt                time.Time
	StartedAt, FinishedAt    *time.Time
	Steps                    []seenStep
}

const runFields = `id workflowName status cancelRequested createdAt startedAt finishedAt
	steps { name device dependsOn status phase startedAt finishedAt error dataRef
		attempts { number jobId phase decided claimed outcome startedAt finishedAt instrumentError retryable unresolved
			data { ledgerSha256 verified dataLost checks { ref sha256 matches } } } }`

func getRun(t *testing.T, gql http.Handler, id string) *seenRun {
	t.Helper()
	var out struct{ Run *seenRun }
	ask(t, gql, `query($id: ID!) { run(id: $id) { `+runFields+` } }`, map[string]any{"id": id}, &out)
	return out.Run
}

// K53: a retried step shows every attempt, each with its own ledger times, so
// the device time an earlier attempt took is in the record. The step itself
// says exactly what REST says.
func TestGraphQLShowsEveryAttemptOfARetriedStep(t *testing.T) {
	h := newHarness(t)
	_, inc := h.drivers()
	rv := h.startRun(sendWorkflow)
	first, second := protocol.JobID(rv.ID, "incubate", 1), protocol.JobID(rv.ID, "incubate", 2)
	inc.outcome[first], inc.retryable["incubate"] = "failed", true
	rest := h.waitRun(rv.ID, RunCompleted, 10*time.Second)

	got := getRun(t, h.graphql(), rv.ID)
	if got == nil {
		t.Fatal("run not found")
	}
	if got.Status != "COMPLETED" || !got.FinishedAt.Equal(*rest.FinishedAt) || got.CancelRequested {
		t.Errorf("run %s finished %v cancel %v; REST says %s at %v", got.Status, got.FinishedAt, got.CancelRequested, rest.Status, rest.FinishedAt)
	}
	for i, s := range got.Steps {
		r := rest.Steps[i]
		same := seenStep{Name: r.Name, Device: r.DeviceID, DependsOn: r.DependsOn, Status: strings.ToUpper(r.Status), Phase: s.Phase,
			StartedAt: r.StartedAt, FinishedAt: r.FinishedAt, Error: r.Error, DataRef: r.DataRef, Attempts: s.Attempts}
		if s.DependsOn == nil {
			s.DependsOn = []string{}
		}
		if !reflect.DeepEqual(s, same) {
			t.Errorf("step %d differs from REST:\n graphql %+v\n rest    %+v", i, s, r)
		}
	}

	incubate := got.Steps[1]
	if incubate.Phase != "COMPLETED" || len(incubate.Attempts) != 2 {
		t.Fatalf("incubate is %s with %d attempts, want completed with 2", incubate.Phase, len(incubate.Attempts))
	}
	a1, a2 := incubate.Attempts[0], incubate.Attempts[1]
	inc.mu.Lock()
	end1, end2 := inc.at[first], inc.at[second]
	inc.mu.Unlock()
	if a1.Number != 1 || a1.JobID != first || a1.Phase != "FAILED" || deref(a1.Outcome) != "FAILED" || !a1.Retryable ||
		deref(a1.InstrumentError) != "instrument error" || a1.Decided || !a1.Claimed || a1.StartedAt == nil || !a1.FinishedAt.Equal(end1) {
		t.Errorf("attempt 1 = %+v; want the failed, retryable job %s that ended at %v", a1, first, end1)
	}
	if a2.Number != 2 || a2.JobID != second || a2.Phase != "COMPLETED" || !a2.Decided || a2.Retryable || !a2.FinishedAt.Equal(end2) {
		t.Errorf("attempt 2 = %+v; want the completed job %s that ended at %v", a2, second, end2)
	}
	if !a1.FinishedAt.Before(*a2.StartedAt) {
		t.Errorf("attempt 1 ended %v, after attempt 2 started %v", a1.FinishedAt, a2.StartedAt)
	}
	if a1.Data != nil {
		t.Errorf("incubator attempt has data %+v, want none", a1.Data)
	}
}

// A tool creates, starts, follows and cancels runs through GraphQL alone, and
// a run or workflow that does not exist is said so, not a server fault.
func TestGraphQLRunOperations(t *testing.T) {
	h := newHarness(t)
	h.drivers()
	gql := h.graphql()

	var wf struct {
		Workflows []struct {
			Name      string
			IsDefault bool
			Steps     []string
		}
	}
	ask(t, gql, `{ workflows { name isDefault steps } }`, nil, &wf)
	if len(wf.Workflows) != 1 || !wf.Workflows[0].IsDefault || !reflect.DeepEqual(wf.Workflows[0].Steps, []string{"fill", "incubate"}) {
		t.Errorf("workflows = %+v", wf.Workflows)
	}

	var created struct{ CreateRun seenRun }
	ask(t, gql, `mutation { createRun { `+runFields+` } }`, nil, &created)
	id := created.CreateRun.ID
	if created.CreateRun.Status != "PENDING" || created.CreateRun.WorkflowName != "Fill and incubate" || len(created.CreateRun.Steps) != 2 {
		t.Fatalf("created %+v", created.CreateRun)
	}
	var started struct{ StartRun struct{ Status string } }
	ask(t, gql, `mutation($id: ID!) { startRun(id: $id) { status } }`, map[string]any{"id": id}, &started)
	if started.StartRun.Status != "RUNNING" {
		t.Errorf("startRun says %s", started.StartRun.Status)
	}
	h.waitRun(id, RunCompleted, 10*time.Second)
	if r := getRun(t, gql, id); r.Status != "COMPLETED" || r.Steps[1].Attempts[0].Phase != "COMPLETED" {
		t.Errorf("followed run is %+v", r)
	}

	var pending struct{ CreateRun struct{ ID string } }
	ask(t, gql, `mutation { createRun(workflowName: "Fill and incubate") { id } }`, nil, &pending)
	var cancelled struct{ CancelRun seenRun }
	ask(t, gql, `mutation($id: ID!) { cancelRun(id: $id) { `+runFields+` } }`, map[string]any{"id": pending.CreateRun.ID}, &cancelled)
	if c := cancelled.CancelRun; c.Status != "CANCELLED" || !c.CancelRequested || c.Steps[0].Phase != "CANCELLED" {
		t.Errorf("cancelled pending run = %+v", c)
	}

	var byStatus struct{ Runs []struct{ ID string } }
	ask(t, gql, `{ runs(status: CANCELLED) { id } }`, nil, &byStatus)
	if len(byStatus.Runs) != 1 || byStatus.Runs[0].ID != pending.CreateRun.ID {
		t.Errorf("cancelled runs = %+v, want only %s", byStatus.Runs, pending.CreateRun.ID)
	}
	var all struct{ Runs []struct{ ID string } }
	ask(t, gql, `{ runs { id } }`, nil, &all)
	if len(all.Runs) != 2 || all.Runs[0].ID != pending.CreateRun.ID {
		t.Errorf("runs = %+v, want both, newest first", all.Runs)
	}

	var unknown struct{ Run *seenRun }
	if resp := ask(t, gql, `{ run(id: "run-nope") { id } }`, nil, &unknown); unknown.Run != nil || len(resp.Errors) != 0 {
		t.Errorf("unknown run = %+v, errors %+v; want null and no error", unknown.Run, resp.Errors)
	}
	for q, code := range map[string]string{
		`mutation { startRun(id: "run-nope") { id } }`:        "NOT_FOUND",
		`mutation { cancelRun(id: "run-nope") { id } }`:       "NOT_FOUND",
		`mutation { createRun(workflowName: "Nope") { id } }`: "BAD_USER_INPUT",
	} {
		resp := ask(t, gql, q, nil, nil)
		if len(resp.Errors) != 1 || resp.Errors[0].Extensions["code"] != code {
			t.Errorf("%s: errors %+v, want one with code %s", q, resp.Errors, code)
		}
	}
}

// A read's attempt carries its data evidence: what the instrument measured and
// each read-back of storage, so a reader can see why a read did or did not
// complete (K1).
func TestGraphQLShowsTheEvidenceForARead(t *testing.T) {
	h := newHarness(t, testStorage(t))
	gql := h.graphql()
	readOf := func(id string) seenAttempt {
		t.Helper()
		for _, s := range getRun(t, gql, id).Steps {
			if s.Name == "read_plate" && len(s.Attempts) == 1 && s.Attempts[0].Data != nil {
				return s.Attempts[0]
			}
		}
		t.Fatalf("no read_plate attempt with data in %s", id)
		return seenAttempt{}
	}
	data := []byte("well,value\nA1,0.42\n")

	good, job, ref := h.readDone(data)
	h.put(job, data)
	h.stored(job, ref)
	h.waitStep(good.ID, "read_plate", StepCompleted)
	a := readOf(good.ID)
	if d := a.Data; a.Phase != "COMPLETED" || deref(d.LedgerSha256) != digest(data) || !d.Verified || d.DataLost ||
		len(d.Checks) != 1 || !d.Checks[0].Matches || d.Checks[0].Ref != ref || d.Checks[0].Sha256 != digest(data) {
		t.Errorf("stored read = %s %+v, want completed, verified by one matching check of %s", a.Phase, d, ref)
	}

	bad, job, ref := h.readDone(data)
	h.put(job, []byte("not what was measured"))
	h.stored(job, ref)
	for deadline := time.Now().Add(5 * time.Second); stepView(t, h.e, bad.ID, "read_plate").Error == nil; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("mismatch never recorded")
		}
	}
	a = readOf(bad.ID)
	if d := a.Data; a.Phase != "UNRESOLVED" || a.Decided || !strings.Contains(deref(a.Unresolved), "does not match") || d.Verified || len(d.Checks) != 1 || d.Checks[0].Matches || d.Checks[0].Sha256 == digest(data) {
		t.Errorf("mismatched read = %s %+v, want unresolved, not verified, one check that does not match", a.Phase, d)
	}
}
