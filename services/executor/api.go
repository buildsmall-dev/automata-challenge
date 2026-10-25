package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

type API struct {
	executor      *Executor
	workflowsPath string
}

func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /workflows", a.listWorkflows)
	mux.HandleFunc("GET /runs", a.listRuns)
	mux.HandleFunc("POST /runs", a.createRun)
	mux.HandleFunc("GET /runs/{id}", a.getRun)
	mux.HandleFunc("POST /runs/{id}/start", a.startRun)
	mux.HandleFunc("POST /runs/{id}/cancel", a.cancelRun)
	mux.HandleFunc("GET /runs/{id}/timeline", a.timeline)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("api: write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRunNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "run not found"})
	case errors.Is(err, ErrNotImplemented):
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": err.Error()})
	default:
		// The detail goes to the log, not to the client.
		log.Printf("api: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

// listWorkflows reports what is currently defined in workflows.yaml.
func (a *API) listWorkflows(w http.ResponseWriter, r *http.Request) {
	set, err := LoadWorkflows(a.workflowsPath)
	if err != nil {
		// A broken workflows.yaml is the operator's to fix, so say what is wrong.
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := []map[string]any{}
	for _, name := range set.Names() {
		steps, err := set.Get(name)
		if err != nil {
			writeError(w, err)
			return
		}
		names := []string{}
		for _, st := range steps {
			names = append(names, st.Name)
		}
		out = append(out, map[string]any{"name": name, "is_default": name == set.Default(), "steps": names})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.executor.ListRuns(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (a *API) createRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowName string `json:"workflow_name"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body is not JSON: " + err.Error()})
			return
		}
	}

	// Read fresh so edits to workflows.yaml take effect without a restart.
	set, err := LoadWorkflows(a.workflowsPath)
	if err != nil {
		// A broken workflows.yaml is the operator's to fix, so say what is wrong.
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	name := body.WorkflowName
	if name == "" {
		name = set.Default()
	}
	tmpl, err := set.Get(name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	rv, err := a.executor.CreateRun(r.Context(), name, tmpl)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rv)
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	rv, err := a.executor.GetRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rv)
}

func (a *API) startRun(w http.ResponseWriter, r *http.Request) {
	rv, err := a.executor.StartRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rv)
}

func (a *API) cancelRun(w http.ResponseWriter, r *http.Request) {
	rv, err := a.executor.CancelRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rv)
}

func offsetMs(t *time.Time, origin time.Time) *int64 {
	if t == nil {
		return nil
	}
	ms := t.Sub(origin).Milliseconds()
	return &ms
}

// timeline shows when each step occupied its device, relative to the start of the run.
func (a *API) timeline(w http.ResponseWriter, r *http.Request) {
	rv, err := a.executor.GetRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	origin := rv.CreatedAt
	if rv.StartedAt != nil {
		origin = *rv.StartedAt
	}
	steps := []map[string]any{}
	for _, st := range rv.Steps {
		steps = append(steps, map[string]any{
			"name":            st.Name,
			"device_id":       st.DeviceID,
			"status":          st.Status,
			"depends_on":      st.DependsOn,
			"start_offset_ms": offsetMs(st.StartedAt, origin),
			"end_offset_ms":   offsetMs(st.FinishedAt, origin),
			"error":           st.Error,
			"data_ref":        st.DataRef,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id":            rv.ID,
		"status":            rv.Status,
		"total_duration_ms": offsetMs(rv.FinishedAt, origin),
		"steps":             steps,
	})
}
