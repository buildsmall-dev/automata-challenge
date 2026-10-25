package main

import "time"

// What the API reports. This shape is fixed: the checks read it.
//
// Your Executor returns RunView. Keep the JSON field names and the status
// values. How you store a run, and what other states you keep internally, is
// up to you.

const (
	RunPending   = "pending"
	RunRunning   = "running"
	RunCompleted = "completed"
	RunFailed    = "failed"
	RunCancelled = "cancelled"

	StepPending   = "pending"
	StepRunning   = "running"
	StepCompleted = "completed"
	StepFailed    = "failed"
	StepCancelled = "cancelled"
)

type StepView struct {
	Name      string   `json:"name"`
	DeviceID  string   `json:"device_id"`
	DependsOn []string `json:"depends_on"`
	Status    string   `json:"status"`
	// When the step started and finished on its device, as far as you know.
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Error      *string    `json:"error"`
	// Where the step's data is stored, as s3://bucket/key. Only steps that
	// produce data (the plate reader) have one.
	DataRef *string `json:"data_ref"`
}

type RunView struct {
	ID           string     `json:"id"`
	WorkflowName string     `json:"workflow_name"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	Steps        []StepView `json:"steps"`
}
