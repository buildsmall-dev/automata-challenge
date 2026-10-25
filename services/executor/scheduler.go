package main

// Decides what should start next. Given, and working.
//
// A pure function: it does no I/O and keeps no state. You call it with the
// steps of one run as they are now, and it tells you which to start. When and
// how often you call it, and what you do with the answer, is up to you.

type StepState struct {
	Name      string
	DeviceID  string
	DependsOn []string
	Status    string
}

// NextSteps returns the names of the steps that should start now, in workflow
// order.
//
// A step should start when it is pending, everything it depends on has
// completed, and its device is not already running a step of this run. A
// device does one thing at a time, so at most one step per device is
// returned.
func NextSteps(steps []StepState) []string {
	completed := map[string]bool{}
	occupied := map[string]bool{}
	for _, s := range steps {
		if s.Status == StepCompleted {
			completed[s.Name] = true
		}
		if s.Status == StepRunning {
			occupied[s.DeviceID] = true
		}
	}

	var chosen []string
	for _, s := range steps {
		if s.Status != StepPending || occupied[s.DeviceID] {
			continue
		}
		ready := true
		for _, dep := range s.DependsOn {
			if !completed[dep] {
				ready = false
				break
			}
		}
		if ready {
			chosen = append(chosen, s.Name)
			occupied[s.DeviceID] = true
		}
	}
	return chosen
}
