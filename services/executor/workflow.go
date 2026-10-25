package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// A workflow is a DAG of steps. Each step names the device that must run it and
// the steps within the same run that have to finish first.
type StepTemplate struct {
	Name      string   `yaml:"name"`
	DeviceID  string   `yaml:"device"`
	DependsOn []string `yaml:"depends_on"`
}

type workflowTemplate struct {
	Name  string         `yaml:"name"`
	Steps []StepTemplate `yaml:"steps"`
}

type workflowFile struct {
	Default   string             `yaml:"default"`
	Workflows []workflowTemplate `yaml:"workflows"`
}

type WorkflowSet struct {
	def       string
	byName    map[string]workflowTemplate
	nameOrder []string
}

// LoadWorkflows reads and validates the workflow definitions.
func LoadWorkflows(path string) (*WorkflowSet, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var file workflowFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(file.Workflows) == 0 {
		return nil, fmt.Errorf("%s defines no workflows", path)
	}

	set := &WorkflowSet{
		def:    file.Default,
		byName: make(map[string]workflowTemplate, len(file.Workflows)),
	}

	for _, wf := range file.Workflows {
		if wf.Name == "" {
			return nil, fmt.Errorf("a workflow is missing its name")
		}
		if _, dup := set.byName[wf.Name]; dup {
			return nil, fmt.Errorf("workflow %q is defined twice", wf.Name)
		}
		if err := validateWorkflow(wf); err != nil {
			return nil, fmt.Errorf("workflow %q: %w", wf.Name, err)
		}
		set.byName[wf.Name] = wf
		set.nameOrder = append(set.nameOrder, wf.Name)
	}

	if set.def == "" {
		set.def = set.nameOrder[0]
	}
	if _, ok := set.byName[set.def]; !ok {
		return nil, fmt.Errorf("default workflow %q is not defined", set.def)
	}
	return set, nil
}

func (s *WorkflowSet) Get(name string) ([]StepTemplate, error) {
	wf, ok := s.byName[name]
	if !ok {
		return nil, fmt.Errorf("unknown workflow %q (have: %v)", name, s.nameOrder)
	}
	return wf.Steps, nil
}

func (s *WorkflowSet) Default() string { return s.def }
func (s *WorkflowSet) Names() []string { return s.nameOrder }

// validateWorkflow rejects definitions that could not run: duplicate or missing
// step names, dependencies that do not exist, and cycles.
func validateWorkflow(wf workflowTemplate) error {
	if len(wf.Steps) == 0 {
		return fmt.Errorf("has no steps")
	}

	seen := make(map[string]bool, len(wf.Steps))
	for _, st := range wf.Steps {
		if st.Name == "" {
			return fmt.Errorf("a step is missing its name")
		}
		if seen[st.Name] {
			return fmt.Errorf("step %q is defined twice", st.Name)
		}
		if st.DeviceID == "" {
			return fmt.Errorf("step %q is missing its device", st.Name)
		}
		seen[st.Name] = true
	}

	for _, st := range wf.Steps {
		for _, dep := range st.DependsOn {
			if dep == st.Name {
				return fmt.Errorf("step %q depends on itself", st.Name)
			}
			if !seen[dep] {
				return fmt.Errorf("step %q depends on %q, which is not defined", st.Name, dep)
			}
		}
	}

	return detectCycle(wf.Steps)
}

func detectCycle(steps []StepTemplate) error {
	deps := make(map[string][]string, len(steps))
	for _, st := range steps {
		deps[st.Name] = st.DependsOn
	}

	const (
		unvisited = 0
		inStack   = 1
		done      = 2
	)
	state := make(map[string]int, len(steps))

	var walk func(name string, path []string) error
	walk = func(name string, path []string) error {
		switch state[name] {
		case inStack:
			return fmt.Errorf("dependency cycle: %v -> %s", path, name)
		case done:
			return nil
		}
		state[name] = inStack
		for _, dep := range deps[name] {
			if err := walk(dep, append(path, name)); err != nil {
				return err
			}
		}
		state[name] = done
		return nil
	}

	for _, st := range steps {
		if err := walk(st.Name, nil); err != nil {
			return err
		}
	}
	return nil
}
