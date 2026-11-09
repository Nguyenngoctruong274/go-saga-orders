// Package saga is the domain of an orchestrated saga: a fixed sequence of
// steps, each a command to a participant with an optional compensating
// command, and the state machine that drives one saga instance forward,
// retries it, times it out and compensates it. It does no I/O and takes
// the clock as an argument, so every transition is deterministic and
// testable; the orchestrator (day 5) persists it and moves the messages.
package saga

import (
	"errors"
	"fmt"
	"time"
)

// StepDef describes one step of a definition.
type StepDef struct {
	// Name identifies the step inside the saga and in every message.
	Name string
	// Action is the command sent to run the step, as the participant
	// knows it, for example "inventory.reserve".
	Action string
	// Compensation is the command that undoes a step that ran, for example
	// "inventory.release". Empty for a step that cannot be undone; such
	// steps belong at the end of a definition (the pivot), since a failure
	// after one cannot be fully compensated.
	Compensation string
	// Timeout is how long the orchestrator waits for a reply to one
	// attempt before treating it as lost.
	Timeout time.Duration
	// MaxAttempts bounds how many times one command (the action, and
	// separately the compensation) is sent before giving up: lost replies
	// and retriable refusals count, a definitive failure does not.
	MaxAttempts int
}

// Definition is the ordered list of steps a saga runs.
type Definition struct {
	Name  string
	Steps []StepDef
}

// Validate reports every defect of a definition at once.
func (d Definition) Validate() error {
	var errs []error
	if d.Name == "" {
		errs = append(errs, errors.New("definition: name is required"))
	}
	if len(d.Steps) == 0 {
		errs = append(errs, errors.New("definition: at least one step is required"))
	}
	seen := make(map[string]struct{}, len(d.Steps))
	for i, st := range d.Steps {
		at := fmt.Sprintf("step %d", i)
		if st.Name != "" {
			at = fmt.Sprintf("step %q", st.Name)
		}
		switch {
		case st.Name == "":
			errs = append(errs, fmt.Errorf("%s: name is required", at))
		default:
			if _, dup := seen[st.Name]; dup {
				errs = append(errs, fmt.Errorf("%s: duplicate name", at))
			}
			seen[st.Name] = struct{}{}
		}
		if st.Action == "" {
			errs = append(errs, fmt.Errorf("%s: action is required", at))
		}
		if st.Timeout <= 0 {
			errs = append(errs, fmt.Errorf("%s: timeout must be positive", at))
		}
		if st.MaxAttempts < 1 {
			errs = append(errs, fmt.Errorf("%s: max attempts must be at least 1", at))
		}
	}
	return errors.Join(errs...)
}
