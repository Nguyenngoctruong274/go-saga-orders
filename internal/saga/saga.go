package saga

import (
	"errors"
	"fmt"
	"time"
)

// State is where a saga is in its life.
type State string

// Saga states. Completed, Compensated and Failed are terminal.
const (
	// StatePending: created, no command sent yet.
	StatePending State = "pending"
	// StateRunning: executing steps forward; exactly one step is in flight.
	StateRunning State = "running"
	// StateCompensating: a step failed or the saga was aborted; undoing
	// the steps that ran, last first; exactly one compensation in flight.
	StateCompensating State = "compensating"
	// StateCompleted: every step succeeded.
	StateCompleted State = "completed"
	// StateCompensated: the saga did not complete and everything that ran
	// has been undone. The business outcome is "rejected".
	StateCompensated State = "compensated"
	// StateFailed: a compensation was refused or kept timing out. Nothing
	// more happens without an operator; the saga is dead-lettered.
	StateFailed State = "failed"
)

// Terminal reports whether no further transition is possible.
func (s State) Terminal() bool {
	return s == StateCompleted || s == StateCompensated || s == StateFailed
}

// StepStatus is where one step of a saga instance is.
type StepStatus string

// Step statuses.
const (
	StepPending      StepStatus = "pending"
	StepRunning      StepStatus = "running"
	StepSucceeded    StepStatus = "succeeded"
	StepFailed       StepStatus = "failed"    // the participant refused; nothing to undo
	StepTimedOut     StepStatus = "timed_out" // no reply after every attempt; outcome unknown, must be undone
	StepAborted      StepStatus = "aborted"   // aborted while in flight; outcome unknown, must be undone
	StepCompensating StepStatus = "compensating"
	StepCompensated  StepStatus = "compensated"
	// StepCompensationFailed: the compensation was refused or kept timing
	// out. The saga is Failed.
	StepCompensationFailed StepStatus = "compensation_failed"
)

// Step is the record of one step inside a saga instance.
type Step struct {
	Name   string
	Status StepStatus
	// Attempt is the fencing token of the command in flight: it increases
	// on every command sent for this step, action or compensation, and a
	// reply must quote it to be accepted.
	Attempt int
	// Tries counts the attempts of the current kind of command (action,
	// then compensation) against StepDef.MaxAttempts.
	Tries int
	// Deadline is when the command in flight is considered lost; zero when
	// nothing is in flight.
	Deadline time.Time
	// Reason is the last failure reason reported for the step.
	Reason string
}

// CommandKind tells a participant whether to do or to undo.
type CommandKind string

// Command kinds.
const (
	KindAction     CommandKind = "action"
	KindCompensate CommandKind = "compensate"
)

// Command is what the orchestrator must send next. It is a value the
// messaging layer turns into a Kafka record; the domain only decides it.
type Command struct {
	SagaID  string
	Subject string
	Step    string
	// Attempt must be echoed in the reply; replies for another attempt are
	// stale and ignored.
	Attempt int
	Kind    CommandKind
	// Name is the participant-facing command name from the definition.
	Name     string
	Deadline time.Time
}

// Outcome is what a participant answered.
type Outcome string

// Reply outcomes.
const (
	// OutcomeSucceeded: the command took effect.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeFailed: the participant refused for good (out of stock,
	// card declined) and did nothing. Not retried.
	OutcomeFailed Outcome = "failed"
	// OutcomeRetry: the participant could not do it right now and did
	// nothing. Retried until MaxAttempts.
	OutcomeRetry Outcome = "retry"
)

// Reply is a participant's answer to a Command.
type Reply struct {
	SagaID  string
	Step    string
	Attempt int
	Outcome Outcome
	Reason  string
}

// EventKind names an entry of a saga's timeline.
type EventKind string

// Timeline event kinds.
const (
	EventStarted              EventKind = "started"
	EventStepSent             EventKind = "step_sent"
	EventStepSucceeded        EventKind = "step_succeeded"
	EventStepFailed           EventKind = "step_failed"
	EventStepRetried          EventKind = "step_retried"
	EventStepTimedOut         EventKind = "step_timed_out"
	EventStepAborted          EventKind = "step_aborted"
	EventCompensationSent     EventKind = "compensation_sent"
	EventStepCompensated      EventKind = "step_compensated"
	EventCompensationRetried  EventKind = "compensation_retried"
	EventCompensationTimedOut EventKind = "compensation_timed_out"
	EventCompensationFailed   EventKind = "compensation_failed"
	EventCompleted            EventKind = "completed"
	EventCompensated          EventKind = "compensated"
	EventFailed               EventKind = "failed"
	EventAborted              EventKind = "aborted"
)

// Event is one entry of the timeline a saga keeps of what happened to it.
type Event struct {
	At      time.Time
	Kind    EventKind
	Step    string
	Attempt int
	Reason  string
}

// Errors returned by transitions. Callers match them with errors.Is.
var (
	// ErrStaleReply: the reply is for another saga, step or attempt than
	// the one in flight. Ignoring it is the correct handling.
	ErrStaleReply = errors.New("stale reply")
	// ErrInvalidState: the transition does not apply in the saga's state.
	ErrInvalidState = errors.New("invalid state")
	// ErrTerminal: the saga is finished and accepts no transition.
	ErrTerminal = errors.New("saga is terminal")
	// ErrNotDue: Timeout was called before the deadline.
	ErrNotDue = errors.New("deadline not reached")
)

// Saga is one instance of a definition. Every method that changes it takes
// the current time and returns the command to send next, if any; the
// caller persists the saga and sends the command.
type Saga struct {
	ID      string
	Subject string
	// Definition is the recipe; it is not persisted with the instance but
	// attached again by name when one is loaded.
	Definition Definition
	State      State
	Steps      []Step
	// Current is the index of the step in flight, or -1.
	Current int
	// Version increases by one on every accepted transition, for
	// optimistic locking when the saga is stored.
	Version int
	// Reason says why the saga stopped going forward, once it has.
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
	Timeline  []Event
}

// New creates a pending saga of def for subject.
func New(def Definition, id, subject string, now time.Time) (*Saga, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("saga: id is required")
	}
	steps := make([]Step, len(def.Steps))
	for i, st := range def.Steps {
		steps[i] = Step{Name: st.Name, Status: StepPending}
	}
	return &Saga{
		ID: id, Subject: subject, Definition: def,
		State: StatePending, Steps: steps, Current: -1,
		CreatedAt: now, UpdatedAt: now,
	}, nil
}

// Terminal reports whether the saga is finished.
func (s *Saga) Terminal() bool { return s.State.Terminal() }

// Deadline returns when the command in flight is considered lost, and
// whether one is in flight.
func (s *Saga) Deadline() (time.Time, bool) {
	if s.Current < 0 {
		return time.Time{}, false
	}
	return s.Steps[s.Current].Deadline, true
}

// Start sends the first step. The saga must be pending.
func (s *Saga) Start(now time.Time) (Command, error) {
	if s.State != StatePending {
		return Command{}, fmt.Errorf("%w: start requires %s, saga is %s", ErrInvalidState, StatePending, s.State)
	}
	s.State = StateRunning
	s.record(now, EventStarted, "", 0, "")
	cmd := s.send(0, KindAction, now)
	s.touch(now)
	return cmd, nil
}

// Apply consumes a participant's reply. A reply that does not quote the
// saga, step and attempt in flight is stale: it returns ErrStaleReply and
// changes nothing. The returned command is nil when nothing more is to be
// sent, which is the case exactly when the saga became terminal.
func (s *Saga) Apply(r Reply, now time.Time) (*Command, error) {
	if s.Terminal() {
		return nil, ErrTerminal
	}
	if s.Current < 0 {
		return nil, fmt.Errorf("%w: no step in flight, saga is %s", ErrInvalidState, s.State)
	}
	cur := &s.Steps[s.Current]
	if r.SagaID != s.ID || r.Step != cur.Name || r.Attempt != cur.Attempt {
		return nil, fmt.Errorf("%w: reply for saga %q step %q attempt %d, in flight is saga %q step %q attempt %d",
			ErrStaleReply, r.SagaID, r.Step, r.Attempt, s.ID, cur.Name, cur.Attempt)
	}
	switch r.Outcome {
	case OutcomeSucceeded, OutcomeFailed, OutcomeRetry:
	default:
		return nil, fmt.Errorf("unknown outcome %q", r.Outcome)
	}
	defer s.touch(now)

	if s.State == StateRunning {
		switch r.Outcome {
		case OutcomeSucceeded:
			s.finish(cur, StepSucceeded, "")
			s.record(now, EventStepSucceeded, cur.Name, cur.Attempt, "")
			return s.advance(now), nil
		case OutcomeFailed:
			s.finish(cur, StepFailed, r.Reason)
			s.record(now, EventStepFailed, cur.Name, cur.Attempt, r.Reason)
			return s.compensate(now, fmt.Sprintf("step %s failed: %s", cur.Name, r.Reason)), nil
		default: // retry
			if cur.Tries < s.def(s.Current).MaxAttempts {
				s.record(now, EventStepRetried, cur.Name, cur.Attempt, r.Reason)
				cmd := s.send(s.Current, KindAction, now)
				return &cmd, nil
			}
			reason := fmt.Sprintf("retries exhausted after %d attempts: %s", cur.Tries, r.Reason)
			s.finish(cur, StepFailed, reason)
			s.record(now, EventStepFailed, cur.Name, cur.Attempt, reason)
			return s.compensate(now, fmt.Sprintf("step %s %s", cur.Name, reason)), nil
		}
	}

	// Compensating.
	switch r.Outcome {
	case OutcomeSucceeded:
		s.finish(cur, StepCompensated, "")
		s.record(now, EventStepCompensated, cur.Name, cur.Attempt, "")
		return s.continueCompensation(now), nil
	case OutcomeFailed:
		s.finish(cur, StepCompensationFailed, r.Reason)
		s.record(now, EventCompensationFailed, cur.Name, cur.Attempt, r.Reason)
		s.fail(now, fmt.Sprintf("compensation of %s failed: %s", cur.Name, r.Reason))
		return nil, nil
	default: // retry
		if cur.Tries < s.def(s.Current).MaxAttempts {
			s.record(now, EventCompensationRetried, cur.Name, cur.Attempt, r.Reason)
			cmd := s.send(s.Current, KindCompensate, now)
			return &cmd, nil
		}
		reason := fmt.Sprintf("retries exhausted after %d attempts: %s", cur.Tries, r.Reason)
		s.finish(cur, StepCompensationFailed, reason)
		s.record(now, EventCompensationFailed, cur.Name, cur.Attempt, reason)
		s.fail(now, fmt.Sprintf("compensation of %s %s", cur.Name, reason))
		return nil, nil
	}
}

// Timeout handles a command whose deadline has passed without a reply. It
// is retried while attempts remain; after the last one the step's outcome
// is unknown, so a forward step is compensated along with those before
// it, and a compensation that keeps timing out fails the saga. Before the
// deadline it returns ErrNotDue and changes nothing.
func (s *Saga) Timeout(now time.Time) (*Command, error) {
	if s.Terminal() {
		return nil, ErrTerminal
	}
	if s.Current < 0 {
		return nil, fmt.Errorf("%w: no step in flight, saga is %s", ErrInvalidState, s.State)
	}
	cur := &s.Steps[s.Current]
	if now.Before(cur.Deadline) {
		return nil, fmt.Errorf("%w: step %s attempt %d is due at %s", ErrNotDue, cur.Name, cur.Attempt, cur.Deadline.Format(time.RFC3339))
	}
	defer s.touch(now)
	limit := s.def(s.Current).MaxAttempts

	if s.State == StateRunning {
		if cur.Tries < limit {
			s.record(now, EventStepTimedOut, cur.Name, cur.Attempt, "")
			cmd := s.send(s.Current, KindAction, now)
			return &cmd, nil
		}
		reason := fmt.Sprintf("no reply after %d attempts", cur.Tries)
		s.finish(cur, StepTimedOut, reason)
		s.record(now, EventStepTimedOut, cur.Name, cur.Attempt, reason)
		return s.compensate(now, fmt.Sprintf("step %s timed out: %s", cur.Name, reason)), nil
	}

	if cur.Tries < limit {
		s.record(now, EventCompensationTimedOut, cur.Name, cur.Attempt, "")
		cmd := s.send(s.Current, KindCompensate, now)
		return &cmd, nil
	}
	reason := fmt.Sprintf("no reply after %d attempts", cur.Tries)
	s.finish(cur, StepCompensationFailed, reason)
	s.record(now, EventCompensationTimedOut, cur.Name, cur.Attempt, reason)
	s.fail(now, fmt.Sprintf("compensation of %s timed out: %s", cur.Name, reason))
	return nil, nil
}

// Abort stops a saga that has not finished going forward: a pending saga
// is compensated on the spot (nothing ran), a running one has its step in
// flight marked aborted (its outcome is unknown, so it is undone too) and
// starts compensating. A saga already compensating or terminal is left
// alone with an error.
func (s *Saga) Abort(reason string, now time.Time) (*Command, error) {
	if s.Terminal() {
		return nil, ErrTerminal
	}
	if s.State == StateCompensating {
		return nil, fmt.Errorf("%w: saga is already compensating", ErrInvalidState)
	}
	defer s.touch(now)
	s.record(now, EventAborted, "", 0, reason)
	if s.State == StatePending {
		s.Reason = "aborted: " + reason
		s.State = StateCompensated
		s.record(now, EventCompensated, "", 0, s.Reason)
		return nil, nil
	}
	cur := &s.Steps[s.Current]
	s.finish(cur, StepAborted, reason)
	s.record(now, EventStepAborted, cur.Name, cur.Attempt, reason)
	return s.compensate(now, "aborted: "+reason), nil
}

func (s *Saga) def(i int) StepDef { return s.Definition.Steps[i] }

// send puts step i in flight with a fresh attempt and returns the command.
func (s *Saga) send(i int, kind CommandKind, now time.Time) Command {
	def := s.def(i)
	st := &s.Steps[i]
	s.Current = i
	st.Attempt++
	st.Tries++
	st.Deadline = now.Add(def.Timeout)
	name, status, ev := def.Action, StepRunning, EventStepSent
	if kind == KindCompensate {
		name, status, ev = def.Compensation, StepCompensating, EventCompensationSent
	}
	st.Status = status
	s.record(now, ev, st.Name, st.Attempt, "")
	return Command{
		SagaID: s.ID, Subject: s.Subject, Step: st.Name, Attempt: st.Attempt,
		Kind: kind, Name: name, Deadline: st.Deadline,
	}
}

// finish takes the current step out of flight with its final status.
func (s *Saga) finish(st *Step, status StepStatus, reason string) {
	st.Status = status
	st.Reason = reason
	st.Deadline = time.Time{}
	s.Current = -1
}

// advance sends the next forward step, or completes the saga.
func (s *Saga) advance(now time.Time) *Command {
	for i, st := range s.Steps {
		if st.Status == StepPending {
			cmd := s.send(i, KindAction, now)
			return &cmd
		}
	}
	s.State = StateCompleted
	s.record(now, EventCompleted, "", 0, "")
	return nil
}

// compensate turns the saga around and undoes the last step that ran.
func (s *Saga) compensate(now time.Time, reason string) *Command {
	s.State = StateCompensating
	s.Reason = reason
	return s.continueCompensation(now)
}

// continueCompensation sends the compensation of the last step whose
// effect may stand (it succeeded, or its outcome is unknown) and has one,
// walking backwards; when none is left the saga is compensated.
func (s *Saga) continueCompensation(now time.Time) *Command {
	for i := len(s.Steps) - 1; i >= 0; i-- {
		st := s.Steps[i]
		if !needsCompensation(st.Status) || s.def(i).Compensation == "" {
			continue
		}
		s.Steps[i].Tries = 0 // the compensation gets its own attempts
		cmd := s.send(i, KindCompensate, now)
		return &cmd
	}
	s.State = StateCompensated
	s.record(now, EventCompensated, "", 0, s.Reason)
	return nil
}

// needsCompensation reports whether a step in this status may have had an
// effect that must be undone.
func needsCompensation(st StepStatus) bool {
	return st == StepSucceeded || st == StepTimedOut || st == StepAborted
}

func (s *Saga) fail(now time.Time, reason string) {
	s.State = StateFailed
	s.Reason = reason
	s.record(now, EventFailed, "", 0, reason)
}

func (s *Saga) record(at time.Time, kind EventKind, step string, attempt int, reason string) {
	s.Timeline = append(s.Timeline, Event{At: at, Kind: kind, Step: step, Attempt: attempt, Reason: reason})
}

func (s *Saga) touch(now time.Time) {
	s.Version++
	s.UpdatedAt = now
}
