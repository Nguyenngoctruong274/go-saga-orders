package saga

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

func newSaga(t *testing.T) *Saga {
	t.Helper()
	s, err := New(placeOrder, "saga-1", "order-42", t0)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// started returns a running saga with the reserve command in flight.
func started(t *testing.T) (*Saga, Command) {
	t.Helper()
	s := newSaga(t)
	cmd, err := s.Start(t0)
	if err != nil {
		t.Fatal(err)
	}
	return s, cmd
}

func reply(cmd Command, outcome Outcome, reason string) Reply {
	return Reply{SagaID: cmd.SagaID, Step: cmd.Step, Attempt: cmd.Attempt, Outcome: outcome, Reason: reason}
}

func mustApply(t *testing.T, s *Saga, r Reply, now time.Time) *Command {
	t.Helper()
	cmd, err := s.Apply(r, now)
	if err != nil {
		t.Fatalf("apply %+v: %v", r, err)
	}
	return cmd
}

func wantCommand(t *testing.T, got *Command, step string, attempt int, kind CommandKind, name string) Command {
	t.Helper()
	if got == nil {
		t.Fatalf("want command %s %s attempt %d, got none", kind, step, attempt)
	}
	if got.Step != step || got.Attempt != attempt || got.Kind != kind || got.Name != name {
		t.Fatalf("got command %+v, want step %s attempt %d kind %s name %s", *got, step, attempt, kind, name)
	}
	if got.SagaID != "saga-1" || got.Subject != "order-42" {
		t.Fatalf("command %+v lost saga id or subject", *got)
	}
	return *got
}

func wantState(t *testing.T, s *Saga, state State, current int) {
	t.Helper()
	if s.State != state || s.Current != current {
		t.Fatalf("saga is %s current %d, want %s current %d", s.State, s.Current, state, current)
	}
}

func wantStep(t *testing.T, s *Saga, i int, status StepStatus) {
	t.Helper()
	if s.Steps[i].Status != status {
		t.Fatalf("step %s is %s, want %s", s.Steps[i].Name, s.Steps[i].Status, status)
	}
}

func TestNew(t *testing.T) {
	s := newSaga(t)
	wantState(t, s, StatePending, -1)
	if len(s.Steps) != 3 || s.Steps[0].Name != "reserve" || s.Steps[2].Status != StepPending {
		t.Fatalf("steps %+v", s.Steps)
	}
	if s.Version != 0 || !s.CreatedAt.Equal(t0) || len(s.Timeline) != 0 {
		t.Fatalf("fresh saga has version %d, created %s, %d events", s.Version, s.CreatedAt, len(s.Timeline))
	}
	if _, ok := s.Deadline(); ok {
		t.Fatal("pending saga reports a deadline")
	}
}

func TestStart(t *testing.T) {
	s, cmd := started(t)
	wantCommand(t, &cmd, "reserve", 1, KindAction, "inventory.reserve")
	wantState(t, s, StateRunning, 0)
	wantStep(t, s, 0, StepRunning)
	if !cmd.Deadline.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("deadline %s, want start + 5s", cmd.Deadline)
	}
	if d, ok := s.Deadline(); !ok || !d.Equal(cmd.Deadline) {
		t.Fatalf("Deadline() = %s %v, want the command's", d, ok)
	}
	if s.Version != 1 {
		t.Fatalf("version %d after start, want 1", s.Version)
	}
	if _, err := s.Start(t0); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second start: %v, want ErrInvalidState", err)
	}
}

func TestHappyPath(t *testing.T) {
	s, cmd := started(t)
	now := t0.Add(time.Second)

	next := mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), now)
	cmd = wantCommand(t, next, "pay", 1, KindAction, "payment.authorize")
	wantStep(t, s, 0, StepSucceeded)
	if !cmd.Deadline.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("pay deadline %s, want now + 10s", cmd.Deadline)
	}

	next = mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), now)
	cmd = wantCommand(t, next, "confirm", 1, KindAction, "order.confirm")

	next = mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), now)
	if next != nil {
		t.Fatalf("completed saga returned command %+v", *next)
	}
	wantState(t, s, StateCompleted, -1)
	for i := range s.Steps {
		wantStep(t, s, i, StepSucceeded)
	}
	if !s.Terminal() || s.Reason != "" {
		t.Fatalf("terminal %v reason %q", s.Terminal(), s.Reason)
	}
	if s.Version != 4 {
		t.Fatalf("version %d, want 4 (start + 3 replies)", s.Version)
	}
	if last := s.Timeline[len(s.Timeline)-1]; last.Kind != EventCompleted {
		t.Fatalf("last event %s, want completed", last.Kind)
	}
}

func TestPaymentDeclinedReleasesTheReservation(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)

	next := mustApply(t, s, reply(cmd, OutcomeFailed, "card declined"), t0.Add(time.Second))
	// pay refused and did nothing: it is not compensated. reserve is.
	cmd = wantCommand(t, next, "reserve", 2, KindCompensate, "inventory.release")
	wantState(t, s, StateCompensating, 0)
	wantStep(t, s, 1, StepFailed)
	wantStep(t, s, 0, StepCompensating)
	wantStep(t, s, 2, StepPending)
	if s.Steps[1].Reason != "card declined" || s.Reason != "step pay failed: card declined" {
		t.Fatalf("reasons: step %q saga %q", s.Steps[1].Reason, s.Reason)
	}
	if !cmd.Deadline.Equal(t0.Add(6 * time.Second)) {
		t.Fatalf("release deadline %s, want the reserve step's 5s timeout from now", cmd.Deadline)
	}

	next = mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0.Add(2*time.Second))
	if next != nil {
		t.Fatalf("compensated saga returned %+v", *next)
	}
	wantState(t, s, StateCompensated, -1)
	wantStep(t, s, 0, StepCompensated)
	wantStep(t, s, 2, StepPending)
}

func TestFirstStepFailureCompensatesNothing(t *testing.T) {
	s, cmd := started(t)
	next := mustApply(t, s, reply(cmd, OutcomeFailed, "out of stock"), t0)
	if next != nil {
		t.Fatalf("got %+v, want no command", *next)
	}
	wantState(t, s, StateCompensated, -1)
	wantStep(t, s, 0, StepFailed)
	if s.Reason != "step reserve failed: out of stock" {
		t.Fatalf("reason %q", s.Reason)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	s, cmd := started(t)
	next := mustApply(t, s, reply(cmd, OutcomeRetry, "warehouse busy"), t0.Add(time.Second))
	cmd = wantCommand(t, next, "reserve", 2, KindAction, "inventory.reserve")
	if s.Steps[0].Tries != 2 || !cmd.Deadline.Equal(t0.Add(6*time.Second)) {
		t.Fatalf("tries %d deadline %s", s.Steps[0].Tries, cmd.Deadline)
	}
	next = mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	wantCommand(t, next, "pay", 1, KindAction, "payment.authorize")
}

func TestRetriesExhaustedCompensates(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	// pay allows 2 attempts.
	cmd = *mustApply(t, s, reply(cmd, OutcomeRetry, "gateway busy"), t0)
	wantCommand(t, &cmd, "pay", 2, KindAction, "payment.authorize")
	next := mustApply(t, s, reply(cmd, OutcomeRetry, "gateway busy"), t0)
	wantCommand(t, next, "reserve", 2, KindCompensate, "inventory.release")
	wantStep(t, s, 1, StepFailed)
	if s.Steps[1].Reason != "retries exhausted after 2 attempts: gateway busy" {
		t.Fatalf("reason %q", s.Steps[1].Reason)
	}
}

func TestTimeoutRetriesThenGivesUp(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0) // pay in flight, 10s, 2 attempts

	if _, err := s.Timeout(cmd.Deadline.Add(-time.Nanosecond)); !errors.Is(err, ErrNotDue) {
		t.Fatalf("timeout before deadline: %v, want ErrNotDue", err)
	}
	v := s.Version
	next, err := s.Timeout(cmd.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	cmd = wantCommand(t, next, "pay", 2, KindAction, "payment.authorize")
	if s.Version != v+1 || !cmd.Deadline.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("version %d deadline %s", s.Version, cmd.Deadline)
	}

	// A late reply to attempt 1 is stale and ignored.
	if _, err := s.Apply(Reply{SagaID: s.ID, Step: "pay", Attempt: 1, Outcome: OutcomeSucceeded}, cmd.Deadline); !errors.Is(err, ErrStaleReply) {
		t.Fatalf("late reply: %v, want ErrStaleReply", err)
	}
	wantStep(t, s, 1, StepRunning)

	next, err = s.Timeout(cmd.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	// The outcome of pay is unknown, so it is voided too, before reserve
	// is released.
	cmd = wantCommand(t, next, "pay", 3, KindCompensate, "payment.void")
	wantStep(t, s, 1, StepCompensating)
	wantState(t, s, StateCompensating, 1)
	if s.Reason != "step pay timed out: no reply after 2 attempts" {
		t.Fatalf("reason %q", s.Reason)
	}
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	wantCommand(t, &cmd, "reserve", 2, KindCompensate, "inventory.release")
	if next := mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0); next != nil {
		t.Fatal("expected the saga to be compensated")
	}
	wantState(t, s, StateCompensated, -1)
	wantStep(t, s, 1, StepCompensated)
	wantStep(t, s, 0, StepCompensated)
}

func TestCompensationGetsItsOwnAttempts(t *testing.T) {
	s, cmd := started(t)
	// Use up 2 of reserve's 3 action attempts, then fail pay.
	cmd = *mustApply(t, s, reply(cmd, OutcomeRetry, ""), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeFailed, "declined"), t0)
	wantCommand(t, &cmd, "reserve", 3, KindCompensate, "inventory.release")
	if s.Steps[0].Tries != 1 {
		t.Fatalf("compensation tries %d, want 1 (fresh budget)", s.Steps[0].Tries)
	}
	// Two retriable refusals still leave one attempt.
	cmd = *mustApply(t, s, reply(cmd, OutcomeRetry, "busy"), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeRetry, "busy"), t0)
	wantCommand(t, &cmd, "reserve", 5, KindCompensate, "inventory.release")
	// The third refusal exhausts the budget: an operator has to look.
	next := mustApply(t, s, reply(cmd, OutcomeRetry, "busy"), t0)
	if next != nil {
		t.Fatalf("got %+v, want none", *next)
	}
	wantState(t, s, StateFailed, -1)
	wantStep(t, s, 0, StepCompensationFailed)
	if s.Reason != "compensation of reserve retries exhausted after 3 attempts: busy" {
		t.Fatalf("reason %q", s.Reason)
	}
}

func TestCompensationRefusedFailsTheSaga(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeFailed, "declined"), t0)
	next := mustApply(t, s, reply(cmd, OutcomeFailed, "reservation already consumed"), t0)
	if next != nil {
		t.Fatal("failed saga returned a command")
	}
	wantState(t, s, StateFailed, -1)
	if s.Reason != "compensation of reserve failed: reservation already consumed" {
		t.Fatalf("reason %q", s.Reason)
	}
	for _, try := range []func() error{
		func() error { _, err := s.Apply(reply(cmd, OutcomeSucceeded, ""), t0); return err },
		func() error { _, err := s.Timeout(t0.Add(time.Hour)); return err },
		func() error { _, err := s.Abort("x", t0); return err },
	} {
		if err := try(); !errors.Is(err, ErrTerminal) {
			t.Fatalf("transition on a failed saga: %v, want ErrTerminal", err)
		}
	}
}

func TestCompensationTimeoutFailsAfterAttempts(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeFailed, "declined"), t0)
	for want := 2; want <= 3; want++ {
		wantCommand(t, &cmd, "reserve", want, KindCompensate, "inventory.release")
		next, err := s.Timeout(cmd.Deadline)
		if err != nil {
			t.Fatal(err)
		}
		if next == nil {
			t.Fatalf("attempt %d: expected a retry", want)
		}
		cmd = *next
	}
	wantCommand(t, &cmd, "reserve", 4, KindCompensate, "inventory.release")
	next, err := s.Timeout(cmd.Deadline)
	if err != nil || next != nil {
		t.Fatalf("got %v, %v; want no command and no error", next, err)
	}
	wantState(t, s, StateFailed, -1)
	wantStep(t, s, 0, StepCompensationFailed)
}

func TestPivotStepIsNotCompensated(t *testing.T) {
	// A definition with a non-compensable step in the middle: a failure
	// after it undoes everything else and leaves it standing.
	def := Definition{Name: "d", Steps: []StepDef{
		{Name: "a", Action: "a", Compensation: "undo-a", Timeout: time.Second, MaxAttempts: 1},
		{Name: "b", Action: "b", Timeout: time.Second, MaxAttempts: 1},
		{Name: "c", Action: "c", Compensation: "undo-c", Timeout: time.Second, MaxAttempts: 1},
	}}
	s, err := New(def, "saga-1", "order-42", t0)
	if err != nil {
		t.Fatal(err)
	}
	cmd, _ := s.Start(t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
	next := mustApply(t, s, reply(cmd, OutcomeFailed, "nope"), t0)
	wantCommand(t, next, "a", 2, KindCompensate, "undo-a")
	wantStep(t, s, 1, StepSucceeded)
	if next := mustApply(t, s, reply(*next, OutcomeSucceeded, ""), t0); next != nil {
		t.Fatal("expected compensated")
	}
	wantState(t, s, StateCompensated, -1)
}

func TestAbort(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		s := newSaga(t)
		next, err := s.Abort("customer cancelled", t0)
		if err != nil || next != nil {
			t.Fatalf("got %v, %v", next, err)
		}
		wantState(t, s, StateCompensated, -1)
		if s.Reason != "aborted: customer cancelled" || s.Version != 1 {
			t.Fatalf("reason %q version %d", s.Reason, s.Version)
		}
	})
	t.Run("running undoes the step in flight", func(t *testing.T) {
		s, cmd := started(t)
		cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
		next, err := s.Abort("operator", t0)
		if err != nil {
			t.Fatal(err)
		}
		// pay may or may not have gone through: void it, then release.
		cmd = wantCommand(t, next, "pay", 2, KindCompensate, "payment.void")
		wantStep(t, s, 1, StepCompensating)
		cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
		wantCommand(t, &cmd, "reserve", 2, KindCompensate, "inventory.release")
		if next := mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0); next != nil {
			t.Fatal("expected compensated")
		}
		wantState(t, s, StateCompensated, -1)
		if s.Reason != "aborted: operator" {
			t.Fatalf("reason %q", s.Reason)
		}
	})
	t.Run("compensating is left alone", func(t *testing.T) {
		s, cmd := started(t)
		mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0)
		s.Abort("once", t0) //nolint:errcheck // the state is asserted below
		v := s.Version
		if _, err := s.Abort("twice", t0); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("got %v, want ErrInvalidState", err)
		}
		if s.Version != v {
			t.Fatal("rejected abort changed the saga")
		}
	})
}

func TestStaleRepliesChangeNothing(t *testing.T) {
	s, cmd := started(t)
	before := *s
	beforeSteps := append([]Step(nil), s.Steps...)
	for name, r := range map[string]Reply{
		"other saga":    {SagaID: "saga-2", Step: cmd.Step, Attempt: cmd.Attempt, Outcome: OutcomeSucceeded},
		"other step":    {SagaID: s.ID, Step: "pay", Attempt: 1, Outcome: OutcomeSucceeded},
		"older attempt": {SagaID: s.ID, Step: cmd.Step, Attempt: 0, Outcome: OutcomeSucceeded},
		"newer attempt": {SagaID: s.ID, Step: cmd.Step, Attempt: 2, Outcome: OutcomeSucceeded},
	} {
		if _, err := s.Apply(r, t0); !errors.Is(err, ErrStaleReply) {
			t.Fatalf("%s: %v, want ErrStaleReply", name, err)
		}
	}
	if _, err := s.Apply(Reply{SagaID: s.ID, Step: cmd.Step, Attempt: cmd.Attempt, Outcome: "maybe"}, t0); err == nil || errors.Is(err, ErrStaleReply) {
		t.Fatalf("unknown outcome: %v", err)
	}
	if s.Version != before.Version || s.State != before.State || len(s.Timeline) != len(before.Timeline) {
		t.Fatal("rejected replies changed the saga")
	}
	for i := range beforeSteps {
		if s.Steps[i] != beforeSteps[i] {
			t.Fatalf("step %d changed: %+v", i, s.Steps[i])
		}
	}
}

func TestPendingSagaRejectsRepliesAndTimeouts(t *testing.T) {
	s := newSaga(t)
	if _, err := s.Apply(Reply{SagaID: s.ID, Step: "reserve", Attempt: 1, Outcome: OutcomeSucceeded}, t0); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("apply on pending: %v", err)
	}
	if _, err := s.Timeout(t0.Add(time.Hour)); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("timeout on pending: %v", err)
	}
}

func TestTimelineTellsTheStory(t *testing.T) {
	s, cmd := started(t)
	cmd = *mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0.Add(time.Second))
	cmd = *mustApply(t, s, reply(cmd, OutcomeFailed, "declined"), t0.Add(2*time.Second))
	mustApply(t, s, reply(cmd, OutcomeSucceeded, ""), t0.Add(3*time.Second))

	want := []struct {
		kind    EventKind
		step    string
		attempt int
	}{
		{EventStarted, "", 0},
		{EventStepSent, "reserve", 1},
		{EventStepSucceeded, "reserve", 1},
		{EventStepSent, "pay", 1},
		{EventStepFailed, "pay", 1},
		{EventCompensationSent, "reserve", 2},
		{EventStepCompensated, "reserve", 2},
		{EventCompensated, "", 0},
	}
	if len(s.Timeline) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(s.Timeline), len(want), s.Timeline)
	}
	for i, w := range want {
		got := s.Timeline[i]
		if got.Kind != w.kind || got.Step != w.step || got.Attempt != w.attempt {
			t.Errorf("event %d: got %s %s/%d, want %s %s/%d", i, got.Kind, got.Step, got.Attempt, w.kind, w.step, w.attempt)
		}
	}
	if s.Timeline[4].Reason != "declined" || s.Timeline[7].Reason != "step pay failed: declined" {
		t.Fatalf("reasons: %q, %q", s.Timeline[4].Reason, s.Timeline[7].Reason)
	}
	if !s.UpdatedAt.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("updated at %s", s.UpdatedAt)
	}
}
