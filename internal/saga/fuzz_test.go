package saga

import (
	"errors"
	"testing"
	"time"
)

// FuzzSagaInvariants drives a saga with an arbitrary sequence of replies,
// timeouts and aborts, including stale and malformed ones, and checks
// after every operation that the saga is in a state the orchestrator can
// rely on: a single command in flight, fencing that rejects everything
// but the live attempt, bounded attempts, monotonic versions, terminal
// states that stay terminal and compensation that undoes exactly the
// steps whose effect may stand.
func FuzzSagaInvariants(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 1, 0, 2, 0})
	f.Add([]byte{0, 1, 0, 2, 0, 3, 3, 3, 3, 3})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 2, 2, 5, 9})
	f.Add([]byte{0, 4, 0, 4, 6, 6, 7, 1, 0})

	f.Fuzz(func(t *testing.T, ops []byte) {
		def := Definition{Name: "fuzz", Steps: []StepDef{
			{Name: "a", Action: "a", Compensation: "undo-a", Timeout: time.Second, MaxAttempts: 2},
			{Name: "b", Action: "b", Timeout: 2 * time.Second, MaxAttempts: 3},
			{Name: "c", Action: "c", Compensation: "undo-c", Timeout: time.Second, MaxAttempts: 1},
			{Name: "d", Action: "d", Compensation: "undo-d", Timeout: 3 * time.Second, MaxAttempts: 2},
		}}
		s, err := New(def, "s", "subj", t0)
		if err != nil {
			t.Fatal(err)
		}
		now := t0
		var inflight *Command
		if cmd, err := s.Start(now); err == nil {
			inflight = &cmd
		}
		check(t, s, inflight)

		for i := 0; i+1 < len(ops); i += 2 {
			op, arg := ops[i], ops[i+1]
			now = now.Add(time.Duration(arg%8) * 500 * time.Millisecond)
			version := s.Version
			var next *Command
			var err error
			var bogus bool
			switch op % 8 {
			case 0, 1, 2: // a reply, usually well-formed
				outcomes := []Outcome{OutcomeSucceeded, OutcomeFailed, OutcomeRetry, OutcomeSucceeded, OutcomeRetry, "bogus"}
				r := Reply{SagaID: "s", Outcome: outcomes[int(arg/5)%len(outcomes)]}
				bogus = r.Outcome == "bogus"
				if inflight != nil {
					r.Step, r.Attempt = inflight.Step, inflight.Attempt
				}
				switch arg % 5 {
				case 1:
					r.Attempt--
				case 2:
					r.Step = def.Steps[int(arg)%len(def.Steps)].Name
				case 3:
					r.SagaID = "other"
				}
				next, err = s.Apply(r, now)
			case 3, 4, 5: // the clock ran out, maybe
				next, err = s.Timeout(now)
			case 6:
				next, err = s.Abort("fuzz", now)
			default: // a second start is never valid
				if _, err := s.Start(now); !errors.Is(err, ErrInvalidState) {
					t.Fatalf("op %d: second start: %v", i, err)
				}
				continue
			}

			if err != nil {
				if s.Version != version {
					t.Fatalf("op %d: rejected transition (%v) changed the version %d -> %d", i, err, version, s.Version)
				}
				switch {
				case errors.Is(err, ErrStaleReply), errors.Is(err, ErrInvalidState), errors.Is(err, ErrTerminal), errors.Is(err, ErrNotDue):
				case bogus: // an unknown outcome is rejected with its own error
				default:
					t.Fatalf("op %d: unexpected error %v", i, err)
				}
				continue
			}
			if s.Version != version+1 {
				t.Fatalf("op %d: accepted transition moved the version %d -> %d", i, version, s.Version)
			}
			inflight = next
			check(t, s, inflight)
		}
	})
}

// check asserts the structural invariants of a saga against the command
// it last returned.
func check(t *testing.T, s *Saga, inflight *Command) {
	t.Helper()
	var running, compensating int
	for i, st := range s.Steps {
		def := s.Definition.Steps[i]
		switch st.Status {
		case StepRunning:
			running++
		case StepCompensating:
			compensating++
		}
		if st.Tries > def.MaxAttempts {
			t.Fatalf("step %s tries %d > max %d", st.Name, st.Tries, def.MaxAttempts)
		}
		if st.Tries > st.Attempt {
			t.Fatalf("step %s tries %d > attempt %d", st.Name, st.Tries, st.Attempt)
		}
		inFlight := st.Status == StepRunning || st.Status == StepCompensating
		if inFlight != !st.Deadline.IsZero() {
			t.Fatalf("step %s status %s deadline %v", st.Name, st.Status, st.Deadline)
		}
		if inFlight != (s.Current == i) {
			t.Fatalf("step %s is %s but Current is %d", st.Name, st.Status, s.Current)
		}
		if (st.Status == StepCompensating || st.Status == StepCompensated || st.Status == StepCompensationFailed) && def.Compensation == "" {
			t.Fatalf("step %s has no compensation but is %s", st.Name, st.Status)
		}
	}

	switch s.State {
	case StatePending:
		t.Fatal("a started saga is never pending")
	case StateRunning:
		if running != 1 || compensating != 0 {
			t.Fatalf("running saga has %d running and %d compensating steps", running, compensating)
		}
		// Steps after the one in flight have not started.
		for _, st := range s.Steps[s.Current+1:] {
			if st.Status != StepPending {
				t.Fatalf("step %s after the one in flight is %s", st.Name, st.Status)
			}
		}
		for _, st := range s.Steps[:s.Current] {
			if st.Status != StepSucceeded {
				t.Fatalf("step %s before the one in flight is %s", st.Name, st.Status)
			}
		}
	case StateCompensating:
		if running != 0 || compensating != 1 {
			t.Fatalf("compensating saga has %d running and %d compensating steps", running, compensating)
		}
		if s.Reason == "" {
			t.Fatal("compensating without a reason")
		}
		// Everything after the step being undone that may have had an
		// effect has been undone already.
		for i := s.Current + 1; i < len(s.Steps); i++ {
			st := s.Steps[i]
			if needsCompensation(st.Status) && s.Definition.Steps[i].Compensation != "" {
				t.Fatalf("step %s (%s) is still standing behind the compensation in flight", st.Name, st.Status)
			}
		}
	case StateCompleted:
		for _, st := range s.Steps {
			if st.Status != StepSucceeded {
				t.Fatalf("completed saga has step %s %s", st.Name, st.Status)
			}
		}
	case StateCompensated:
		for i, st := range s.Steps {
			if needsCompensation(st.Status) && s.Definition.Steps[i].Compensation != "" {
				t.Fatalf("compensated saga left step %s %s", st.Name, st.Status)
			}
			if st.Status == StepCompensationFailed {
				t.Fatalf("compensated saga has a failed compensation on %s", st.Name)
			}
		}
	case StateFailed:
		var failed int
		for _, st := range s.Steps {
			if st.Status == StepCompensationFailed {
				failed++
			}
		}
		if failed != 1 || s.Reason == "" {
			t.Fatalf("failed saga has %d failed compensations, reason %q", failed, s.Reason)
		}
	}

	if s.Terminal() {
		if inflight != nil || s.Current != -1 || running+compensating != 0 {
			t.Fatalf("terminal saga %s has something in flight: cmd %v current %d", s.State, inflight, s.Current)
		}
		if last := s.Timeline[len(s.Timeline)-1].Kind; last != EventCompleted && last != EventCompensated && last != EventFailed {
			t.Fatalf("terminal saga's last event is %s", last)
		}
		return
	}
	if inflight == nil {
		t.Fatalf("non-terminal saga %s returned no command", s.State)
	}
	cur := s.Steps[s.Current]
	if inflight.Step != cur.Name || inflight.Attempt != cur.Attempt || !inflight.Deadline.Equal(cur.Deadline) {
		t.Fatalf("command %+v does not match the step in flight %+v", *inflight, cur)
	}
	wantKind := KindAction
	if s.State == StateCompensating {
		wantKind = KindCompensate
	}
	if inflight.Kind != wantKind {
		t.Fatalf("command kind %s in state %s", inflight.Kind, s.State)
	}
}
