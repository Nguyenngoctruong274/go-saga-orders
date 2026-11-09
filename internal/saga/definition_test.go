package saga

import (
	"strings"
	"testing"
	"time"
)

// placeOrder is the saga ordersd will run: reserve stock, take payment,
// confirm. Confirmation is local and cannot be undone, so it is the pivot.
var placeOrder = Definition{
	Name: "place-order",
	Steps: []StepDef{
		{Name: "reserve", Action: "inventory.reserve", Compensation: "inventory.release", Timeout: 5 * time.Second, MaxAttempts: 3},
		{Name: "pay", Action: "payment.authorize", Compensation: "payment.void", Timeout: 10 * time.Second, MaxAttempts: 2},
		{Name: "confirm", Action: "order.confirm", Timeout: time.Second, MaxAttempts: 5},
	},
}

func TestDefinitionValidate(t *testing.T) {
	ok := func(mutate func(*Definition)) Definition {
		d := Definition{Name: placeOrder.Name, Steps: append([]StepDef(nil), placeOrder.Steps...)}
		mutate(&d)
		return d
	}
	tests := []struct {
		name string
		def  Definition
		want []string // substrings; empty means valid
	}{
		{"place-order is valid", placeOrder, nil},
		{"missing name", ok(func(d *Definition) { d.Name = "" }), []string{"name is required"}},
		{"no steps", ok(func(d *Definition) { d.Steps = nil }), []string{"at least one step"}},
		{"step without name", ok(func(d *Definition) { d.Steps[1].Name = "" }), []string{"step 1: name is required"}},
		{"duplicate step", ok(func(d *Definition) { d.Steps[2].Name = "reserve" }), []string{`step "reserve": duplicate name`}},
		{"step without action", ok(func(d *Definition) { d.Steps[0].Action = "" }), []string{`step "reserve": action is required`}},
		{"zero timeout", ok(func(d *Definition) { d.Steps[1].Timeout = 0 }), []string{`step "pay": timeout must be positive`}},
		{"zero attempts", ok(func(d *Definition) { d.Steps[2].MaxAttempts = 0 }), []string{`step "confirm": max attempts must be at least 1`}},
		{"every defect at once", Definition{Steps: []StepDef{{}}}, []string{"name is required", "step 0: name is required", "action is required", "timeout must be positive", "max attempts"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.def.Validate()
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q lacks %q", err, w)
				}
			}
		})
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	if _, err := New(Definition{}, "s1", "o1", time.Now()); err == nil {
		t.Fatal("invalid definition accepted")
	}
	if _, err := New(placeOrder, "", "o1", time.Now()); err == nil {
		t.Fatal("empty id accepted")
	}
}
