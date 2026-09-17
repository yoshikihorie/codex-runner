package recovery

import (
	"fmt"
	"testing"
)

func validateRequiredRecoveryLogFields(record map[string]any) error {
	for _, key := range []string{"task_id", "error"} {
		value, ok := record[key].(string)
		if !ok || value == "" {
			return fmt.Errorf("log field %q must be a non-empty string", key)
		}
	}
	return nil
}

func TestValidateRequiredRecoveryLogFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record map[string]any
		want   bool
	}{
		{name: "valid", record: map[string]any{"task_id": "task", "error": "failure"}, want: true},
		{name: "missing task id", record: map[string]any{"error": "failure"}},
		{name: "nil task id", record: map[string]any{"task_id": nil, "error": "failure"}},
		{name: "non string task id", record: map[string]any{"task_id": 1, "error": "failure"}},
		{name: "empty task id", record: map[string]any{"task_id": "", "error": "failure"}},
		{name: "missing error", record: map[string]any{"task_id": "task"}},
		{name: "nil error", record: map[string]any{"task_id": "task", "error": nil}},
		{name: "non string error", record: map[string]any{"task_id": "task", "error": 1}},
		{name: "empty error", record: map[string]any{"task_id": "task", "error": ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validateRequiredRecoveryLogFields(tc.record) == nil; got != tc.want {
				t.Fatalf("validateRequiredRecoveryLogFields(%#v) success=%v, want %v", tc.record, got, tc.want)
			}
		})
	}
}
