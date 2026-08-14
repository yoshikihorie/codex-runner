package domain

import "testing"

func TestTaskStateIsTerminal(t *testing.T) {
	tests := []struct {
		state    TaskState
		terminal bool
	}{
		{StateCompleted, true},
		{StateFailed, true},
		{StateRecovered, true},
		{StateTimeoutLost, true},
		{StateKilled, true},
		{StateLost, true},
		{StateQueued, false},
		{StateStarting, false},
		{StateRunning, false},
		{StateStalled, false},
		{StateTimeout, false},
		{StateRecovering, false},
		{StateCancelling, false},
		{StateAdopted, false},
		{StateOrphaned, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			if got := tt.state.IsTerminal(); got != tt.terminal {
				t.Fatalf("IsTerminal() = %t, want %t", got, tt.terminal)
			}
		})
	}
}

func TestTaskStateIsTerminalMatchesTerminalForAllStates(t *testing.T) {
	states := []TaskState{
		StateQueued, StateStarting, StateRunning, StateStalled, StateCompleted,
		StateFailed, StateTimeout, StateRecovering, StateRecovered, StateTimeoutLost,
		StateCancelling, StateKilled, StateAdopted, StateOrphaned, StateLost,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			if got, want := state.IsTerminal(), state.terminal(); got != want {
				t.Fatalf("IsTerminal() = %t, terminal() = %t", got, want)
			}
		})
	}
}
