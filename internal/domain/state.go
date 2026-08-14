package domain

func (s TaskState) terminal() bool {
	return s == StateCompleted || s == StateFailed || s == StateRecovered || s == StateTimeoutLost || s == StateKilled || s == StateLost
}

// IsTerminal reports whether the task state is terminal.
func (s TaskState) IsTerminal() bool {
	return s.terminal()
}
