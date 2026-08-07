package domain

func (s TaskState) terminal() bool {
	return s == StateCompleted || s == StateFailed || s == StateRecovered || s == StateTimeoutLost || s == StateKilled || s == StateLost
}
