package execution

// TaskLifecycleStarter owns asynchronous lifecycle execution startup.
type TaskLifecycleStarter interface {
	Start(TaskLaunchPayload)
}
