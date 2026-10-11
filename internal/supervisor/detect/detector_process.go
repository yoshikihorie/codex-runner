package detect

// ProcessExitDetector recognizes an exited child process.
type ProcessExitDetector struct{}

func NewProcessExitDetector() *ProcessExitDetector { return &ProcessExitDetector{} }
func (*ProcessExitDetector) Detect(in DetectInput) DetectResult {
	if in.ProcessExited {
		return DetectResult{State: StateExited, Basis: []DetectionBasis{BasisProcessExit}}
	}
	return DetectResult{}
}
