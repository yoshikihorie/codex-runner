package detect

import "time"

// QuietDetector measures the shared configured quiet period.
type QuietDetector struct{ Period time.Duration }

func NewQuietDetector() *QuietDetector { return &QuietDetector{Period: QuietPeriod} }
func (d *QuietDetector) IsQuiet(in DetectInput) bool {
	period := d.Period
	if period <= 0 {
		period = QuietPeriod
	}
	return !in.LastScreenChangeAt.IsZero() && !in.Now.IsZero() && !in.Now.Before(in.LastScreenChangeAt) && in.Now.Sub(in.LastScreenChangeAt) >= period
}
func (d *QuietDetector) Detect(in DetectInput) DetectResult {
	if d.IsQuiet(in) {
		return DetectResult{Basis: []DetectionBasis{BasisOutputQuiet}}
	}
	return DetectResult{}
}
