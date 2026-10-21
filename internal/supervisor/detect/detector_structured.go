package detect

// StructuredInfoDetector validates the freshness and range of statusline values.
type StructuredInfoDetector struct{}

func NewStructuredInfoDetector() *StructuredInfoDetector { return &StructuredInfoDetector{} }
func (*StructuredInfoDetector) Detect(in DetectInput) DetectResult {
	if in.Structured == nil || in.Structured.UsedPercentage < 0 || in.Structured.UsedPercentage > 100 {
		return DetectResult{}
	}
	if in.LastSendConfirmedAt != nil && in.Structured.ObservedAt.Before(*in.LastSendConfirmedAt) {
		return DetectResult{}
	}
	v := in.Structured.UsedPercentage
	return DetectResult{ContextPercent: &v, SessionID: in.Structured.SessionID, Basis: []DetectionBasis{BasisStructuredInfo}}
}
