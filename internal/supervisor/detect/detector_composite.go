package detect

import "context"
import "time"
import "github.com/yoshikihorie/codex-runner/internal/supervisor"

// CompositeDetector applies the canonical rules in priority order.
type CompositeDetector struct {
	Patterns   DetectorPatternSet
	Screen     *ScreenPatternDetector
	Quiet      *QuietDetector
	Process    *ProcessExitDetector
	Structured *StructuredInfoDetector
}

func NewCompositeDetector(p DetectorPatternSet) *CompositeDetector {
	return &CompositeDetector{Patterns: p, Screen: NewScreenPatternDetector(p), Quiet: NewQuietDetector(), Process: NewProcessExitDetector(), Structured: NewStructuredInfoDetector()}
}
func (d *CompositeDetector) Detect(_ context.Context, in DetectInput) (DetectResult, error) {
	p := d.Patterns
	if in.CLI != "" {
		if selected, ok := in.PatternsByCLI[in.CLI]; ok {
			p = selected
		}
	}
	if in.Patterns != nil {
		p = *in.Patterns
	}
	if p.IdleTop == "" && p.IdleBottom == "" && p.Prompt == "" && len(p.Working) == 0 && len(p.Permission) == 0 && len(p.Recommendation) == 0 && len(p.UnsafeFooter) == 0 {
		p = DefaultPatternSet()
	}
	if d.Screen == nil {
		d.Screen = NewScreenPatternDetector(p)
	}
	d.Screen.Patterns = p
	if d.Quiet == nil {
		d.Quiet = NewQuietDetector()
	}
	if d.Process == nil {
		d.Process = NewProcessExitDetector()
	}
	if d.Structured == nil {
		d.Structured = NewStructuredInfoDetector()
	}
	screen := d.Screen.Detect(in)
	quiet := d.Quiet.IsQuiet(in)
	working := containsAny(in.ScreenRows, p.Working) || p.ObservedWorking && hasObservedWorking(in.ScreenRows)
	permission := false
	for _, group := range p.Permission {
		if orderedLinePatternFound(in.ScreenRows, group) {
			permission = true
			break
		}
	}
	idle := hasIdle(in.ScreenRows, p)
	recommendation := containsAny(in.ScreenRows, p.Recommendation)
	if p.ObservedRecommendation && idle && quiet {
		_, _, observed := supervisor.ObservedRecommendationBounds(in.ScreenRows)
		recommendation = recommendation || observed
	}
	result := DetectResult{State: StateUndetermined, PermissionMode: screen.PermissionMode}
	switch {
	case in.ProcessExited:
		result.State = StateExited
		result.Basis = []DetectionBasis{BasisProcessExit}
	case working:
		result.State = StateWorking
		result.Basis = []DetectionBasis{BasisScreenPattern}
	case permission && recommendation:
		result.State = StateUndetermined
		result.Basis = []DetectionBasis{BasisScreenPattern}
	case permission:
		result.State = StateAwaitingPermission
		result.Basis = []DetectionBasis{BasisScreenPattern}
	case recommendation && quiet:
		result.State = StateAwaitingRecommendation
		result.Basis = []DetectionBasis{BasisScreenPattern, BasisOutputQuiet}
	case idle && quiet:
		result.State = StateIdle
		result.Basis = []DetectionBasis{BasisScreenPattern, BasisOutputQuiet}
	case recentlyChanged(in, d.Quiet.Period):
		result.State = StateWorking
		result.Basis = []DetectionBasis{BasisOutputQuiet}
	}
	structured := d.Structured.Detect(in)
	result.ContextPercent = structured.ContextPercent
	result.SessionID = structured.SessionID
	if structured.ContextPercent != nil {
		result.Basis = addBasis(result.Basis, BasisStructuredInfo)
	}
	return result, nil
}
func recentlyChanged(in DetectInput, period time.Duration) bool {
	return !in.LastScreenChangeAt.IsZero() && !in.Now.IsZero() && !in.Now.Before(in.LastScreenChangeAt) && in.Now.Sub(in.LastScreenChangeAt) < period
}
