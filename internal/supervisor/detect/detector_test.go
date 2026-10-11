package detect

import (
	"context"
	supervisor "github.com/yoshikihorie/codex-runner/internal/supervisor"
	"os"
	"strings"
	"testing"
	"time"
)

func testPatterns() DetectorPatternSet {
	p := DefaultPatternSet()
	p.Working = [][]string{{"work"}}
	p.Permission = [][]LinePattern{{{Text: "permission", Mode: LinePrefix}}}
	p.Recommendation = [][]string{{"recommendation"}}
	return p
}
func detect(t *testing.T, d *CompositeDetector, in DetectInput) DetectResult {
	t.Helper()
	r, e := d.Detect(context.Background(), in)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestCompositeRulesAndWinningBasis(t *testing.T) {
	now := time.Now()
	p := testPatterns()
	prompt := []string{"────", "❯\u00a0", "────"}
	tests := []struct {
		name  string
		in    DetectInput
		want  DetectedState
		basis []DetectionBasis
	}{
		{"rule1 exit", DetectInput{ProcessExited: true, ScreenRows: []string{"work"}}, StateExited, []DetectionBasis{BasisProcessExit}},
		{"rule2 pattern beats permission", DetectInput{ScreenRows: []string{"work", "permission"}}, StateWorking, []DetectionBasis{BasisScreenPattern}},
		{"rule3 conflict", DetectInput{ScreenRows: []string{"permission", "recommendation"}}, StateUndetermined, []DetectionBasis{BasisScreenPattern}},
		{"rule4 permission", DetectInput{ScreenRows: []string{"permission"}}, StateAwaitingPermission, []DetectionBasis{BasisScreenPattern}},
		{"rule5 quiet recommendation", DetectInput{ScreenRows: []string{"recommendation"}, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)}, StateAwaitingRecommendation, []DetectionBasis{BasisScreenPattern, BasisOutputQuiet}},
		{"rule5 moving recommendation", DetectInput{ScreenRows: []string{"recommendation"}, Now: now, LastScreenChangeAt: now.Add(-time.Second)}, StateWorking, []DetectionBasis{BasisOutputQuiet}},
		{"rule6 prompt nonempty and quiet", DetectInput{ScreenRows: []string{"────", "❯\u00a0x", "────"}, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)}, StateIdle, []DetectionBasis{BasisScreenPattern, BasisOutputQuiet}},
		{"rule7 moving", DetectInput{Now: now, LastScreenChangeAt: now.Add(-time.Second)}, StateWorking, []DetectionBasis{BasisOutputQuiet}},
		{"rule8 unknown", DetectInput{}, StateUndetermined, nil},
		{"quiet idle", DetectInput{ScreenRows: prompt, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)}, StateIdle, []DetectionBasis{BasisScreenPattern, BasisOutputQuiet}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.Patterns = &p
			got := detect(t, NewCompositeDetector(p), tt.in)
			if got.State != tt.want {
				t.Fatalf("state=%q want %q", got.State, tt.want)
			}
			if strings.Join(basisStrings(got.Basis), ",") != strings.Join(basisStrings(tt.basis), ",") {
				t.Fatalf("basis=%v want %v", got.Basis, tt.basis)
			}
		})
	}
	d := &CompositeDetector{}
	_ = detect(t, d, DetectInput{})
	d.Patterns.Working = [][]string{{"changed"}}
	got := detect(t, d, DetectInput{ScreenRows: []string{"changed"}})
	if got.State != StateWorking {
		t.Fatal("zero-value detector did not use updated patterns")
	}
}
func basisStrings(b []DetectionBasis) []string {
	r := make([]string, len(b))
	for i, v := range b {
		r[i] = string(v)
	}
	return r
}
func TestPermissionDialogExactOrderedAndFooter(t *testing.T) {
	p := DefaultPatternSet()
	d := NewCompositeDetector(p)
	cases := []struct {
		name  string
		rows  []string
		state DetectedState
		mode  PermissionMode
	}{
		{"dialog", []string{"Quick safety check: Is this a project you created or one you trust?", "Enter to confirm · Esc to cancel"}, StateAwaitingPermission, PermissionDefaultOrUnknown},
		{"question embedded", []string{"conversation Quick safety check: Is this a project you created or one you trust?", "Enter to confirm · Esc to cancel"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"confirmation partial", []string{"Quick safety check: Is this a project you created or one you trust?", "x Enter to confirm · Esc to cancel y"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"reverse", []string{"Enter to confirm · Esc to cancel", "Quick safety check: Is this a project you created or one you trust?"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"question only", []string{"Quick safety check: Is this a project you created or one you trust?"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"confirmation only", []string{"Enter to confirm · Esc to cancel"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"unsafe footer", []string{"────", "❯\u00a0", "────", "⏵⏵ auto mode on"}, StateUndetermined, PermissionUnsafe},
		{"unsafe body", []string{"auto mode on", "────", "❯\u00a0", "────"}, StateUndetermined, PermissionDefaultOrUnknown},
		{"accept edits", []string{"────", "❯\u00a0", "────", "accept edits on"}, StateUndetermined, PermissionDefaultOrUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := DetectInput{ScreenRows: tc.rows}
			got := detect(t, d, in)
			if got.State != tc.state || got.PermissionMode != tc.mode {
				t.Fatalf("got (%s,%s)", got.State, got.PermissionMode)
			}
		})
	}
}
func TestInputBlankFindsCellRow(t *testing.T) {
	p := DefaultPatternSet()
	rows := make([]string, 40)
	cells := make([][]supervisor.Cell, 40)
	for i := range rows {
		rows[i] = strings.Repeat(" ", 120)
		cells[i] = make([]supervisor.Cell, 120)
	}
	rows[33] = strings.Repeat("─", 120)
	rows[34] = "❯\u00a0Try "
	rows[35] = strings.Repeat("─", 120)
	for i := 2; i < len([]rune(rows[34])); i++ {
		cells[34][i] = supervisor.Cell{Text: string([]rune(rows[34])[i]), Faint: true}
	}
	if !IsInputBlank(p, rows, cells) {
		t.Fatal("expected located dim prompt to be blank")
	}
	rows[34] = "❯\u00a0x"
	cells[34][2] = supervisor.Cell{Text: "x"}
	if IsInputBlank(p, rows, cells) {
		t.Fatal("visible cell is not blank")
	}
	cells[34][2] = supervisor.Cell{Text: "x", Faint: true}
	if !IsInputBlank(p, rows, cells) {
		t.Fatal("dim text should be blank")
	}
	if IsInputBlank(p, rows, nil) {
		t.Fatal("unknown attributes should fail closed")
	}
	rows[33] = "old ❯\u00a0"
	if IsInputBlank(p, rows, cells) {
		t.Fatal("prompt without enclosing borders must fail")
	}
}
func TestStructuredInfoFreshnessRangeAndBasis(t *testing.T) {
	at := time.Now()
	for _, tc := range []struct {
		name     string
		v        int
		observed time.Time
		ok       bool
	}{{"valid", 42, at, true}, {"negative", -1, at, false}, {"over", 250, at, false}, {"stale", 42, at.Add(-time.Nanosecond), false}} {
		t.Run(tc.name, func(t *testing.T) {
			info := StructuredInfo{ObservedAt: tc.observed, UsedPercentage: tc.v}
			r := NewStructuredInfoDetector().Detect(DetectInput{Structured: &info, LastSendConfirmedAt: &at})
			if (r.ContextPercent != nil) != tc.ok {
				t.Fatalf("valid=%v want %v", r.ContextPercent != nil, tc.ok)
			}
		})
	}
	_ = supervisor.DetectionScreenPattern
}
func TestReconstructedStartupFixtures(t *testing.T) {
	for _, tc := range []struct {
		name string
		want DetectedState
	}{{"claude-default-startup.txt", StateIdle}, {"claude-trust-startup.txt", StateAwaitingPermission}} {
		t.Run(tc.name, func(t *testing.T) {
			data, e := os.ReadFile("../testdata/" + tc.name)
			if e != nil {
				t.Fatal(e)
			}
			vt := supervisor.NewVirtualTerminal(120, 40)
			if _, e = vt.Write(data); e != nil {
				t.Fatal(e)
			}
			rows := vt.Screen()
			dim := make([][]bool, len(rows))
			for i := range rows {
				cells := vt.Cells(i)
				dim[i] = make([]bool, len(cells))
				for j, c := range cells {
					dim[i][j] = c.Faint
				}
			}
			now := time.Now()
			got := detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: rows, ScreenDim: dim, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
			if got.State != tc.want {
				t.Fatalf("state=%q want=%q rows=%q", got.State, tc.want, strings.Join(rows, "\n"))
			}
		})
	}
}

func TestCLIPatternSelectionAndOROfANDGroups(t *testing.T) {
	base := DefaultPatternSet()
	claude := testPatterns()
	codex := DefaultPatternSet()
	codex.Working = [][]string{{"codex", "busy"}, {"processing"}}
	d := NewCompositeDetector(base)
	in := DetectInput{CLI: supervisor.CLICodex, PatternsByCLI: map[supervisor.SupervisorCli]DetectorPatternSet{supervisor.CLIClaude: claude, supervisor.CLICodex: codex}, ScreenRows: []string{"codex busy"}}
	if got := detect(t, d, in); got.State != StateUndetermined {
		t.Fatalf("partial AND group state=%q", got.State)
	}
	in.ScreenRows = []string{"codex", "busy"}
	if got := detect(t, d, in); got.State != StateWorking {
		t.Fatalf("full AND group state=%q", got.State)
	}
	in.ScreenRows = []string{"processing"}
	if got := detect(t, d, in); got.State != StateWorking {
		t.Fatalf("alternate OR group state=%q", got.State)
	}
}
