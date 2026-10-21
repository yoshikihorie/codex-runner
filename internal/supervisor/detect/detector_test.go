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

func TestDefaultPatternsObservedScreens(t *testing.T) {
	now := time.Now()
	border := strings.Repeat("─", 24)
	idle := []string{"❯ please answer", "⏺ The answer is two.", "✻ Cooked for 2s · done 10:00", border, "❯\u00a0Try a prompt", border, "  ⏸ manual mode on"}
	quiet := func(rows []string) DetectResult {
		return detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: rows, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
	}
	for _, tc := range []struct {
		name string
		rows []string
		want DetectedState
	}{
		{"recommendation", idle, StateAwaitingRecommendation},
		{"working", append([]string{"✳ Effecting…"}, idle[3:]...), StateWorking},
		{"startup idle", []string{border, "❯\u00a0", border, "  ⏸ manual mode on"}, StateIdle},
		{"missing completion", []string{"❯ please answer", "⏺ The answer is two.", border, "❯\u00a0", border}, StateIdle},
		{"last echo has no completion", append([]string{"❯ older", "⏺ older response", "✻ Cooked for 2s · done 10:00", "❯ newer", "⏺ newer response"}, border, "❯\u00a0", border), StateIdle},
		{"tool permission", []string{" Bash command", " Do you want to proceed?", " ❯ 1. Yes", "   2. Yes, and switch to auto mode", "   3. No"}, StateAwaitingPermission},
		{"permission and recommendation", append(append([]string{}, idle...), " Do you want to proceed?", " ❯ 1. Yes"), StateUndetermined},
		{"teach auto mode", []string{"  Teach auto mode about your environment?", "  ❯ 1. Yes", "    2. Not now", "  Enter to confirm · Esc to cancel"}, StateUndetermined},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := quiet(tc.rows)
			if got.State != tc.want {
				t.Fatalf("state=%s want %s", got.State, tc.want)
			}
			if tc.name == "recommendation" && got.PermissionMode != PermissionDefaultOrUnknown {
				t.Fatalf("manual footer mode=%s", got.PermissionMode)
			}
		})
	}
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
			if got.ProcessingIndicator != (tt.name == "rule2 pattern beats permission") {
				t.Fatalf("processing indicator=%v", got.ProcessingIndicator)
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
			if got.ProcessingIndicator {
				t.Fatal("startup fixture must not indicate processing")
			}
			if tc.want != StateIdle {
				return
			}
			// The fixture is a sanitized capture. Add only synthetic status/body rows.
			input := -1
			for i, row := range rows {
				if strings.HasPrefix(row, DefaultPatternSet().Prompt) {
					input = i
				}
			}
			if input < 4 {
				t.Fatal("fixture has no room above its input")
			}
			cells := make([][]supervisor.Cell, len(rows))
			for i := range rows {
				cells[i] = vt.Cells(i)
			}
			if !IsInputBlank(DefaultPatternSet(), rows, cells) {
				t.Fatal("fixture's dim placeholder must be blank")
			}
			for _, variant := range []struct {
				name    string
				above   []string
				working bool
			}{
				{"working adjacent", []string{"✳ Reviewing…"}, true},
				{"working with blank", []string{"✳ Reviewing…", ""}, true},
				{"history", []string{"✳ Reviewing…", "⏺ Synthetic answer."}, false},
				{"completed", []string{"✳ Reviewing…", "✻ Cooked for 2s · done 10:00"}, false},
			} {
				t.Run(variant.name, func(t *testing.T) {
					screen := append([]string{}, rows...)
					copy(screen[input-1-len(variant.above):input-1], variant.above)
					got := detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: screen, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
					want := StateIdle
					if variant.working {
						want = StateWorking
					}
					if got.ProcessingIndicator != variant.working || got.State != want {
						t.Fatalf("processing=%v state=%s want (%v,%s)", got.ProcessingIndicator, got.State, variant.working, want)
					}
				})
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

func TestInputBlankUsesLowestBorderedPrompt(t *testing.T) {
	for _, tc := range []struct {
		name         string
		upper, lower string
		want         bool
	}{
		{"lower retains text", "", "pending", false},
		{"lower is blank", "old text", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []string{"────", "❯\u00a0" + tc.upper, "────", "history", "────", "❯\u00a0" + tc.lower, "────"}
			cells := make([][]supervisor.Cell, len(rows))
			for i, row := range rows {
				for _, r := range row {
					cells[i] = append(cells[i], supervisor.Cell{Text: string(r)})
				}
			}
			if got := IsInputBlank(DefaultPatternSet(), rows, cells); got != tc.want {
				t.Fatalf("IsInputBlank=%v want %v", got, tc.want)
			}
		})
	}
}

func TestObservedWorkingOnlyAboveLowestInput(t *testing.T) {
	prompt := []string{"────", "❯\u00a0", "────"}
	for _, tc := range []struct {
		name string
		rows []string
		want bool
	}{
		{"history", append([]string{"✳ Reviewing…", "⏺ Finished response."}, prompt...), false},
		{"completed", append([]string{"✳ Reviewing…", "✻ Cooked for 2s · done 10:00"}, prompt...), false},
		{"adjacent", append([]string{"✳ Reviewing…"}, prompt...), true},
		{"blank separator", append([]string{"✳ Reviewing…", "", "   "}, prompt...), true},
		{"body after blank", append([]string{"✳ Reviewing…", "", "⏺ Finished response.", ""}, prompt...), false},
		{"above old input", append(append([]string{"✳ Reviewing…"}, prompt...), append([]string{"response"}, prompt...)...), false},
		{"footer", append(append([]string{}, prompt...), "✳ Reviewing…"), false},
		{"missing input", []string{"✳ Reviewing…"}, false},
		{"missing borders", []string{"✳ Reviewing…", "❯\u00a0"}, false},
		{"completion", append([]string{"✻ Cooked for 2s · done 10:00"}, prompt...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			got := detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: tc.rows, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
			if got.ProcessingIndicator != tc.want {
				t.Fatalf("ProcessingIndicator=%v want %v (state=%s)", got.ProcessingIndicator, tc.want, got.State)
			}
		})
	}
}

// SCN-sup-01-67/68: use synthetic rows only.
func TestObservedWorkingSkipsAuxiliaryRows(t *testing.T) {
	prompt := []string{"────", "❯\u00a0", "────"}
	for _, tc := range []struct {
		name  string
		above []string
		want  bool
	}{
		{"task", []string{"✳ Effecting…", "⎿ ☐ Synthetic task"}, true},
		{"tip", []string{"✳ Effecting…", "  ⎿ Tip: Synthetic tip"}, true},
		{"continuations", []string{"✳ Effecting…", "⎿ Tip: Synthetic tip", "  wrapped text", "\tmore text"}, true},
		{"unicode indentation", []string{"✳ Effecting…", "\u00a0⎿ Tip: Synthetic tip", "\u00a0wrapped text"}, true},
		{"mixed blanks", []string{"✳ Effecting…", "", "⎿ ☐ First", "  continuation", "", "   ", "  ⎿ ☐ Second", ""}, true},
		{"history", []string{"- Reviewing…", "Synthetic response", "⎿ Tip: Synthetic tip", "  continuation", ""}, false},
		{"completion", []string{"✳ Effecting…", "✻ Cooked for 2s · done 10:00", "⎿ ☐ Synthetic task"}, false},
		{"indented body", []string{"✳ Effecting…", "  Synthetic body", "⎿ Tip: Synthetic tip"}, false},
		{"unindented breaks continuation", []string{"✳ Effecting…", "⎿ Tip: Synthetic tip", "Synthetic body", "  continuation"}, false},
		{"empty row breaks continuation", []string{"✳ Effecting…", "⎿ Tip: Synthetic tip", "", "  Synthetic body"}, false},
		{"embedded marker", []string{"✳ Effecting…", "Synthetic body ⎿ Tip: text"}, false},
		{"only auxiliary", []string{"⎿ Tip: Synthetic tip", "  continuation"}, false},
		{"english word only", []string{"✳ Sautéing…", "⎿ Tip: Synthetic tip"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(append([]string{}, tc.above...), prompt...)
			now := time.Now()
			got := detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: rows, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
			wantState := StateIdle
			if tc.want {
				wantState = StateWorking
			}
			if got.ProcessingIndicator != tc.want || got.State != wantState {
				t.Fatalf("processing=%v state=%s want (%v,%s)", got.ProcessingIndicator, got.State, tc.want, wantState)
			}
		})
	}
}

func TestLowestInputWorkingAndUnsafeFooter(t *testing.T) {
	prompt := []string{"────", "❯\u00a0", "────"}
	for _, tc := range []struct {
		name    string
		rows    []string
		working bool
		mode    PermissionMode
	}{
		{"old unsafe footer", append(append([]string{}, prompt...), append([]string{"auto mode on", "✳ Effecting…", "⎿ ☐ Synthetic task"}, prompt...)...), true, PermissionDefaultOrUnknown},
		{"active unsafe footer", append(append(append([]string{}, prompt...), prompt...), "bypass permissions on"), false, PermissionUnsafe},
		{"above old input", append(append([]string{"✳ Effecting…", "⎿ ☐ Synthetic task"}, prompt...), append([]string{"Synthetic response"}, prompt...)...), false, PermissionDefaultOrUnknown},
		{"missing input", []string{"✳ Effecting…", "⎿ Tip: Synthetic tip", "  continuation", "auto mode on"}, false, PermissionDefaultOrUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			got := detect(t, NewCompositeDetector(DefaultPatternSet()), DetectInput{ScreenRows: tc.rows, Now: now, LastScreenChangeAt: now.Add(-QuietPeriod)})
			if got.ProcessingIndicator != tc.working || got.PermissionMode != tc.mode {
				t.Fatalf("processing=%v mode=%s want (%v,%s)", got.ProcessingIndicator, got.PermissionMode, tc.working, tc.mode)
			}
		})
	}
}
