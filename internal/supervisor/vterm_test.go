package supervisor

import (
	_ "embed"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

//go:embed testdata/claude-default-startup.txt
var claudeDefaultStartup string

//go:embed testdata/claude-trust-startup.txt
var claudeTrustScreen string

func TestVirtualTerminalControlsAndUnicode(t *testing.T) {
	v := NewVirtualTerminal(8, 4)
	v.Write([]byte("abc\rZ\tX\bY\x1b[2;3H界🙂\x1b[2;5H!"))
	rows := v.Screen()
	if rows[0] != "Zbc   YX" {
		t.Fatalf("CR/TAB/BS: %#v", rows)
	}
	if v.CursorPos() != (CursorPosition{Row: 1, Col: 5}) {
		t.Fatalf("cursor=%+v", v.CursorPos())
	}
	v.Write([]byte("\x1b[3;1H界🙂"))
	if v.Screen()[2] != "界 🙂 " || v.CursorPos() != (CursorPosition{Row: 2, Col: 4}) {
		t.Fatalf("wide characters did not occupy two cells: %#v %+v", v.Screen(), v.CursorPos())
	}
	v.Write([]byte("\x1b[3;1H👨‍👩‍👧‍👦"))
	if v.CursorPos() != (CursorPosition{Row: 2, Col: 2}) {
		t.Fatalf("emoji grapheme width: %+v", v.CursorPos())
	}
}

func TestVirtualTerminalEscapeCarryEraseAndAlternateRestore(t *testing.T) {
	v := NewVirtualTerminal(10, 3)
	v.Write([]byte("main\x1b[?104"))
	v.Write([]byte("9h\x1b[2;2Halt\x1b[?1049l"))
	if !strings.HasPrefix(v.Screen()[0], "main") {
		t.Fatalf("primary not restored: %#v", v.Screen())
	}
	v.Write([]byte("\x1b[2;2H\x1b[K"))
	if v.Screen()[1] != "" {
		t.Fatalf("EL failed: %#v", v.Screen())
	}
}

func TestVirtualTerminalScrollRegionAndLineOperations(t *testing.T) {
	v := NewVirtualTerminal(6, 3)
	v.Write([]byte("one\r\ntwo\r\ntri\x1b[1;3r\x1b[3;1H\n"))
	if !strings.Contains(strings.Join(v.Screen(), "|"), "two") {
		t.Fatalf("scroll region lost expected row: %#v", v.Screen())
	}
	v.Write([]byte("\x1b[2;1H\x1b[LX\x1b[M\x1b[@Y\x1b[P\x1b[X"))
}

func TestVirtualTerminalCursorEraseInsertDeleteAndIgnoredSequences(t *testing.T) {
	v := NewVirtualTerminal(8, 4)
	v.Write([]byte("abcdefgh\x1b[1;1H\x1b[2CXY\x1b[1;2H\x1b[P\x1b[2@Z"))
	if got := v.Screen()[0]; got != "aZ XYefg" {
		t.Fatalf("ICH/DCH result=%q", got)
	}
	v.Write([]byte("\x1b[2;1H123456\x1b[2;3H\x1b[1K"))
	if got := v.Screen()[1]; got != "   456" {
		t.Fatalf("EL mode 1 result=%q", got)
	}
	v.Write([]byte("\x1b[2;3H\x1b[0K"))
	if got := v.Screen()[1]; got != "" {
		t.Fatalf("EL mode 0 result=%q", got)
	}
	v.Write([]byte("\x1b[1;1H\x1b[0J"))
	if got := strings.Join(v.Screen(), ""); got != "" {
		t.Fatalf("ED mode 0 result=%q", got)
	}
	v.Write([]byte("keep\x1b[?25l\x1b[6n\x1b[31m\x1b]0;title\a\x1bPignored\x1b\\safe"))
	if !strings.HasPrefix(v.Screen()[0], "keepsafe") {
		t.Fatalf("ignored sequences damaged screen: %#v", v.Screen())
	}
}

func TestVirtualTerminalScrollAndSavedCursor(t *testing.T) {
	v := NewVirtualTerminal(6, 3)
	v.Write([]byte("one\r\ntwo\r\ntri\x1b[1;2r\x1b[2;1H\x1b7\x1b[1S"))
	v.Write([]byte("X\x1b8"))
	if v.CursorPos() != (CursorPosition{Row: 1, Col: 0}) {
		t.Fatalf("saved cursor=%+v", v.CursorPos())
	}
	v.Write([]byte("\x1b[1;3r\x1b[1T\x1b[2T"))
}

func TestVirtualTerminalReconstructsCapturedClaudeScreens(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		cols, rows int
		want       []string
	}{
		{"startup", claudeDefaultStartup, 120, 40, []string{"❯\u00a0", "──────────"}},
	} {
		v := NewVirtualTerminal(tc.cols, tc.rows)
		v.Write([]byte(tc.data))
		screen := strings.Join(v.Screen(), "\n")
		for _, want := range tc.want {
			if !strings.Contains(screen, want) {
				t.Errorf("%s screen missing %q:\n%s", tc.name, want, screen)
			}
		}
		rows := v.Screen()
		inputRow := -1
		for i, row := range rows {
			if strings.HasPrefix(row, "❯\u00a0") {
				inputRow = i
				break
			}
		}
		isRule := func(row string) bool {
			runes := []rune(row)
			if len(runes) != 120 {
				return false
			}
			for _, ch := range runes {
				if ch != '─' {
					return false
				}
			}
			return true
		}
		if inputRow <= 0 || inputRow+1 >= len(rows) || !isRule(rows[inputRow-1]) || !isRule(rows[inputRow+1]) {
			t.Fatalf("input row is not between adjacent rules: row=%d screen=%q", inputRow, rows)
		}
		inputCells := v.Cells(inputRow)
		promptCol := 0
		if promptCol < 0 || len(inputCells) <= promptCol+2 || inputCells[promptCol].Text != "❯" || inputCells[promptCol].Faint || inputCells[promptCol+1].Text != " " || inputCells[promptCol+1].Faint {
			t.Fatalf("prompt cells should not be faint: row=%q col=%d cells=%+v", rows[inputRow], promptCol, inputCells[max(0, promptCol-1):min(len(inputCells), promptCol+3)])
		}
		for col := promptCol + 2; col < len(inputCells) && inputCells[col].Text != ""; col++ {
			if !inputCells[col].Faint {
				t.Fatalf("startup example cell at column %d is not faint: %+v", col, inputCells[col])
			}
		}
	}
	for _, tc := range []struct{ marker, want string }{
		{"\x1b[2GQuick", " Quick safety check: Is this a project you created or one you trust? (Like your own code, a well-known open source"},
		{"\x1b[2GEnter", " Enter to confirm · Esc to cancel"},
	} {
		start := strings.Index(claudeTrustScreen, tc.marker)
		if start < 0 {
			t.Fatalf("raw trust capture lacks marker %q", tc.marker)
		}
		end := strings.IndexAny(claudeTrustScreen[start:], "\r\n")
		if end < 0 {
			t.Fatalf("raw trust row %q has no line terminator", tc.marker)
		}
		trust := NewVirtualTerminal(120, 40)
		trust.Write([]byte(claudeTrustScreen[start : start+end]))
		if got := strings.TrimRight(trust.Screen()[0], " "); got != tc.want {
			t.Errorf("raw trust row mismatch: got %q want %q", got, tc.want)
		}
	}
}

func TestVirtualTerminalClampsMalformedAndHugeCSI(t *testing.T) {
	for _, seq := range []string{"\x1b[-3C!", "\x1b[-2@!", "\x1b[-2P!", "\x1b[-9A!", "\x1b[-9B!", "\x1b[20000000S", "\x1b[20000000L"} {
		v := NewVirtualTerminal(5, 3)
		v.Write([]byte(seq))
		pos := v.CursorPos()
		if pos.Row < 0 || pos.Row >= 3 || pos.Col < 0 || pos.Col >= 5 {
			t.Fatalf("%q cursor out of bounds: %+v", seq, pos)
		}
	}
}

func TestVirtualTerminalCSIParameterMatrixStaysInBounds(t *testing.T) {
	for _, final := range []byte{'A', 'B', 'C', 'D', 'G', 'd', 'H', '@', 'P', 'X', 'L', 'M', 'S', 'T', 'r'} {
		for _, value := range []string{"-3", "0", "20000000", "999999999999999999999999999999999999"} {
			t.Run(string(final)+"/"+value, func(t *testing.T) {
				v := NewVirtualTerminal(7, 4)
				sequence := "\x1b[" + value + string(final) + "Q"
				_, _ = v.Write([]byte(sequence))
				pos := v.CursorPos()
				if pos.Row < 0 || pos.Row >= 4 || pos.Col < 0 || pos.Col >= 7 || len(v.Screen()) != 4 {
					t.Fatalf("%q produced invalid terminal state: %+v %#v", sequence, pos, v.Screen())
				}
			})
		}
	}
}

func TestVirtualTerminalEscapeAndWideCellRegressions(t *testing.T) {
	v := NewVirtualTerminal(5, 4)
	v.Write([]byte("a\x1b(Bb\x1b[3;1H\x1bM X\x1b[1;1H界界\x1b[1;2H漢"))
	if got := v.Screen()[0]; got != " 漢 " {
		t.Fatalf("wide overwrite=%q", got)
	}
	wide := NewVirtualTerminal(1, 1)
	wide.Write([]byte("界"))
	if wide.CursorPos().Col != 0 {
		t.Fatalf("one-column cursor=%+v", wide.CursorPos())
	}
}

func TestVirtualTerminalKittyCSIAndC1Ignored(t *testing.T) {
	v := NewVirtualTerminal(10, 3)
	v.Write([]byte("\x1b[3;3H\x1b[<uX\x1b[=1;1uY\x1b[>1uZ\x1b[?uW"))
	if v.CursorPos() != (CursorPosition{Row: 2, Col: 6}) {
		t.Fatalf("cursor=%+v", v.CursorPos())
	}
	v.Write([]byte("a\xc2\x9b2Jb"))
	if strings.Contains(strings.Join(v.Screen(), ""), "\u009b") {
		t.Fatalf("C1 rendered: %#v", v.Screen())
	}
}

func TestVirtualTerminalHugeScrollingIsBounded(t *testing.T) {
	v := NewVirtualTerminal(30, 20)
	start := time.Now()
	v.Write([]byte("\x1b[20000000S\x1b[20000000L"))
	if time.Since(start) > 10*time.Millisecond {
		t.Fatalf("large scroll took %s", time.Since(start))
	}
}

func TestVirtualTerminalChunkingMatchesSingleWrite(t *testing.T) {
	inputs := [][]byte{[]byte(claudeDefaultStartup), []byte("ab界🙂\x1b]title\x1b\\\x1b[2;2Hq"), []byte("a\xc2\x9b2Jb\xffz")}
	for _, input := range inputs {
		all, split := NewVirtualTerminal(120, 40), NewVirtualTerminal(120, 40)
		all.Write(input)
		for _, b := range input {
			split.Write([]byte{b})
		}
		if fmt.Sprint(all.Screen()) != fmt.Sprint(split.Screen()) || all.CursorPos() != split.CursorPos() {
			t.Fatalf("chunked input differs")
		}
	}
}

func TestVirtualTerminalConcurrentWriteAndScreen(t *testing.T) {
	v := NewVirtualTerminal(80, 24)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				v.Write([]byte("x\x1b[1;1H"))
				_ = v.Screen()
				_ = v.CursorPos()
			}
		}()
	}
	wg.Wait()
}

func TestVirtualTerminalTextRowsOmitWideContinuation(t *testing.T) {
	v := NewVirtualTerminal(40, 2)
	if _, err := v.Write([]byte("⏺ 本番にデプロイ\r\nforce push")); err != nil {
		t.Fatal(err)
	}
	if got := v.TextRows(); got[0] != "⏺ 本番にデプロイ" || got[1] != "force push" {
		t.Fatalf("text rows=%q", got)
	}
	if v.Screen()[0] == v.TextRows()[0] {
		t.Fatal("column-preserving screen unexpectedly changed")
	}
}

func TestVirtualTerminalOSCInterruptAndEscapeSequenceLimit(t *testing.T) {
	v := NewVirtualTerminal(20, 4)
	v.Write([]byte("\x1b]title\x1b\x1b[2;1HX\x1b]unterminated"))
	if !strings.HasPrefix(v.Screen()[1], "X") {
		t.Fatalf("CSI after OSC interruption: %#v", v.Screen())
	}
	v.Write([]byte(strings.Repeat("q", 5000) + "Z"))
	if v.CursorPos().Row < 0 || v.CursorPos().Row >= 4 {
		t.Fatalf("cursor=%+v", v.CursorPos())
	}
}

func TestVirtualTerminalWideRuneTableOverrides(t *testing.T) {
	for _, tc := range []struct {
		r     rune
		width int
	}{{'✅', 2}, {'⭐', 2}, {'⚡', 2}, {'⌚', 2}, {'⌛', 2}, {'⏩', 2}, {'☔', 2}, {'⭕', 2}, {'❯', 1}, {'⏵', 1}, {'✳', 1}, {'─', 1}, {'🄰', 1}, {'e', 1}} {
		if got := runeWidth(tc.r); got != tc.width {
			t.Errorf("rune %U width=%d want %d", tc.r, got, tc.width)
		}
	}
	if got := runeWidth('\u0301'); got != 0 {
		t.Errorf("combining width=%d", got)
	}
}

func TestVirtualTerminalFaintAttribute(t *testing.T) {
	v := NewVirtualTerminal(20, 2)
	v.Write([]byte("\x1b[2mTry \"example\"\x1b[22mX"))
	cells := v.Cells(0)
	for _, cell := range cells[:13] {
		if cell.Text != "" && !cell.Faint {
			t.Errorf("cell %q lost faint attribute", cell.Text)
		}
	}
	if cells[13].Text != "X" || cells[13].Faint {
		t.Fatalf("SGR 22 cell=%+v", cells[13])
	}
	v.Write([]byte("\x1b[2mY\x1b[0mZ"))
	if !v.Cells(0)[14].Faint || v.Cells(0)[15].Faint {
		t.Fatalf("SGR 0 did not clear faint: %+v", v.Cells(0)[14:16])
	}
}

func TestVirtualTerminalFaintErasureAlternateAndChunking(t *testing.T) {
	input := []byte("\x1b[2mAAAA\r\nBBBB\x1b[1;2H\x1b[K\x1b[2;2H\x1b[X\x1b[2;3H\x1b[?1049hALT\x1b[?1049l")
	all, split := NewVirtualTerminal(8, 3), NewVirtualTerminal(8, 3)
	_, _ = all.Write(input)
	for _, b := range input {
		_, _ = split.Write([]byte{b})
	}
	if fmt.Sprint(all.Screen()) != fmt.Sprint(split.Screen()) {
		t.Fatalf("split write differed: all=%q split=%q", all.Screen(), split.Screen())
	}
	for _, v := range []*VirtualTerminal{all, split} {
		cells := v.Cells(0)
		if cells[0].Text != "A" || !cells[0].Faint || cells[1].Text != "" || cells[1].Faint {
			t.Fatalf("EL did not clear character and faint state: %+v", cells[:4])
		}
		if cells[2].Text != "" || cells[2].Faint {
			t.Fatalf("alternate screen did not restore primary faint state: %+v", cells[:4])
		}
		second := v.Cells(1)
		if second[0].Text != "B" || !second[0].Faint || second[1].Text != "" || second[1].Faint {
			t.Fatalf("ECH did not clear character and faint state: %+v", second[:4])
		}
	}
}

func TestVirtualTerminalEDClearsFaintCells(t *testing.T) {
	v := NewVirtualTerminal(5, 2)
	_, _ = v.Write([]byte("\x1b[2mABCDE\r\nFGHIJ\x1b[1;3H\x1b[0J"))
	for row, cells := range [][]Cell{v.Cells(0), v.Cells(1)} {
		for col, cell := range cells {
			if row == 0 && col < 2 {
				if cell.Text == "" || !cell.Faint {
					t.Fatalf("ED unexpectedly changed cell (%d,%d): %+v", row, col, cell)
				}
				continue
			}
			if cell.Text != "" || cell.Faint {
				t.Fatalf("ED left cell (%d,%d) populated: %+v", row, col, cell)
			}
		}
	}
}

func TestVirtualTerminalSGRColorsDoNotBecomeAttributes(t *testing.T) {
	v := NewVirtualTerminal(12, 2)
	v.Write([]byte("\x1b[48;2;2;2;2m ab \x1b[39m c"))
	for i, cell := range v.Cells(0)[:5] {
		if cell.Faint {
			t.Fatalf("background color parameter marked cell %d faint: %+v", i, cell)
		}
	}
	v = NewVirtualTerminal(12, 2)
	v.Write([]byte("\x1b[38;2;138;138;138mabc\x1b[39mX"))
	for i, cell := range v.Cells(0)[:4] {
		if cell.Faint {
			t.Fatalf("foreground color parameter marked cell %d faint: %+v", i, cell)
		}
	}
	v = NewVirtualTerminal(8, 1)
	v.Write([]byte("\x1b[38;5;2;48;5;2;58;5;2mA\x1b[38:2::2:2:2mB\x1b[22mX\x1b[2mY\x1b[2mZ\x1b[mQ"))
	if cells := v.Cells(0); cells[0].Faint || cells[1].Faint || cells[2].Faint || !cells[3].Faint || !cells[4].Faint || cells[5].Faint {
		t.Fatalf("SGR extended colors/faint transitions: %+v", cells[:8])
	}
}

func TestVirtualTerminalSGRColorParameterBoundaries(t *testing.T) {
	for _, color := range []string{"38", "48", "58"} {
		for _, format := range []string{
			";2;0;2;22", ":2:0:2:22", ":2::0:2:22", ":2:0:0:2:22", ":2:1:0:2:22",
			";5;2", ":5:2", ":5:0",
		} {
			for _, tc := range []struct {
				name, before, after string
				wantFaint           bool
			}{
				{"clear", "2", "22", false},
				{"set", "22", "2", true},
				{"preserve_faint", "2", "", true},
				{"preserve_normal", "22", "", false},
			} {
				t.Run(color+format+"/"+tc.name, func(t *testing.T) {
					v := NewVirtualTerminal(8, 1)
					sequence := tc.before + ";" + color + format
					if tc.after != "" {
						sequence += ";" + tc.after
					}
					v.Write([]byte("\x1b[" + sequence + "mX"))
					if cell := v.Cells(0)[0]; cell.Text != "X" || cell.Faint != tc.wantFaint {
						t.Fatalf("SGR %q: cell=%+v want faint=%t", sequence, cell, tc.wantFaint)
					}
				})
			}
		}
	}
	for _, sequence := range []string{
		"2;38:2:0:2:22;48;2;0;2;22;58:2:1:0:2:22;22",
		"2;38;2;0;2;22;48:2::0:2:22;58;5;2;22",
		"2;38:2:0:2:22;48:5:2;58;2;0;2;22;22",
	} {
		t.Run("mixed/"+sequence, func(t *testing.T) {
			v := NewVirtualTerminal(8, 1)
			for _, b := range []byte("\x1b[" + sequence + "mX") {
				v.Write([]byte{b})
			}
			if cell := v.Cells(0)[0]; cell.Text != "X" || cell.Faint {
				t.Fatalf("mixed SGR %q: cell=%+v want normal X", sequence, cell)
			}
		})
	}
}

func TestVirtualTerminalSavedCursorFaintCombinations(t *testing.T) {
	for _, save := range []string{"\x1b7", "\x1b[s"} {
		for _, restore := range []string{"\x1b8", "\x1b[u"} {
			for _, faint := range []bool{false, true} {
				t.Run(fmt.Sprintf("%q/%q/faint=%t", save, restore, faint), func(t *testing.T) {
					v := NewVirtualTerminal(8, 3)
					before, after := "22", "2"
					if faint {
						before, after = after, before
					}
					// Seed an older DECSC attribute opposite to the new saved state.
					v.Write([]byte("\x1b[" + after + "m\x1b7\x1b[" + before + "m\x1b[2;3H" + save))
					v.Write([]byte("\x1b[" + after + "m\x1b[1;1H" + restore + "X"))
					if cell := v.Cells(1)[2]; cell.Text != "X" || cell.Faint != faint {
						t.Fatalf("restored cell=%+v want faint=%t", cell, faint)
					}
					if got := v.CursorPos(); got != (CursorPosition{Row: 1, Col: 3}) {
						t.Fatalf("restored cursor after X=%+v", got)
					}
				})
			}
		}
	}
}

func TestVirtualTerminalCSIEmptyParametersAndPendingWrap(t *testing.T) {
	v := NewVirtualTerminal(3, 3)
	v.Write([]byte("abc\x1b[2m"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 0, Col: 2}) {
		t.Fatalf("SGR canceled pending wrap: %+v", got)
	}
	v.Write([]byte("X"))
	if got := v.Screen()[1]; got != "X" {
		t.Fatalf("pending wrap was lost: %#v", v.Screen())
	}
	v = NewVirtualTerminal(5, 4)
	v.Write([]byte("\x1b[;3HX"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 0, Col: 3}) || v.Screen()[0] != "  X" {
		t.Fatalf("empty first CUP parameter: pos=%+v screen=%#v", got, v.Screen())
	}
	v.Write([]byte("\x1b[;2r"))
	if v.primary.scrollTop != 0 || v.primary.scrollBottom != 1 {
		t.Fatalf("empty scroll-region top: %d..%d", v.primary.scrollTop, v.primary.scrollBottom)
	}
	v = NewVirtualTerminal(20, 2)
	v.Write([]byte("Quick\x1b[8Gsafety"))
	if got := v.Screen()[0]; got != "Quick  safety" {
		t.Fatalf("CHA cursor positioning: %q", got)
	}
	v = NewVirtualTerminal(80, 2)
	v.Write([]byte("\x1b[2GQuick\x1b[8Gsafety\x1b[15Gcheck:"))
	if got := v.Screen()[0]; got != " Quick safety check:" {
		t.Fatalf("captured prompt cursor positioning: %q", got)
	}
}

func TestVirtualTerminalEscapeLimitAndStringTerminators(t *testing.T) {
	for index, input := range []string{
		"\x1b[" + strings.Repeat("1", 4100) + "mVISIBLE",
		"\x1b]" + strings.Repeat("x", 4100) + "\x18cd",
		"\x1bP" + strings.Repeat("x", 4100) + "\x07cd\x1b\\OK",
	} {
		v := NewVirtualTerminal(20, 3)
		v.Write([]byte(input))
		if got := strings.Join(v.Screen(), ""); strings.Contains(got, strings.Repeat("1", 8)) || strings.Contains(got, "x") {
			t.Fatalf("escape body leaked onto screen: %q", got)
		}
		if v.state == terminalOSC || v.state == terminalDCS || v.state == terminalDiscardOSC || v.state == terminalDiscardDCS {
			t.Fatalf("string escape %d was not terminated: state=%d", index, v.state)
		}
	}
	for _, tc := range []struct{ input, want string }{
		{"\x1b]0;x\x18cd", "cd"},
		{"\x1bPq\x07cd\x1b\\", ""},
	} {
		v := NewVirtualTerminal(10, 2)
		v.Write([]byte(tc.input))
		if got := strings.TrimSpace(v.Screen()[0]); got != tc.want {
			t.Fatalf("terminator handling %q: got %q want %q", tc.input, got, tc.want)
		}
	}
}

func TestVirtualTerminalResizeAndFaintState(t *testing.T) {
	v := NewVirtualTerminal(5, 2)
	v.Write([]byte("1\r\n2\r\n3\r\n4\r\n5"))
	v.Resize(5, 4)
	if got := v.Screen(); got[0] != "4" || got[1] != "5" || got[2] != "" || got[3] != "" {
		t.Fatalf("resized screen retained stale scroll region: %#v", got)
	}
	v = NewVirtualTerminal(4, 2)
	v.Write([]byte("ab界"))
	v.Resize(3, 2)
	if got := v.Screen()[0]; strings.Contains(got, "界") {
		t.Fatalf("resize retained half of wide glyph: %q", got)
	}
	v = NewVirtualTerminal(8, 2)
	v.Write([]byte("\x1b[2m\x1b[?1049hX\x1b[?1049lY"))
	if cells := v.Cells(0); cells[0].Text != "Y" || !cells[0].Faint {
		t.Fatalf("alternate screen did not preserve faint state: %+v", cells[:2])
	}
}

func TestVirtualTerminalUnicodeWidthAndSavedFaint(t *testing.T) {
	for _, tc := range []struct {
		r     rune
		width int
	}{
		{'\u0378', 1}, {'\u061d', 1}, {'\u0870', 1}, {'\u0875', 1},
		{'✅', 2}, {'⭐', 2}, {'⚡', 2}, {'⌚', 2}, {'あ', 2}, {'Ａ', 2},
		{'❯', 1}, {'⏵', 1}, {'✳', 1}, {'─', 1},
	} {
		if got := runeWidth(tc.r); got != tc.width {
			t.Errorf("U+%04X width=%d want=%d", tc.r, got, tc.width)
		}
	}
	v := NewVirtualTerminal(3, 2)
	v.Write([]byte("\x1b[2mabc\x1b7\x1b[22m\x1b8X"))
	if cells := v.Cells(0); !cells[2].Faint {
		t.Fatalf("DECRC did not restore faint state: %+v", cells)
	}
}

func TestVirtualTerminalCombiningAtWrapAndCursorRestore(t *testing.T) {
	v := NewVirtualTerminal(3, 2)
	v.Write([]byte("abc\u0301"))
	if got := v.Screen()[0]; got != "abć" {
		t.Fatalf("combining mark at pending wrap: %q", got)
	}
	v = NewVirtualTerminal(3, 2)
	v.Write([]byte("\x1b7abc\x1b8X"))
	if v.Screen()[0] != "Xbc" || v.CursorPos() != (CursorPosition{Row: 0, Col: 1}) {
		t.Fatalf("DECRC cursor restore: %#v %+v", v.Screen(), v.CursorPos())
	}
}

func TestVirtualTerminalIgnoredFeaturesLeaveScreenUnchanged(t *testing.T) {
	v := NewVirtualTerminal(30, 5)
	v.Write([]byte("baseline"))
	before := fmt.Sprint(v.Screen())
	for _, seq := range []string{
		"\x1b[?1000h", "\x1b[?1002h", "\x1b[?1003h", "\x1b[?1006h",
		"\x1b[?1004h", "\x1b[?2026h", "\x1b[c", "\x1b[>0q", "\x1b[?6n",
	} {
		v.Write([]byte(seq))
		if got := fmt.Sprint(v.Screen()); got != before {
			t.Fatalf("unsupported sequence %q changed screen: %q", seq, v.Screen())
		}
	}
	v.Write([]byte("\x1b)0"))
	if got := fmt.Sprint(v.Screen()); got != before {
		t.Fatalf("ESC ) 0 rendered content: %q", v.Screen())
	}
	v.Write([]byte("\x1b#8"))
	if got := fmt.Sprint(v.Screen()); got != before {
		t.Fatalf("ESC # 8 rendered content: %q", v.Screen())
	}
}

func TestVirtualTerminalPrivateAlternateModesAndSavedCursorCSI(t *testing.T) {
	for _, mode := range []string{"47", "1047", "1049"} {
		v := NewVirtualTerminal(8, 2)
		v.Write([]byte("main"))
		v.Write([]byte("\x1b[?" + mode + "hALT"))
		if !strings.HasPrefix(v.Screen()[0], "ALT") {
			t.Fatalf("mode %s failed to enter alternate: %#v", mode, v.Screen())
		}
		v.Write([]byte("\x1b[?" + mode + "l"))
		if !strings.HasPrefix(v.Screen()[0], "main") {
			t.Fatalf("mode %s failed to restore primary: %#v", mode, v.Screen())
		}
	}
	v := NewVirtualTerminal(8, 3)
	v.Write([]byte("\x1b[2;3H\x1b[s\x1b[1;1HX\x1b[uY"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 1, Col: 3}) || v.Screen()[1] != "  Y" {
		t.Fatalf("CSI s/u cursor restore: pos=%+v screen=%#v", got, v.Screen())
	}
}

func TestVirtualTerminalIndexAndScrollRegionLineOperations(t *testing.T) {
	v := NewVirtualTerminal(4, 4)
	v.Write([]byte("A\r\nB\r\nC\r\nD\x1b[2;3r\x1b[2;1H\x1b[L"))
	if got := v.Screen(); got[0] != "A" || got[1] != "" || got[2] != "B" || got[3] != "D" {
		t.Fatalf("IL changed rows outside scroll region: %#v", got)
	}
	v.Write([]byte("\x1b[M"))
	if got := v.Screen(); got[0] != "A" || got[1] != "B" || got[2] != "" || got[3] != "D" {
		t.Fatalf("DL result or outside row: %#v", got)
	}
	v = NewVirtualTerminal(4, 3)
	v.Write([]byte("A\r\nB\r\nC\x1b[2;1H\x1bM"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 0, Col: 0}) {
		t.Fatalf("reverse index cursor=%+v", got)
	}
	v.Write([]byte("\x1b[1;1H\x1bD"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 1, Col: 0}) {
		t.Fatalf("index cursor=%+v", got)
	}
	v.Write([]byte("\x1bE"))
	if got := v.CursorPos(); got != (CursorPosition{Row: 2, Col: 0}) {
		t.Fatalf("next-line cursor=%+v", got)
	}
	v = NewVirtualTerminal(4, 4)
	v.Write([]byte("A\r\nB\r\nC\r\nD\x1b[2;3r\x1b[2;1H\x1b[S"))
	if got := v.Screen(); got[0] != "A" || got[1] != "C" || got[2] != "" || got[3] != "D" {
		t.Fatalf("SU escaped scroll region: %#v", got)
	}
	v.Write([]byte("\x1b[T"))
	if got := v.Screen(); got[0] != "A" || got[1] != "" || got[2] != "C" || got[3] != "D" {
		t.Fatalf("SD escaped scroll region: %#v", got)
	}
}

func TestVirtualTerminalWideEraseAndCharacterShifts(t *testing.T) {
	for _, seq := range []string{"\x1b[2K", "\x1b[0J", "\x1b[X", "\x1b[@", "\x1b[P"} {
		v := NewVirtualTerminal(6, 2)
		v.Write([]byte("界AB\x1b[1;2H" + seq))
		cells := v.Cells(0)
		for col, cell := range cells {
			if cell.Text == "" && cell.Wide && (col == 0 || cells[col-1].Text == "") {
				t.Fatalf("orphan continuation at %d: %+v", col, cell)
			}
		}
	}
}

func TestVirtualTerminalCombiningAtRowStartAndZWJAfterCR(t *testing.T) {
	v := NewVirtualTerminal(8, 2)
	v.Write([]byte("\u0301X"))
	if got := v.Screen()[0]; got != "X" {
		t.Fatalf("row-head combining mark changed cells: %q", got)
	}
	v.Write([]byte("\r👨‍\rX"))
	if got := v.Screen()[0]; got != "X" {
		t.Fatalf("ZWJ attachment survived CR: %q", got)
	}
}

func FuzzVirtualTerminal(f *testing.F) {
	f.Add([]byte("\x1b[-3Cabc\x1b[20000000S界"))
	f.Add([]byte("\x1b]unterminated"))
	f.Fuzz(func(t *testing.T, input []byte) {
		v := NewVirtualTerminal(30, 10)
		v.Write(input)
		if len(v.Screen()) != 10 {
			t.Fatalf("screen rows=%d", len(v.Screen()))
		}
		p := v.CursorPos()
		if p.Row < 0 || p.Row >= 10 || p.Col < 0 || p.Col >= 30 {
			t.Fatalf("cursor out of bounds: %+v", p)
		}
	})
}
