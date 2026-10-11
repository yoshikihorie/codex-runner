package supervisor

import (
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type terminalCell struct {
	text         string
	continuation bool
	faint        bool
}

const (
	terminalGround byte = iota
	terminalEscape
	terminalCSI
	terminalOSC
	terminalDCS
	terminalStringEscape
	terminalEscapeIntermediate
	terminalDiscardCSI
	terminalDiscardOSC
	terminalDiscardDCS
)

// CursorPosition is a zero-based position on the visible terminal screen.
type CursorPosition struct{ Row, Col int }

// Cell is one terminal column and its rendered character attributes.
type Cell struct {
	Text  string
	Faint bool
	Wide  bool
}

type terminalScreen struct {
	cells                                                 [][]terminalCell
	row, col, savedRow, savedCol, scrollTop, scrollBottom int
	savedFaint                                            bool
	joinNext                                              bool
	pendingWrap                                           bool
	faint                                                 bool
}

// VirtualTerminal incrementally reconstructs a fixed-size terminal screen.
type VirtualTerminal struct {
	mu                 sync.Mutex
	cols, rows         int
	primary, alternate terminalScreen
	useAlternate       bool
	state              byte
	sequence           strings.Builder
	sequenceBytes      int
	escapeIntermediate bool
	utf8Pending        []byte
}

// NewVirtualTerminal creates a blank terminal with one-based CSI coordinates.
func NewVirtualTerminal(cols, rows int) *VirtualTerminal {
	if cols < 1 {
		cols = 1
	}
	if rows < 1 {
		rows = 1
	}
	v := &VirtualTerminal{cols: cols, rows: rows}
	v.primary = v.newScreen()
	v.alternate = v.newScreen()
	return v
}

func (v *VirtualTerminal) newScreen() terminalScreen {
	s := terminalScreen{cells: make([][]terminalCell, v.rows), scrollBottom: v.rows - 1}
	for i := range s.cells {
		s.cells[i] = make([]terminalCell, v.cols)
	}
	return s
}
func (v *VirtualTerminal) current() *terminalScreen {
	if v.useAlternate {
		return &v.alternate
	}
	return &v.primary
}

// Write consumes a chunk while retaining incomplete UTF-8 and escape sequences.
func (v *VirtualTerminal) Write(data []byte) (int, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	n := len(data)
	if len(v.utf8Pending) > 0 {
		data = append(v.utf8Pending, data...)
		v.utf8Pending = nil
	}
	v.utf8Pending = nil
	for len(data) > 0 {
		if v.state != 0 {
			b := data[0]
			data = data[1:]
			v.consumeEscape(b)
			continue
		}
		if data[0] < utf8.RuneSelf {
			b := data[0]
			data = data[1:]
			v.consumeControlOrRune(rune(b))
			continue
		}
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 && !utf8.FullRune(data) {
			v.utf8Pending = append(v.utf8Pending, data...)
			return n, nil
		}
		if r == utf8.RuneError && size == 1 {
			data = data[1:]
			continue
		}
		data = data[size:]
		if r >= 0x80 && r <= 0x9f {
			continue
		}
		v.put(r)
	}
	return n, nil
}

func (v *VirtualTerminal) consumeEscape(b byte) {
	if v.state != terminalDiscardCSI && v.state != terminalDiscardOSC && v.state != terminalDiscardDCS && v.state != terminalStringEscape {
		v.sequenceBytes++
	}
	if v.sequenceBytes > 4096 && v.state != terminalDiscardCSI && v.state != terminalDiscardOSC && v.state != terminalDiscardDCS && v.state != terminalStringEscape {
		switch v.state {
		case terminalCSI:
			v.state = terminalDiscardCSI
		case terminalOSC, terminalStringEscape:
			v.state = terminalDiscardOSC
		case terminalDCS:
			v.state = terminalDiscardDCS
		default:
			v.state = 0
		}
		v.sequence.Reset()
		if v.state == 0 {
			return
		}
	}
	switch v.state {
	case terminalEscape:
		switch b {
		case '[':
			v.state = terminalCSI
			v.sequence.Reset()
			v.sequenceBytes = 0
		case ']':
			v.state = terminalOSC
			v.sequenceBytes = 0
		case 'P', '^', '_':
			v.state = terminalDCS
			v.sequenceBytes = 0
		case '7':
			s := v.current()
			s.savedRow, s.savedCol = s.row, s.col
			s.savedFaint = s.faint
			v.state = 0
		case '8':
			s := v.current()
			s.row, s.col = clamp(s.savedRow, 0, v.rows-1), clamp(s.savedCol, 0, v.cols-1)
			s.faint = s.savedFaint
			s.pendingWrap = false
			v.state = 0
		case 'D':
			v.lineFeed()
			v.state = 0
		case 'E':
			v.current().col = 0
			v.lineFeed()
			v.state = 0
		case 'M':
			v.reverseIndex()
			v.state = 0
		case 'c':
			v.primary, v.alternate = v.newScreen(), v.newScreen()
			v.useAlternate = false
			v.state = 0
		case ' ', '(', ')', '*', '+', '-', '.', '/', '#':
			v.state = terminalEscapeIntermediate
		default:
			v.state = 0
		}
	case terminalCSI:
		if b == 0x1b {
			v.state = terminalEscape
			v.sequence.Reset()
			return
		}
		if b == 0x18 || b == 0x1a {
			v.state = 0
			v.sequence.Reset()
			return
		}
		if b >= 0x40 && b <= 0x7e {
			v.executeCSI(rune(b), v.sequence.String())
			v.state = 0
			return
		}
		if b >= 0x20 && b <= 0x3f && v.sequence.Len() < 4096 {
			v.sequence.WriteByte(b)
		} else if v.sequence.Len() >= 4096 {
			v.state = terminalDiscardCSI
			v.sequence.Reset()
		}
	case terminalOSC:
		if b == 0x18 || b == 0x1a || b == 0x07 {
			v.state = 0
		} else if b == 0x1b {
			v.state = terminalStringEscape
		}
	case terminalDCS:
		if b == 0x18 || b == 0x1a {
			v.state = 0
		} else if b == 0x1b {
			v.state = terminalStringEscape
		}
	case terminalStringEscape:
		if b == '\\' {
			v.state = 0
		} else if b == 0x1b {
			v.state = terminalEscape
			v.sequenceBytes = 0
		} else {
			v.state = terminalEscape
			v.consumeEscape(b)
		}
	case terminalEscapeIntermediate:
		if b >= 0x30 && b <= 0x7e {
			v.state = 0
		}
	case terminalDiscardCSI:
		if b >= 0x40 && b <= 0x7e || b == 0x18 || b == 0x1a {
			v.state = 0
		}
	case terminalDiscardOSC:
		if b == 0x18 || b == 0x1a || b == 0x07 {
			v.state = 0
		} else if b == 0x1b {
			v.state = terminalStringEscape
		}
	case terminalDiscardDCS:
		if b == 0x18 || b == 0x1a {
			v.state = 0
		} else if b == 0x1b {
			v.state = terminalStringEscape
		}
	}
}
func (v *VirtualTerminal) consumeControlOrRune(ch rune) {
	if ch == 0x1b {
		v.current().joinNext = false
		v.state = terminalEscape
		v.sequenceBytes = 0
		return
	}
	s := v.current()
	s.joinNext = false
	switch ch {
	case '\r':
		s.col, s.pendingWrap = 0, false
	case '\n', '\v', '\f':
		v.lineFeed()
	case '\b':
		s.pendingWrap = false
		if s.col > 0 {
			s.col--
		}
	case '\t':
		s.col = min(v.cols-1, (s.col/8+1)*8)
	default:
		if ch >= 0x20 && ch != 0x7f {
			v.put(ch)
		}
	}
}
func (v *VirtualTerminal) lineFeed() {
	s := v.current()
	if s.row == s.scrollBottom {
		v.scrollUp(1)
	} else if s.row < v.rows-1 {
		s.row++
	}
}
func (v *VirtualTerminal) reverseIndex() {
	s := v.current()
	if s.row == s.scrollTop {
		v.scrollDown(1)
	} else if s.row > 0 {
		s.row--
	}
}
func (v *VirtualTerminal) put(ch rune) {
	s := v.current()
	w := runeWidth(ch)
	if w == 0 || s.joinNext {
		if c := v.previousCell(s); c >= 0 {
			s.cells[s.row][c].text += string(ch)
		}
		s.joinNext = ch == 0x200d
		return
	}
	if s.pendingWrap {
		s.col, s.pendingWrap = 0, false
		v.lineFeed()
	}
	if v.cols < 2 && w == 2 {
		w = 1
	}
	if w == 2 && s.col == v.cols-1 {
		s.col = 0
		v.lineFeed()
	}
	v.eraseWideAt(s, s.row, s.col)
	if w == 2 {
		v.eraseWideAt(s, s.row, s.col+1)
	}
	s.cells[s.row][s.col] = terminalCell{text: string(ch), faint: s.faint}
	if w == 2 {
		s.cells[s.row][s.col+1] = terminalCell{continuation: true, faint: s.faint}
	}
	s.col += w
	if s.col >= v.cols {
		s.col = v.cols - 1
		s.pendingWrap = true
	}
}
func (v *VirtualTerminal) previousCell(s *terminalScreen) int {
	c := s.col - 1
	if s.pendingWrap {
		c = s.col
	}
	if c >= 0 && s.cells[s.row][c].continuation {
		c--
	}
	return c
}
func (v *VirtualTerminal) eraseWideAt(s *terminalScreen, row, col int) {
	if col < 0 || col >= v.cols {
		return
	}
	if s.cells[row][col].continuation && col > 0 {
		s.cells[row][col-1] = terminalCell{}
	}
	if col+1 < v.cols && s.cells[row][col+1].continuation {
		s.cells[row][col+1] = terminalCell{}
	}
	s.cells[row][col] = terminalCell{}
}

func (v *VirtualTerminal) executeCSI(final rune, raw string) {
	private := byte(0)
	if len(raw) > 0 && strings.ContainsRune("?><=!", rune(raw[0])) {
		private, raw = raw[0], raw[1:]
	}
	if private == '?' {
		if final == 'J' {
			v.eraseDisplay(privateEraseMode(raw))
			return
		}
		if final == 'K' {
			v.eraseLine(privateEraseMode(raw))
			return
		}
		v.privateMode(final, raw)
		return
	}
	if private != 0 {
		return
	}
	if strings.Contains(raw, "-") || strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' && r != ';' && r != ':' }) >= 0 {
		return
	}
	params := parseTerminalParams(raw)
	hasSubparameters := strings.Contains(raw, ":")
	param := func(i, def int) int {
		if i >= len(params) || params[i] == 0 {
			return def
		}
		return params[i]
	}
	n := param(0, 1)
	s := v.current()
	if strings.ContainsRune("HfABCDG`du", final) || final == 'r' {
		s.pendingWrap = false
		s.joinNext = false
	}
	switch final {
	case 'm':
		for i := 0; i < len(params); i++ {
			p := params[i]
			switch p {
			case 0:
				s.faint = false
			case 2:
				s.faint = true
			case 22:
				s.faint = false
			case 38, 48, 58:
				if i+1 < len(params) {
					if params[i+1] == 5 {
						i += 2
					} else if params[i+1] == 2 {
						if hasSubparameters && i+5 < len(params) && params[i+2] == 0 {
							i += 5
						} else {
							i += 4
						}
					}
				}
			}
		}
	case 'H', 'f':
		s.row = clamp(param(0, 1)-1, 0, v.rows-1)
		s.col = clamp(param(1, 1)-1, 0, v.cols-1)
	case 'A':
		s.row = max(0, s.row-n)
	case 'B':
		s.row = min(v.rows-1, s.row+n)
	case 'C':
		s.col = min(v.cols-1, s.col+n)
	case 'D':
		s.col = max(0, s.col-n)
	case 'G', '`':
		s.col = clamp(param(0, 1)-1, 0, v.cols-1)
	case 'd':
		s.row = clamp(param(0, 1)-1, 0, v.rows-1)
	case 'J':
		v.eraseDisplay(param(0, 0))
	case 'K':
		v.eraseLine(param(0, 0))
	case 'r':
		top := clamp(param(0, 1)-1, 0, v.rows-1)
		bottom := clamp(param(1, v.rows)-1, 0, v.rows-1)
		if top < bottom {
			s.scrollTop, s.scrollBottom = top, bottom
			s.row, s.col = 0, 0
		}
	case 'S':
		v.scrollUp(n)
	case 'T':
		v.scrollDown(n)
	case 'L':
		if s.row >= s.scrollTop && s.row <= s.scrollBottom {
			v.scrollDownAt(s.row, n)
		}
	case 'M':
		if s.row >= s.scrollTop && s.row <= s.scrollBottom {
			v.scrollUpAt(s.row, n)
		}
	case '@':
		v.shiftChars(n, true)
	case 'P':
		v.shiftChars(n, false)
	case 'X':
		for c := s.col; c < min(v.cols, s.col+n); c++ {
			v.eraseWideAt(s, s.row, c)
		}
	case 's':
		if raw != "" {
			return
		}
		s.savedRow, s.savedCol = s.row, s.col
	case 'u':
		if raw != "" {
			return
		}
		s.row, s.col = clamp(s.savedRow, 0, v.rows-1), clamp(s.savedCol, 0, v.cols-1)
		s.faint = s.savedFaint
	}
}

// Cells returns a copy of the requested row, including blank cells.
func (v *VirtualTerminal) Cells(row int) []Cell {
	v.mu.Lock()
	defer v.mu.Unlock()
	if row < 0 || row >= v.rows {
		return nil
	}
	s := v.current()
	out := make([]Cell, v.cols)
	for col, cell := range s.cells[row] {
		out[col] = Cell{Text: cell.text, Faint: cell.faint, Wide: cell.continuation || cell.text != "" && runeWidth([]rune(cell.text)[0]) == 2}
	}
	return out
}

func privateEraseMode(raw string) int {
	if len(raw) < 2 {
		return 0
	}
	return parseTerminalParams(raw[1:])[0]
}
func parseTerminalParams(raw string) []int {
	parts := make([]string, 0, strings.Count(raw, ";")+1)
	for _, field := range strings.Split(raw, ";") {
		parts = append(parts, strings.Split(field, ":")...)
	}
	out := make([]int, len(parts))
	for i, f := range parts {
		if f != "" {
			value := 0
			for _, c := range f {
				if c < '0' || c > '9' {
					value = 0
					break
				}
				if value > 6553 || value == 6553 && int(c-'0') > 5 {
					value = 65535
					break
				}
				value = value*10 + int(c-'0')
			}
			out[i] = value
		}
	}
	return out
}
func (v *VirtualTerminal) privateMode(final rune, raw string) {
	if final != 'h' && final != 'l' {
		return
	}
	for _, p := range parseTerminalParams(raw) {
		if p == 47 || p == 1047 || p == 1049 {
			enable := final == 'h'
			if enable && !v.useAlternate {
				if p == 1049 {
					v.primary.savedRow, v.primary.savedCol = v.primary.row, v.primary.col
					v.primary.savedFaint = v.primary.faint
				}
				if p == 1049 || p == 1047 {
					v.alternate = v.newScreen()
				}
				v.alternate.faint = v.primary.faint
				v.useAlternate = true
			} else if !enable && v.useAlternate {
				v.primary.faint = v.alternate.faint
				v.useAlternate = false
				if p == 1049 {
					v.primary.row, v.primary.col = clamp(v.primary.savedRow, 0, v.rows-1), clamp(v.primary.savedCol, 0, v.cols-1)
					v.primary.faint = v.primary.savedFaint
				}
			}
		}
	}
}
func (v *VirtualTerminal) eraseLine(mode int) {
	s := v.current()
	start, end := s.col, v.cols
	if mode == 1 {
		start, end = 0, s.col+1
	} else if mode == 2 {
		start, end = 0, v.cols
	} else if mode != 0 {
		return
	}
	for c := start; c < end; c++ {
		v.eraseWideAt(s, s.row, c)
	}
}
func (v *VirtualTerminal) eraseDisplay(mode int) {
	s := v.current()
	switch mode {
	case 0:
		v.eraseLine(0)
		for r := s.row + 1; r < v.rows; r++ {
			clear(s.cells[r])
		}
	case 1:
		for r := 0; r < s.row; r++ {
			clear(s.cells[r])
		}
		v.eraseLine(1)
	case 2:
		for r := range s.cells {
			clear(s.cells[r])
		}
	}
}
func (v *VirtualTerminal) scrollUp(n int) {
	s := v.current()
	v.scrollUpAt(s.scrollTop, n)
}
func (v *VirtualTerminal) scrollDown(n int) {
	s := v.current()
	v.scrollDownAt(s.scrollTop, n)
}
func (v *VirtualTerminal) scrollUpAt(row, n int) {
	s := v.current()
	if row < s.scrollTop || row > s.scrollBottom {
		return
	}
	n = min(n, s.scrollBottom-row+1)
	copy(s.cells[row:s.scrollBottom+1-n], s.cells[row+n:s.scrollBottom+1])
	for i := s.scrollBottom - n + 1; i <= s.scrollBottom; i++ {
		s.cells[i] = make([]terminalCell, v.cols)
	}
}
func (v *VirtualTerminal) scrollDownAt(row, n int) {
	s := v.current()
	if row < s.scrollTop || row > s.scrollBottom {
		return
	}
	n = min(n, s.scrollBottom-row+1)
	copy(s.cells[row+n:s.scrollBottom+1], s.cells[row:s.scrollBottom+1-n])
	for i := row; i < row+n; i++ {
		s.cells[i] = make([]terminalCell, v.cols)
	}
}
func (v *VirtualTerminal) shiftChars(n int, insert bool) {
	s := v.current()
	r := s.row
	c := s.col
	n = min(n, v.cols-c)
	if insert {
		copy(s.cells[r][c+n:], s.cells[r][c:v.cols-n])
		for i := c; i < c+n; i++ {
			s.cells[r][i] = terminalCell{}
		}
	} else {
		copy(s.cells[r][c:], s.cells[r][c+n:])
		for i := v.cols - n; i < v.cols; i++ {
			s.cells[r][i] = terminalCell{}
		}
	}
	v.repairWideRow(s, r)
}

func (v *VirtualTerminal) repairWideRow(s *terminalScreen, row int) {
	if v.cols < 2 {
		return
	}
	for col := 0; col < v.cols; col++ {
		cell := &s.cells[row][col]
		if cell.continuation {
			if col == 0 || s.cells[row][col-1].continuation || s.cells[row][col-1].text == "" || runeWidth([]rune(s.cells[row][col-1].text)[0]) != 2 {
				*cell = terminalCell{}
			}
			continue
		}
		if cell.text != "" && runeWidth([]rune(cell.text)[0]) == 2 && (col+1 >= v.cols || !s.cells[row][col+1].continuation) {
			*cell = terminalCell{}
		}
	}
}

// Screen returns current rows with trailing blank cells removed.
func (v *VirtualTerminal) Screen() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.current()
	out := make([]string, v.rows)
	for r := range s.cells {
		var b strings.Builder
		last := len(s.cells[r]) - 1
		for last >= 0 && !s.cells[r][last].continuation && (s.cells[r][last].text == "" || s.cells[r][last].text == " ") {
			last--
		}
		for c := 0; c <= last; c++ {
			cell := s.cells[r][c]
			if cell.continuation {
				b.WriteByte(' ')
			} else if cell.text != "" {
				b.WriteString(cell.text)
			} else if !cell.continuation {
				b.WriteByte(' ')
			}
		}
		out[r] = b.String()
	}
	return out
}

// Resize changes the screen dimensions while preserving its upper-left contents.
func (v *VirtualTerminal) Resize(cols, rows int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if cols < 1 {
		cols = 1
	}
	if rows < 1 {
		rows = 1
	}
	resize := func(s *terminalScreen) {
		cells := make([][]terminalCell, rows)
		for r := range cells {
			cells[r] = make([]terminalCell, cols)
			if r < len(s.cells) {
				copy(cells[r], s.cells[r][:min(cols, len(s.cells[r]))])
			}
		}
		s.cells = cells
		for r := range s.cells {
			v.repairWideRow(s, r)
		}
		s.row = clamp(s.row, 0, rows-1)
		s.col = clamp(s.col, 0, cols-1)
		s.savedRow = clamp(s.savedRow, 0, rows-1)
		s.savedCol = clamp(s.savedCol, 0, cols-1)
		s.scrollTop, s.scrollBottom = 0, rows-1
		s.pendingWrap = false
	}
	v.cols, v.rows = cols, rows
	resize(&v.primary)
	resize(&v.alternate)
}

// CursorPos returns the current cursor position, clamped to the visible screen.
func (v *VirtualTerminal) CursorPos() CursorPosition {
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.current()
	return CursorPosition{Row: s.row, Col: s.col}
}
func runeWidth(r rune) int {
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || r == 0x200d || r == 0xfe0f || (r >= 0x1f3fb && r <= 0x1f3ff) {
		return 0
	}
	if isWideRune(r) {
		return 2
	}
	return 1
}
func isWideRune(r rune) bool {
	if r == 0x276f || r == 0x23f5 || r == 0x2733 || r == 0x2500 {
		return false
	}
	low, high := 0, len(eastAsianWideRanges)
	for low < high {
		mid := low + (high-low)/2
		if r < eastAsianWideRanges[mid].first {
			high = mid
		} else if r > eastAsianWideRanges[mid].last {
			low = mid + 1
		} else {
			return true
		}
	}
	return false
}
func clamp(n, low, high int) int { return min(max(n, low), high) }
