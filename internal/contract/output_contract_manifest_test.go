package contract_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const manifestSchemaVersion = 1
const defaultPartialSeparator = "---"

var requiredOutputFiles = []string{"prompt.md", "exit-code", "recovered-after-timeout", "partial-output.md", "stdout.log", "stderr.log", "last-message.md", "input.txt", "combined-prompt.md"}

type outputManifest struct {
	SchemaVersion    int            `json:"schema_version"`
	Scenario         string         `json:"scenario"`
	SubcommandFamily string         `json:"subcommand_family"`
	Canon            []string       `json:"canon"`
	Files            []manifestFile `json:"files"`
	AllowedExtra     []string       `json:"allowed_extra"`
}
type manifestFile struct {
	Name      string        `json:"name"`
	Presence  string        `json:"presence"`
	Class     string        `json:"class"`
	Encoding  string        `json:"encoding"`
	EOL       string        `json:"eol"`
	Immutable bool          `json:"immutable"`
	Match     manifestMatch `json:"match"`
}
type manifestMatch struct {
	Kind            string `json:"kind"`
	Values          []int  `json:"values"`
	Pattern         string `json:"pattern"`
	Value           string `json:"value"`
	MaxLogicalLines int    `json:"max_logical_lines"`
	MaxBytes        int    `json:"max_bytes"`
	Separator       string `json:"separator"`
}

var ansiCSI = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
var timestampToken = regexp.MustCompile(`\b\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:?\d\d)?`)
var sessionToken = regexp.MustCompile(`(?i)session id: [0-9a-f]{8}-[0-9a-f-]{27,}`)
var pidToken = regexp.MustCompile(`\bpid=\d+`)

func manifestPath(scenario string) string {
	_, source, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(source), "..", "..", "testdata", "golden", scenario, "manifest.json")
}
func loadManifest(scenario string) (outputManifest, error) {
	b, err := os.ReadFile(manifestPath(scenario))
	if err != nil {
		return outputManifest{}, err
	}
	return decodeManifest(b)
}
func decodeManifest(b []byte) (outputManifest, error) {
	var m outputManifest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("invalid manifest JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return m, fmt.Errorf("invalid manifest JSON: trailing value")
	}
	return m, validateManifest(m)
}

func validateManifest(m outputManifest) error {
	if m.SchemaVersion != manifestSchemaVersion || m.Scenario == "" || !oneOf(m.SubcommandFamily, "research", "review") {
		return fmt.Errorf("invalid manifest header")
	}
	if len(m.Files) != len(requiredOutputFiles) {
		return fmt.Errorf("manifest must declare exactly nine files")
	}
	seen, extra := map[string]bool{}, map[string]bool{}
	for _, x := range m.AllowedExtra {
		if x == "" || extra[x] {
			return fmt.Errorf("duplicate or empty allowed_extra %q", x)
		}
		extra[x] = true
	}
	for _, f := range m.Files {
		if f.Name == "" || seen[f.Name] || extra[f.Name] {
			return fmt.Errorf("duplicate, empty, or extra file name %q", f.Name)
		}
		seen[f.Name] = true
		if !oneOf(f.Presence, "required", "forbidden", "optional", "ignored") {
			return fmt.Errorf("invalid presence for %s", f.Name)
		}
		if !oneOf(f.Class, "", "snapshot", "stream", "once", "external") {
			return fmt.Errorf("invalid class for %s", f.Name)
		}
		if !oneOf(f.Encoding, "", "utf8") {
			return fmt.Errorf("invalid encoding for %s", f.Name)
		}
		if !oneOf(f.EOL, "", "lf") {
			return fmt.Errorf("invalid eol for %s", f.Name)
		}
		if !oneOf(f.Match.Kind, "", "nonempty", "int-in", "regexp", "line-complete", "header-and-tail", "exact") {
			return fmt.Errorf("invalid match kind for %s", f.Name)
		}
		switch f.Match.Kind {
		case "int-in":
			if len(f.Match.Values) == 0 {
				return fmt.Errorf("int-in values missing for %s", f.Name)
			}
		case "regexp":
			if _, err := regexp.Compile(f.Match.Pattern); err != nil || !strings.HasPrefix(f.Match.Pattern, "^") || !strings.HasSuffix(f.Match.Pattern, "$") {
				return fmt.Errorf("invalid anchored regexp for %s", f.Name)
			}
		case "header-and-tail":
			if f.Match.MaxLogicalLines <= 0 || f.Match.MaxBytes <= 0 {
				return fmt.Errorf("header-and-tail limits missing for %s", f.Name)
			}
		}
		if f.Presence == "forbidden" {
			if f.Class != "" || f.Encoding != "" || f.EOL != "" || f.Match.Kind != "" {
				return fmt.Errorf("forbidden file must not specify comparison rules: %s", f.Name)
			}
			continue
		}
		if f.Presence == "ignored" {
			if f.Class != "external" || f.Encoding != "" || f.EOL != "" || f.Match.Kind != "" {
				return fmt.Errorf("ignored %s must be external and non-applicable", f.Name)
			}
			continue
		}
		if f.Class == "external" {
			if f.Encoding != "" || f.EOL != "" || f.Match.Kind != "" {
				return fmt.Errorf("external file must not specify comparison rules: %s", f.Name)
			}
			continue
		}
		if f.Encoding != "utf8" || f.EOL != "lf" {
			return fmt.Errorf("applicable file must declare utf8 and lf: %s", f.Name)
		}
		if f.Class == "" || f.Encoding != "utf8" || f.EOL != "lf" || f.Match.Kind == "" {
			return fmt.Errorf("applicable file must declare class, utf8, lf, and match: %s", f.Name)
		}
	}
	for _, n := range requiredOutputFiles {
		if !seen[n] {
			return fmt.Errorf("required declaration missing: %s", n)
		}
	}
	return nil
}
func oneOf(v string, allowed ...string) bool {
	for _, x := range allowed {
		if v == x {
			return true
		}
	}
	return false
}

func normalizeOutput(b []byte, taskID, root string) string {
	s := strings.ReplaceAll(string(b), "\r\n", "\n")
	s = ansiCSI.ReplaceAllString(s, "")
	if taskID != "" {
		s = strings.ReplaceAll(s, taskID, "<TASK-ID>")
	}
	if root != "" {
		s = strings.ReplaceAll(s, root, "<TASKS-ROOT>")
	}
	s = timestampToken.ReplaceAllString(s, "<TS>")
	s = sessionToken.ReplaceAllString(s, "session id: <UUID>")
	return pidToken.ReplaceAllString(s, "pid=<PID>")
}

func verifyOutputDirectory(dir string, m outputManifest, baseline map[string][]byte) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	declared, allowed := map[string]manifestFile{}, map[string]bool{}
	for _, x := range m.AllowedExtra {
		allowed[x] = true
	}
	for _, f := range m.Files {
		declared[f.Name] = f
	}
	for _, e := range entries {
		if _, ok := declared[e.Name()]; !ok && !allowed[e.Name()] {
			return fmt.Errorf("unexpected extra file %s", e.Name())
		}
	}
	for _, f := range m.Files {
		path := filepath.Join(dir, f.Name)
		info, statErr := os.Lstat(path)
		exists := statErr == nil
		if statErr != nil && !os.IsNotExist(statErr) {
			return statErr
		}
		if f.Presence == "forbidden" {
			if exists {
				return fmt.Errorf("forbidden file exists: %s", f.Name)
			}
			continue
		}
		if !exists {
			if f.Presence == "required" {
				return fmt.Errorf("required file missing: %s", f.Name)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("file is not regular: %s", f.Name)
		}
		if f.Presence == "ignored" || f.Class == "external" {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if f.Encoding == "utf8" && !utf8.Valid(b) {
			return fmt.Errorf("invalid UTF-8: %s", f.Name)
		}
		if f.EOL == "lf" && strings.Contains(normalizeOutput(b, "", ""), "\r") {
			return fmt.Errorf("non-LF EOL: %s", f.Name)
		}
		if f.Class == "once" && f.Immutable {
			before, ok := baseline[f.Name]
			if !ok || !bytes.Equal(before, b) {
				return fmt.Errorf("once file changed: %s", f.Name)
			}
		}
		if err := verifyClassAndMatch(f, b, dir); err != nil {
			return fmt.Errorf("%s: %w", f.Name, err)
		}
	}
	return nil
}
func verifyClassAndMatch(f manifestFile, b []byte, dir string) error {
	if f.Class == "external" {
		return nil
	}
	if f.Class == "stream" {
		if len(b) > 0 && b[len(b)-1] != '\n' {
			return fmt.Errorf("stream is not LF complete")
		}
		return nil
	}
	v := normalizeOutput(b, filepath.Base(dir), filepath.Dir(dir))
	if f.Class == "snapshot" {
		v = strings.TrimSpace(v)
	}
	switch f.Match.Kind {
	case "", "line-complete":
		return nil
	case "nonempty":
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("must be nonempty")
		}
	case "int-in":
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("not integer")
		}
		for _, x := range f.Match.Values {
			if n == x {
				return nil
			}
		}
		return fmt.Errorf("integer %d not allowed", n)
	case "regexp":
		candidate := v
		if f.Name == "recovered-after-timeout" {
			candidate = strings.TrimSpace(ansiCSI.ReplaceAllString(strings.ReplaceAll(string(b), "\r\n", "\n"), ""))
		}
		if !regexp.MustCompile(f.Match.Pattern).MatchString(candidate) {
			return fmt.Errorf("regexp mismatch")
		}
	case "exact":
		if v != f.Match.Value {
			return fmt.Errorf("exact mismatch")
		}
	case "header-and-tail":
		return verifyHeaderAndTail(v, f.Match)
	default:
		return fmt.Errorf("unknown match kind")
	}
	return nil
}
func verifyHeaderAndTail(v string, match manifestMatch) error {
	lines := strings.Split(v, "\n")
	if len(lines) == 0 || lines[0] == "" {
		return fmt.Errorf("header block missing")
	}
	i := 0
	for i < len(lines) && lines[i] != "" {
		i++
	}
	if i == 0 || i >= len(lines) {
		return fmt.Errorf("header block missing")
	}
	start := i + 1
	sep := match.Separator
	if sep == "" {
		sep = defaultPartialSeparator
	}
	// recovery.partialOutputHeader has exactly one heading line, a one-line
	// explanation, and a blank line after the separator. Do not search for a
	// separator: any other occurrence belongs to the legacy tail.
	if i == 1 && start+3 < len(lines) && lines[start] != "" && lines[start+1] == "" && lines[start+2] == sep && lines[start+3] == "" {
		start += 4
	}
	tail := strings.Join(lines[start:], "\n")
	if len([]byte(tail)) > match.MaxBytes {
		return fmt.Errorf("tail byte limit exceeded")
	}
	trim := strings.TrimSuffix(tail, "\n")
	logical := 0
	if trim != "" {
		logical = len(strings.Split(trim, "\n"))
	}
	if logical > match.MaxLogicalLines {
		return fmt.Errorf("tail line limit exceeded")
	}
	return nil
}

func requireDirectoryResult(t *testing.T, f manifestFile, data []byte, baseline map[string][]byte, wantOK bool) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, f.Name), data, 0o600); err != nil {
		t.Fatal(err)
	}
	err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, baseline)
	if (err == nil) != wantOK {
		t.Fatalf("err=%v wantOK=%v", err, wantOK)
	}
}

func mutatedManifest(t *testing.T, scenario string, mutate func(*outputManifest)) outputManifest {
	t.Helper()
	m, err := loadManifest(scenario)
	if err != nil {
		t.Fatal(err)
	}
	m.Files = append([]manifestFile(nil), m.Files...)
	for i := range m.Files {
		m.Files[i].Match.Values = append([]int(nil), m.Files[i].Match.Values...)
	}
	m.AllowedExtra = append([]string(nil), m.AllowedExtra...)
	mutate(&m)
	return m
}

func partialDaemon(tail string) string { return "heading\n\nexplanation\n\n---\n\n" + tail }
func partialLegacy(tail string) string {
	return "legacy heading\nlegacy explanation one\nlegacy explanation two\n\n" + tail
}

func TestOutputManifestComparator(t *testing.T) {
	// These cases intentionally stay as direct children so each required contract
	// check can be selected with -run TestOutputManifestComparator/<name>.
	snapshot := func(kind string) manifestFile {
		return manifestFile{Name: "x", Presence: "required", Class: "snapshot", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: kind}}
	}
	stream := func() manifestFile {
		return manifestFile{Name: "x", Presence: "required", Class: "stream", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: "line-complete"}}
	}
	partial := manifestMatch{MaxLogicalLines: 400, MaxBytes: 200000}
	checks := map[string]func(*testing.T){
		"class-snapshot-positive": func(t *testing.T) {
			f := snapshot("exact")
			f.Match.Value = "ok"
			requireDirectoryResult(t, f, []byte(" ok\n"), nil, true)
		},
		"class-snapshot-negative": func(t *testing.T) {
			f := snapshot("exact")
			f.Match.Value = "ok"
			requireDirectoryResult(t, f, []byte("bad"), nil, false)
		},
		"class-stream-positive": func(t *testing.T) { requireDirectoryResult(t, stream(), nil, nil, true) },
		"class-stream-negative": func(t *testing.T) { requireDirectoryResult(t, stream(), []byte("x"), nil, false) },
		"class-once-positive": func(t *testing.T) {
			f := snapshot("nonempty")
			f.Class, f.Immutable = "once", true
			requireDirectoryResult(t, f, []byte("x\n"), map[string][]byte{"x": []byte("x\n")}, true)
		},
		"class-once-negative-content": func(t *testing.T) {
			f := snapshot("nonempty")
			f.Class, f.Immutable = "once", true
			requireDirectoryResult(t, f, []byte("y\n"), map[string][]byte{"x": []byte("x\n")}, false)
		},
		"class-once-negative-baseline-missing": func(t *testing.T) {
			f := snapshot("nonempty")
			f.Class, f.Immutable = "once", true
			requireDirectoryResult(t, f, []byte("x\n"), nil, false)
		},
		"class-external-ignored-positive": func(t *testing.T) {
			requireDirectoryResult(t, manifestFile{Name: "x", Presence: "required", Class: "external"}, []byte{0xff, '\r'}, nil, true)
		},
		"match-nonempty-positive": func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x"), nil, true) },
		"match-nonempty-negative": func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), nil, nil, false) },
		"match-int-in-positive": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("0"), nil, true)
		},
		"match-int-in-negative-out-of-set": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("1"), nil, false)
		},
		"match-int-in-negative-not-integer": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("abc"), nil, false)
		},
		"match-regexp-positive": func(t *testing.T) {
			f := snapshot("regexp")
			f.Match.Pattern = "^abc$"
			requireDirectoryResult(t, f, []byte("abc"), nil, true)
		},
		"match-regexp-negative": func(t *testing.T) {
			f := snapshot("regexp")
			f.Match.Pattern = "^abc$"
			requireDirectoryResult(t, f, []byte("xabc"), nil, false)
		},
		"match-line-complete-positive": func(t *testing.T) { requireDirectoryResult(t, stream(), []byte("x\n"), nil, true) },
		"match-line-complete-negative": func(t *testing.T) { requireDirectoryResult(t, stream(), []byte("x"), nil, false) },
		"match-exact-positive": func(t *testing.T) {
			f := snapshot("exact")
			f.Match.Value = "x"
			requireDirectoryResult(t, f, []byte("x"), nil, true)
		},
		"match-exact-negative": func(t *testing.T) {
			f := snapshot("exact")
			f.Match.Value = "x"
			requireDirectoryResult(t, f, []byte("y"), nil, false)
		},
		"match-header-and-tail-positive": func(t *testing.T) {
			f := snapshot("header-and-tail")
			f.Match = partial
			requireDirectoryResult(t, f, []byte(partialDaemon("x")), nil, true)
		},
		"match-header-and-tail-negative": func(t *testing.T) {
			f := snapshot("header-and-tail")
			f.Match = manifestMatch{Kind: "header-and-tail", MaxLogicalLines: 1, MaxBytes: 1}
			requireDirectoryResult(t, f, []byte(partialDaemon("xx")), nil, false)
		},
		"exit-code-plain-positive": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("0"), nil, true)
		},
		"exit-code-trailing-lf-positive": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("0\n"), nil, true)
		},
		"exit-code-surrounding-space-positive": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte(" 0 \n"), nil, true)
		},
		"exit-code-out-of-set-negative": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("1"), nil, false)
		},
		"exit-code-not-integer-negative": func(t *testing.T) {
			f := snapshot("int-in")
			f.Match.Values = []int{0}
			requireDirectoryResult(t, f, []byte("no"), nil, false)
		},
		"recovered-after-timeout-regexp-positive": func(t *testing.T) {
			f := snapshot("regexp")
			f.Name = "recovered-after-timeout"
			f.Match.Pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{4}$"
			requireDirectoryResult(t, f, []byte("2026-08-22T01:02:03+0900"), nil, true)
		},
		"recovered-after-timeout-regexp-negative": func(t *testing.T) {
			f := snapshot("regexp")
			f.Name = "recovered-after-timeout"
			f.Match.Pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{4}$"
			requireDirectoryResult(t, f, []byte("bad"), nil, false)
		},
		"encoding-utf8-ascii-positive":     func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x"), nil, true) },
		"encoding-utf8-multibyte-positive": func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("あ"), nil, true) },
		"encoding-utf8-invalid-negative":   func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte{0xff, 0xfe}, nil, false) },
		"eol-lf-positive":                  func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x\n"), nil, true) },
		"eol-crlf-normalized-positive":     func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x\r\n"), nil, true) },
		"eol-lone-cr-negative":             func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x\ry"), nil, false) },
		"partial-daemon-structure-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialDaemon("tail\n"), manifestMatch{MaxLogicalLines: 1, MaxBytes: 10}); err != nil {
				t.Fatal(err)
			}
		},
		"partial-daemon-400-lines-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialDaemon(strings.Repeat("x\n", 400)), partial); err != nil {
				t.Fatal(err)
			}
		},
		"partial-daemon-401-lines-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialDaemon(strings.Repeat("x\n", 401)), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-daemon-200000-bytes-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialDaemon(strings.Repeat("x", 200000)), partial); err != nil {
				t.Fatal(err)
			}
		},
		"partial-daemon-200001-bytes-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialDaemon(strings.Repeat("x", 200001)), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-legacy-400-lines-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialLegacy(strings.Repeat("x\n", 400)), partial); err != nil {
				t.Fatal(err)
			}
		},
		"partial-legacy-401-lines-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialLegacy(strings.Repeat("x\n", 401)), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-legacy-200000-bytes-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialLegacy(strings.Repeat("x", 200000)), partial); err != nil {
				t.Fatal(err)
			}
		},
		"partial-legacy-200001-bytes-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialLegacy(strings.Repeat("x", 200001)), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-multibyte-200000-bytes-positive": func(t *testing.T) {
			if err := verifyHeaderAndTail("h\n\n"+strings.Repeat("あ", 66666)+"xx", manifestMatch{MaxLogicalLines: 1, MaxBytes: 200000}); err != nil {
				t.Fatal(err)
			}
		},
		"partial-multibyte-200001-bytes-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail("h\n\n"+strings.Repeat("あ", 66666)+"xxx", manifestMatch{MaxLogicalLines: 1, MaxBytes: 200000}); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-via-directory-200000-bytes-positive": func(t *testing.T) {
			f := snapshot("header-and-tail")
			f.Name = "partial-output.md"
			f.Match = manifestMatch{Kind: "header-and-tail", MaxLogicalLines: 400, MaxBytes: 200000}
			requireDirectoryResult(t, f, []byte(partialDaemon(strings.Repeat("x", 200000))), nil, true)
		},
		"partial-via-directory-200001-bytes-negative": func(t *testing.T) {
			f := snapshot("header-and-tail")
			f.Name = "partial-output.md"
			f.Match = manifestMatch{Kind: "header-and-tail", MaxLogicalLines: 400, MaxBytes: 200000}
			requireDirectoryResult(t, f, []byte(partialDaemon(strings.Repeat("x", 200001))), nil, false)
		},
		"partial-header-block-missing-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail("\nbody", manifestMatch{MaxLogicalLines: 1, MaxBytes: 1}); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-legacy-separator-in-tail-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail("legacy heading\n\n"+strings.Repeat("x\n", 200)+"---\n"+strings.Repeat("x\n", 201), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		// Guards the i == 1 condition: with a multi-line legacy header block, the
		// daemon header must not be stripped even when the tail starts with the
		// daemon-looking "line, blank, separator, blank" shape.
		"partial-legacy-multiline-heading-not-daemon-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail(partialLegacy("explanation\n\n---\n\n"+strings.Repeat("x\n", 400)), partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-daemon-without-blank-after-separator-is-legacy-negative": func(t *testing.T) {
			v := "legacy heading\n\nexplanation\n\n---\n" + strings.Repeat("x\n", 400)
			if err := verifyHeaderAndTail(v, partial); err == nil {
				t.Fatal("accepted")
			}
		},
		"partial-daemon-without-blank-after-separator-bytes-negative": func(t *testing.T) {
			if err := verifyHeaderAndTail("h\n\ne\n\n---\n"+strings.Repeat("x", 100), manifestMatch{MaxLogicalLines: 400, MaxBytes: 100}); err == nil {
				t.Fatal("accepted")
			}
		},
	}
	for _, name := range []string{"class-snapshot-positive", "class-snapshot-negative", "class-stream-positive", "class-stream-negative", "class-once-positive", "class-once-negative-content", "class-once-negative-baseline-missing", "class-external-ignored-positive", "match-nonempty-positive", "match-nonempty-negative", "match-int-in-positive", "match-int-in-negative-out-of-set", "match-int-in-negative-not-integer", "match-regexp-positive", "match-regexp-negative", "match-line-complete-positive", "match-line-complete-negative", "match-exact-positive", "match-exact-negative", "match-header-and-tail-positive", "match-header-and-tail-negative", "exit-code-plain-positive", "exit-code-trailing-lf-positive", "exit-code-surrounding-space-positive", "exit-code-out-of-set-negative", "exit-code-not-integer-negative", "recovered-after-timeout-regexp-positive", "recovered-after-timeout-regexp-negative", "encoding-utf8-ascii-positive", "encoding-utf8-multibyte-positive", "encoding-utf8-invalid-negative", "eol-lf-positive", "eol-crlf-normalized-positive", "eol-lone-cr-negative", "partial-daemon-structure-positive", "partial-daemon-400-lines-positive", "partial-daemon-401-lines-negative", "partial-daemon-200000-bytes-positive", "partial-daemon-200001-bytes-negative", "partial-legacy-400-lines-positive", "partial-legacy-401-lines-negative", "partial-legacy-200000-bytes-positive", "partial-legacy-200001-bytes-negative", "partial-multibyte-200000-bytes-positive", "partial-multibyte-200001-bytes-negative", "partial-via-directory-200000-bytes-positive", "partial-via-directory-200001-bytes-negative", "partial-header-block-missing-negative", "partial-legacy-separator-in-tail-negative", "partial-legacy-multiline-heading-not-daemon-negative", "partial-daemon-without-blank-after-separator-is-legacy-negative", "partial-daemon-without-blank-after-separator-bytes-negative"} {
		t.Run(name, checks[name])
	}
	presence := func(t *testing.T, value string, present, want bool) {
		f := snapshot("nonempty")
		f.Presence = value
		if value == "forbidden" {
			f.Class, f.Encoding, f.EOL, f.Match = "", "", "", manifestMatch{}
		}
		if value == "ignored" {
			f.Class, f.Encoding, f.EOL, f.Match = "external", "", "", manifestMatch{}
		}
		dir := t.TempDir()
		if present {
			if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, nil)
		if (err == nil) != want {
			t.Fatalf("err=%v", err)
		}
	}
	for _, tc := range []struct {
		name, value   string
		present, want bool
	}{{"presence-required-absent-negative", "required", false, false}, {"presence-required-present-positive", "required", true, true}, {"presence-forbidden-absent-positive", "forbidden", false, true}, {"presence-forbidden-present-negative", "forbidden", true, false}, {"presence-optional-absent-positive", "optional", false, true}, {"presence-optional-present-positive", "optional", true, true}, {"presence-ignored-absent-positive", "ignored", false, true}, {"presence-ignored-present-positive", "ignored", true, true}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) { presence(t, tc.value, tc.present, tc.want) })
	}
	manifestCases := []struct {
		name   string
		mutate func(*outputManifest)
	}{
		{"manifest-schema-version-negative", func(m *outputManifest) { m.SchemaVersion = 2 }}, {"manifest-duplicate-file-name-negative", func(m *outputManifest) { m.Files[1].Name = m.Files[0].Name }}, {"manifest-allowed-extra-duplicate-negative", func(m *outputManifest) { m.AllowedExtra = append(m.AllowedExtra, m.AllowedExtra[0]) }}, {"manifest-allowed-extra-empty-negative", func(m *outputManifest) { m.AllowedExtra = append(m.AllowedExtra, "") }}, {"manifest-allowed-extra-collides-with-file-negative", func(m *outputManifest) { m.AllowedExtra = append(m.AllowedExtra, m.Files[0].Name) }}, {"manifest-missing-required-entry-negative", func(m *outputManifest) { m.Files[8].Name = "other" }}, {"manifest-unknown-presence-negative", func(m *outputManifest) { m.Files[0].Presence = "bad" }}, {"manifest-unknown-class-negative", func(m *outputManifest) { m.Files[0].Class = "bad" }}, {"manifest-unknown-match-kind-negative", func(m *outputManifest) { m.Files[0].Match.Kind = "bad" }}, {"manifest-unknown-encoding-negative", func(m *outputManifest) { m.Files[0].Encoding = "bad" }}, {"manifest-unknown-eol-negative", func(m *outputManifest) { m.Files[0].EOL = "bad" }}, {"manifest-external-required-unknown-encoding-negative", func(m *outputManifest) { m.Files[6].Presence = "required"; m.Files[6].Encoding = "bad" }}, {"manifest-external-optional-unknown-eol-negative", func(m *outputManifest) { m.Files[6].Presence = "optional"; m.Files[6].EOL = "bad" }}, {"manifest-ignored-unknown-match-kind-negative", func(m *outputManifest) { m.Files[6].Match.Kind = "bad" }}, {"manifest-int-in-values-missing-negative", func(m *outputManifest) { m.Files[1].Match.Values = nil }}, {"manifest-regexp-unanchored-negative", func(m *outputManifest) { m.Files[1].Match = manifestMatch{Kind: "regexp", Pattern: "x"} }}, {"manifest-regexp-uncompilable-negative", func(m *outputManifest) { m.Files[1].Match = manifestMatch{Kind: "regexp", Pattern: "^[$"} }}, {"manifest-header-and-tail-limits-negative", func(m *outputManifest) { m.Files[1].Match = manifestMatch{Kind: "header-and-tail"} }},
	}
	for _, tc := range manifestCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validateManifest(mutatedManifest(t, "research-normal", tc.mutate)); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	t.Run("manifest-all-scenarios-load-positive", func(t *testing.T) {
		for _, s := range []string{"research-normal", "research-recovered", "research-recovery-failed", "review-normal"} {
			if _, err := loadManifest(s); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("manifest-unknown-json-field-negative", func(t *testing.T) {
		if _, err := decodeManifest([]byte(`{"schema_version":1,"unknown":true}`)); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("manifest-trailing-json-negative", func(t *testing.T) {
		for _, b := range [][]byte{[]byte(`{} {}`), []byte(`{} 1`)} {
			if _, err := decodeManifest(b); err == nil {
				t.Fatal("accepted")
			}
		}
	})
	t.Run("directory-declared-regular-file-positive", func(t *testing.T) { requireDirectoryResult(t, snapshot("nonempty"), []byte("x"), nil, true) })
	t.Run("directory-allowed-extra-positive", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "extra"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{AllowedExtra: []string{"extra"}}, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("directory-unexpected-extra-negative", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "extra"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{}, nil); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("directory-symlink-negative", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Symlink("target", filepath.Join(dir, "x")); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{snapshot("nonempty")}}, nil); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("directory-entry-is-directory-negative", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "x"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{snapshot("nonempty")}}, nil); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("directory-real-manifest-positive", func(t *testing.T) {
		m, err := loadManifest("research-recovery-failed")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		write := func(n, s string) {
			if err := os.WriteFile(filepath.Join(dir, n), []byte(s), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		write("prompt.md", "prompt\n")
		write("exit-code", "6\n")
		write("stdout.log", "")
		write("stderr.log", "err\n")
		write("partial-output.md", partialDaemon("tail\n"))
		if err := verifyOutputDirectory(dir, m, map[string][]byte{"prompt.md": []byte("prompt\n")}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("normalization-crlf-positive", func(t *testing.T) {
		if got := normalizeOutput([]byte("a\r\nb"), "", ""); got != "a\nb" {
			t.Fatal(got)
		}
	})
	t.Run("normalization-crlf-negative", func(t *testing.T) {
		if got := normalizeOutput([]byte("a\rb"), "", ""); !strings.Contains(got, "\r") {
			t.Fatal("lone CR removed")
		}
	})
	t.Run("normalization-ansi-positive", func(t *testing.T) {
		if got := normalizeOutput([]byte("\x1b[31mx\x1b[0m"), "", ""); got != "x" {
			t.Fatal(got)
		}
	})
	t.Run("normalization-ansi-negative", func(t *testing.T) {
		if got := normalizeOutput([]byte("[31mx"), "", ""); got != "[31mx" {
			t.Fatal(got)
		}
	})
	t.Run("normalization-task-root-positive", func(t *testing.T) {
		if got := normalizeOutput([]byte("id root"), "id", "root"); got != "<TASK-ID> <TASKS-ROOT>" {
			t.Fatal(got)
		}
	})
	t.Run("normalization-task-root-negative", func(t *testing.T) {
		if got := normalizeOutput([]byte("id root"), "", ""); got != "id root" {
			t.Fatal(got)
		}
	})
	t.Run("normalization-timestamp-positive", func(t *testing.T) {
		for _, s := range []string{"2026-08-22T01:02:03+0900", "2026-08-22 01:02:03", "2026-08-22T01:02:03Z"} {
			if normalizeOutput([]byte(s), "", "") != "<TS>" {
				t.Fatal(s)
			}
		}
	})
	t.Run("normalization-timestamp-negative", func(t *testing.T) {
		if normalizeOutput([]byte("2026-08-22"), "", "") != "2026-08-22" {
			t.Fatal("date replaced")
		}
	})
	t.Run("normalization-session-positive", func(t *testing.T) {
		if normalizeOutput([]byte("session id: 123e4567-e89b-12d3-a456-426614174000"), "", "") != "session id: <UUID>" {
			t.Fatal("uuid not replaced")
		}
	})
	t.Run("normalization-session-negative", func(t *testing.T) {
		if normalizeOutput([]byte("session id: nope"), "", "") != "session id: nope" {
			t.Fatal("bad session replaced")
		}
	})
	t.Run("normalization-pid-positive", func(t *testing.T) {
		if normalizeOutput([]byte("pid=42"), "", "") != "pid=<PID>" {
			t.Fatal("pid not replaced")
		}
	})
	t.Run("normalization-pid-negative", func(t *testing.T) {
		if normalizeOutput([]byte("xpid=42 pid=abc"), "", "") != "xpid=42 pid=abc" {
			t.Fatal("pid boundary")
		}
	})
	t.Run("normalization", func(t *testing.T) {
		got := normalizeOutput([]byte("a\r\n\x1b[31mx\x1b[0m research-20260822-010203-a1b2-x /tmp/root 2026-08-22T01:02:03+0900 session id: 123e4567-e89b-12d3-a456-426614174000 pid=42"), "research-20260822-010203-a1b2-x", "/tmp/root")
		for _, want := range []string{"a\nx", "<TASK-ID>", "<TASKS-ROOT>", "<TS>", "session id: <UUID>", "pid=<PID>"} {
			if !strings.Contains(got, want) {
				t.Fatalf("normalization missing %q: %q", want, got)
			}
		}
		if strings.Contains(normalizeOutput([]byte("xpid=abc session id: nope"), "", ""), "<PID>") {
			t.Fatal("over-normalized boundary text")
		}
	})
	t.Run("header-and-tail-boundaries", func(t *testing.T) {
		match := manifestMatch{MaxLogicalLines: 400, MaxBytes: 200000}
		production := "## heading\n\nexplanation\n\n---\n\n"
		if err := verifyHeaderAndTail(production+strings.Repeat("x\n", 400), match); err != nil {
			t.Fatal(err)
		}
		if err := verifyHeaderAndTail(production+strings.Repeat("x\n", 401), match); err == nil {
			t.Fatal("401 lines accepted")
		}
		if err := verifyHeaderAndTail(production+strings.Repeat("x", 200000), match); err != nil {
			t.Fatal(err)
		}
		if err := verifyHeaderAndTail(production+strings.Repeat("x", 200001), match); err == nil {
			t.Fatal("200001 bytes accepted")
		}
	})
	t.Run("header-and-tail-legacy-separator-in-tail-negative", func(t *testing.T) {
		m := manifestMatch{MaxLogicalLines: 400, MaxBytes: 200000}
		v := "legacy heading\n\n" + strings.Repeat("x\n", 200) + "---\n" + strings.Repeat("x\n", 201)
		if err := verifyHeaderAndTail(v, m); err == nil {
			t.Fatal("legacy tail over limit accepted")
		}
	})
	t.Run("header-and-tail-daemon-structure-positive", func(t *testing.T) {
		if err := verifyHeaderAndTail("heading\n\nexplanation\n\n---\n\ntail\n", manifestMatch{MaxLogicalLines: 1, MaxBytes: 10}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("header-and-tail-missing-negative", func(t *testing.T) {
		if err := verifyHeaderAndTail("\nbody", manifestMatch{MaxLogicalLines: 1, MaxBytes: 1}); err == nil {
			t.Fatal("missing header accepted")
		}
	})
	t.Run("header-and-tail-multibyte-byte-boundaries", func(t *testing.T) {
		m := manifestMatch{MaxLogicalLines: 1, MaxBytes: 200000}
		h := "heading\n\n"
		good := strings.Repeat("あ", 66666) + "xx"
		if err := verifyHeaderAndTail(h+good, m); err != nil {
			t.Fatal(err)
		}
		if err := verifyHeaderAndTail(h+good+"x", m); err == nil {
			t.Fatal("200001 byte tail accepted")
		}
	})
	t.Run("manifest-validation", func(t *testing.T) {
		m, err := loadManifest("research-normal")
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			mutate func(*outputManifest)
		}{{"unknown encoding", func(x *outputManifest) { x.Files[0].Encoding = "utf-8" }}, {"unknown eol", func(x *outputManifest) { x.Files[0].EOL = "LF" }}, {"duplicate", func(x *outputManifest) { x.Files[1].Name = x.Files[0].Name }}, {"missing parameter", func(x *outputManifest) { x.Files[1].Match.Values = nil }}, {"external unknown encoding", func(x *outputManifest) { x.Files[6].Presence = "required"; x.Files[6].Encoding = "utf-8" }}, {"external unknown eol", func(x *outputManifest) { x.Files[6].Presence = "optional"; x.Files[6].EOL = "LF" }}, {"ignored unknown match", func(x *outputManifest) { x.Files[6].Match.Kind = "bogus" }}, {"unanchored regexp", func(x *outputManifest) { x.Files[1].Match = manifestMatch{Kind: "regexp", Pattern: "x"} }}, {"header limit", func(x *outputManifest) {
			x.Files[1].Match = manifestMatch{Kind: "header-and-tail", MaxBytes: 0, MaxLogicalLines: 0}
		}}} {
			t.Run(tc.name, func(t *testing.T) {
				copyM := m
				copyM.Files = append([]manifestFile(nil), m.Files...)
				tc.mutate(&copyM)
				if err := validateManifest(copyM); err == nil {
					t.Fatal("invalid manifest accepted")
				}
			})
		}
	})
	t.Run("manifest-json-positive-and-negative", func(t *testing.T) {
		if _, err := loadManifest("research-normal"); err != nil {
			t.Fatal(err)
		}
		for _, b := range [][]byte{[]byte(`{"schema_version":1,"unknown":true}`), []byte(`{} {}`), []byte(`{} 1`)} {
			if _, err := decodeManifest(b); err == nil {
				t.Fatal("invalid JSON accepted")
			}
		}
	})
	t.Run("directory-rules", func(t *testing.T) {
		m, err := loadManifest("research-recovery-failed")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		write := func(n, s string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(dir, n), []byte(s), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		write("prompt.md", "prompt\n")
		write("exit-code", "6\n")
		write("stderr.log", "err\n")
		write("stdout.log", "")
		write("partial-output.md", "header\n\nbody\n")
		base := map[string][]byte{"prompt.md": []byte("prompt\n")}
		if err := verifyOutputDirectory(dir, m, base); err != nil {
			t.Fatal(err)
		}
		write("recovered-after-timeout", "bad\rvalue\n")
		m2 := m
		for i := range m2.Files {
			if m2.Files[i].Name == "recovered-after-timeout" {
				m2.Files[i].Presence = "optional"
				m2.Files[i].Class = "snapshot"
				m2.Files[i].Encoding = "utf8"
				m2.Files[i].EOL = "lf"
				m2.Files[i].Match = manifestMatch{Kind: "nonempty"}
			}
		}
		if err := verifyOutputDirectory(dir, m2, base); err == nil {
			t.Fatal("snapshot lone CR accepted")
		}
	})
	t.Run("class-and-match-matrix", func(t *testing.T) {
		check := func(t *testing.T, f manifestFile, data []byte, baseline map[string][]byte, want bool) {
			t.Helper()
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, f.Name), data, 0o600); err != nil {
				t.Fatal(err)
			}
			err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, baseline)
			if (err == nil) != want {
				t.Fatalf("err=%v want=%v", err, want)
			}
		}
		t.Run("snapshot-positive-and-negative", func(t *testing.T) {
			f := manifestFile{Name: "x", Presence: "required", Class: "snapshot", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: "exact", Value: "ok"}}
			check(t, f, []byte(" ok\n"), nil, true)
			check(t, f, []byte("bad"), nil, false)
		})
		t.Run("stream-positive-and-negative", func(t *testing.T) {
			f := manifestFile{Name: "x", Presence: "required", Class: "stream", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: "line-complete"}}
			check(t, f, []byte(""), nil, true)
			check(t, f, []byte("x"), nil, false)
		})
		t.Run("once-positive-and-negative", func(t *testing.T) {
			f := manifestFile{Name: "x", Presence: "required", Class: "once", Encoding: "utf8", EOL: "lf", Immutable: true, Match: manifestMatch{Kind: "nonempty"}}
			check(t, f, []byte("x\n"), map[string][]byte{"x": []byte("x\n")}, true)
			check(t, f, []byte("y\n"), map[string][]byte{"x": []byte("x\n")}, false)
		})
		t.Run("external-positive", func(t *testing.T) {
			f := manifestFile{Name: "x", Presence: "required", Class: "external"}
			check(t, f, []byte{0xff, '\r'}, nil, true)
		})
		for _, tc := range []struct {
			name, kind, good, bad string
			values                []int
		}{{"nonempty", "nonempty", "x", "", nil}, {"int-in", "int-in", "0", "1", []int{0}}, {"regexp", "regexp", "abc", "xabc", nil}, {"line-complete", "line-complete", "x\n", "x", nil}, {"exact", "exact", "x", "y", nil}} {
			tc := tc
			t.Run("match-"+tc.name, func(t *testing.T) {
				class := "snapshot"
				if tc.kind == "line-complete" {
					class = "stream"
				}
				f := manifestFile{Name: "x", Presence: "required", Class: class, Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: tc.kind, Values: tc.values, Pattern: "^abc$", Value: "x"}}
				check(t, f, []byte(tc.good), nil, true)
				check(t, f, []byte(tc.bad), nil, false)
			})
		}
	})
	t.Run("presence-matrix", func(t *testing.T) {
		for _, tc := range []struct {
			name, presence string
			present, want  bool
		}{{"required-absent", "required", false, false}, {"required-present", "required", true, true}, {"forbidden-absent", "forbidden", false, true}, {"forbidden-present", "forbidden", true, false}, {"optional-absent", "optional", false, true}, {"optional-present", "optional", true, true}, {"ignored-absent", "ignored", false, true}, {"ignored-present", "ignored", true, true}} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				f := manifestFile{Name: "x", Presence: tc.presence, Class: "snapshot", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: "nonempty"}}
				if tc.presence == "forbidden" {
					f.Class = ""
					f.Encoding = ""
					f.EOL = ""
					f.Match = manifestMatch{}
				}
				if tc.presence == "ignored" {
					f.Class = "external"
					f.Encoding = ""
					f.EOL = ""
					f.Match = manifestMatch{}
				}
				if tc.present {
					if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, nil)
				if (err == nil) != tc.want {
					t.Fatalf("err=%v", err)
				}
			})
		}
	})
	t.Run("directory-safety", func(t *testing.T) {
		f := manifestFile{Name: "x", Presence: "required", Class: "snapshot", Encoding: "utf8", EOL: "lf", Match: manifestMatch{Kind: "nonempty"}}
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "extra"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, nil); err == nil {
			t.Fatal("extra accepted")
		}
		if err := os.Remove(filepath.Join(dir, "extra")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, "x")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("target", filepath.Join(dir, "x")); err != nil {
			t.Fatal(err)
		}
		if err := verifyOutputDirectory(dir, outputManifest{Files: []manifestFile{f}}, nil); err == nil {
			t.Fatal("symlink accepted")
		}
	})
}
