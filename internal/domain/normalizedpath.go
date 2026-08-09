package domain

import (
	"encoding/json"
	"fmt"
	"path/filepath"
)

// NewNormalizedPath validates a result already normalized by the later I/O-layer NormalizePath function; it performs no I/O.
type NormalizedPath struct{ value string }

func NewNormalizedPath(value string) (NormalizedPath, error) {
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value || (value != "/" && value[len(value)-1] == filepath.Separator) {
		return NormalizedPath{}, fmt.Errorf("invalid normalized path")
	}
	return NormalizedPath{value}, nil
}
func (v NormalizedPath) String() string               { return v.value }
func (v NormalizedPath) MarshalJSON() ([]byte, error) { return json.Marshal(v.value) }
func (v *NormalizedPath) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewNormalizedPath(raw)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
