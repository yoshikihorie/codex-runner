package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
)

const slugMaxLength = 40

var slugPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

type Slug struct{ value string }

func NewSlug(value string) (Slug, error) {
	if len(value) == 0 || len(value) > slugMaxLength || !slugPattern.MatchString(value) {
		return Slug{}, fmt.Errorf("invalid slug")
	}
	return Slug{value}, nil
}
func (s Slug) String() string               { return s.value }
func (s Slug) MarshalJSON() ([]byte, error) { return json.Marshal(s.value) }
func (s *Slug) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewSlug(raw)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}
