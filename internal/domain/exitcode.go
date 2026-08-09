package domain

import "encoding/json"

type ExitCode struct {
	raw   int
	class ExitCodeClass
}

func NewExitCode(raw int) ExitCode {
	c := ExitCodeClassFailure
	switch raw {
	case 0:
		c = ExitCodeClassSuccess
	case 6, 124, 137:
		c = ExitCodeClassTimeout
	case 130:
		c = ExitCodeClassCancelled
	}
	return ExitCode{raw, c}
}
func (v ExitCode) Raw() int             { return v.raw }
func (v ExitCode) Class() ExitCodeClass { return v.class }
func (v ExitCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Raw   int           `json:"raw"`
		Class ExitCodeClass `json:"class"`
	}{v.raw, v.class})
}
func (v *ExitCode) UnmarshalJSON(data []byte) error {
	var raw struct {
		Raw int `json:"raw"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*v = NewExitCode(raw.Raw)
	return nil
}
