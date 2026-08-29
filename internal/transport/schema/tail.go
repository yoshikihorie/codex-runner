package schema

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	LineTypeProgress           = "progress"
	LineTypeComplete           = "complete"
	CompleteReasonTaskTerminal = "task-terminal"
	CompleteReasonIdleTimeout  = "idle-timeout"
)

type TailTaskInput struct {
	TaskID  domain.TaskID `json:"task_id"`
	FromSeq int           `json:"from_seq,omitempty"`
}

type ProgressLine struct {
	LineType   string           `json:"line_type"`
	Seq        int              `json:"seq"`
	RecordedAt time.Time        `json:"recorded_at"`
	EventType  string           `json:"event_type"`
	Raw        any              `json:"raw"`
	TaskState  domain.TaskState `json:"task_state"`
	Truncated  bool             `json:"truncated,omitempty"`
	RawBytes   int              `json:"raw_bytes,omitempty"`
}

type CompleteLine struct {
	LineType  string           `json:"line_type"`
	Reason    string           `json:"reason"`
	TaskState domain.TaskState `json:"task_state"`
	LastSeq   int              `json:"last_seq"`
}

type ProgressWriter interface {
	WriteProgress(line ProgressLine) error
	WriteComplete(line CompleteLine) error
}
