package usecase

import (
	"bytes"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestNewTaskIDUsesExpectedFormat(t *testing.T) {
	slug, err := domain.NewSlug("submit-task")
	if err != nil {
		t.Fatal(err)
	}
	id, err := newTaskID(domain.SubcommandImpl, slug, time.Date(2026, 8, 9, 12, 34, 56, 0, time.UTC), bytes.NewReader([]byte{0xab, 0xcd}))
	if err != nil || id.String() != "impl-20260809-123456-abcd-submit-task" {
		t.Fatalf("id=%s err=%v", id.String(), err)
	}
}
