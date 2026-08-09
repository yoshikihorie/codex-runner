package usecase

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const taskIDSuffixBytes = 2

var errTaskIDRandomRead = errors.New("task ID random read failed")

func newTaskID(subcommand domain.Subcommand, slug domain.Slug, at time.Time, reader io.Reader) (domain.TaskID, error) {
	bytes := make([]byte, taskIDSuffixBytes)
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return domain.TaskID{}, fmt.Errorf("%w: %v", errTaskIDRandomRead, err)
	}
	value := fmt.Sprintf("%s-%s-%s-%s-%s", subcommand, at.Format("20060102"), at.Format("150405"), hex.EncodeToString(bytes), slug.String())
	return domain.NewTaskID(value)
}

func productionTaskIDReader() io.Reader { return rand.Reader }
