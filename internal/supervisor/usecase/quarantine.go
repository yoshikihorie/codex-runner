package usecase

import "github.com/yoshikihorie/codex-runner/internal/supervisor"

func bindQuarantine(source any, report func(supervisor.SessionID)) {
	if source, ok := source.(interface {
		SetQuarantineHandler(func(supervisor.SessionID))
	}); ok {
		source.SetQuarantineHandler(report)
	}
}
