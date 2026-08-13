package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

const daemonAuthoritativeStateBasis = "daemon-authoritative"

type GetTaskStatusInput struct {
	TaskID domain.TaskID
}

type GetTaskStatusOutput struct {
	View     TaskStatusView
	NotFound bool
}

type TaskStatusView struct {
	TaskID                 domain.TaskID         `json:"task_id"`
	Subcommand             domain.Subcommand     `json:"subcommand"`
	State                  domain.TaskState      `json:"state"`
	LivenessConfirmed      bool                  `json:"liveness_confirmed"`
	StateBasis             string                `json:"state_basis"`
	StateUpdatedAt         time.Time             `json:"state_updated_at"`
	Model                  string                `json:"model"`
	ReasoningEffort        *string               `json:"reasoning_effort"`
	ResolvedTimeoutSeconds int                   `json:"resolved_timeout_seconds"`
	RequestedAt            time.Time             `json:"requested_at"`
	LastEventAt            *time.Time            `json:"last_event_at"`
	GapSeconds             *int                  `json:"gap_seconds"`
	ExitCode               *domain.ExitCode      `json:"exit_code"`
	Recovered              bool                  `json:"recovered"`
	AdoptedAfterRestart    bool                  `json:"adopted_after_restart"`
	Route                  domain.ExecutionRoute `json:"route"`
	QueuePosition          *int                  `json:"queue_position"`
	PresumedActive         *bool                 `json:"presumed_active"`
}

type GetTaskStatusUseCase struct {
	provider transport.TaskSnapshotProvider
	clock    domain.Clock
	logger   *slog.Logger
}

func NewGetTaskStatusUseCase(provider transport.TaskSnapshotProvider, clock domain.Clock, loggers ...*slog.Logger) *GetTaskStatusUseCase {
	if isNilStatusUseCaseDependency(provider) || isNilStatusUseCaseDependency(clock) {
		panic("get task status use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("get task status use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &GetTaskStatusUseCase{provider: provider, clock: clock, logger: logger}
}

func (uc *GetTaskStatusUseCase) Execute(_ context.Context, in GetTaskStatusInput) (GetTaskStatusOutput, error) {
	snapshot, err := uc.provider.Snapshot(in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return GetTaskStatusOutput{NotFound: true}, nil
		}
		return GetTaskStatusOutput{}, err
	}
	view := taskStatusView(snapshot, uc.clock.Now())
	if snapshot.State == domain.StateQueued {
		position, found, err := uc.provider.QueuePosition(in.TaskID)
		if err != nil {
			return GetTaskStatusOutput{}, err
		}
		if found {
			view.QueuePosition = &position
		}
	}
	return GetTaskStatusOutput{View: view}, nil
}

func taskStatusView(snapshot domain.TaskSnapshot, now time.Time) TaskStatusView {
	view := TaskStatusView{
		TaskID:                 snapshot.TaskID,
		Subcommand:             snapshot.Subcommand,
		State:                  snapshot.State,
		LivenessConfirmed:      true,
		StateBasis:             daemonAuthoritativeStateBasis,
		StateUpdatedAt:         snapshot.StateUpdatedAt,
		Model:                  snapshot.Model,
		ReasoningEffort:        snapshot.ReasoningEffort,
		ResolvedTimeoutSeconds: snapshot.ResolvedTimeoutSeconds,
		RequestedAt:            snapshot.RequestedAt,
		LastEventAt:            snapshot.LastEventAt,
		ExitCode:               snapshot.ExitCode,
		Recovered:              snapshot.Recovered,
		AdoptedAfterRestart:    snapshot.AdoptedAfterRestart,
		Route:                  snapshot.Route,
	}
	if snapshot.State == domain.StateStalled {
		basis := snapshot.ProcessStartedAt
		if snapshot.LastEventAt != nil {
			basis = snapshot.LastEventAt
		}
		if basis != nil {
			gap := int(now.Sub(*basis).Seconds())
			if gap < 0 {
				gap = 0
			}
			view.GapSeconds = &gap
		}
	}
	return view
}

func (uc *GetTaskStatusUseCase) Handle(req transport.Request) transport.Response {
	id, err := domain.NewTaskID(req.TaskID)
	if err != nil {
		return statusErrorResponse(req.RequestID, "TASK_ID_INVALID_FORMAT", "error.task.idInvalidFormat", map[string]any{"task_id": req.TaskID})
	}
	out, err := uc.Execute(context.Background(), GetTaskStatusInput{TaskID: id})
	if err != nil {
		return uc.statusMappedError(req.RequestID, id, err)
	}
	if out.NotFound {
		return statusErrorResponse(req.RequestID, "TASK_NOT_FOUND", "error.task.notFound", map[string]any{"task_id": id.String()})
	}
	body, err := json.Marshal(out.View)
	if err != nil {
		panic(fmt.Errorf("marshal status response: %w", err))
	}
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: body}
}

func (uc *GetTaskStatusUseCase) statusMappedError(requestID string, id domain.TaskID, err error) transport.Response {
	uc.logger.Warn("task status read failed", "task_id", id.String(), "error", err.Error())
	return statusErrorResponse(requestID, "TASK_STATUS_READ_FAILED", "error.task.statusReadFailed", map[string]any{"task_id": id.String()})
}

func statusErrorResponse(requestID, code, message string, detail map[string]any) transport.Response {
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: requestID, OK: false, Error: &transport.ErrorBody{Code: code, MessageKey: message, Detail: detail}}
}

func isNilStatusUseCaseDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
