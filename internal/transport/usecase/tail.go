package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

var errTailFromSeqInvalid = errors.New("tail from_seq is invalid")

// TailTaskUseCase replays task progress and establishes the live-follow subscription point.
type TailTaskUseCase struct {
	provider transport.TaskSnapshotProvider
	events   store.EventReader
	notifier execution.TaskChangeNotifier
}

// NewTailTaskUseCase creates a tail use case with its required dependencies.
func NewTailTaskUseCase(provider transport.TaskSnapshotProvider, events store.EventReader, notifier execution.TaskChangeNotifier) *TailTaskUseCase {
	if isNilStatusUseCaseDependency(provider) || isNilStatusUseCaseDependency(events) || isNilStatusUseCaseDependency(notifier) {
		panic("tail task use case requires non-nil dependencies")
	}
	return &TailTaskUseCase{provider: provider, events: events, notifier: notifier}
}

// Handle validates wire input and writes protocol responses to out.
func (uc *TailTaskUseCase) Handle(ctx context.Context, req transport.Request, out io.Writer) error {
	id, err := domain.NewTaskID(req.TaskID)
	if err != nil {
		return writeTailError(out, req.RequestID, "TASK_ID_INVALID_FORMAT", "error.task.idInvalidFormat", map[string]any{"task_id": req.TaskID})
	}

	fromSeq, rawFromSeq, err := tailFromSeq(req.Params)
	if err != nil {
		return writeTailError(out, req.RequestID, "TAIL_FROM_SEQ_INVALID", "error.tail.fromSeqInvalid", map[string]any{"from_seq": rawFromSeq})
	}

	writer := transport.NewProgressWriter(out, req.RequestID)
	err = uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: fromSeq}, writer)
	if errors.Is(err, domain.ErrTaskNotFound) {
		return writeTailError(out, req.RequestID, "TASK_NOT_FOUND", "error.task.notFound", map[string]any{"task_id": id.String()})
	}
	return err
}

// Execute replays saved progress and establishes a subscription for non-terminal tasks.
func (uc *TailTaskUseCase) Execute(_ context.Context, in schema.TailTaskInput, out schema.ProgressWriter) error {
	snapshot, err := uc.provider.Snapshot(in.TaskID)
	if err != nil {
		return err
	}

	session := &tailSession{
		taskID:    in.TaskID,
		taskState: snapshot.State,
		nextSeq:   in.FromSeq,
	}
	if session.taskState.IsTerminal() {
		if err := replayTailHistory(uc.events, session, out); err != nil {
			return err
		}
		return out.WriteComplete(schema.CompleteLine{
			LineType:  schema.LineTypeComplete,
			Reason:    schema.CompleteReasonTaskTerminal,
			TaskState: session.taskState,
			LastSeq:   session.lastDeliveredSeq,
		})
	}

	changes, unsubscribe := uc.notifier.Subscribe(in.TaskID)
	session.changes = changes
	defer unsubscribe()
	return replayTailHistory(uc.events, session, out)
}

type tailSession struct {
	taskID           domain.TaskID
	taskState        domain.TaskState
	nextSeq          int
	lastDeliveredSeq int
	changes          <-chan struct{}
}

func replayTailHistory(events store.EventReader, session *tailSession, out schema.ProgressWriter) error {
	records, err := events.ReadFrom(session.taskID, session.nextSeq)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := out.WriteProgress(schema.ProgressLine{
			LineType:   schema.LineTypeProgress,
			Seq:        record.Seq,
			RecordedAt: record.RecordedAt,
			EventType:  record.EventType,
			Raw:        record.Raw,
			TaskState:  session.taskState,
		}); err != nil {
			return err
		}
		session.nextSeq = record.Seq + 1
		session.lastDeliveredSeq = record.Seq
	}
	return nil
}

func tailFromSeq(params json.RawMessage) (int, any, error) {
	if len(params) == 0 {
		return 1, nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return 0, nil, errTailFromSeqInvalid
	}

	var input map[string]json.RawMessage
	if err := json.Unmarshal(params, &input); err != nil {
		return 0, nil, errTailFromSeqInvalid
	}
	rawFromSeq, ok := input["from_seq"]
	if !ok {
		return 1, nil, nil
	}

	var detail any
	decoder := json.NewDecoder(bytes.NewReader(rawFromSeq))
	decoder.UseNumber()
	if err := decoder.Decode(&detail); err != nil {
		return 0, nil, errTailFromSeqInvalid
	}
	var fromSeq int
	if err := json.Unmarshal(rawFromSeq, &fromSeq); err != nil || fromSeq < 1 {
		return 0, detail, errTailFromSeqInvalid
	}
	return fromSeq, detail, nil
}

func writeTailError(out io.Writer, requestID, code, messageKey string, detail map[string]any) error {
	return json.NewEncoder(out).Encode(statusErrorResponse(requestID, code, messageKey, detail))
}
