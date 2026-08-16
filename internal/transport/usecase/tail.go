package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

var errTailFromSeqInvalid = errors.New("tail from_seq is invalid")

const (
	tailIdleTimeout = 1500 * time.Second
	// TAIL_TERMINAL_DRAIN_RETRY_INTERVAL_MS
	tailTerminalDrainRetryInterval = 250 * time.Millisecond
	// TAIL_TERMINAL_DRAIN_MAX_WAIT_MS
	tailTerminalDrainMaxWait = 2000 * time.Millisecond
)

type tailTimerFactory func(time.Duration) (<-chan time.Time, func())

// TailTaskUseCase replays task progress and establishes the live-follow subscription point.
type TailTaskUseCase struct {
	provider transport.TaskSnapshotProvider
	events   store.EventReader
	notifier execution.TaskChangeNotifier

	timerFactory tailTimerFactory
}

// NewTailTaskUseCase creates a tail use case with its required dependencies.
func NewTailTaskUseCase(provider transport.TaskSnapshotProvider, events store.EventReader, notifier execution.TaskChangeNotifier) *TailTaskUseCase {
	if isNilStatusUseCaseDependency(provider) || isNilStatusUseCaseDependency(events) || isNilStatusUseCaseDependency(notifier) {
		panic("tail task use case requires non-nil dependencies")
	}
	return &TailTaskUseCase{
		provider:     provider,
		events:       events,
		notifier:     notifier,
		timerFactory: newTailTimer,
	}
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

// Execute replays saved progress and follows changes for non-terminal tasks.
func (uc *TailTaskUseCase) Execute(ctx context.Context, in schema.TailTaskInput, out schema.ProgressWriter) error {
	snapshot, err := uc.provider.Snapshot(in.TaskID)
	if err != nil {
		return err
	}

	session := &tailSession{
		taskID:    in.TaskID,
		taskState: snapshot.State,
		nextSeq:   in.FromSeq,
	}
	defer session.stopTimers()

	if session.taskState.IsTerminal() {
		uc.enterTerminalDrain(session)
		if _, err := uc.replayUntilEmpty(ctx, session, out, nil); err != nil {
			return err
		}
		session.startDrainRetry(uc.timerFactory)
	} else {
		changes, unsubscribe := uc.notifier.Subscribe(in.TaskID)
		session.changes = changes
		defer unsubscribe()

		if _, err := replayTailHistoryWithCallback(ctx, uc.events, session, out, nil); err != nil {
			return err
		}
		session.startIdle(uc.timerFactory)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.changes:
			done, err := uc.followChange(ctx, session, out, false)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case <-session.idleTimer:
			done, err := uc.followChange(ctx, session, out, true)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case <-session.drainRetryTimer:
			done, err := uc.followTerminalDrain(ctx, session, out)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case <-session.drainMaximumTimer:
			done, err := uc.finishTerminalDrain(ctx, session, out)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

type tailSession struct {
	taskID           domain.TaskID
	taskState        domain.TaskState
	nextSeq          int
	lastDeliveredSeq int
	changes          <-chan struct{}

	idleTimer             <-chan time.Time
	stopIdleTimer         func()
	drainRetryTimer       <-chan time.Time
	stopDrainRetryTimer   func()
	drainMaximumTimer     <-chan time.Time
	stopDrainMaximumTimer func()
	terminalPending       bool
}

func replayTailHistory(ctx context.Context, events store.EventReader, session *tailSession, out schema.ProgressWriter) error {
	_, err := replayTailHistoryWithCallback(ctx, events, session, out, nil)
	return err
}

func replayTailHistoryWithCallback(ctx context.Context, events store.EventReader, session *tailSession, out schema.ProgressWriter, afterProgress func()) (int, error) {
	records, err := events.ReadFrom(session.taskID, session.nextSeq)
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		if err := out.WriteProgress(schema.ProgressLine{
			LineType:   schema.LineTypeProgress,
			Seq:        record.Seq,
			RecordedAt: record.RecordedAt,
			EventType:  record.EventType,
			Raw:        record.Raw,
			TaskState:  session.taskState,
		}); err != nil {
			return delivered, err
		}
		session.nextSeq = record.Seq + 1
		session.lastDeliveredSeq = record.Seq
		delivered++
		if afterProgress != nil {
			afterProgress()
		}
	}
	return delivered, nil
}

func newTailTimer(duration time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(duration)
	return timer.C, func() { timer.Stop() }
}

func (session *tailSession) startIdle(factory tailTimerFactory) {
	session.stopIdle()
	session.idleTimer, session.stopIdleTimer = factory(tailIdleTimeout)
}

func (session *tailSession) stopIdle() {
	if session.stopIdleTimer != nil {
		session.stopIdleTimer()
	}
	session.idleTimer = nil
	session.stopIdleTimer = nil
}

func (session *tailSession) startDrainRetry(factory tailTimerFactory) {
	session.stopDrainRetry()
	session.drainRetryTimer, session.stopDrainRetryTimer = factory(tailTerminalDrainRetryInterval)
}

func (session *tailSession) stopDrainRetry() {
	if session.stopDrainRetryTimer != nil {
		session.stopDrainRetryTimer()
	}
	session.drainRetryTimer = nil
	session.stopDrainRetryTimer = nil
}

func (session *tailSession) startDrainMaximum(factory tailTimerFactory) {
	if session.drainMaximumTimer != nil {
		return
	}
	session.drainMaximumTimer, session.stopDrainMaximumTimer = factory(tailTerminalDrainMaxWait)
}

func (session *tailSession) stopDrainMaximum() {
	if session.stopDrainMaximumTimer != nil {
		session.stopDrainMaximumTimer()
	}
	session.drainMaximumTimer = nil
	session.stopDrainMaximumTimer = nil
}

func (session *tailSession) stopTimers() {
	session.stopIdle()
	session.stopDrainRetry()
	session.stopDrainMaximum()
}

func (uc *TailTaskUseCase) replayUntilEmpty(ctx context.Context, session *tailSession, out schema.ProgressWriter, afterProgress func()) (int, error) {
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		delivered, err := replayTailHistoryWithCallback(ctx, uc.events, session, out, afterProgress)
		if err != nil {
			return total, err
		}
		total += delivered
		if delivered == 0 {
			return total, nil
		}
	}
}

func (uc *TailTaskUseCase) enterTerminalDrain(session *tailSession) {
	session.terminalPending = true
	session.stopIdle()
	session.startDrainMaximum(uc.timerFactory)
}

func (uc *TailTaskUseCase) followChange(ctx context.Context, session *tailSession, out schema.ProgressWriter, fromIdle bool) (bool, error) {
	if session.terminalPending {
		snapshot, err := uc.provider.Snapshot(session.taskID)
		if err != nil {
			return false, err
		}
		session.taskState = snapshot.State
		delivered, err := uc.replayUntilEmpty(ctx, session, out, nil)
		if err != nil {
			return false, err
		}
		if !session.taskState.IsTerminal() {
			session.terminalPending = false
			session.stopDrainRetry()
			session.stopDrainMaximum()
			session.startIdle(uc.timerFactory)
			return false, nil
		}
		if delivered == 0 {
			return true, uc.writeTerminalComplete(ctx, session, out)
		}
		session.startDrainRetry(uc.timerFactory)
		return false, nil
	}

	delivered, err := uc.replayUntilEmpty(ctx, session, out, func() { session.startIdle(uc.timerFactory) })
	if err != nil {
		return false, err
	}
	snapshot, err := uc.provider.Snapshot(session.taskID)
	if err != nil {
		return false, err
	}
	session.taskState = snapshot.State
	if session.taskState.IsTerminal() {
		uc.enterTerminalDrain(session)
		session.startDrainRetry(uc.timerFactory)
		return false, nil
	}
	if fromIdle && delivered == 0 {
		return true, out.WriteComplete(schema.CompleteLine{
			LineType:  schema.LineTypeComplete,
			Reason:    schema.CompleteReasonIdleTimeout,
			TaskState: session.taskState,
			LastSeq:   session.lastDeliveredSeq,
		})
	}
	return false, nil
}

func (uc *TailTaskUseCase) followTerminalDrain(ctx context.Context, session *tailSession, out schema.ProgressWriter) (bool, error) {
	snapshot, err := uc.provider.Snapshot(session.taskID)
	if err != nil {
		return false, err
	}
	session.taskState = snapshot.State
	delivered, err := uc.replayUntilEmpty(ctx, session, out, nil)
	if err != nil {
		return false, err
	}
	if !session.taskState.IsTerminal() {
		session.terminalPending = false
		session.stopDrainRetry()
		session.stopDrainMaximum()
		session.startIdle(uc.timerFactory)
		return false, nil
	}
	if delivered > 0 {
		session.startDrainRetry(uc.timerFactory)
		return false, nil
	}
	return true, uc.writeTerminalComplete(ctx, session, out)
}

func (uc *TailTaskUseCase) finishTerminalDrain(ctx context.Context, session *tailSession, out schema.ProgressWriter) (bool, error) {
	if _, err := uc.replayUntilEmpty(ctx, session, out, nil); err != nil {
		return false, err
	}
	return true, uc.writeTerminalComplete(ctx, session, out)
}

func (uc *TailTaskUseCase) writeTerminalComplete(ctx context.Context, session *tailSession, out schema.ProgressWriter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return out.WriteComplete(schema.CompleteLine{
		LineType:  schema.LineTypeComplete,
		Reason:    schema.CompleteReasonTaskTerminal,
		TaskState: session.taskState,
		LastSeq:   session.lastDeliveredSeq,
	})
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
