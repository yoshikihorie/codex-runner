package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

// Pagination limits are copied from specifications/10-shared/validation-rules.md.
const (
	eventsPageDefault  = 100
	eventsPageMax      = 1000
	projectIDMaxLength = 40 // SLUG_MAX_LENGTH in specifications/10-shared/validation-rules.md.
)

var projectIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

type Dispatch struct {
	Sessions interface {
		List() ([]supervisor.ManagedSession, error)
		Load(supervisor.SessionID) (supervisor.ManagedSession, error)
	}
	Control interface {
		Stop(context.Context, *supervisor.SessionID) (usecase.StopOutput, error)
		Resume(context.Context, *supervisor.SessionID, bool, bool) (usecase.ResumeOutput, error)
	}
	StartSession *usecase.StartSessionUseCase
	StopSession  *usecase.StopSessionUseCase
	Events       interface {
		ReadAll() ([]store.Event, int, error)
	}
	ScreenRead        *usecase.ScreenReadUseCase
	OperatorInput     *usecase.SendOperatorInputUseCase
	ObserveFor        func(string) *usecase.ObserveSessionUseCase
	NewSessionID      func(string) (supervisor.SessionID, error)
	SupervisorContext context.Context
	Observed          *usecase.ObservedStateStore
	ContextPercent    func(supervisor.SessionID) *int
	StartedAt         time.Time
	GlobalStopped     func() bool
	ShuttingDown      func() bool
	PeerResume        func(int, map[int]bool) (bool, bool, error)
}

type dispatchParams struct {
	SessionID string  `json:"session_id"`
	ProjectID string  `json:"project_id"`
	Text      *string `json:"text"`
	SinceSeq  *uint64 `json:"since_seq"`
	Limit     *int    `json:"limit"`
}

func (d *Dispatch) Handle(ctx context.Context, request Request, peerPID int) Response {
	if request.SupervisorProtocolVersion != SupervisorProtocolVersion {
		return malformedResponse()
	}
	verb := request.Verb
	switch verb {
	case "status", "stop", "resume", "start-session", "stop-session", "events", "screen", "send-input":
	default:
		return Response{Error: &ErrorBody{Code: "SUP_VERB_UNKNOWN", MessageKey: "error.supervisor.verbUnknown", Details: map[string]any{"verb": cleanVerb(verb)}}}
	}
	var p dispatchParams
	if verb == "send-input" && !utf8.Valid(request.Params) {
		return malformedResponse()
	}
	if len(request.Params) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(request.Params))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&p); err != nil {
			return malformedResponse()
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return malformedResponse()
		}
	}
	if p.SessionID != "" {
		if _, err := supervisor.NewSessionID(p.SessionID); err != nil {
			return malformedResponse()
		}
	}
	if p.ProjectID != "" && (!projectIDPattern.MatchString(p.ProjectID) || len(p.ProjectID) > projectIDMaxLength) {
		return malformedResponse()
	}
	if p.Limit != nil && *p.Limit < 1 {
		return malformedResponse()
	}
	if verb != "events" && (p.SinceSeq != nil || p.Limit != nil) {
		return malformedResponse()
	}
	if verb != "start-session" && p.ProjectID != "" {
		return malformedResponse()
	}
	if verb == "start-session" && (p.ProjectID == "" || p.SessionID != "") {
		return malformedResponse()
	}
	if (verb == "stop-session" || verb == "screen") && p.SessionID == "" {
		return malformedResponse()
	}
	if verb == "screen" && len(request.Params) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(request.Params, &fields); err != nil || len(fields) != 1 || fields["session_id"] == nil {
			return malformedResponse()
		}
	}
	var operatorText supervisor.OperatorText
	if verb == "send-input" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(request.Params, &fields); err != nil || len(fields) != 2 || fields["session_id"] == nil || fields["text"] == nil || p.SessionID == "" || p.Text == nil || strings.ContainsRune(*p.Text, utf8.RuneError) {
			return malformedResponse()
		}
		var err error
		operatorText, err = supervisor.NewOperatorText(*p.Text)
		if err != nil {
			return malformedResponse()
		}
	} else if len(request.Params) != 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(request.Params, &fields); err != nil {
			return malformedResponse()
		}
		if _, hasText := fields["text"]; hasText {
			return malformedResponse()
		}
	}
	if verb == "events" && p.ProjectID != "" {
		return malformedResponse()
	}
	var id *supervisor.SessionID
	if p.SessionID != "" {
		parsed := supervisor.SessionID(p.SessionID)
		id = &parsed
	}
	switch verb {
	case "status":
		if d.Sessions == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		all, err := d.Sessions.List()
		if err != nil {
			return dispatchError(err)
		}
		views := make([]map[string]any, 0, len(all))
		for _, s := range all {
			if id != nil && s.SessionID != *id {
				continue
			}
			view := map[string]any{"session_id": s.SessionID, "project_id": s.ProjectID, "cli": s.CLI, "generation": s.Generation, "state": s.State, "state_since": s.StateSince, "observed_state": nil, "stop_reason": s.StopReason, "context_percent": nil, "restart_count": s.RestartCount, "rotation_step": nil}
			if d.Observed != nil {
				if observed, ok := d.Observed.Get(s.SessionID); ok {
					view["observed_state"] = observed
				}
			}
			if d.ContextPercent != nil {
				view["context_percent"] = d.ContextPercent(s.SessionID)
			}
			if s.Rotation != nil {
				view["rotation_step"] = s.Rotation.Step
			}
			views = append(views, view)
		}
		if id != nil && len(views) == 0 {
			return dispatchError(usecase.ErrSessionNotFound)
		}
		stopped := false
		if d.GlobalStopped != nil {
			stopped = d.GlobalStopped()
		}
		return success(map[string]any{"supervisor": map[string]any{"started_at": d.StartedAt, "global_stop": stopped, "session_count": len(all)}, "sessions": views})
	case "stop":
		if d.Control == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		if _, err := d.Control.Stop(ctx, id); err != nil {
			return dispatchError(err)
		}
		scope := "global"
		if id != nil {
			scope = "session"
		}
		return success(map[string]any{"scope": scope, "stop_file": scope})
	case "resume":
		if d.Control == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		probe := d.PeerResume
		if probe == nil {
			probe = proc.PeerResumeConditions
		}
		isatty, supervised, err := probe(peerPID, usecase.OwnedChildPIDs())
		if err != nil {
			isatty = false
			supervised = false
		}
		out, err := d.Control.Resume(ctx, id, isatty, supervised)
		if err != nil {
			return dispatchError(err)
		}
		if out.Resumed == nil {
			out.Resumed = []supervisor.SessionID{}
		}
		if out.StillStopped == nil {
			out.StillStopped = []usecase.StillStopped{}
		}
		return success(out)
	case "start-session":
		if d.SupervisorContext != nil && d.SupervisorContext.Err() != nil || d.ShuttingDown != nil && d.ShuttingDown() {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		if d.StartSession == nil || d.Sessions == nil || d.ObserveFor == nil || d.SupervisorContext == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		newID := d.NewSessionID
		if newID == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		if d.StartSession.Stop != nil && d.StartSession.Stop.IsGlobalStopped() {
			return dispatchError(usecase.ErrStopActive)
		}
		if _, ok := d.StartSession.Projects[p.ProjectID]; !ok {
			return dispatchError(usecase.ErrProjectNotFound)
		}
		sessionID, err := newID(p.ProjectID)
		if err != nil {
			return dispatchError(err)
		}
		observer := d.ObserveFor(p.ProjectID)
		if observer == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		pty, err := d.StartSession.Execute(p.ProjectID, sessionID)
		if err != nil {
			return dispatchError(err)
		}
		if d.ShuttingDown != nil && d.ShuttingDown() {
			d.StartSession.AbortUnobservedStart(sessionID, pty)
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		session, err := d.Sessions.Load(sessionID)
		if err != nil {
			d.StartSession.AbortUnobservedStart(sessionID, pty)
			return dispatchError(err)
		}
		observer.StartObserved(d.SupervisorContext, session, pty, d.StartSession.Config.Columns, d.StartSession.Config.Rows)
		return success(map[string]any{"session_id": sessionID})
	case "stop-session":
		if d.StopSession == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		out, err := d.StopSession.Execute(ctx, *id)
		if err != nil {
			return dispatchError(err)
		}
		result := map[string]any{"session_id": out.SessionID, "state": out.State}
		if out.State == supervisor.SessionStopped {
			result["reason"] = out.Reason
		}
		return success(result)
	case "events":
		if d.Events == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		all, _, err := d.Events.ReadAll()
		if err != nil {
			return dispatchError(err)
		}
		limit := eventsPageDefault
		if p.Limit != nil {
			limit = *p.Limit
		}
		if limit > eventsPageMax {
			limit = eventsPageMax
		}
		since := uint64(0)
		if p.SinceSeq != nil {
			since = *p.SinceSeq
		}
		page := make([]store.Event, 0)
		next := since
		for _, event := range all {
			if event.Seq <= since || id != nil && event.SessionID != id.String() {
				continue
			}
			event.Pattern = ""
			page = append(page, event)
			next = event.Seq
			if len(page) >= limit {
				break
			}
		}
		return success(map[string]any{"events": page, "next_seq": next})
	case "screen":
		if d.ScreenRead == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		result, err := d.ScreenRead.Execute(*id)
		if err != nil {
			response := dispatchError(err)
			if errors.Is(err, usecase.ErrSessionNotFound) || errors.Is(err, usecase.ErrScreenUnavailable) {
				response.Error.Details = map[string]any{"session_id": p.SessionID}
			}
			return response
		}
		return success(result)
	case "send-input":
		if d.OperatorInput == nil {
			return dispatchError(errors.New("SUP_CONFIG_INVALID"))
		}
		probe := d.PeerResume
		if probe == nil {
			probe = proc.PeerResumeConditions
		}
		_, supervised, probeErr := probe(peerPID, usecase.OwnedChildPIDs())
		if probeErr != nil {
			supervised = true
		}
		result, err := d.OperatorInput.Execute(ctx, *id, operatorText, supervised)
		if result.Blocked != "" {
			response := dispatchError(errors.New("SUP_SEND_BLOCKED"))
			response.Error.Details = map[string]any{"session_id": p.SessionID, "reason": string(result.Blocked)}
			return response
		}
		if err != nil {
			response := dispatchError(err)
			var operatorErr *usecase.OperatorInputError
			if errors.As(err, &operatorErr) {
				if operatorErr.Reason != "" {
					response.Error.Details = map[string]any{"session_id": p.SessionID, "reason": string(operatorErr.Reason)}
				} else if operatorErr.SendID != "" {
					response.Error.Details = map[string]any{"session_id": p.SessionID, "send_id": operatorErr.SendID}
				}
			}
			return response
		}
		return success(map[string]any{"session_id": p.SessionID, "send_id": result.SendID, "confirmed": true})
	}
	return malformedResponse()
}

func cleanVerb(value string) string {
	var b strings.Builder
	count := 0
	for _, r := range value {
		if unicode.IsControl(r) {
			continue
		}
		if count >= 64 {
			break
		}
		b.WriteRune(r)
		count++
	}
	return b.String()
}

func success(value any) Response {
	data, err := json.Marshal(value)
	if err != nil {
		return dispatchError(err)
	}
	return Response{OK: true, Result: data}
}

func dispatchError(err error) Response {
	code := "SUP_CONFIG_INVALID"
	if err != nil && strings.HasPrefix(err.Error(), "SUP_") {
		code = strings.TrimSuffix(strings.Fields(err.Error())[0], ":")
	}
	keys := map[string]string{
		"SUP_CONFIG_INVALID":                       "error.supervisor.configInvalid",
		"SUP_CONFIG_PATH_CONFLICT":                 "error.supervisor.configPathConflict",
		"SUP_PROJECT_NOT_FOUND":                    "error.supervisor.projectNotFound",
		"SUP_PROJECT_CWD_UNAVAILABLE":              "error.supervisor.projectCwdUnavailable",
		"SUP_SESSION_ALREADY_RUNNING":              "error.supervisor.sessionAlreadyRunning",
		"SUP_SESSION_LIMIT_REACHED":                "error.supervisor.sessionLimitReached",
		"SUP_SESSION_NOT_FOUND":                    "error.supervisor.sessionNotFound",
		"SUP_SCREEN_UNAVAILABLE":                   "error.supervisor.screenUnavailable",
		"SUP_SPAWN_FAILED":                         "error.supervisor.spawnFailed",
		"SUP_ORPHAN_CHILD_ALIVE":                   "error.supervisor.orphanChildAlive",
		"SUP_STATE_FILE_CORRUPT":                   "error.supervisor.stateFileCorrupt",
		"SUP_ILLEGAL_TRANSITION":                   "error.supervisor.illegalTransition",
		"SUP_BILLING_GUARD_FAILED":                 "error.supervisor.billingGuardFailed",
		"SUP_RESUME_FORBIDDEN":                     "error.supervisor.resumeForbidden",
		supervisor.ErrorCodeOperatorInputForbidden: supervisor.MessageKeyOperatorInputForbidden,
		"SUP_STOP_ACTIVE":                          "error.supervisor.stopActive",
		"SUP_REQUEST_MALFORMED":                    "error.supervisor.requestMalformed",
		"SUP_INSTANCE_LOCK_HELD":                   "error.supervisor.instanceLockHeld",
		"SUP_SEND_NOT_CONFIRMED":                   "error.supervisor.sendNotConfirmed",
		"SUP_SEND_BLOCKED":                         "error.supervisor.sendBlocked",
		"SUP_RECOMMENDATION_FORBIDDEN":             "error.supervisor.recommendationForbidden",
		"SUP_RECOMMENDATION_UNDETERMINED":          "error.supervisor.recommendationUndetermined",
		"SUP_PERMISSION_PENDING":                   "info.supervisor.permissionPending",
		"SUP_PERMISSION_MODE_UNSAFE":               "info.supervisor.permissionModeUnsafe",
		"SUP_SEND_ABORTED":                         "error.supervisor.sendAborted",
		"SUP_RECOMMENDATION_REPEATED":              "error.supervisor.recommendationRepeated",
		"SUP_AUTO_CONTINUE_LIMIT_REACHED":          "error.supervisor.autoContinueLimitReached",
		"SUP_VERIFY_FAILED":                        "error.supervisor.verifyFailed",
		"SUP_SENSITIVE_PATH_BLOCKED":               "error.supervisor.sensitivePathBlocked",
		"SUP_COMMIT_FAILED":                        "error.supervisor.commitFailed",
		"SUP_PUSH_REJECTED_POLICY":                 "error.supervisor.pushRejectedPolicy",
		"SUP_PUSH_FAILED":                          "error.supervisor.pushFailed",
		"SUP_ROTATE_STEP_FAILED":                   "error.supervisor.rotateStepFailed",
		"SUP_RESTART_LIMIT_EXCEEDED":               "error.supervisor.restartLimitExceeded",
		"SUP_NOTIFY_FAILED":                        "error.supervisor.notifyFailed",
		"SUP_CLIENT_UNAVAILABLE":                   "error.supervisor.clientUnavailable",
		"SUP_VERB_UNKNOWN":                         "error.supervisor.verbUnknown",
		"SUP_PEER_REJECTED":                        "error.supervisor.peerRejected",
	}
	key := keys[code]
	if key == "" {
		key = "error.supervisor.configInvalid"
	}
	return Response{Error: &ErrorBody{Code: code, MessageKey: key}}
}
