package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func snapshotTime(offset int) time.Time {
	return time.Date(2026, time.August, 6, 12, 0, offset, 0, time.UTC)
}

func snapshotInt(value int) *int { return &value }

func snapshotString(value string) *string { return &value }

func TestNewInitialTaskSnapshot(t *testing.T) {
	reasoning := "high"
	workingDir := "/tmp/work"
	snapshot := NewInitialTaskSnapshot(ExecutionRouteDaemon, &reasoning, "workspace-write", &workingDir)
	if snapshot.Route != ExecutionRouteDaemon || snapshot.ReasoningEffort == nil || *snapshot.ReasoningEffort != reasoning || snapshot.ReasoningEffort == &reasoning || snapshot.SchemaVersion != 4 {
		t.Fatalf("initial snapshot = %#v", snapshot)
	}
	reasoning = "low"
	if *snapshot.ReasoningEffort != "high" {
		t.Fatalf("initial snapshot reasoning effort was aliased: %q", *snapshot.ReasoningEffort)
	}
	if snapshot.TaskID.String() != "" || snapshot.Subcommand != "" || snapshot.PID != nil || snapshot.ProcessStartedAt != nil || snapshot.Model != "" || !snapshot.RequestedAt.IsZero() || snapshot.State != "" || !snapshot.StateUpdatedAt.IsZero() {
		t.Fatalf("initial snapshot has non-zero task fields: %#v", snapshot)
	}
	if NewInitialTaskSnapshot(ExecutionRouteDaemon, nil, "workspace-write", nil).ReasoningEffort != nil {
		t.Fatal("nil reasoning effort was not retained")
	}
	workingDir = "/tmp/changed"
	if snapshot.WorkingDir == nil || *snapshot.WorkingDir != "/tmp/work" {
		t.Fatalf("initial snapshot working dir was aliased: %v", snapshot.WorkingDir)
	}
}

func TestTaskSnapshotSchemaVersionOneFailsClosed(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	snapshot.SchemaVersion = 1
	if err := snapshot.Validate(); err == nil {
		t.Fatal("schema version 1 snapshot was accepted")
	}
}

func TestTaskSnapshotFailureCodeValidation(t *testing.T) {
	base := validRunningSnapshot(t)
	base.State = StateFailed
	base.PID, base.ProcessStartedAt = nil, nil
	base.SchemaVersion = 4
	for _, code := range []string{"LIVENESS_LOCK_IO_ERROR", "CONTRACT_WRITE_FAILED", "WORKTREE_CREATE_FAILED", "PTY_ALLOCATION_FAILED", "CHILD_PROCESS_LAUNCH_FAILED"} {
		snapshot := base
		snapshot.FailureCode = &code
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("%s: %v", code, err)
		}
	}
	for _, change := range []func(*TaskSnapshot){
		func(s *TaskSnapshot) { s.SchemaVersion = 2; s.WorkingDir = nil },
		func(s *TaskSnapshot) { s.SchemaVersion = 3 },
		func(s *TaskSnapshot) { s.State = StateStarting },
		func(s *TaskSnapshot) { pid := 42; at := snapshotTime(2); s.PID, s.ProcessStartedAt = &pid, &at },
		func(s *TaskSnapshot) { unknown := "UNKNOWN"; s.FailureCode = &unknown },
	} {
		snapshot := base
		code := "CONTRACT_WRITE_FAILED"
		snapshot.FailureCode = &code
		change(&snapshot)
		if err := snapshot.Validate(); err == nil {
			t.Fatalf("accepted invalid snapshot: %#v", snapshot)
		}
	}
	base.SchemaVersion = 3
	if err := base.Validate(); err != nil || !base.SupportsWorkingDir() || base.SupportsFailureCode() {
		t.Fatalf("version 3: %v", err)
	}
}

func admissionTask(t *testing.T, requested *int) *Task {
	t.Helper()
	id, err := NewTaskID("impl-20260806-120000-a1b2-admission")
	if err != nil {
		t.Fatal(err)
	}
	slug, err := NewSlug("admission")
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := NewTask(id, SubcommandImpl, slug, requested, snapshotTime(0), 1)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestNewTaskSnapshotFromAdmission(t *testing.T) {
	requested, reasoning := timeoutMinSeconds+10, "high"
	task := admissionTask(t, &requested)
	timeout, err := NewTimeout(&requested, timeoutMinSeconds+20)
	if err != nil {
		t.Fatal(err)
	}
	workingDir := "/tmp/work"
	snapshot, err := NewTaskSnapshotFromAdmission(task, timeout, "gpt-5", &reasoning, "workspace-write", &workingDir, ExecutionRouteDaemon, snapshotTime(1))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PID != nil || snapshot.ProcessStartedAt != nil || snapshot.LastEventAt != nil || snapshot.SessionRef != nil || snapshot.ExitCode != nil || snapshot.RecoveryOrigin != nil || snapshot.Recovered || snapshot.AdoptedAfterRestart || snapshot.State != StateQueued || snapshot.Model != "gpt-5" || snapshot.Validate() != nil {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestNewTaskSnapshotFromAdmission_RejectsInvalidInput(t *testing.T) {
	requested := timeoutMinSeconds + 10
	task := admissionTask(t, &requested)
	timeout, err := NewTimeout(&requested, timeoutMinSeconds+20)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		task    *Task
		timeout Timeout
		model   string
		route   ExecutionRoute
		at      time.Time
	}{
		{"nil task", nil, timeout, "gpt-5", ExecutionRouteDaemon, snapshotTime(1)},
		{"empty model", task, timeout, "", ExecutionRouteDaemon, snapshotTime(1)},
		{"invalid route", task, timeout, "gpt-5", ExecutionRouteLegacy, snapshotTime(1)},
		{"zero time", task, timeout, "gpt-5", ExecutionRouteDaemon, time.Time{}},
		{"invalid timeout", task, Timeout{}, "gpt-5", ExecutionRouteDaemon, snapshotTime(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := *task
			if _, err := NewTaskSnapshotFromAdmission(tc.task, tc.timeout, tc.model, nil, "workspace-write", snapshotString("/tmp/work"), tc.route, tc.at); err == nil {
				t.Fatal("invalid input accepted")
			}
			if tc.task != nil && !reflect.DeepEqual(*task, before) {
				t.Fatal("task mutated on validation failure")
			}
		})
	}
}

func TestNewTaskSnapshotFromAdmission_DefensivelyCopiesPointers(t *testing.T) {
	requested, reasoning := timeoutMinSeconds+10, "high"
	task := admissionTask(t, &requested)
	timeout, err := NewTimeout(&requested, timeoutMinSeconds+20)
	if err != nil {
		t.Fatal(err)
	}
	workingDir := "/tmp/work"
	snapshot, err := NewTaskSnapshotFromAdmission(task, timeout, "gpt-5", &reasoning, "workspace-write", &workingDir, ExecutionRouteDaemon, snapshotTime(1))
	if err != nil {
		t.Fatal(err)
	}
	requested, reasoning, workingDir = timeoutMinSeconds+30, "low", "/tmp/changed"
	if *snapshot.RequestedTimeoutSeconds == requested || *snapshot.ReasoningEffort == reasoning || *snapshot.WorkingDir == workingDir {
		t.Fatalf("snapshot aliases input: %#v", snapshot)
	}
}

func validRunningSnapshot(t *testing.T) TaskSnapshot {
	t.Helper()
	id, err := NewTaskID("impl-20260806-120000-a1b2-example")
	if err != nil {
		t.Fatal(err)
	}
	pid := 42
	started := snapshotTime(1)
	requested := timeoutMinSeconds + 60
	workingDir := "/tmp/work"
	return TaskSnapshot{
		TaskID: id, Subcommand: SubcommandImpl, PID: &pid, ProcessStartedAt: &started,
		ResolvedTimeoutSeconds: timeoutMinSeconds + 120, RequestedTimeoutSeconds: &requested,
		Model: "gpt-5", SandboxMode: "workspace-write", RequestedAt: snapshotTime(0), Route: ExecutionRouteDaemon,
		State: StateRunning, StateUpdatedAt: snapshotTime(2), WorkingDir: &workingDir, SchemaVersion: taskSnapshotSchemaVersion,
	}
}

func TestTaskSnapshotValidateWorkingDirBySchema(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int
		state   TaskState
		pid     bool
		dir     *string
		wantErr bool
	}{
		{name: "version 2 omitted", version: 2, state: StateRunning, pid: true},
		{name: "version 2 rejects value", version: 2, state: StateRunning, pid: true, dir: snapshotString("/tmp/work"), wantErr: true},
		{name: "version 3 running valid", version: 3, state: StateRunning, pid: true, dir: snapshotString("/tmp/work")},
		{name: "version 3 running missing", version: 3, state: StateRunning, pid: true, wantErr: true},
		{name: "version 3 queued missing", version: 3, state: StateQueued},
		{name: "version 3 failed missing", version: 3, state: StateFailed},
		{name: "version 3 failed with pid missing", version: 3, state: StateFailed, pid: true, wantErr: true},
		{name: "version 3 relative", version: 3, state: StateQueued, dir: snapshotString("relative"), wantErr: true},
		{name: "version 3 unclean", version: 3, state: StateQueued, dir: snapshotString("/tmp/a/.."), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validRunningSnapshot(t)
			snapshot.SchemaVersion = tc.version
			snapshot.State = tc.state
			snapshot.WorkingDir = tc.dir
			if !tc.pid {
				snapshot.PID, snapshot.ProcessStartedAt = nil, nil
			}
			err := snapshot.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestTaskSnapshotVersion2MarshalOmitsWorkingDir(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	snapshot.SchemaVersion = 2
	snapshot.WorkingDir = nil
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["working_dir"]; exists {
		t.Fatalf("version 2 JSON contains working_dir: %s", data)
	}
}

func TestTaskSnapshotValidateRejectsZeroValue(t *testing.T) {
	if (TaskSnapshot{}).Validate() == nil {
		t.Fatal("zero snapshot accepted")
	}
}

func TestTaskSnapshotValidateAllowsFailedWithoutPID(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	snapshot.State = StateFailed
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("failed task without process info rejected: %v", err)
	}
}

func TestTaskSnapshotAllowsUnadoptedOrphanFinalizationWithoutProcessInfo(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	snapshot.State = StateOrphaned
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("orphan snapshot rejected: %v", err)
	}
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.RecordExit(NewExitCode(0), true, true, false, snapshotTime(3)); err != nil {
		t.Fatal(err)
	}
	completed, err := snapshot.WithTask(task, snapshotTime(4))
	if err != nil {
		t.Fatalf("terminal snapshot rejected: %v", err)
	}
	if completed.State != StateCompleted || completed.PID != nil || completed.ProcessStartedAt != nil || completed.AdoptedAfterRestart {
		t.Fatalf("completed snapshot=%+v", completed)
	}
}

func TestTaskSnapshotValidateRunningWithoutPIDAfterAdoption(t *testing.T) {
	for _, tc := range []struct {
		name                string
		adoptedAfterRestart bool
		wantErr             bool
	}{
		{"adopted after restart", true, false},
		{"not adopted after restart", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validRunningSnapshot(t)
			snapshot.PID, snapshot.ProcessStartedAt = nil, nil
			snapshot.AdoptedAfterRestart = tc.adoptedAfterRestart

			err := snapshot.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestTaskSnapshotValidateRejectsInvalidFields(t *testing.T) {
	valid := validRunningSnapshot(t)
	recoveryOrigin := RecoveryOriginTimeout
	unknownOrigin := RecoveryOrigin("other")
	zero := time.Time{}
	for _, tc := range []struct {
		name   string
		mutate func(*TaskSnapshot)
	}{
		{"unknown subcommand", func(s *TaskSnapshot) { s.Subcommand = SubcommandStatus }},
		{"empty model", func(s *TaskSnapshot) { s.Model = "" }},
		{"empty sandbox mode", func(s *TaskSnapshot) { s.SandboxMode = "" }},
		{"unknown sandbox mode", func(s *TaskSnapshot) { s.SandboxMode = "danger-full-access" }},
		{"unknown state", func(s *TaskSnapshot) { s.State = TaskState("other") }},
		{"non-daemon route", func(s *TaskSnapshot) { s.Route = ExecutionRouteLegacy }},
		{"pid without started at", func(s *TaskSnapshot) { s.ProcessStartedAt = nil }},
		{"started at without pid", func(s *TaskSnapshot) { s.PID = nil }},
		{"running without process info", func(s *TaskSnapshot) { s.PID, s.ProcessStartedAt = nil, nil }},
		{"exit code before terminal", func(s *TaskSnapshot) { code := NewExitCode(0); s.ExitCode = &code }},
		{"recovered inconsistent", func(s *TaskSnapshot) { s.Recovered = true }},
		{"origin without lineage", func(s *TaskSnapshot) { s.RecoveryOrigin = &recoveryOrigin }},
		{"zero requested at", func(s *TaskSnapshot) { s.RequestedAt = zero }},
		{"zero state updated at", func(s *TaskSnapshot) { s.StateUpdatedAt = zero }},
		{"unsupported schema", func(s *TaskSnapshot) { s.SchemaVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := valid
			tc.mutate(&snapshot)
			if err := snapshot.Validate(); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
	recovery := valid
	recovery.State = StateRecovering
	recovery.RecoveryOrigin = &recoveryOrigin
	if err := recovery.Validate(); err != nil {
		t.Fatalf("valid recovery snapshot rejected: %v", err)
	}
	snapshot := valid
	snapshot.RecoveryOrigin = &unknownOrigin
	if snapshot.Validate() == nil {
		t.Fatal("unknown recovery origin accepted")
	}
}

func TestTaskSnapshotValidateRejectsNonPositivePID(t *testing.T) {
	for _, pid := range []int{0, -1} {
		snapshot := validRunningSnapshot(t)
		snapshot.PID = &pid
		if err := snapshot.Validate(); err == nil {
			t.Fatalf("pid %d accepted", pid)
		}
	}
}

func TestTaskSnapshotValidateRejectsZeroProcessStartedAt(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	zero := time.Time{}
	snapshot.ProcessStartedAt = &zero
	if err := snapshot.Validate(); err == nil {
		t.Fatal("zero process start time accepted")
	}
}

func TestTaskSnapshotValidateRejectsInvalidRequestedTimeout(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	snapshot.RequestedTimeoutSeconds = snapshotInt(timeoutMinSeconds - 1)
	if err := snapshot.Validate(); err == nil {
		t.Fatal("requested timeout below minimum accepted")
	}
}

func TestTaskSnapshotValidateRejectsUnknownRecoveryOrigin(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	origin := RecoveryOrigin("unknown")
	snapshot.RecoveryOrigin = &origin
	if err := snapshot.Validate(); err == nil {
		t.Fatal("unknown recovery origin accepted")
	}
}

func TestTaskSnapshotJSONFieldNames(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	reasoning := "high"
	session, err := NewSessionRef("01234567-89ab-cdef-0123-456789abcdef", snapshotTime(3), false)
	if err != nil {
		t.Fatal(err)
	}
	lastEvent := snapshotTime(4)
	exitCode := NewExitCode(0)
	snapshot.ReasoningEffort, snapshot.SessionRef, snapshot.LastEventAt, snapshot.ExitCode = &reasoning, &session, &lastEvent, &exitCode
	snapshot.State, snapshot.Recovered = StateCompleted, false
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"task_id", "subcommand", "pid", "process_started_at", "resolved_timeout_seconds", "requested_timeout_seconds", "model", "reasoning_effort", "sandbox_mode", "working_dir", "requested_at", "route", "state", "state_updated_at", "session_ref", "last_event_at", "exit_code", "recovered", "adopted_after_restart", "recovery_origin", "schema_version"}
	if len(fields) != len(want) {
		t.Fatalf("field count = %d, want %d: %s", len(fields), len(want), data)
	}
	if _, exists := fields["failure_code"]; exists {
		t.Fatal("nil failure_code was not omitted")
	}
	for _, name := range want {
		if _, ok := fields[name]; !ok {
			t.Errorf("missing JSON field %q", name)
		}
	}
	var restored TaskSnapshot
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, snapshot) {
		t.Fatalf("JSON round trip changed snapshot\n got: %#v\nwant: %#v", restored, snapshot)
	}

	snapshot.RequestedTimeoutSeconds = nil
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	snapshot.ReasoningEffort, snapshot.SessionRef, snapshot.LastEventAt, snapshot.ExitCode, snapshot.RecoveryOrigin = nil, nil, nil, nil, nil
	snapshot.State = StateFailed
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fields = map[string]json.RawMessage{}
	if json.Unmarshal(data, &fields) != nil {
		t.Fatal("could not decode JSON fields")
	}
	if _, ok := fields["requested_timeout_seconds"]; ok {
		t.Fatal("nil requested timeout was not omitted")
	}
	for _, name := range []string{"pid", "process_started_at", "reasoning_effort", "session_ref", "last_event_at", "exit_code", "recovery_origin"} {
		if string(fields[name]) != "null" {
			t.Errorf("%s = %s, want null", name, fields[name])
		}
	}
}

func TestTaskSnapshotRestoreRoundTripAndTimeoutError(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	lastEvent := snapshotTime(5)
	snapshot.LastEventAt = &lastEvent
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if task.id != snapshot.TaskID || task.subcommand != snapshot.Subcommand || task.state != snapshot.State || task.model != snapshot.Model || !task.requestedAt.Equal(snapshot.RequestedAt) || task.processStartTime == nil || task.processStartTime.PID() != *snapshot.PID || !task.processStartTime.StartedAt().Equal(*snapshot.ProcessStartedAt) || !task.lastEventAt.Equal(lastEvent) {
		t.Fatal("Restore did not transfer all task fields")
	}
	snapshot.ResolvedTimeoutSeconds = timeoutMinSeconds - 1
	if _, err := snapshot.Restore(); err == nil {
		t.Fatal("Restore accepted invalid timeout")
	}
}

func TestTaskSnapshotWithTaskTransfersMutableFields(t *testing.T) {
	snapshot := validRunningSnapshot(t)
	reasoning := "high"
	snapshot.ReasoningEffort = &reasoning
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSessionRef("01234567-89ab-cdef-0123-456789abcdef", snapshotTime(3), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.MarkTimedOut(&session, snapshotTime(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := task.BeginRecovery(&session, snapshotTime(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := task.CompleteRecovery(NewExitCode(0), snapshotTime(6)); err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.WithTask(task, snapshotTime(7))
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StateRecovered || !result.Recovered || result.PID == nil || result.ProcessStartedAt == nil || result.ExitCode == nil || result.ExitCode.Raw() != 0 || result.SessionRef == nil || result.RecoveryOrigin == nil || *result.RecoveryOrigin != RecoveryOriginTimeout || result.AdoptedAfterRestart || result.ReasoningEffort == nil || *result.ReasoningEffort != reasoning || result.Route != ExecutionRouteDaemon || result.SchemaVersion != taskSnapshotSchemaVersion || !result.StateUpdatedAt.Equal(snapshotTime(7)) {
		t.Fatalf("WithTask lost fields: %#v", result)
	}
}

func TestSlugFromTaskIDAndZeroTaskID(t *testing.T) {
	id, err := NewTaskID("impl-20260806-120000-a1b2-example-slug")
	if err != nil {
		t.Fatal(err)
	}
	slug, err := slugFromTaskID(id)
	if err != nil || slug.String() != "example-slug" {
		t.Fatalf("slug=%q err=%v", slug.String(), err)
	}
	if _, err := slugFromTaskID(TaskID{}); err == nil {
		t.Fatal("zero TaskID accepted")
	}
}
