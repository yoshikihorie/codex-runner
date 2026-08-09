package execution

import "github.com/yoshikihorie/codex-runner/internal/domain"

type activeTaskRegistry struct {
	taskIDs map[domain.TaskID]struct{}
}

func NewActiveTaskRegistry() ActiveTaskRegistry {
	return &activeTaskRegistry{taskIDs: make(map[domain.TaskID]struct{})}
}

func (r *activeTaskRegistry) Size() int { return len(r.taskIDs) }

func (r *activeTaskRegistry) Add(taskID domain.TaskID) { r.taskIDs[taskID] = struct{}{} }

func (r *activeTaskRegistry) Remove(taskID domain.TaskID) { delete(r.taskIDs, taskID) }

func (r *activeTaskRegistry) Reset(taskIDs []domain.TaskID) {
	r.taskIDs = make(map[domain.TaskID]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		r.taskIDs[taskID] = struct{}{}
	}
}
