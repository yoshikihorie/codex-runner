package execution

import "github.com/yoshikihorie/codex-runner/internal/domain"

type activeTaskRegistry struct {
	reservations map[domain.TaskID]domain.Subcommand
}

func NewActiveTaskRegistry() ActiveTaskRegistry {
	return &activeTaskRegistry{reservations: make(map[domain.TaskID]domain.Subcommand)}
}

func (r *activeTaskRegistry) Size() int { return len(r.reservations) }

func (r *activeTaskRegistry) ImplSize() int {
	count := 0
	for _, subcommand := range r.reservations {
		if subcommand == domain.SubcommandImpl {
			count++
		}
	}
	return count
}

func (r *activeTaskRegistry) Add(taskID domain.TaskID, subcommand domain.Subcommand) {
	r.reservations[taskID] = subcommand
}

func (r *activeTaskRegistry) Remove(taskID domain.TaskID) { delete(r.reservations, taskID) }

func (r *activeTaskRegistry) Reset(reservations map[domain.TaskID]domain.Subcommand) {
	r.reservations = make(map[domain.TaskID]domain.Subcommand, len(reservations))
	for taskID, subcommand := range reservations {
		r.reservations[taskID] = subcommand
	}
}
