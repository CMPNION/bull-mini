package domain

import "time"

type JobState string

const (
	StateWaiting   JobState = "waiting"
	StateActive    JobState = "active"
	StateCompleted JobState = "completed"
	StateFailed    JobState = "failed"
	StateDelayed   JobState = "delayed"
)

type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type Job[T any] struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Data        T             `json:"data"`
	State       JobState      `json:"state"`
	Attempts    int           `json:"attempts"`
	MaxAttempts int           `json:"max_attempts"`
	Backoff     time.Duration `json:"backoff,omitempty"`
	Progress    int           `json:"progress"`
	Error       string        `json:"error,omitempty"`
	WorkerID    string        `json:"worker_id,omitempty"`
	ExecuteAt   *time.Time    `json:"execute_at,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	ProcessedAt *time.Time    `json:"processed_at,omitempty"`
	FinishedAt  *time.Time    `json:"finished_at,omitempty"`
}

func NewJob[T any](id, name string, data T, maxAttempts int) *Job[T] {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	return &Job[T]{
		ID:          id,
		Name:        name,
		Data:        data,
		State:       StateWaiting,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
	}
}

func (j *Job[T]) MarkActive(workerID string) {
	j.State = StateActive
	j.WorkerID = workerID
	now := time.Now()
	j.ProcessedAt = &now
	j.Attempts++
}

func (j *Job[T]) MarkCompleted() {
	j.State = StateCompleted
	j.WorkerID = ""
	now := time.Now()
	j.FinishedAt = &now
}

func (j *Job[T]) MarkFailed(err error) {
	j.State = StateFailed
	j.WorkerID = ""
	j.Error = err.Error()
	now := time.Now()
	j.FinishedAt = &now
}

func (j *Job[T]) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}

func (j *Job[T]) PrepareRetry() {
	if j.Backoff > 0 {
		j.State = StateDelayed
		executeAt := time.Now().Add(j.Backoff)
		j.ExecuteAt = &executeAt
	} else {
		j.State = StateWaiting
		j.ExecuteAt = nil
	}
}
