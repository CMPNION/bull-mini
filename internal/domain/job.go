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

type Job struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Data        map[string]any `json:"data"`
	State       JobState       `json:"state"`
	Attempts    int            `json:"attempts"`
	MaxAttempts int            `json:"max_attempts"`
	Progress    int            `json:"progress"`
	Error       string         `json:"error,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	ProcessedAt *time.Time     `json:"processed_at,omitempty"`
	FinishedAt  *time.Time     `json:"finished_at,omitempty"`
}

func NewJob(id, name string, data map[string]any, maxAttempts int) *Job {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	return &Job{
		ID:          id,
		Name:        name,
		Data:        data,
		State:       StateWaiting,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
	}
}

func (j *Job) MarkActive() {
	j.State = StateActive
	now := time.Now()
	j.ProcessedAt = &now
	j.Attempts++
}

func (j *Job) MarkCompleted() {
	j.State = StateCompleted
	now := time.Now()
	j.FinishedAt = &now
}

func (j *Job) MarkFailed(err error) {
	j.State = StateFailed
	j.Error = err.Error()
	now := time.Now()
	j.FinishedAt = &now
}

func (j *Job) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}
