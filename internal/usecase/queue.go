package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
)

type Queue[T any] struct {
	name string
	repo domain.QueueRepository[T]
}

func NewQueue[T any](name string, repo domain.QueueRepository[T]) *Queue[T] {
	return &Queue[T]{
		name: name,
		repo: repo,
	}
}

type JobOptions struct {
	JobID       string
	MaxAttempts int
	Delay       time.Duration
	Backoff     time.Duration
}

type JobOption func(*JobOptions)

func WithJobID(id string) JobOption {
	return func(o *JobOptions) {
		o.JobID = id
	}
}

func WithMaxAttempts(attempts int) JobOption {
	return func(o *JobOptions) {
		o.MaxAttempts = attempts
	}
}

func WithDelay(d time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Delay = d
	}
}

func WithBackoff(d time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Backoff = d
	}
}

func (q *Queue[T]) Add(ctx context.Context, jobName string, data T, opts ...JobOption) (*domain.Job[T], error) {
	options := JobOptions{
		MaxAttempts: 1,
	}
	for _, opt := range opts {
		opt(&options)
	}

	if options.JobID == "" {
		options.JobID = generateID()
	}

	job := domain.NewJob(options.JobID, jobName, data, options.MaxAttempts)
	job.Backoff = options.Backoff
	if options.Delay > 0 {
		job.State = domain.StateDelayed
		executeAt := time.Now().Add(options.Delay)
		job.ExecuteAt = &executeAt
	}

	err := q.repo.Enqueue(ctx, q.name, job)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue job: %w", err)
	}

	return job, nil
}

func (q *Queue[T]) GetJob(ctx context.Context, jobID string) (*domain.Job[T], error) {
	return q.repo.GetJob(ctx, q.name, jobID)
}

func (q *Queue[T]) Name() string {
	return q.name
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
