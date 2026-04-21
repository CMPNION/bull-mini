package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/CMPNION/bull-mini/internal/domain"
)

type Queue struct {
	name string
	repo domain.QueueRepository
}

func NewQueue(name string, repo domain.QueueRepository) *Queue {
	return &Queue{
		name: name,
		repo: repo,
	}
}

type JobOptions struct {
	JobID       string
	MaxAttempts int
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

func (q *Queue) Add(ctx context.Context, jobName string, data map[string]any, opts ...JobOption) (*domain.Job, error) {
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

	err := q.repo.Enqueue(ctx, q.name, job)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue job: %w", err)
	}

	return job, nil
}

func (q *Queue) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	return q.repo.GetJob(ctx, q.name, jobID)
}

func (q *Queue) Name() string {
	return q.name
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
