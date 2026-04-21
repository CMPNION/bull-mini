package domain

import "context"

type QueueRepository interface {
	Enqueue(ctx context.Context, queueName string, job *Job) error
	Dequeue(ctx context.Context, queueName string) (*Job, error)
	Update(ctx context.Context, queueName string, job *Job) error
	GetJob(ctx context.Context, queueName string, jobID string) (*Job, error)
}
