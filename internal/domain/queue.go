package domain

import "context"

type QueueRepository[T any] interface {
	Enqueue(ctx context.Context, queueName string, job *Job[T]) error
	Dequeue(ctx context.Context, queueName, workerID string) (*Job[T], error)
	Update(ctx context.Context, queueName string, job *Job[T]) error
	GetJob(ctx context.Context, queueName, jobID string) (*Job[T], error)
	Acknowledge(ctx context.Context, queueName, workerID, jobID string) error
}
