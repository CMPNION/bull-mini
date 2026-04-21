package domain

import (
	"context"
	"time"
)

type QueueRepository[T any] interface {
	Enqueue(ctx context.Context, queueName string, job *Job[T]) error
	Dequeue(ctx context.Context, queueName, workerID string) (*Job[T], error)
	Update(ctx context.Context, queueName string, job *Job[T]) error
	GetJob(ctx context.Context, queueName, jobID string) (*Job[T], error)
	Acknowledge(ctx context.Context, queueName, workerID, jobID string) error
	PromoteDelayed(ctx context.Context, queueName string) error
	Heartbeat(ctx context.Context, queueName, workerID string, timeout time.Duration) error
	ReclaimStalled(ctx context.Context, queueName string, timeout time.Duration) error
}
