package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
)

type MemoryQueueRepository[T any] struct {
	mu          sync.Mutex
	jobs        map[string]map[string]*domain.Job[T]
	wait        map[string][]string
	active      map[string]map[string][]string
	delayed     map[string]map[string]time.Time
	idempotency map[string]map[string]time.Time
	heartbeats  map[string]map[string]time.Time
}

func NewMemoryQueueRepository[T any]() *MemoryQueueRepository[T] {
	return &MemoryQueueRepository[T]{
		jobs:        make(map[string]map[string]*domain.Job[T]),
		wait:        make(map[string][]string),
		active:      make(map[string]map[string][]string),
		delayed:     make(map[string]map[string]time.Time),
		idempotency: make(map[string]map[string]time.Time),
		heartbeats:  make(map[string]map[string]time.Time),
	}
}

func (r *MemoryQueueRepository[T]) ensureQueue(queueName string) {
	if _, ok := r.jobs[queueName]; !ok {
		r.jobs[queueName] = make(map[string]*domain.Job[T])
		r.wait[queueName] = make([]string, 0)
		r.active[queueName] = make(map[string][]string)
		r.delayed[queueName] = make(map[string]time.Time)
		r.idempotency[queueName] = make(map[string]time.Time)
		r.heartbeats[queueName] = make(map[string]time.Time)
	}
}

func (r *MemoryQueueRepository[T]) Enqueue(ctx context.Context, queueName string, job *domain.Job[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)

	if job.IdempotencyKey != "" {
		if exp, exists := r.idempotency[queueName][job.IdempotencyKey]; exists {
			if time.Now().Before(exp) {
				return domain.ErrDuplicateJob
			}
		}
		r.idempotency[queueName][job.IdempotencyKey] = time.Now().Add(24 * time.Hour)
	}

	r.jobs[queueName][job.ID] = job

	if job.State == domain.StateDelayed && job.ExecuteAt != nil {
		r.delayed[queueName][job.ID] = *job.ExecuteAt
	} else {
		r.wait[queueName] = append(r.wait[queueName], job.ID)
	}

	return nil
}

func (r *MemoryQueueRepository[T]) Dequeue(ctx context.Context, queueName, workerID string) (*domain.Job[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)

	if len(r.wait[queueName]) == 0 {
		return nil, nil
	}

	jobID := r.wait[queueName][0]
	r.wait[queueName] = r.wait[queueName][1:]

	if r.active[queueName] == nil {
		r.active[queueName] = make(map[string][]string)
	}
	r.active[queueName][workerID] = append(r.active[queueName][workerID], jobID)

	job, ok := r.jobs[queueName][jobID]
	if !ok {
		return nil, fmt.Errorf("job not found in store")
	}

	return job, nil
}

func (r *MemoryQueueRepository[T]) Update(ctx context.Context, queueName string, job *domain.Job[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)
	r.jobs[queueName][job.ID] = job
	return nil
}

func (r *MemoryQueueRepository[T]) GetJob(ctx context.Context, queueName, jobID string) (*domain.Job[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)
	job, ok := r.jobs[queueName][jobID]
	if !ok {
		return nil, fmt.Errorf("job not found")
	}
	return job, nil
}

func (r *MemoryQueueRepository[T]) Acknowledge(ctx context.Context, queueName, workerID, jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)

	activeList := r.active[queueName][workerID]
	newList := make([]string, 0, len(activeList))
	for _, id := range activeList {
		if id != jobID {
			newList = append(newList, id)
		}
	}
	r.active[queueName][workerID] = newList

	return nil
}

func (r *MemoryQueueRepository[T]) PromoteDelayed(ctx context.Context, queueName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)
	now := time.Now()

	for jobID, executeAt := range r.delayed[queueName] {
		if !now.Before(executeAt) {
			r.wait[queueName] = append(r.wait[queueName], jobID)
			delete(r.delayed[queueName], jobID)
		}
	}

	return nil
}

func (r *MemoryQueueRepository[T]) Heartbeat(ctx context.Context, queueName, workerID string, timeout time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)
	r.heartbeats[queueName][workerID] = time.Now().Add(timeout)
	return nil
}

func (r *MemoryQueueRepository[T]) ReclaimStalled(ctx context.Context, queueName string, timeout time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureQueue(queueName)
	now := time.Now()

	for workerID, expiresAt := range r.heartbeats[queueName] {
		if now.After(expiresAt) {
			stalledJobs := r.active[queueName][workerID]
			if len(stalledJobs) > 0 {
				r.wait[queueName] = append(r.wait[queueName], stalledJobs...)
				r.active[queueName][workerID] = nil
			}
			delete(r.heartbeats[queueName], workerID)
		}
	}

	return nil
}
