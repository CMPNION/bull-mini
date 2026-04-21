package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
)

type Processor[T any] func(ctx context.Context, job *domain.Job[T]) error

type Worker[T any] struct {
	queueName   string
	workerID    string
	repo        domain.QueueRepository[T]
	processor   Processor[T]
	concurrency int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type WorkerOptions struct {
	Concurrency int
}

type WorkerOption func(*WorkerOptions)

func WithConcurrency(concurrency int) WorkerOption {
	return func(o *WorkerOptions) {
		if concurrency > 0 {
			o.Concurrency = concurrency
		}
	}
}

func NewWorker[T any](queueName string, repo domain.QueueRepository[T], processor Processor[T], opts ...WorkerOption) *Worker[T] {
	options := WorkerOptions{
		Concurrency: 1,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &Worker[T]{
		queueName:   queueName,
		workerID:    generateWorkerID(),
		repo:        repo,
		processor:   processor,
		concurrency: options.Concurrency,
	}
}

func (w *Worker[T]) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.loop(ctx)
	}
}

func (w *Worker[T]) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func (w *Worker[T]) loop(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			job, err := w.repo.Dequeue(ctx, w.queueName, w.workerID)
			if err != nil || job == nil {
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
				continue
			}

			w.processJob(ctx, job)
		}
	}
}

func (w *Worker[T]) processJob(ctx context.Context, job *domain.Job[T]) {
	job.MarkActive(w.workerID)
	_ = w.repo.Update(ctx, w.queueName, job)

	err := w.processor(ctx, job)

	if err != nil {
		job.MarkFailed(err)

		if job.CanRetry() {
			job.State = domain.StateWaiting
			_ = w.repo.Update(ctx, w.queueName, job)
			_ = w.repo.Enqueue(ctx, w.queueName, job)
		} else {
			_ = w.repo.Update(ctx, w.queueName, job)
		}
	} else {
		job.MarkCompleted()
		_ = w.repo.Update(ctx, w.queueName, job)
	}

	_ = w.repo.Acknowledge(ctx, w.queueName, w.workerID, job.ID)
}

func generateWorkerID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
