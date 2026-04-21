package usecase

import (
	"context"
	"sync"
	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
)

type Processor func(ctx context.Context, job *domain.Job) error

type Worker struct {
	queueName   string
	repo        domain.QueueRepository
	processor   Processor
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

func NewWorker(queueName string, repo domain.QueueRepository, processor Processor, opts ...WorkerOption) *Worker {
	options := WorkerOptions{
		Concurrency: 1,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &Worker{
		queueName:   queueName,
		repo:        repo,
		processor:   processor,
		concurrency: options.Concurrency,
	}
}

func (w *Worker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.loop(ctx)
	}
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			job, err := w.repo.Dequeue(ctx, w.queueName)
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

func (w *Worker) processJob(ctx context.Context, job *domain.Job) {
	job.MarkActive()
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
}
