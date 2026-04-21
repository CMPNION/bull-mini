package bullmini

import (
	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
	"github.com/CMPNION/bull-mini/internal/infrastructure/memory"
	"github.com/CMPNION/bull-mini/internal/infrastructure/redis"
	"github.com/CMPNION/bull-mini/internal/usecase"
	redisclient "github.com/redis/go-redis/v9"
)

type Job[T any] = domain.Job[T]

type Queue[T any] = usecase.Queue[T]

type Worker[T any] = usecase.Worker[T]

type Processor[T any] = usecase.Processor[T]

type JobOption = usecase.JobOption

type WorkerOption = usecase.WorkerOption

var ErrDuplicateJob = domain.ErrDuplicateJob

func NewQueue[T any](name string, redisOpts *redisclient.Options) *Queue[T] {
	client := redis.NewRedisClient(redisOpts)
	repo := redis.NewRedisQueueRepository[T](client, "bullmini")
	return usecase.NewQueue[T](name, repo)
}

func NewWorker[T any](queueName string, redisOpts *redisclient.Options, processor Processor[T], opts ...WorkerOption) *Worker[T] {
	client := redis.NewRedisClient(redisOpts)
	repo := redis.NewRedisQueueRepository[T](client, "bullmini")
	return usecase.NewWorker[T](queueName, repo, processor, opts...)
}

func NewQueueWithRepo[T any](name string, repo domain.QueueRepository[T]) *Queue[T] {
	return usecase.NewQueue[T](name, repo)
}

func NewWorkerWithRepo[T any](queueName string, repo domain.QueueRepository[T], processor Processor[T], opts ...WorkerOption) *Worker[T] {
	return usecase.NewWorker[T](queueName, repo, processor, opts...)
}

func NewMemoryQueueRepository[T any]() domain.QueueRepository[T] {
	return memory.NewMemoryQueueRepository[T]()
}

func WithConcurrency(concurrency int) WorkerOption {
	return usecase.WithConcurrency(concurrency)
}

func WithVisibilityTimeout(timeout time.Duration) WorkerOption {
	return usecase.WithVisibilityTimeout(timeout)
}

func WithJobID(id string) JobOption {
	return usecase.WithJobID(id)
}

func WithMaxAttempts(attempts int) JobOption {
	return usecase.WithMaxAttempts(attempts)
}

func WithDelay(d time.Duration) JobOption {
	return usecase.WithDelay(d)
}

func WithBackoff(d time.Duration) JobOption {
	return usecase.WithBackoff(d)
}

func WithExponentialBackoff(initial, max time.Duration, factor float64, jitter bool) JobOption {
	return usecase.WithExponentialBackoff(initial, max, factor, jitter)
}

func WithIdempotencyKey(key string) JobOption {
	return usecase.WithIdempotencyKey(key)
}
