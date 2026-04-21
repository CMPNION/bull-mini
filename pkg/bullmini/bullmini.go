package bullmini

import (
	"github.com/CMPNION/bull-mini/internal/domain"
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

func WithConcurrency(concurrency int) WorkerOption {
	return usecase.WithConcurrency(concurrency)
}

func WithJobID(id string) JobOption {
	return usecase.WithJobID(id)
}

func WithMaxAttempts(attempts int) JobOption {
	return usecase.WithMaxAttempts(attempts)
}
