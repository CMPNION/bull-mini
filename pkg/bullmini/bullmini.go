package bullmini

import (
	"github.com/CMPNION/bull-mini/internal/domain"
	"github.com/CMPNION/bull-mini/internal/infrastructure/redis"
	"github.com/CMPNION/bull-mini/internal/usecase"
	redisclient "github.com/redis/go-redis/v9"
)

type Job = domain.Job

type Queue = usecase.Queue

type Worker = usecase.Worker

type Processor = usecase.Processor

type JobOption = usecase.JobOption

type WorkerOption = usecase.WorkerOption

func NewQueue(name string, redisOpts *redisclient.Options) *Queue {
	client := redis.NewRedisClient(redisOpts)
	repo := redis.NewRedisQueueRepository(client, "bullmini")
	return usecase.NewQueue(name, repo)
}

func NewWorker(queueName string, redisOpts *redisclient.Options, processor Processor, opts ...WorkerOption) *Worker {
	client := redis.NewRedisClient(redisOpts)
	repo := redis.NewRedisQueueRepository(client, "bullmini")
	return usecase.NewWorker(queueName, repo, processor, opts...)
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
