package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/CMPNION/bull-mini/internal/domain"
	"github.com/redis/go-redis/v9"
)

type RedisQueueRepository struct {
	client *redis.Client
	prefix string
}

func NewRedisQueueRepository(client *RedisClient, prefix string) *RedisQueueRepository {
	if prefix == "" {
		prefix = "bullmini"
	}
	return &RedisQueueRepository{
		client: client.Client,
		prefix: prefix,
	}
}

func (r *RedisQueueRepository) waitKey(queueName string) string {
	return fmt.Sprintf("%s:%s:wait", r.prefix, queueName)
}

func (r *RedisQueueRepository) jobsKey(queueName string) string {
	return fmt.Sprintf("%s:%s:jobs", r.prefix, queueName)
}

func (r *RedisQueueRepository) Enqueue(ctx context.Context, queueName string, job *domain.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	pipe := r.client.Pipeline()

	pipe.HSet(ctx, r.jobsKey(queueName), job.ID, data)

	pipe.LPush(ctx, r.waitKey(queueName), job.ID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis enqueue pipeline failed: %w", err)
	}

	return nil
}

func (r *RedisQueueRepository) Dequeue(ctx context.Context, queueName string) (*domain.Job, error) {
	res, err := r.client.BRPop(ctx, 1*time.Second, r.waitKey(queueName)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue: %w", err)
	}

	if len(res) < 2 {
		return nil, nil
	}

	jobID := res[1]

	return r.GetJob(ctx, queueName, jobID)
}

func (r *RedisQueueRepository) Update(ctx context.Context, queueName string, job *domain.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	err = r.client.HSet(ctx, r.jobsKey(queueName), job.ID, data).Err()
	if err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	return nil
}

func (r *RedisQueueRepository) GetJob(ctx context.Context, queueName string, jobID string) (*domain.Job, error) {
	data, err := r.client.HGet(ctx, r.jobsKey(queueName), jobID).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("job not found")
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var job domain.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}
