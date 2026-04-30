package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/CMPNION/queue-wrap/internal/domain"
	"github.com/redis/go-redis/v9"
)

type JSONSerializer struct{}

func (s *JSONSerializer) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (s *JSONSerializer) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type RedisQueueRepository[T any] struct {
	client     *redis.Client
	prefix     string
	serializer domain.Serializer
}

func NewRedisQueueRepository[T any](client *RedisClient, prefix string) *RedisQueueRepository[T] {
	if prefix == "" {
		prefix = "queuewrap"
	}
	return &RedisQueueRepository[T]{
		client:     client.Client,
		prefix:     prefix,
		serializer: &JSONSerializer{},
	}
}

func (r *RedisQueueRepository[T]) waitKey(queueName string) string {
	return fmt.Sprintf("%s:%s:wait", r.prefix, queueName)
}

func (r *RedisQueueRepository[T]) activeKey(queueName, workerID string) string {
	return fmt.Sprintf("%s:%s:active:%s", r.prefix, queueName, workerID)
}

func (r *RedisQueueRepository[T]) jobsKey(queueName string) string {
	return fmt.Sprintf("%s:%s:jobs", r.prefix, queueName)
}

func (r *RedisQueueRepository[T]) delayedKey(queueName string) string {
	return fmt.Sprintf("%s:%s:delayed", r.prefix, queueName)
}

func (r *RedisQueueRepository[T]) heartbeatKey(queueName, workerID string) string {
	return fmt.Sprintf("%s:%s:heartbeat:%s", r.prefix, queueName, workerID)
}

func (r *RedisQueueRepository[T]) completedKey(queueName string) string {
	return fmt.Sprintf("%s:%s:completed", r.prefix, queueName)
}

func (r *RedisQueueRepository[T]) failedKey(queueName string) string {
	return fmt.Sprintf("%s:%s:failed", r.prefix, queueName)
}

func (r *RedisQueueRepository[T]) idempotencyKey(queueName, key string) string {
	return fmt.Sprintf("%s:%s:idempotency:%s", r.prefix, queueName, key)
}

func (r *RedisQueueRepository[T]) Enqueue(ctx context.Context, queueName string, job *domain.Job[T]) error {
	if job.IdempotencyKey != "" {
		ok, err := r.client.SetNX(ctx, r.idempotencyKey(queueName, job.IdempotencyKey), job.ID, 24*time.Hour).Result()
		if err != nil {
			return fmt.Errorf("failed to check idempotency key: %w", err)
		}
		if !ok {
			return domain.ErrDuplicateJob
		}
	}

	data, err := r.serializer.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.jobsKey(queueName), job.ID, data)

	if job.State == domain.StateDelayed && job.ExecuteAt != nil {
		pipe.ZAdd(ctx, r.delayedKey(queueName), redis.Z{
			Score:  float64(job.ExecuteAt.UnixMilli()),
			Member: job.ID,
		})
	} else {
		pipe.LPush(ctx, r.waitKey(queueName), job.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis enqueue pipeline failed: %w", err)
	}

	return nil
}

func (r *RedisQueueRepository[T]) Dequeue(ctx context.Context, queueName, workerID string) (*domain.Job[T], error) {
	res, err := r.client.BLMove(ctx, r.waitKey(queueName), r.activeKey(queueName, workerID), "RIGHT", "LEFT", 1*time.Second).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue: %w", err)
	}

	return r.GetJob(ctx, queueName, res)
}

func (r *RedisQueueRepository[T]) Update(ctx context.Context, queueName string, job *domain.Job[T]) error {
	data, err := r.serializer.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.jobsKey(queueName), job.ID, data)

	if job.State == domain.StateCompleted {
		pipe.SAdd(ctx, r.completedKey(queueName), job.ID)
	} else if job.State == domain.StateFailed {
		pipe.SAdd(ctx, r.failedKey(queueName), job.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	return nil
}

func (r *RedisQueueRepository[T]) GetJob(ctx context.Context, queueName, jobID string) (*domain.Job[T], error) {
	data, err := r.client.HGet(ctx, r.jobsKey(queueName), jobID).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("job not found")
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var job domain.Job[T]
	if err := r.serializer.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

func (r *RedisQueueRepository[T]) Acknowledge(ctx context.Context, queueName, workerID, jobID string) error {
	err := r.client.LRem(ctx, r.activeKey(queueName, workerID), 1, jobID).Err()
	if err != nil {
		return fmt.Errorf("failed to acknowledge job: %w", err)
	}
	return nil
}

func (r *RedisQueueRepository[T]) Heartbeat(ctx context.Context, queueName, workerID string, timeout time.Duration) error {
	return r.client.Set(ctx, r.heartbeatKey(queueName, workerID), "1", timeout).Err()
}

func (r *RedisQueueRepository[T]) ReclaimStalled(ctx context.Context, queueName string, timeout time.Duration) error {
	match := fmt.Sprintf("%s:%s:active:*", r.prefix, queueName)
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, match, 10).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			workerID := key[len(fmt.Sprintf("%s:%s:active:", r.prefix, queueName)):]
			heartbeatKey := r.heartbeatKey(queueName, workerID)

			exists, err := r.client.Exists(ctx, heartbeatKey).Result()
			if err != nil || exists > 0 {
				continue
			}

			for {
				jobID, err := r.client.LMove(ctx, key, r.waitKey(queueName), "RIGHT", "LEFT").Result()
				if err != nil || jobID == "" {
					break
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (r *RedisQueueRepository[T]) PromoteDelayed(ctx context.Context, queueName string) error {
	now := float64(time.Now().UnixMilli())

	script := `
		local delayedKey = KEYS[1]
		local waitKey = KEYS[2]
		local now = ARGV[1]

		local jobs = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now)
		if #jobs > 0 then
			for _, jobID in ipairs(jobs) do
				redis.call("LPUSH", waitKey, jobID)
				redis.call("ZREM", delayedKey, jobID)
			end
		end
		return #jobs
	`

	err := r.client.Eval(ctx, script, []string{r.delayedKey(queueName), r.waitKey(queueName)}, now).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to promote delayed jobs: %w", err)
	}

	return nil
}
