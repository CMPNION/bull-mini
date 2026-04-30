# 🔗 queue-wrap

[![Go Reference](https://pkg.go.dev/badge/github.com/CMPNION/queue-wrap.svg)](https://pkg.go.dev/github.com/CMPNION/queue-wrap)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**queue-wrap** is an enterprise-grade, strictly typed, and highly reliable distributed job queue for Go. Built with **Clean Architecture** principles and leveraging **Go 1.18+ Generics**, it provides a robust foundation for distributed task processing, delayed execution, and background worker orchestration.

Out of the box, it's powered by **Redis**. Extensible **transport layer** allows seamless integration with **NATS**, **RabbitMQ**, **Kafka**, and other messaging systems.

## 🎯 What's Inside

- **Pure Generics**: `Job[T]` type safety — zero type assertions, zero runtime panics
- **At-Least-Once Delivery**: Reliable Queue pattern with automatic recovery from worker crashes
- **Idempotency**: Built-in deduplication with atomic Redis locking
- **Scheduling**: First-class support for delayed jobs, exponential backoff with jitter
- **Observability**: Middleware hooks for Prometheus, OpenTelemetry, structured logging
- **Testing Ready**: In-memory backend for fast, deterministic unit tests
- **Transport Agnostic**: Pluggable repository interface for Redis, NATS, RabbitMQ, Kafka

## 📦 Installation

Ensure Go 1.18+:

```bash
go get github.com/CMPNION/queue-wrap
```

**Prerequisites:**
- Redis 6.2+ (for default Redis backend with `BLMOVE` support)

## 🚀 Quick Start

```go
package main

import (
"context"
"fmt"
"log"
"os"
"os/signal"
"syscall"
"time"

"github.com/CMPNION/queue-wrap/pkg/queuewrap"
"github.com/redis/go-redis/v9"
)

type OrderPayload struct {
OrderID   string `json:"order_id"`
UserEmail string `json:"user_email"`
}

func main() {
ctx := context.Background()

redisOpts := &redis.Options{
Addr: "localhost:6379",
}

// Create a queue
queue := queuewrap.NewQueue[OrderPayload]("order-processing", redisOpts)

// Enqueue a job
job, err := queue.Add(ctx, "process-payment", OrderPayload{
OrderID:   "ORD-778899",
UserEmail: "customer@example.com",
}, queuewrap.WithExponentialBackoff(
1*time.Second, 30*time.Second, 2.0, true,
))
if err != nil {
log.Fatalf("Enqueue failed: %v", err)
}
fmt.Printf("Job enqueued: %s\n", job.ID)

// Define processor
processor := func(ctx context.Context, j *queuewrap.Job[OrderPayload]) error {
fmt.Printf("Processing order %s for %s\n", j.Data.OrderID, j.Data.UserEmail)
time.Sleep(500 * time.Millisecond)
return nil
}

// Start worker
worker := queuewrap.NewWorker[OrderPayload](
"order-processing",
redisOpts,
processor,
queuewrap.WithConcurrency[OrderPayload](10),
queuewrap.WithVisibilityTimeout[OrderPayload](5*time.Minute),
)

worker.Start(ctx)
fmt.Println("Worker started. Press Ctrl+C to exit.")

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan

fmt.Println("Shutting down gracefully...")
worker.Stop()
}
```

## 📚 Core Features

### 1. Type Safety with Generics

Every Queue and Worker is parameterized with your payload type `[T any]`. Serialization happens automatically—no manual type assertions needed.

```go
queue := queuewrap.NewQueue[OrderPayload]("orders", redisOpts)
job, err := queue.Add(ctx, "process", OrderPayload{...})
// Type-safe: j.Data is OrderPayload, not map[string]interface{}
```

### 2. Bulletproof Reliability

Uses Redis **BLMOVE** (Reliable Queue pattern) + automatic Reaper to recover orphaned jobs:
1. Jobs move from `wait` → `active:<worker_id>` atomically
2. Worker maintains heartbeat (TTL-based)
3. If worker crashes, Reaper sweeps stale active lists and restores jobs

```go
queuewrap.WithVisibilityTimeout[Payload](10 * time.Minute)
```

### 3. Idempotency (Prevent Duplicates)

Atomic Redis `SETNX` prevents duplicate processing for 24 hours:

```go
job, err := queue.Add(ctx, "charge-card", payload,
queuewrap.WithIdempotencyKey("charge-req-uuid-1234"),
)
if err == queuewrap.ErrDuplicateJob {
// Handle duplicate safely
}
```

### 4. Scheduling & Backoff

Delayed jobs stored in Redis `ZSET`, promoted by Worker scheduler:

```go
// Delay 15 minutes
queuewrap.WithDelay(15 * time.Minute)

// Fixed backoff on retry
queuewrap.WithBackoff(10 * time.Second)

// Exponential with jitter (prevents thundering herd)
queuewrap.WithExponentialBackoff(initial, max, factor, true)
```

### 5. Observability Hooks

Middleware-like hooks for APM, metrics, error tracking:

```go
hooks := queuewrap.WorkerHooks[MyPayload]{
BeforeProcess: func(ctx context.Context, job *queuewrap.Job[MyPayload]) {
metrics.IncActiveJobs(job.Name)
},
AfterProcess: func(ctx context.Context, job *queuewrap.Job[MyPayload], err error) {
if err != nil {
metrics.IncFailedJobs(job.Name)
} else {
metrics.IncCompletedJobs(job.Name)
}
},
OnRetry: func(ctx context.Context, job *queuewrap.Job[MyPayload], err error) {
logger.Warn("Retrying", "attempt", job.Attempts, "err", err)
},
}

worker := queuewrap.NewWorker[MyPayload](..., queuewrap.WithHooks[MyPayload](hooks))
```

### 6. Testing with In-Memory Backend

Fast, deterministic unit tests without Redis:

```go
memRepo := queuewrap.NewMemoryQueueRepository[MyPayload]()
queue := queuewrap.NewQueueWithRepo("test-queue", memRepo)
worker := queuewrap.NewWorkerWithRepo("test-queue", memRepo, processor)

// Run tests...
```

## 🏗️ Architecture

**Clean Architecture** ensures maintainability and extensibility:

- **`internal/domain`**: Business rules, interfaces (`QueueRepository[T]`), entities (`Job[T]`)
- **`internal/usecase`**: Application logic (Queue, Worker orchestration, Reaper, Heartbeat)
- **`internal/infrastructure`**: Data persistence (Redis & Memory implementations)
- **`pkg/queuewrap`**: Public Facade API (all consumers import this)

### Pluggable Repository Pattern

Extend with custom transports by implementing `QueueRepository[T]`:

```go
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
```

Implement for **NATS**, **RabbitMQ**, **Kafka** and swap seamlessly:

```go
// Example: NATS backend
repo, _ := nats.NewNATSRepository[T](conn, "queuewrap")
queue := queuewrap.NewQueueWithRepo("orders", repo)
```

## 🔧 Configuration

### Job Options

```go
queuewrap.WithJobID("custom-id")                    // Custom job ID
queuewrap.WithMaxAttempts(5)                        // Retry limit
queuewrap.WithDelay(time.Minute)                    // Delayed execution
queuewrap.WithBackoff(time.Minute)                  // Fixed backoff
queuewrap.WithExponentialBackoff(i, m, f, true)    // Exponential + jitter
queuewrap.WithIdempotencyKey("unique-key")          // Prevent duplicates
```

### Worker Options

```go
queuewrap.WithConcurrency[T](10)                           // Parallel jobs
queuewrap.WithVisibilityTimeout[T](5 * time.Minute)        // Crash recovery window
queuewrap.WithHooks[T](hooks)                              // Observability
```

## 📊 Comparison: Transport Backends

| Aspect | Redis | NATS | RabbitMQ | Kafka |
|--------|-------|------|----------|-------|
| **Latency** | ~1ms | ~1ms | ~10ms | ~50ms |
| **Throughput** | 100K msg/s | 100K msg/s | 1M msg/s | 10M+ msg/s |
| **Push Model** | ✅ (BLMOVE) | ✅ (JetStream) | ✅ (Native) | ❌ (Pull) |
| **Delayed Jobs** | ✅ (ZSET) | ✅ | ✅ | ⚠️ Complex |
| **Recovery** | ✅ (Reaper) | ✅ | ✅ (DLQ) | ✅ |
| **Setup Complexity** | Low | Low | Medium | High |
| **Best For** | General purpose | Microservices | Enterprise | Real-time streams |

## 🛡️ Production Checklist

- [ ] Configure appropriate `VisibilityTimeout` based on job duration
- [ ] Set `MaxAttempts` to balance retry vs failure
- [ ] Use exponential backoff with jitter for high-concurrency scenarios
- [ ] Attach observability hooks (metrics, logging, tracing)
- [ ] Monitor worker heartbeats and stalled job metrics
- [ ] Configure Redis persistence or replicate for durability
- [ ] Use idempotency keys for payment/billing operations
- [ ] Test graceful shutdown with `worker.Stop()`

## 🚀 Performance Tips

1. **Batch Processing**: Use higher concurrency for I/O-bound jobs
2. **Job Serialization**: Keep payloads small (< 1MB recommended)
3. **Backoff Strategy**: Start low (1s) and scale exponentially to avoid thundering herd
4. **Memory Backend**: Use for tests, not production
5. **Redis Persistence**: Enable AOF or RDB snapshots for durability

## 📝 License

Licensed under **GPL-3.0**. See [LICENSE](LICENSE) for details.

## 🤝 Contributing

Contributions welcome! Please open issues and PRs.

---

**Future**: Native NATS, RabbitMQ, and Kafka transports coming soon.
