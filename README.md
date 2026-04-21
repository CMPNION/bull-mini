# 🐂 bull-mini

[![Go Reference](https://pkg.go.dev/badge/github.com/CMPNION/bull-mini.svg)](https://pkg.go.dev/github.com/CMPNION/bull-mini)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**bull-mini** is an enterprise-grade, strictly typed, and highly reliable Redis-backed background job queue for Go.

Engineered with **Clean Architecture** principles and leveraging **Go 1.18+ Generics**, `bull-mini` provides a robust foundation for distributed task processing, delayed execution, and background worker orchestration without sacrificing developer experience or type safety.

## 📑 Table of Contents

- [✨ Enterprise Features](#-enterprise-features)
- [📦 Installation](#-installation)
- [🚀 Quick Start](#-quick-start)
- [📚 Comprehensive Documentation](#-comprehensive-documentation)
  - [Type-Safe Payloads (Generics)](#1-type-safe-payloads-generics)
  - [Bulletproof Reliability & Recovery](#2-bulletproof-reliability--recovery)
  - [Idempotency (Unique Jobs)](#3-idempotency-unique-jobs)
  - [Scheduling & Exponential Backoff](#4-scheduling--exponential-backoff)
  - [Observability & Hooks](#5-observability--hooks)
  - [Testing (In-Memory Backend)](#6-testing-in-memory-backend)
- [🏗️ Architecture](#️-architecture)
- [📝 License](#-license)

## ✨ Enterprise Features

- **Strict Type Safety:** Fully powered by Generics (`Job[T]`). No more `map[string]any` type assertions or runtime panics.
- **At-Least-Once Delivery Guarantees:** Utilizes Redis `BLMOVE` (Reliable Queue pattern) combined with a background Reaper to recover orphaned jobs if a worker crashes (OOM, SIGKILL).
- **Idempotence Support:** Prevent duplicate jobs natively with atomic Redis `SETNX` locking.
- **Advanced Scheduling:** First-class support for delayed jobs (`ZSET`) and granular retry policies including Exponential Backoff with Jitter to prevent Thundering Herd problems.
- **Observability Built-in:** Middleware hooks (`BeforeProcess`, `AfterProcess`, `OnRetry`) for seamless integration with Prometheus, OpenTelemetry, and structured logging.
- **CI/CD Ready:** Includes a thread-safe `MemoryQueueRepository` allowing you to run fast, deterministic unit tests without spinning up a Redis container.

## 📦 Installation

Ensure you are using Go 1.18 or later.

```bash
go get github.com/CMPNION/bull-mini
```

*Prerequisite: A running instance of Redis 6.2+ (required for `BLMOVE` support).*

## 🚀 Quick Start

A complete, production-ready example demonstrating queue initialization, job enqueuing, and worker processing with graceful shutdown.

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

	"github.com/CMPNION/bull-mini/pkg/bullmini"
	"github.com/redis/go-redis/v9"
)

// 1. Define your strict payload schema
type OrderPayload struct {
	OrderID   string `json:"order_id"`
	UserEmail string `json:"user_email"`
}

func main() {
	ctx := context.Background()

	redisOpts := &redis.Options{
		Addr: "localhost:6379",
	}

	// 2. Initialize a strictly typed Queue
	queue := bullmini.NewQueue[OrderPayload]("order-processing", redisOpts)

	// 3. Enqueue a job with a retry policy
	job, err := queue.Add(ctx, "process-payment", OrderPayload{
		OrderID:   "ORD-778899",
		UserEmail: "customer@example.com",
	}, bullmini.WithExponentialBackoff(
		1*time.Second,  // Initial delay
		30*time.Second, // Max delay
		2.0,            // Multiplier
		true,           // Enable Jitter
	))

	if err != nil {
		log.Fatalf("Failed to enqueue job: %v", err)
	}
	fmt.Printf("Enqueued job: %s\n", job.ID)

	// 4. Define the Processor function
	processor := func(ctx context.Context, j *bullmini.Job[OrderPayload]) error {
		// Native type access - no casting required!
		fmt.Printf("Processing order %s for %s\n", j.Data.OrderID, j.Data.UserEmail)
		
		// Simulate processing latency
		time.Sleep(500 * time.Millisecond)
		return nil 
	}

	// 5. Initialize the Worker with concurrency and timeouts
	worker := bullmini.NewWorker[OrderPayload](
		"order-processing", 
		redisOpts, 
		processor,
		bullmini.WithConcurrency[OrderPayload](10),
		bullmini.WithVisibilityTimeout[OrderPayload](5*time.Minute),
	)

	worker.Start(ctx)
	fmt.Println("Worker is consuming jobs. Press Ctrl+C to exit.")

	// Graceful shutdown orchestration
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down worker gracefully...")
	worker.Stop() // Blocks until active jobs finish
}
```

## 📚 Comprehensive Documentation

### 1. Type-Safe Payloads (Generics)

Every Queue and Worker requires a defined type parameter `[T any]`. The underlying Repository layer automatically serializes and deserializes this payload using Go's standard JSON encoder, ensuring data integrity across process boundaries.

### 2. Bulletproof Reliability & Recovery

Standard list-based queues (`BRPOP`) suffer from data loss if the worker dies post-retrieval. 

`bull-mini` mitigates this via the **Reliable Queue** architecture:
1. Jobs are atomically moved from the `wait` list to an `active:<worker_id>` list using `BLMOVE`.
2. The Worker maintains a background heartbeat.
3. If a worker crashes, the heartbeat expires. A background Reaper routine systematically sweeps for stale active lists and atomically restores orphaned jobs back to the `wait` queue based on the `VisibilityTimeout`.

```go
// Configure how long a job can remain active before being considered orphaned
bullmini.WithVisibilityTimeout[MyPayload](10 * time.Minute)
```

### 3. Idempotency (Unique Jobs)

Prevent duplicate processing (e.g., double-charging a credit card) by defining an Idempotency Key. `bull-mini` uses an atomic `SETNX` lock to guarantee uniqueness for 24 hours.

```go
job, err := queue.Add(ctx, "charge-card", payload, 
	bullmini.WithIdempotencyKey("charge-req-uuid-1234"),
)

if err == bullmini.ErrDuplicateJob {
	// Safely ignore or return 409 Conflict to the client
}
```

### 4. Scheduling & Exponential Backoff

Jobs can be scheduled for future execution or configured to back off dynamically upon failure. Delayed jobs are stored in a Redis `ZSET` and promoted to the active queue by the Worker's internal scheduler.

```go
// Delay execution by 15 minutes
bullmini.WithDelay(15 * time.Minute)

// Fixed retry backoff
bullmini.WithBackoff(10 * time.Second)

// Exponential Backoff with Thundering Herd protection (Jitter)
bullmini.WithExponentialBackoff(initial, max, factor, true)
```

### 5. Observability & Hooks

Attach middleware-like hooks to monitor job lifecycles. Excellent for integrating with APM tools, emitting Prometheus metrics, or reporting errors to Sentry.

```go
hooks := bullmini.WorkerHooks[MyPayload]{
	BeforeProcess: func(ctx context.Context, job *bullmini.Job[MyPayload]) {
		metrics.IncActiveJobs(job.Name)
	},
	AfterProcess: func(ctx context.Context, job *bullmini.Job[MyPayload], err error) {
		if err != nil {
			metrics.IncFailedJobs(job.Name)
		} else {
			metrics.IncCompletedJobs(job.Name)
		}
	},
	OnRetry: func(ctx context.Context, job *bullmini.Job[MyPayload], err error) {
		logger.Warn("Job failed, scheduling retry", "attempt", job.Attempts, "err", err)
	},
}

worker := bullmini.NewWorker[MyPayload](..., bullmini.WithHooks[MyPayload](hooks))
```

### 6. Testing (In-Memory Backend)

Avoid brittle, slow integration tests that depend on Redis. `bull-mini` ships with a thread-safe, mutex-backed `MemoryQueueRepository` that fully complies with the `QueueRepository[T]` interface.

```go
// Inject the memory backend in your unit tests
memRepo := bullmini.NewMemoryQueueRepository[MyPayload]()

// Initialize queue and worker against memory
queue := bullmini.NewQueueWithRepo("test-queue", memRepo)
worker := bullmini.NewWorkerWithRepo("test-queue", memRepo, mockProcessor)

// Run deterministic, sub-millisecond tests...
```

## 🏗️ Architecture

`bull-mini` adheres strictly to **Clean Architecture** to ensure the library is maintainable, extendable, and easily embeddable:

- **`internal/domain`**: Business rules, state definitions, Interfaces (`QueueRepository`), and Generic structs (`Job[T]`). Completely agnostic of infrastructure.
- **`internal/usecase`**: Application orchestration. Contains the `Queue` logic (ID generation, options parsing) and the `Worker` logic (Concurrency, Reapers, Schedulers, Heartbeats).
- **`internal/infrastructure`**: Data persistence layer. Contains the concrete `RedisQueueRepository` (Lua scripts, Pipelines, `BLMOVE`) and the `MemoryQueueRepository` for testing.
- **`pkg/bullmini`**: The unified, developer-friendly Facade API. Consumers only ever import this package.

## 📝 License

This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**. 

Permissions of this strong copyleft license are conditioned on making available complete source code of licensed works and modifications, which include larger works using a licensed work, under the same license. Copyright and license notices must be preserved. See the [LICENSE](LICENSE) file for more details.