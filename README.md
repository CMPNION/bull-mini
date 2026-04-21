# 🐂 bull-mini

**bull-mini** is a lightweight, fast, and robust Redis-based background job queue for Go, heavily inspired by the awesome [BullMQ](https://bullmq.io/) from the Node.js ecosystem. 

I built this library using **Clean Architecture** principles to ensure it is highly decoupled, easy to test, and simple to integrate into any Go microservice. If you need a reliable way to process background tasks, send emails, or handle delayed jobs without pulling in a massive framework, you're in the right place!

## 📑 Table of Contents

- [✨ Features](#-features)
- [📦 Installation](#-installation)
- [🚀 Quick Start](#-quick-start)
- [🛠️ API Overview](#️-api-overview)
- [🏗️ Architecture](#️-architecture)
- [🤝 Contributing](#-contributing)
- [📝 License](#-license)

## ✨ Features

- **Redis-Backed**: Uses Redis hashes and lists for fast, atomic job state management.
- **Concurrent Processing**: Run multiple jobs in parallel using Go's lightweight goroutines.
- **Automatic Retries**: Easily configure max attempts for jobs that might fail (e.g., network requests).
- **Clean Architecture**: Domain, Use Case, and Infrastructure layers are strictly separated.
- **Developer Friendly**: Simple, expressive API that feels natural to write.

## 📦 Installation

To install `bull-mini`, use `go get`:

```bash
go get github.com/CMPNION/bull-mini
```

*Note: You will also need a running instance of Redis.*

## 🚀 Quick Start

Here is a complete example of how to create a queue, add a job, and process it using a worker.

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

// Define a payload struct for type safety
type EmailPayload struct {
	UserID string `json:"userID"`
	Email  string `json:"email"`
}

func main() {
	ctx := context.Background()

	// 1. Configure Redis
	redisOpts := &redis.Options{
		Addr: "localhost:6379",
	}

	// 2. Initialize a Queue with the generic payload type
	queue := bullmini.NewQueue[EmailPayload]("email-queue", redisOpts)

	// 3. Add a job to the queue
	job, err := queue.Add(ctx, "send-welcome-email", EmailPayload{
		UserID: "12345",
		Email:  "newuser@example.com",
	}, bullmini.WithMaxAttempts(3)) // Automatically retry up to 3 times if it fails

	if err != nil {
		log.Fatalf("Failed to enqueue job: %v", err)
	}
	fmt.Printf("Added job with ID: %s\n", job.ID)

	// 4. Define your job processing logic
	processor := func(ctx context.Context, j *bullmini.Job[EmailPayload]) error {
		fmt.Printf("Processing job %s: Sending email to %s...\n", j.Name, j.Data.Email)
		
		// Simulate work
		time.Sleep(2 * time.Second)
		
		// Return an error to trigger a retry, or nil for success
		fmt.Println("Email sent successfully!")
		return nil 
	}

	// 5. Initialize and start the Worker
	worker := bullmini.NewWorker(
		"email-queue", 
		redisOpts, 
		processor,
		bullmini.WithConcurrency(5), // Process up to 5 jobs simultaneously
	)

	fmt.Println("Worker is starting...")
	worker.Start(ctx)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down worker gracefully...")
	worker.Stop()
	fmt.Println("Goodbye!")
}
```

## 🛠️ API Overview

### Queue

The `Queue` is responsible for adding tasks to Redis.

- `bullmini.NewQueue(queueName, redisOptions)`: Creates a new queue client.
- `queue.Add(ctx, jobName, payload, options...)`: Enqueues a job.
- `queue.GetJob(ctx, jobID)`: Retrieves the current state and data of a specific job.

**Job Options:**
- `bullmini.WithMaxAttempts(n)`: Sets the number of retries before the job is marked as `failed`.
- `bullmini.WithJobID(id)`: Allows you to specify a custom, deterministic Job ID instead of a random UUID.

### Worker

The `Worker` continuously listens to the queue and processes jobs.

- `bullmini.NewWorker(queueName, redisOptions, processorFunc, options...)`: Creates a worker.
- `worker.Start(ctx)`: Starts the processing loop in the background (non-blocking).
- `worker.Stop()`: Blocks and waits for all currently active jobs to finish before shutting down.

**Worker Options:**
- `bullmini.WithConcurrency(n)`: Determines how many goroutines are spawned to process jobs in parallel (default is 1).

## 🏗️ Architecture

This project is structured around **Clean Architecture**:

- **`internal/domain`**: Contains the core business logic, like the `Job` entity, its states (`waiting`, `active`, `completed`, `failed`), and repository interfaces.
- **`internal/usecase`**: Contains the application logic (`Queue` and `Worker` structs) that orchestrate domain entities and external storage.
- **`internal/infrastructure/redis`**: The concrete implementation of our queue storage using Redis pipelines and blocking pop commands (`BRPOP`).
- **`pkg/bullmini`**: The public-facing API wrapper. This is the only package users of the library need to interact with, keeping the internal complexity hidden.

## 🤝 Contributing

Contributions, issues, and feature requests are always welcome! Feel free to check the issues page.

## 📝 License

This project is licensed under the MIT License.