package pool

import (
	"context"
	"fmt"
	"sync"
)

// Pool represents a worker pool
//
type Pool struct {
	wg sync.WaitGroup

	queue   chan TaskFunc
	workers []*Worker
}

// Config represents the pool configuration
//
type Config struct {
	NumWorkers int
}

// NewPool creates a new pool
//
func NewPool(cfg Config) (*Pool, error) {
	fmt.Printf("==> Initializing pool with [%d] workers...\n", cfg.NumWorkers)
	pool := Pool{
		queue: make(chan TaskFunc, cfg.NumWorkers),
	}

	for i := 1; i <= cfg.NumWorkers; i++ {
		w := &Worker{id: i, ch: pool.queue, wg: &pool.wg}
		pool.workers = append(pool.workers, w)
		w.run()
	}
	pool.wg.Add(cfg.NumWorkers)

	return &pool, nil
}

// Add pushes new task to the pool
//
func (pool *Pool) Add(ctx context.Context, task TaskFunc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pool.queue <- task:
	}
	return nil
}

// Wait closes the pool queue and waits for goroutines to finish
//
func (pool *Pool) Wait() error {
	close(pool.queue)
	pool.wg.Wait()
	return nil
}

// Worker represents a single worker
//
type Worker struct {
	id int
	ch <-chan TaskFunc
	wg *sync.WaitGroup
}

func (w *Worker) run() {
	go func() {
		fmt.Printf("--> [worker-%03d] Started\n", w.id)
		defer w.wg.Done()

		for taskFunc := range w.ch {
			taskFunc()
			fmt.Printf("--> [worker-%03d] Done\n", w.id)
		}
	}()
}

// TaskFunc represents a worker task
//
type TaskFunc func()
