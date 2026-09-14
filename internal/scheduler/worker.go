package scheduler

import (
	"context"
	"log"
	"sync"

	checker "github.com/UllasSG/Uptime-status-checker/internal/Checker"
	"github.com/UllasSG/Uptime-status-checker/internal/database"
)

type WorkerPool struct {
	jobs        <-chan Job
	checker     *checker.Checker
	resultQueue *database.ResultQueue
}

func NewWorkerPool(jobs <-chan Job, checker *checker.Checker, resultQueue *database.ResultQueue) *WorkerPool {
	return &WorkerPool{
		jobs:        jobs,
		checker:     checker,
		resultQueue: resultQueue,
	}
}

func (w *WorkerPool) run(ctx context.Context, workerCount int) {
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-w.jobs:
					if !ok {
						return
					}
					w.processJob(ctx, workerID, job)
				}
			}
		}(i)
	}

	wg.Wait()
}

func (w *WorkerPool) processJob(ctx context.Context, workerID int, job Job) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[worker %d] panic checking %s: %v", workerID, job.Target.URL, r)
		}
	}()

	result, err := w.checker.Check(ctx, job.Target)
	if err != nil {
		log.Printf("[worker %d] check failed for %s: %v", workerID, job.Target.URL, err)
		return
	}
	w.resultQueue.Enqueue(*result)
	if result.Up {
		log.Printf("[worker %d] UP   %s (%d) %s", workerID, result.Target.URL, result.StatusCode, result.Latency)
	} else {
		log.Printf("[worker %d] DOWN %s: %s (%s)", workerID, result.Target.URL, result.Err, result.Latency)
	}
}
