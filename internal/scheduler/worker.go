package scheduler

import (
	"context"
	"log"
	"sync"
)

type WorkerPool struct {
	jobs <-chan Job
}

func NewWorkerPool(jobs <-chan Job) *WorkerPool {
	return &WorkerPool{
		jobs: jobs,
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
					log.Printf("[worker %d] %s (interval=%ds)", workerID, job.Target.URL, job.Target.IntervalSecs)
				}
			}
		}(i)
	}

	wg.Wait()
}
