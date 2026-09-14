package database

import (
	"context"
	"log"
	"time"

	checker "github.com/UllasSG/Uptime-status-checker/internal/Checker"
)

type ResultQueue struct {
	results       chan checker.JobResult
	store         *Store
	flushInterval time.Duration
}

func NewResultQueue(store *Store, buffer int, flushInterval time.Duration) *ResultQueue {
	return &ResultQueue{
		results:       make(chan checker.JobResult, buffer),
		store:         store,
		flushInterval: flushInterval,
	}
}

func (rq *ResultQueue) Enqueue(JobResult checker.JobResult) {
	rq.results <- JobResult
	log.Printf("%s enqued , checked at %s", JobResult.Target.URL, JobResult.CheckedAt)
}

func (rq *ResultQueue) ScheduleBatches(ctx context.Context) {
	ticker := time.NewTicker(rq.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := len(rq.results)
			if n == 0 {
				continue
			}
			resultArray := make([]checker.JobResult, 0, n)
			for range n {
				resultArray = append(resultArray, <-rq.results)
			}
			if err := rq.store.SaveResults(ctx, resultArray); err != nil {
				log.Printf("resultqueue: save %d results: %v", n, err)
			}
		}
	}
}
