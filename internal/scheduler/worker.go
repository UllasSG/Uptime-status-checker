package scheduler

import (
	"context"
	"log"
)

type Worker struct {
	jobs <-chan Job
}

func NewWorker(jobs <-chan Job) *Worker {
	return &Worker{
		jobs: jobs,
	}
}

func (w *Worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.jobs:
			log.Println(job.Target.URL)
		}
	}
}
