package scheduler

import (
	"context"
	"time"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
)

type Scheduler struct {
	targets []config.Target
	jobs    chan<- Job
	worker  *WorkerPool
}

func NewScheduler(targets []config.Target, jobs chan<- Job, worker *WorkerPool) *Scheduler {
	return &Scheduler{
		targets: targets,
		jobs:    jobs,
		worker:  worker,
	}
}

func schedule(ctx context.Context, t config.Target, s *Scheduler) {
	ticker := time.NewTicker(time.Duration(t.IntervalSecs) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case s.jobs <- Job{Target: t}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Scheduler) Dispatch(ctx context.Context, workerCount int) {
	go s.worker.run(ctx, workerCount)
	for _, target := range s.targets {
		go schedule(ctx, target, s)
	}
}
