package scheduler

import (
	"context"
	"time"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
)

type Scheduler struct {
	targets []config.Target
	jobs    chan<- Job
	worker  *Worker
}

func NewScheduler(targets []config.Target, jobs chan<- Job, worker *Worker) *Scheduler {
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
			s.jobs <- Job{Target: t}
		}
	}
}

func (s *Scheduler) Dispatch(ctx context.Context) {
	go s.worker.run(ctx)
	for _, target := range s.targets {
		go schedule(ctx, target, s)
	}
}
