package database

import (
	"log"

	checker "github.com/UllasSG/Uptime-status-checker/internal/Checker"
)

type ResultQueue struct {
	JobResults chan<- checker.JobResult
}

func NewResultQueue(JobResults chan<- checker.JobResult) *ResultQueue {
	return &ResultQueue{
		JobResults: JobResults,
	}
}

func (rq *ResultQueue) Enqueue(JobResult checker.JobResult) {
	rq.JobResults <- JobResult
	log.Printf("%s enqued , checked at %s", JobResult.Target.URL, JobResult.CheckedAt)
}
