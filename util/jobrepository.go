package util

import (
	"context"
	"sync"
)

type JobRepository struct {
	m  map[string]func(ctx context.Context) bool
	mu sync.Mutex
}

func CreateJobRepository() *JobRepository {
	return &JobRepository{m: make(map[string]func(ctx context.Context) bool)}
}

func (j *JobRepository) addJob(jobName string, job func(ctx context.Context) bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.m[jobName] = job
}

func (j *JobRepository) retrieveJob(jobName string) (func(ctx context.Context) bool, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	f, ok := j.m[jobName]
	return f, ok
}
