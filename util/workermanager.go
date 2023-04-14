package util

import (
	"context"
	"errors"
	"sync"
)

var ErrWorkerLimitExceeded = errors.New("error: workers limit exceeded")
var ErrJobDoesNotExist = errors.New("error: jobs does not exist")

type WorkerManager struct {
	maxSize    int
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	repository *JobRepository
	consumed   int
	jobs       map[string]context.CancelFunc
}

func CreateWorkerManager(size int, repository *JobRepository) *WorkerManager {
	ctx, cancel := context.WithCancel(context.Background())
	wm := &WorkerManager{maxSize: size, ctx: ctx, cancel: cancel, repository: repository, jobs: make(map[string]context.CancelFunc)}
	return wm
}

func (wm *WorkerManager) DelegateJob(jobName string, endCbk func()) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.consumed == wm.maxSize {
		return ErrWorkerLimitExceeded
	}

	wm.consumed = wm.consumed + 1
	ctx, cancelFunc := context.WithCancel(wm.ctx)

	wm.jobs[jobName] = cancelFunc

	var fun func(ctx context.Context) bool
	var ok bool
	if fun, ok = wm.repository.retrieveJob(jobName); !ok {
		return ErrJobDoesNotExist
	}

	go func(ctx2 context.Context) {
		ok := fun(ctx2)

		wm.mu.Lock()
		defer wm.mu.Unlock()
		if ok {
			endCbk()
		}
		delete(wm.jobs, jobName)
		wm.consumed = wm.consumed - 1
	}(ctx)
	return nil
}

func (wm *WorkerManager) StopJob(jobName string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if cancel, ok := wm.jobs[jobName]; ok {
		cancel()
		delete(wm.jobs, jobName)
	}
}

func (wm *WorkerManager) ShutDownJobs() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.cancel()
}

func (wm *WorkerManager) GetCapacity() (int, int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	return wm.consumed, wm.maxSize
}
