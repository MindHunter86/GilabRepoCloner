package cloner

import (
	"context"
	"sync"
)

type (
	job struct {
		fn   func(interface{}) error
		args interface{}
	}
	worker struct {
		ctx context.Context

		workerPool chan chan *job
		jobChannel chan *job
	}
	pool struct {
		ctx   context.Context
		abort func()

		wg sync.WaitGroup

		jobQueue   chan *job
		workerPool chan chan *job
	}
)

func newWorker(ctx context.Context, workerPool chan chan *job) *worker {
	return &worker{
		ctx: ctx,

		jobChannel: make(chan *job),
		workerPool: workerPool,
	}
}

func (m *worker) start() {
	var j *job

	for {
		// register the current worker into the worker queue.
		m.workerPool <- m.jobChannel

		select {
		case <-m.ctx.Done():
			return

		case j = <-m.jobChannel:
			// payload
			_ = j.fn(j.args)

			if m.ctx.Err() != nil {
				return
			}
		}
	}
}

func newPool() *pool {
	return &pool{
		jobQueue:   make(chan *job, gCli.Int("queue-job-buffer")),
		workerPool: make(chan chan *job, gCli.Int("queue-workers")),
	}
}

func (m *pool) getJobQueue() chan *job {
	return m.jobQueue
}

func (m *pool) spawnWorkers() {
	for i := 0; i < gCli.Int("queue-workers"); i++ {
		wrk := newWorker(m.ctx, m.workerPool)

		m.wg.Add(1)
		go func(wrk *worker, done func()) {
			wrk.start()
			done()
		}(wrk, m.wg.Done)
	}
}

func (m *pool) dispatch() {
	var j *job
	var jChannel chan *job

	m.ctx, m.abort = context.WithCancel(context.Background())
	m.spawnWorkers()

	for {
		select {
		case <-gCtx.Done():
			m.abort()
			return
		case j = <-m.jobQueue:
			jChannel = <-m.workerPool
			jChannel <- j

			if gCtx.Err() != nil {
				m.abort()
				return
			}
		}
	}

	m.wg.Wait()
}
