package cloner

import (
	"context"
	"sync"
)

const (
	jobStatusCreated = uint8(iota)
	jobStatusPending
	jobStatusWorking
	jobStatusSuccess
	jobStatusFailure
	jobStatusAborted
)

type (
	job struct {
		fn   func(map[string]interface{}) (interface{}, error)
		args map[string]interface{}

		status uint8
		result chan *jobResult

		collector chan *job

		done func()
	}
	jobResult struct {
		err     error
		payload interface{}
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

	collector struct {
		jobsChannel chan *job
		payloads    []interface{}

		wg sync.WaitGroup
	}
)

func newCollector() *collector {
	return &collector{
		jobsChannel: make(chan *job, gCli.Int("queue-workers")+1),
	}
}

func (m *collector) collect() []interface{} {
	defer m.wg.Done()
	defer gLog.Debug().Msg("queue collector has been stopped")

	var jobs []*job

LOOP:
	for {
		select {
		case <-gCtx.Done():
			return nil
		case jb, ok := <-m.jobsChannel:
			if !ok {
				break LOOP
			}

			jobs = append(jobs, jb)

			if gCtx.Err() != nil {
				return nil
			}
		}
	}

	// LIFO stack implementation
	for len(jobs) > 0 {
		l := len(jobs) - 1
		j := jobs[l]

		if payload, ok := j.getResult(); ok {
			m.payloads = append(m.payloads, payload)
			jobs = jobs[:l]
		}
	}

	return m.payloads
}

func newJob(fn func(map[string]interface{}) (interface{}, error), args map[string]interface{}, done func()) *job {
	return &job{
		fn:   fn,
		args: args,

		status: jobStatusCreated,
		result: make(chan *jobResult, 1),

		done: done,
	}
}

func (m *job) assignCollector(coll chan *job) {
	m.collector = coll
}

// non-blocking result pop from result channel
func (m *job) getResult() (interface{}, bool) {
	select {
	case payload, ok := <-m.result:
		if gCtx.Err() != nil {
			break
		}
		return payload, ok
	case <-gCtx.Done():
		break
	default:
		break
	}
	return nil, false
}

func newWorker(ctx context.Context, workerPool chan chan *job) *worker {
	return &worker{
		ctx: ctx,

		jobChannel: make(chan *job),
		workerPool: workerPool,
	}
}

func (m *worker) start() {
	gLog.Debug().Msg("worker has been started")
	defer gLog.Debug().Msg("abort func has been called, closing worker")

	for {
		// register the current worker into the worker queue.
		m.workerPool <- m.jobChannel
		gLog.Debug().Msg("worker has been reregistered")

		select {
		case <-m.ctx.Done():
			close(m.jobChannel)
			return
		case j := <-m.jobChannel:
			j.status = jobStatusWorking

			res := &jobResult{}
			if res.payload, res.err = j.fn(j.args); res.err != nil {
				j.status = jobStatusFailure
			} else {
				j.status = jobStatusSuccess
			}

			// send result to j.getResult()
			if j.result != nil {
				j.result <- res
			}

			// send job to assigned collector if it exists
			if j.collector != nil {
				gLog.Debug().Msg("trying to push job into assigned collector")
				if j == nil {
					panic("JOB IS NILL 3")
				}
				j.collector <- j
			}

			j.done()

			if m.ctx.Err() != nil {
				close(m.jobChannel)
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
	gLog.Debug().Msg("spawning workers")

	for i := 0; i < gCli.Int("queue-workers"); i++ {
		wrk := newWorker(m.ctx, m.workerPool)
		gLog.Debug().Msgf("worker #%d starting", i)

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

	gLog.Debug().Msg("starting queue subsystem")
	m.ctx, m.abort = context.WithCancel(context.Background())
	m.spawnWorkers()

	gLog.Debug().Msg("starting queue job loop")
LOOP:
	for {
		select {
		case <-gCtx.Done():
			gLog.Debug().Msg("main context abort() has been called, stopping dispatcher")
			m.abort()
			break LOOP
		case j = <-m.jobQueue:
			j.status = jobStatusPending

			jChannel = <-m.workerPool
			jChannel <- j

			if gCtx.Err() != nil {
				gLog.Debug().Msg("main context abort() has been called, stopping dispatcher (job case)")
				m.abort()
				break LOOP
			}
		}
	}

	close(m.workerPool)

	gLog.Debug().Msg("waiting for workers death")
	m.wg.Wait()

	gLog.Debug().Msg("workers dead, bye")
}
