package jobs

import (
	"sync"
)

// JobQueue is what dilithium clients will interact with.
// It allows calls to Push, Done, and Get, and also exposes a channel
// for get so that a client can get without blocking.
// Push and Done should also mostly be nonblocking because they are
// bufferend channels.
type JobQueue struct {
	qp                JobQueuePersister
	pushChan          chan jobBundle
	getChan, doneChan chan Job
	runSync           sync.Once
}

// This is the lowest level of the job queue.
// It provides the desired level of persistence and the queueing logic that returns
// Jobs in some order (no need for FIFO).
// JobQueue's robustness guarantees are dictated by what the persister does.
// In general, a persister should make some attempt at storing all jobs that have been
// pushed and not yet Done-ed, and should clean up jobs that have been doned.
// After failure, persisters would then be able to recover and have Get calls return all
// jobs that it is not sure have been Done-ed.
//
// The biggest complication in the code is the following: each job will only be returned
// from Get once, and any job that is received from Get **MUST** be passed to a call
// to Done so that the persister can stop tracking it and storing it. One should try to
// ensure that this happens soon, as many implementations will have overhead correlating
// to the number of jobs between the last job that hasn't been Done-ed and the last job returned
// from Get; one long lost job could make this get large.
// Implications: If a job could be "lost", then instead there should be some process that times out
// jobs that disappear, possibly placing them back in the queue or in an error queue.
type JobQueuePersister interface {
	Get() Job
	Push(job Job) bool
	Done(job Job)
}

// Encapsulates both a job and a resultChan to return the result of the
// underlying call to the persister's Push method.
type jobBundle struct {
	job        Job
	resultChan chan bool
}

// Create a new JobQueue backed by qp
func NewJobQueue(qp JobQueuePersister) *JobQueue {
	queue := new(JobQueue)
	queue.pushChan = make(chan jobBundle, 100)
	queue.getChan = make(chan Job)
	queue.doneChan = make(chan Job, 100)
	queue.qp = qp
	return queue
}

// Do a blocking Get on queue.
func (queue *JobQueue) Get() Job {
	return <-queue.GetChan()
}

// Return a channel to wait on to get jobs from queue.
// Useful if client does not necessarily want to block
// until it is given a job from this queue.
func (queue *JobQueue) GetChan() chan Job {
	return queue.getChan
}

// Push job onto queue. Will block until a Push call to queue's persister
// has been made with this job, and return the value of that push.
// Blocking is necessary because the client does not have any guarantees from
// the persister until this call has been made.
// TODO there should probably be a way for it not to block in case it's ok with
// returning without guarantees from the persister (eg. if persister is just in
// memory anyway.) and doesn't want to wait for things head of it to be pushed.
// TODO should probably return an error instead of bool
func (queue *JobQueue) Push(job Job) bool {
	bundle := jobBundle{job, make(chan bool, 1)}
	queue.pushChan <- bundle
	result := <-bundle.resultChan
	return result
}

// Tells the queue that job has been completed and does not need
// to be persisted, nor replayed after failure.
func (queue *JobQueue) Done(job Job) {
	// can definitely be async
	queue.doneChan <- job
}

// Start running the goroutine that owns this queue
func (queue *JobQueue) Start() {
	// TODO seems like I should be able to pass queue.run without anon function.
	go queue.runSync.Do(func() { queue.run() })
}

// The logic that the JobQueue performs to pass data between clients and the persister
// as that data appears.
// This *MUST* run single-threaded or the persister will be ill-syncronized.
// TODO there should be a way to quit.
func (queue *JobQueue) run() {
	// NOTE: This function assumes strong consistency provided by the JobQueuePersister.
	// In particular, it assumes that we can immediately Get our Push-es. If there is some
	// kind of delay where we might Get nil (meaning there are no jobs waiting in the queue)
	// then receive a push (meaning the queue now has one thing waiting in it) but then
	// receive nil when calling Get, we may get into a state where someone is asking for
	// a job, there is a job in the queue, but we do not pass the job to them (until another
	// push occurs).
	nextGet := queue.qp.Get()
	for {
		if nextGet == nil {
			// Don't try to send on the get channel because we have nothing to send
			select {
			case job := <-queue.doneChan:
				queue.qp.Done(job)
			case bundle := <-queue.pushChan:
				// Push to underlying queue storage
				result := queue.qp.Push(bundle.job)
				// pass the result back to the caller (and let them know it's done)
				bundle.resultChan <- result
				nextGet = queue.qp.Get()
			}
		} else {
			select {
			case job := <-queue.doneChan:
				queue.qp.Done(job)
			case bundle := <-queue.pushChan:
				// Push to underlying queue storage
				result := queue.qp.Push(bundle.job)
				// pass the result back to the caller (and let them know it's done)
				bundle.resultChan <- result
			case queue.getChan <- nextGet:
				// get a new nextGet
				nextGet = queue.qp.Get()
			}
		}
	}
}
