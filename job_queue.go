package dilithium

import (
	"sync"
	)


type JobQueuePersister interface {
	Get() Job
	Push(job Job) bool
	Done(job Job)
}


type JobQueue struct {
	qp JobQueuePersister
	pushChan chan JobBundle
	getChan, doneChan chan Job
	runSync sync.Once
}

type JobBundle struct {
	job Job
	resultChan chan bool
}

func NewJobQueue(qp JobQueuePersister) {
	queue := new(JobQueue)
	queue.pushChan = make(chan JobBundle, 100)
	queue.getChan = make(chan Job)
	queue.doneChan = make(chan Job, 100)
	queue.qp = qp
}

func (queue *JobQueue) Get() Job {
	job := <- queue.getChan
	return job
}

func (queue *JobQueue) Push(bundle JobBundle) bool {
	queue.pushChan <- bundle
	result := <- bundle.resultChan
	return result
}

func (queue *JobQueue)	Done(job Job) {
	// can definitely be async
	queue.doneChan <- job
}

// Start running the goroutine that owns this queue
func (queue *JobQueue) Start() {
	// TODO seems like I should be able to pass queue.run without anon function.
	go queue.runSync.Do(func(){ queue.run() })
}

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
			case job := <- queue.doneChan:
				queue.qp.Done(job)
			case bundle := <- queue.pushChan:
				// Push to underlying queue storage
				result := queue.qp.Push(bundle.job)
				// pass the result back to the caller (and let them know it's done)
				bundle.resultChan <- result
				nextGet = queue.qp.Get()
			}
		} else {
			select {
			case job := <- queue.doneChan:
				queue.qp.Done(job)
			case bundle := <- queue.pushChan:
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
