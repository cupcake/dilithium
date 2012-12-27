package dilithium

type JobQueue interface {
	Get() Job
	Push(job Job) bool
	Commit(job Job)
}


type JobQueueOwner struct {
	q JobQueue
	pushChan chan JobBundle
	getChan, commitChan chan Job
	running bool
}

type JobBundle struct {
	job Job
	resultChan chan bool
}

func NewJobQueueOwner(q JobQueue) {
	owner := new(JobQueueOwner)
	owner.pushChan = make(chan JobBundle, 100)
	owner.getChan = make(chan Job)
	owner.commitChan = make(chan Job, 100)
	owner.q = q
}

func (owner *JobQueueOwner) Get() Job {
	job := <- owner.getChan
	return job
}

func (owner *JobQueueOwner) Push(bundle JobBundle) bool {
	owner.pushChan <- bundle
	result := <- bundle.resultChan
	return result
}

func (owner *JobQueueOwner)	Commit(job Job) {
	// can definitely be async
	owner.commitChan <- job
}

// Start running the goroutine that owns this queue
func (owner *JobQueueOwner) Start() {
	if !owner.running {
		owner.running = true
		go owner.run()
	} else {
		// Don't start another!
		// TODO log
	}
}

func (owner *JobQueueOwner) run() {
	nextGet := owner.q.Get()
	for {
		if nextGet == nil {
			// Don't try to send on the get channel because we have nothing to send
			select {
			case job := <- owner.commitChan:
				owner.q.Commit(job)
			case bundle := <- owner.pushChan:
				// Push to underlying queue storage
				result := owner.q.Push(bundle.job)
				// pass the result back to the caller (and let them know it's done)
				bundle.resultChan <- result
				nextGet = owner.q.Get()
			}
		} else {
			select {
			case job := <- owner.commitChan:
				owner.q.Commit(job)
			case bundle := <- owner.pushChan:
				// Push to underlying queue storage
				result := owner.q.Push(bundle.job)
				// pass the result back to the caller (and let them know it's done)
				bundle.resultChan <- result
			case owner.getChan <- nextGet:
				// get a new nextGet
				nextGet = owner.q.Get()
			}
		}
	}
}
