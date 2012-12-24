package dilithium

import (
//	"bytes"
	"log"
	"os"
	)

// Provides a persistent (journaled) job queue.
// Its goal is that each job in the queue is seen by one reader once.
// Its guarantee (in the face of failures) is that each job in the queue
// is seen by at least one reader at least once.

// Readers must eventually call a matching Commit after calling Get so that
// garbage can be collected. If jobs are held (in between Get and Commit)
// long enough that many other Gets or Pushes have occurred in the meantime,
// performance may suffer.

type PersistentJobQueue struct {
	filename string // Name of the file that backs the queue
	head uint64 // id of the front of the front of the queue (last written)
	tail uint64 // id of the back of the queue (next to read)
	// TODO mmapped file
	// TODO also store the window's element data outside the mmapped files? or ptrs to them?
	// window of outstanding elements. window[i] accounts for the element of id
	// (i + tail). true means that it has been committed.
	window []bool
	readFile *os.File // File handle that we use to read queued jobs
	writeFile *os.File // File handle that we use to write new queued jobs
	headByte int64 // byte offset into journal where head record starts
	tailByte int64 // byte offset into journal where tail record starts
}

type Job interface {
	Id() uint64
	Serialize() []byte
	Deserialize([]byte) Job
}

func NewPersistentJobQueue(name string, recovery bool) *PersistentJobQueue {
	q := new(PersistentJobQueue)
	if !recovery {
		// TODO better erorr handling
		os.Mkdir("journals/", 0777)
		q.filename = "journals/" + name + ".1"
		writeFile, err := os.Create(q.filename)
		if err == nil {
			// TODO
			readFile, _ := os.Open(q.filename)
			q.writeFile = writeFile
			q.readFile = readFile
		}
		if err != nil {
			log.Println("Failure opening journal:", err)
			return nil
		} else {
			return q
		}
	} else {
		panic("Not implemented\n")
	}
	return nil
}

func (q *PersistentJobQueue) Get() Job {
	return nil
}


func (q *PersistentJobQueue) Push(job Job) {
	//bytes := job.Serialize()
	//serializedLen := len(bytes)
}

func (q *PersistentJobQueue) Commit(job Job) {

}
