package dilithium

import (
	"log"
	"os"
	"encoding/binary"
	)

const recordLengthBytes = 4 // Use 32-bit numbers for writing the length in the journal.

// Provides a persistent (journaled) job queue.
// Its goal is that each job in the queue is seen by one reader once.
// Its guarantee (in the face of failures) is that each job in the queue
// is seen by at least one reader at least once.

// Readers must eventually call a matching Done after calling Get so that
// garbage can be collected. If jobs are held (in between Get and Done)
// long enough that many other Gets or Pushes have occurred in the meantime,
// performance may suffer.

// It is not threadsafe. The user must provide some form of synchronization on top.
// The recommendation is to have one goroutine that owns it and has jobs passed in
// and out via channels. See other files.

type FileJobQueuePersister struct {
	filename string // Name of the file that backs the queue
	head uint64 // id of the front of the front of the queue (last written)
	tail uint64 // id of the back of the queue (next to read)
	// window of outstanding elements. window[i] accounts for the element of id
	// (i + tail). true means that it has been doneted.
	window []bool
	readFile *os.File // File handle that we use to read queued jobs
	writeFile *os.File // File handle that we use to write new queued jobs
	headByte int64 // byte offset into journal where head record starts
	tailByte int64 // byte offset into journal where tail record starts
}

type Job interface {
	Id() uint64
	SetId(uint64)
	Serialize() []byte
	Deserialize([]byte)
}

func NewFileJobQueuePersister(name string, recovery bool) *FileJobQueuePersister {
	q := new(FileJobQueuePersister)
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

func (q *FileJobQueuePersister) Get() Job {
	// read from file
	lengthBuffer := make([]byte, recordLengthBytes)
	bytesRead, err := q.readFile.Read(lengthBuffer)
	if bytesRead < recordLengthBytes || err != nil {
		// TODO pass back error?
		return nil
	}
	recordLength := binary.BigEndian.Uint32(lengthBuffer)
	jobBuffer := make([]byte, recordLength)
	bytesRead, err = q.readFile.Read(jobBuffer)
	if uint32(bytesRead) < recordLength || err != nil {
		// TODO pass back error?
		return nil
	}

	j := &job{}
	j.Deserialize(jobBuffer)


	// Extend window to track that this job has not been doneted.
	q.window = append(q.window, false)

	return j
}

func writeLen(f *os.File, length uint32) bool {
	bytes := make([]byte, recordLengthBytes)
	binary.BigEndian.PutUint32(bytes, length)
	return writeBytes(f, bytes)
}

func writeBytes(f *os.File, bytes []byte) bool {
	written, err := f.Write(bytes)
	return err == nil && written == len(bytes)
}


func (q *FileJobQueuePersister) Push(job Job) bool {
	// Give it the next id
	job.SetId(q.head)
	q.head++

	bytes := job.Serialize()
	success := writeLen(q.writeFile, uint32(len(bytes)))
	if success {
		success := writeBytes(q.writeFile, bytes)
		return success
	}
	return success
}

// Returns the index of job in window, or -1 if out of range
func (q *FileJobQueuePersister) windowIndex(job Job) int {
	ind := int(job.Id() - q.tail)
	if ind < 0 || ind > len(q.window) {
		ind = -1
	}
	return ind
}

func firstFalseIndex(slice []bool) int {
	for i, v := range slice {
		if !v {
			return i
		}
	}
	return len(slice)
}

func (q *FileJobQueuePersister) Done(job Job) {
	ind := q.windowIndex(job)
	if ind < 0 {
		// TODO log or return 0 or something
		return
	}
	// Try to slide the window forward
	newTail := firstFalseIndex(q.window)
	if newTail > 0 {
		// Slide window forward.
		q.window = q.window[:newTail]
		q.tail += uint64(newTail)
	}
}
