package dilithium

import (
	"testing"
	)

func TestSinglethreaded(t *testing.T) {
	qp := NewFileJobQueuePersister("job_queue_test")
	if qp == nil {
		t.Error("Failed to create queue")
	}

	q := NewJobQueue(qp)

	q.Start()

	select {
	case <- q.GetChan():
		t.Error("Get when initially empty returned something on the channel")
	default:
		// It should follow this branch because nothing will ever be written
		// to the GetChan
	}

	j1 := &job{}
	q.Push(j1)

	j2Serialized := make([]byte, 10)
	j2Serialized[5] = 200
	j2 := &job{0, j2Serialized}
	q.Push(j2)


	get1 := (<- q.GetChan()).(*job)
	if !equal(j1, get1) {
		t.Error(get1)
	}
	get2 := (<- q.GetChan()).(*job)
	if !equal(j2, get2) {
		t.Error(get2)
	}

	select {
	case <- q.GetChan():
		t.Error("Get after being emptied returned something on the channel")
	default:
		// It should follow this branch because nothing will ever be written
		// to the GetChan
	}

	q.Done(get2)
	q.Done(get1)
}
