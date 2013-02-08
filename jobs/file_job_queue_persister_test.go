package jobs

import (
	"testing"
)

func TestSimple(t *testing.T) {
	q := NewFileJobQueuePersister("file_job_queue_persister_test")
	if q == nil {
		t.Error("Failed to create queue")
	}

	if emptyGet := q.Get(); emptyGet != nil {
		t.Error("Get when initially empty did not return nil", emptyGet)
	}

	j1 := &job{}
	q.Push(j1)

	j2Serialized := make([]byte, 10)
	j2Serialized[5] = 200
	j2 := &job{0, j2Serialized}
	q.Push(j2)

	get1 := q.Get().(*job)
	if !equal(j1, get1) {
		t.Error(get1)
	}
	get2 := q.Get().(*job)
	if !equal(j2, get2) {
		t.Error(get2)
	}

	if emptyGet := q.Get(); emptyGet != nil {
		t.Error("Get after emptied did not return nil", emptyGet)
	}

	q.Done(get2)
	q.Done(get1)
}
