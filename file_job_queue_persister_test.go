package dilithium

import (
	"testing"
	"encoding/binary"
)

const jobIdBytes = 8

type job struct {
	id uint64
	args []byte
}

func (j *job) Id() uint64 {
	return j.id
}

func (j *job) SetId(id uint64) {
	j.id = id
}

func (j *job) Serialize() []byte {
	idBytes := make([]byte, jobIdBytes)
	binary.BigEndian.PutUint64(idBytes, j.Id())
	return append(idBytes, j.args...)
}

func (j *job) Deserialize(serialized [] byte) {
	j.id = binary.BigEndian.Uint64(serialized[:jobIdBytes])
	j.args = serialized[jobIdBytes:]
}

func Equal(j1 *job, j2 *job) bool {
	if j1.id != j2.id || len(j1.args) != len(j2.args) {
		return false
	}

	for i, e1 := range j1.args {
		e2 := j2.args[i]
		if e1 != e2 {
			return false
		}
	}
	return true
}


func TestSimple(t *testing.T) {
	q := NewFileJobQueuePersister("file_job_queue_persister_test", false)
	if q == nil {
		t.Error("Failed to create queue")
	}

	if emptyGet := q.Get(); emptyGet != nil {
		t.Error("Get when initially empty did not return nil")
	}

	j1 := &job{}
	q.Push(j1)

	j2Serialized := make([]byte, 10)
	j2Serialized[5] = 200
	j2 := &job{0, j2Serialized}
	q.Push(j2)


	get1 := q.Get().(*job)
	if !Equal(j1, get1) {
		t.Error(get1)
	}
	get2 := q.Get().(*job)
	if !Equal(j2, get2) {
		t.Error(get2)
	}

	if emptyGet := q.Get(); emptyGet != nil {
		t.Error("Get after emptied did not return nil")
	}

	q.Done(get2)
	q.Done(get1)
}
