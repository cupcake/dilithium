package dilithium

import (
	"testing"
)

type job struct {
	id uint64
	serialization []byte
}

func (j *job) Id() uint64 {
	return j.id
}

func (j *job) Serialize() []byte {
	return j.serialization
}

func (j *job) Deserialize(serialized [] byte) {
	j.serialization = serialized
}

func Equal(j1 *job, j2 *job) bool {
	if j1.id != j2.id || len(j1.serialization) != len(j2.serialization) {
		return false
	}

	for i, e1 := range j1.serialization {
		e2 := j2.serialization[i]
		if e1 != e2 {
			return false
		}
	}
	return true
}


func TestEverything(t *testing.T) {
	q := NewPersistentJobQueue("foo", false)
	if q == nil {
		t.Error("Failed to create queue")
	}

	if emptyGet := q.Get(); emptyGet != nil {
		t.Error("Get when empty did not return nil")
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
		t.Error("Get when empty did not return nil")
	}
}
