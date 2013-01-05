package dilithium

import (
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

func equal(j1 *job, j2 *job) bool {
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

