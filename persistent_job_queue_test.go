package dilithium

import (
	"testing"
)

func TestCreate(t *testing.T) {
	q := NewPersistentJobQueue("foo", false)
	if q == nil {
		t.Error("Failed to create queue")
	}
}
