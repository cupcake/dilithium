package dilithium

import (
	"sync"

	"code.google.com/p/biogo.llrb"
)

type ForwardingTable struct {
	llrb.Tree
	sync.RWMutex
}

func (t *ForwardingTable) Put(e *ForwardingTableEntry) {
	t.Lock()
	t.Insert(e)
	t.Unlock()
}

func (t *ForwardingTable) Lookup(key int) Shard {
	t.RLock()
	// TODO: optimize this?
	e := t.Floor(&ForwardingTableEntry{MaxKey: key})
	t.RUnlock()
	return e.(*ForwardingTableEntry).Shard
}

type ForwardingTableEntry struct {
	MaxKey int
	Shard  Shard
}

func (a *ForwardingTableEntry) Compare(b llrb.Comparable) int {
	return a.MaxKey - b.(*ForwardingTableEntry).MaxKey
}
