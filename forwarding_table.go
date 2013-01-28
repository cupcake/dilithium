package dilithium

import (
	"sync"

	"code.google.com/p/biogo.llrb"
)

type ForwardingTable struct {
	llrb.Tree
	sync.RWMutex
}

func (t *ForwardingTable) Insert(e *ForwardingTableEntry) {
	t.Lock()
	t.Tree.Insert(e)
	t.Unlock()
}

func (t *ForwardingTable) Delete(maxKey int) {
	t.Lock()
	t.Tree.Delete(&ForwardingTableEntry{MaxKey: maxKey})
	t.Unlock()
}

func (t *ForwardingTable) Lookup(key int) Shard {
	t.RLock()
	// TODO: optimize this?
	e := t.Ceil(&ForwardingTableEntry{MaxKey: key})
	t.RUnlock()
	if e == nil {
		return nil
	}
	return e.(*ForwardingTableEntry).Shard
}

type ForwardingTableEntry struct {
	MaxKey int
	Shard  Shard
}

func (a *ForwardingTableEntry) Compare(b llrb.Comparable) int {
	return a.MaxKey - b.(*ForwardingTableEntry).MaxKey
}
