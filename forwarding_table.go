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

func (t *ForwardingTable) Entries() []*ForwardingTableEntry {
	t.RLock()
	defer t.RUnlock()
	entries := make([]*ForwardingTableEntry, 0, t.Len())
	t.Do(func(e llrb.Comparable) bool {
		entries = append(entries, e.(*ForwardingTableEntry))
		return false
	})
	return entries
}

type ForwardingTableEntry struct {
	MaxKey int
	Shard  Shard
}

func (a *ForwardingTableEntry) Compare(b llrb.Comparable) int {
	return a.MaxKey - b.(*ForwardingTableEntry).MaxKey
}
