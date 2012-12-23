package dilithium

import (
	"sort"
	"sync"
	"math/rand"
	)

type Shard interface {
	Read() error // TODO arg(s) and returns
	Mutate() error //TODO arg(s) and returns
	// TODO more?
}

type logicalShard struct {
	children []Shard // shards that are children of this shard.

	Read() error // TODO arg(s) and returns
	Mutate() error //TODO arg(s) and returns
}

type PhysicalShard interface {
	Read() error // TODO arg(s) and returns
	Mutate() error //TODO arg(s) and returns
}

type ForwardingTableEntry struct {
 	// The minimum hash value that matches this range.
	minHashVal int64
	// What shard this maps to. TODO should probably be a pointer to a tree node or something.
	shard Shard
}

type ConsistentHash struct {
	// Sorted on minHashVal. An entry's range will start with minHashValue and continue up
	// to but not including minHashValue of the next entry (or up through the max if it is
	// the last entry.
	forwardingTable []ForwardingTableEntry


	// Synchronizes access to the structure.
	// Only needs to be acquired for write if you are changing the topology in some way
  // (repartitioning, adding or removing a replica, etc)
	// Thus, should be held by readers or no one almost always and add very little overhead.
	mutex RWMutex
}

func (ConsistentHash *ch) AddForwardingTableEntry(e ForwardingTableEntry) error {
	ch.mutex.Lock()

	// Find index for insertion
	ind := Search(len(ch.forwardingTable), func(i int) bool {
		return ch.forwardingTable[i].minHashVal >= e.minHahVal
	})
	
	// Insert into slice at that point.
	ch.forwardingTable = append(ch.forwardingTable[:ind],
		append([]ForwardingTableEntry{e}, ch.forwardingTable[ind:]...)...)

	ch.mutex.Unlock()
}

// Returns the index of the child of ls to read from.
func (logicalShard *ls) pickReadShard() error {
	// Just pick a random one. Could plug in more complex strategy as desired.
	return Intn(len(ls.children))
}

func (logicalShard *ls) Read() error {
	ind := pickReadShard()
	return ls.children[ind].Read()
}

func (logicalShard *ls) Mutate() error {
	for shard := range ls.children {
		// TODO Check failure, buffer, etc.
		shard.Mutate()
	}
}
