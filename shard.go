package dilithium

import (
	"math/rand"
)

func SetDialFunc(f func(network, address string) (interface{}, error))

type Shard interface {
	Parent() Shard
	Children() []Shard
	SetParent(s Shard)
	AddChild(s Shard)
	Query(q *Query)
}

type ReplicateShard struct {
	parent   Shard
	children []Shard
}

func (r *ReplicateShard) Parent() Shard     { return r.parent }
func (r *ReplicateShard) Children() []Shard { return r.children }
func (r *ReplicateShard) SetParent(s Shard) { r.parent = s }
func (r *ReplicateShard) AddChild(s Shard)  { r.children = append(r.children, s) }

func (r *ReplicateShard) Query(q *Query) {
	if q.ReadOnly() {
		r.children[rand.Intn(len(r.children))].Query(q)
	} else {
		for _, s := range r.children {
			s.Query(q)
		}
	}
}

type PhysicalShard struct {
	parent   Shard
	children []Shard
}

func (p *PhysicalShard) Parent() Shard     { return p.parent }
func (p *PhysicalShard) Children() []Shard { return p.children }
func (p *PhysicalShard) SetParent(s Shard) { p.parent = s }
func (p *PhysicalShard) AddChild(s Shard)  { p.children = append(p.children, s) }

func (p *PhysicalShard) Query(q *Query) {

}
