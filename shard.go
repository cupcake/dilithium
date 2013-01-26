package dilithium

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/titanous/guid"
)

type Shard interface {
	// Get the parent of the shard. nil if the Shard is the root.
	Parent() Shard
	Children() []Shard
	SetParent(s Shard)
	AddChild(s Shard)
	// RemoveChild removes the child shard with ID() == id.
	// The receiver is responsible for calling Destroy() on the child shard.
	RemoveChild(id string)
	Query(q *Query) error
	// ID returns the shard ID. It must uniquely identify the shard.
	ID() string
	// Setup is called to setup the shard with the specified config.
	Setup(config map[string]interface{}) error
	// Config returns a JSON-serializable config that could be passed to Setup to recreate this shard.
	Config() map[string]interface{}
	// Destroy is called when the shard is removed from the tree. The shard must call Destroy on child shards.
	Destroy()
	// The shard must be locked before unmarshalling the configuration into it.
	sync.Locker
}

var (
	shardTypes    = make(map[string]reflect.Type)
	shardTypesMtx sync.RWMutex
)

type ReplicateShard struct {
	parent   Shard
	children []Shard
	id       string
	sync.RWMutex
}

func RegisterShardType(s Shard) {
	shardTypesMtx.Lock()
	defer shardTypesMtx.Unlock()
	typ := reflect.Indirect(reflect.ValueOf(s)).Type()
	name := typ.Name()
	if name[len(name)-5:] == "Shard" {
		name = name[:len(name)-5]
	}
	shardTypes[strings.ToLower(name)] = typ
}

func (r *ReplicateShard) Parent() Shard {
	r.RLock()
	p := r.parent
	r.RUnlock()
	return p
}

func (r *ReplicateShard) Children() []Shard {
	r.RLock()
	c := r.children
	r.RUnlock()
	return c
}

func (r *ReplicateShard) SetParent(s Shard) {
	r.Lock()
	r.parent = s
	r.Unlock()
}

func (r *ReplicateShard) AddChild(s Shard) {
	r.Lock()
	r.children = append(r.children, s)
	r.Unlock()
}

func (r *ReplicateShard) RemoveChild(id string) {
	r.Lock()
	for i, s := range r.children {
		if s.ID() == id {
			s.Destroy()
			// remove the element by setting it to the last element and truncating
			r.children[i] = r.children[len(r.children)-1]
			r.children[len(r.children)-1] = nil
			r.children = r.children[:len(r.children)-1]
			break
		}
	}
	r.Unlock()
}

func (r *ReplicateShard) ID() string {
	r.RLock()
	id := r.id
	r.RUnlock()
	return id
}

func (r *ReplicateShard) Setup(config map[string]interface{}) error {
	r.Lock()
	id, _ := guid.NextId()
	r.id = strconv.FormatInt(id, 10)
	r.Unlock()
	return nil
}

func (r *ReplicateShard) Config() map[string]interface{} {
	return nil
}

func (r *ReplicateShard) Destroy() {
	r.Lock()
	for _, s := range r.children {
		s.Destroy()
	}
}

func (r *ReplicateShard) Query(q *Query) (err error) {
	r.RLock()
	if q.ReadOnly() {
		err = r.children[rand.Intn(len(r.children))].Query(q)
	} else {
		for _, s := range r.children {
			s.Query(q)
		}
	}
	r.RUnlock()
	return
}

type PhysicalShard struct {
	parent Shard
	sync.RWMutex
	config map[string]interface{}
	pool   *Pool
}

func (p *PhysicalShard) Parent() Shard {
	p.RLock()
	pa := p.parent
	p.RUnlock()
	return pa
}

func (p *PhysicalShard) SetParent(s Shard) {
	p.Lock()
	p.parent = s
	p.Unlock()
}

func (p *PhysicalShard) Children() []Shard {
	return nil
}

func (p *PhysicalShard) AddChild(s Shard) {
	// no-op
}

func (p *PhysicalShard) RemoveChild(id string) {
	// no-op
}

func (p *PhysicalShard) Setup(config map[string]interface{}) error {
	p.Lock()
	defer p.Unlock()

	u, ok := config["url"]
	if !ok {
		return errors.New("dilithium: Missing 'url' in PhysicalShard config")
	}
	url, ok := u.(string)
	if !ok {
		return errors.New("dilithium: Unexpected type for PhysicalShard 'url' config, expecting string")
	}

	pn, ok := config["pool"]
	if !ok {
		return errors.New("dilithium: Missing 'pool' in PhysicalShard config")
	}
	poolName, ok := pn.(string)
	if !ok {
		return errors.New("dilithium: Unexpected type for PhysicalShard 'pool' config, expecting string")

	}
	poolTypesMtx.RLock()
	defer poolTypesMtx.RUnlock()
	poolType, ok := poolTypes[poolName]
	if !ok {
		return fmt.Errorf("dilithium: Unknown PhysicalShard pool type '%s'", poolName)
	}

	p.pool = &Pool{url: url, Dial: poolType.Dial, TestOnBorrow: poolType.TestOnBorrow, MaxIdle: poolType.MaxIdle, IdleTimeout: poolType.IdleTimeout}
	p.config = config
	return nil
}

func (p *PhysicalShard) Config() map[string]interface{} {
	p.RLock()
	defer p.RUnlock()
	return p.config
}

func (p *PhysicalShard) Destroy() {
	p.Lock()
	p.pool.Close()
}

func (p *PhysicalShard) ID() string {
	p.RLock()
	defer p.RUnlock()
	return p.pool.url
}

func (p *PhysicalShard) Query(q *Query) error {
	p.RLock()
	conn := p.pool.Get()
	err := q.Run(conn)
	conn.Close()
	p.RUnlock()
	return err
}

func init() {
	RegisterShardType(&ReplicateShard{})
	RegisterShardType(&PhysicalShard{})
}
