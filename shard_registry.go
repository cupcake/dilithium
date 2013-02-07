package dilithium

import (
	"reflect"
	"strings"
	"sync"
)

var ShardTypeRegistry = NewShardRegistry()

func RegisterShardType(s Shard) {
	ShardTypeRegistry.Add(s)
}

type ShardRegistry struct {
	types map[string]reflect.Type
	names map[reflect.Type]string
	mu    sync.RWMutex
}

func NewShardRegistry() *ShardRegistry {
	return &ShardRegistry{types: make(map[string]reflect.Type), names: make(map[reflect.Type]string)}
}

func (r *ShardRegistry) Name(s Shard) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.names[reflect.Indirect(reflect.ValueOf(s)).Type()]
}

func (r *ShardRegistry) Type(name string) reflect.Type {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.types[name]
}

func (r *ShardRegistry) Add(s Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	typ := reflect.Indirect(reflect.ValueOf(s)).Type()
	name := typ.Name()
	if name[len(name)-5:] == "Shard" {
		name = name[:len(name)-5]
	}
	name = strings.ToLower(name)
	r.types[name] = typ
	r.names[typ] = name
}
