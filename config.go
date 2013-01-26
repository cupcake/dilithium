package dilithium

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

type ShardConfig struct {
	Type     string                 `json:"type"`
	Config   map[string]interface{} `json:"config"`
	Children []ShardConfig          `json:"children"`
}

func NewForwardingTableFromJSON(r io.Reader) (*ForwardingTable, error) {
	config := make(map[string]ShardConfig)
	err := json.NewDecoder(r).Decode(config)
	if err != nil {
		return nil, err
	}
	return NewForwardingTable(config)
}

func NewForwardingTable(config map[string]ShardConfig) (*ForwardingTable, error) {
	table := &ForwardingTable{}
	shardTypesMtx.RLock()
	defer shardTypesMtx.RUnlock()
	for m, c := range config {
		maxKey, err := strconv.Atoi(m)
		if err != nil {
			return nil, fmt.Errorf("dilithium: Invalid maxKey from JSON config, expecting integer, got '%s'", maxKey)
		}

		shard, err := NewShard(c)
		if err != nil {
			return nil, err
		}
		table.Put(&ForwardingTableEntry{maxKey, shard})
	}
	return table, nil
}

func NewShard(config ShardConfig) (shard Shard, err error) {
	if config.Type == "" {
		return nil, errors.New("dilithium: Missing shard type")
	}

	shardType, ok := shardTypes[config.Type]
	if !ok {
		return nil, fmt.Errorf("dilithium: Unknown shard type '%s'", config.Type)
	}

	shard = reflect.New(shardType).Interface().(Shard)
	err = shard.Setup(config.Config)
	if err != nil {
		return nil, err
	}

	for _, child := range config.Children {
		childShard, err := NewShard(child)
		if err != nil {
			return nil, err
		}
		childShard.SetParent(shard)
		shard.AddChild(childShard)
	}
	return
}
