package shard

import (
	"sync"
)

type Shard[V any] struct {
	mapp map[string]V
	mu sync.RWMutex
}

type ShardedMap[V any] struct {
	shards []*Shard[V]
}

func Constructor[V any] (numberOfShards int) *ShardedMap[V] {
	shards := make([]*Shard[V], numberOfShards)

	for i := 0; i < numberOfShards; i++ {
		shards[i] = &Shard[V]{
			mapp: make(map[string]V),
		}
	}

	return &ShardedMap[V]{
		shards: shards,
	}
} 