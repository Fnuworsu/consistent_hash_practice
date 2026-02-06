package shard

import (
	"errors"
	"hash/crc32"
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

func (this *ShardedMap[V]) getShardIndex(key string) int {
	haskedKey := int(crc32.ChecksumIEEE([]byte(key)))
	return haskedKey % len(this.shards)
}

func (this *ShardedMap[V]) getShard(key string) *Shard[V] {
	return this.shards[this.getShardIndex(key)]
}

func (this *ShardedMap[V]) Get(key string) (V, error) {
	var nullptr V
	shard := this.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if _, ok := shard.mapp[key]; ok {
		return shard.mapp[key], nil
	}

	return nullptr, errors.New("Could not find value associated with key")
}

func (this *ShardedMap[V]) Set(key string, value V) error {
	shard := this.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.mapp[key] = value

	return nil
}