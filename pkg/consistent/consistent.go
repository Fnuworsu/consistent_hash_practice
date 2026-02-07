package consistent_hash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"github.com/Fnuworsu/consistent_hash_practice/pkg/shard"
)

type Node[V any] struct {
	Hash int32
	Server string
	Shards *shard.ShardedMap[V]
}

type HashRing[V any] struct {
	Nodes []*Node[V]
	VirtualNodesPerNode int
}

func NewHashRing[V any] (virtualNodesPerNode int) *HashRing[V] {
	return &HashRing[V]{
		VirtualNodesPerNode: virtualNodesPerNode,
	}
} 

func (this *HashRing[V]) AddServer(serverId string) {
	for i := 0; i < this.VirtualNodesPerNode; i++ {
		virtualNode := serverId + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(virtualNode))

		this.Nodes = append(
			this.Nodes,
			&Node[V]{
				Hash: int32(hash),
				Server: serverId,
			},
		)
	}

	sort.Slice(this.Nodes, func(i, j int) bool {
		return this.Nodes[i].Hash < this.Nodes[j].Hash
	})
}

func (this *HashRing[V]) RemoveServer(serverId string) {
	var newNodes []*Node[V]

	for _, node := range this.Nodes {
		if node.Server != serverId {
			newNodes = append(newNodes, node)
		}
	}

	this.Nodes = newNodes
}

func (this *HashRing[V]) GetServer(key string) (*Node[V], error) {
	if len(this.Nodes) == 0 {
		return nil, errors.New("Node server does not exist")
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(this.Nodes), func(i int) bool {
		return this.Nodes[i].Hash >= int32(hash)
	}) % len(this.Nodes)
	
	return this.Nodes[idx], nil
}

func (this *HashRing[V]) ServerExists(key string) bool {
	_, err := this.GetServer(key)
	return err == nil
}