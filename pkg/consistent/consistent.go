package consistent_hash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
)

type Node struct {
	hash int32
	server string
}

type HashRing struct {
	nodes []*Node
	virtualNodesPerNode int
}

func Constructor(virtualNodesPerNode int) *HashRing {
	return &HashRing{
		virtualNodesPerNode: virtualNodesPerNode,
	}
} 

func (this HashRing) AddServer(serverId string) {
	for i := 0; i < this.virtualNodesPerNode; i++ {
		virtualNode := serverId + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(virtualNode))

		this.nodes = append(
			this.nodes,
			&Node{
				hash: int32(hash),
				server: serverId,
			},
		)
	}

	sort.Slice(this.nodes, func(i, j int) bool {
		return this.nodes[i].hash < this.nodes[j].hash
	})
}

func (this HashRing) RemoveServer(serverId string) {
	var newNodes []*Node

	for _, node := range this.nodes {
		if node.server != serverId {
			newNodes = append(newNodes, node)
		}
	}

	this.nodes = newNodes
}

func (this HashRing) GetServer(key string) (string, error) {
	if len(this.nodes) == 0 {
		return "", errors.New("Node server does not exist")
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(this.nodes), func(i int) bool {
		return this.nodes[i].hash >= int32(hash)
	}) % len(this.nodes)
	
	return this.nodes[idx].server, nil
}