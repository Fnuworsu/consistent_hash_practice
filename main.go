package main

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

func (ring HashRing) AddServer(serverId string) {
	for i := 0; i < ring.virtualNodesPerNode; i++ {
		virtualNode := serverId + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(virtualNode))

		ring.nodes = append(
			ring.nodes,
			&Node{
				hash: int32(hash),
				server: serverId,
			},
		)
	}

	sort.Slice(ring.nodes, func(i, j int) bool {
		return ring.nodes[i].hash < ring.nodes[j].hash
	})
}

func (ring HashRing) RemoveServer(serverId string) {
	var newNodes []*Node

	for _, node := range ring.nodes {
		if node.server != serverId {
			newNodes = append(newNodes, node)
		}
	}

	ring.nodes = newNodes
}

func (ring HashRing) GetServer(key string) (string, error) {
	if len(ring.nodes) == 0 {
		return "", errors.New("Node server does not exist")
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ring.nodes), func(i int) bool {
		return ring.nodes[i].hash >= int32(hash)
	}) % len(ring.nodes)
	
	return ring.nodes[idx].server, nil
}