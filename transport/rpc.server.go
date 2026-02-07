package transport

import (
	"errors"
	"net"
	"net/rpc"
	"github.com/Fnuworsu/consistent_hash_practice/pkg/consistent"
	"github.com/Fnuworsu/consistent_hash_practice/pkg/shard"
	"github.com/Fnuworsu/consistent_hash_practice/types"
)

type RPCServer[V any] struct {
	HashRing *consistent_hash.HashRing[V]
	ShardedMap *shard.ShardedMap[V]
	Port string
}

// port -> serverId
// each server has storage(node: shards)

func NewRPCServer[V any] (port string) *RPCServer[V] {
	return &RPCServer[V]{
		HashRing: new(consistent_hash.HashRing[V]),
		ShardedMap: new(shard.ShardedMap[V]),
		Port: port,
	}
}

func (rs *RPCServer[V]) Set(args *types.Args[V], reply *types.Reply[V]) error {
	if !rs.HashRing.ServerExists(rs.Port) {
		rs.HashRing.AddServer(rs.Port)
	}

	node, _ := rs.HashRing.GetServer(rs.Port)
	node.Shards.Set(args.Key, args.Value)

	return nil
}

func (rs *RPCServer[V]) Get(args *types.Args[V], reply *types.Reply[V]) error {
	if !rs.HashRing.ServerExists(rs.Port) {
		return errors.New("No hash ring exist for this instance yet")
	}

	node, _ := rs.HashRing.GetServer(rs.Port)
	reply.Value, _ = node.Shards.Get(args.Key)

	return nil
}

func (rs *RPCServer[V]) Start() error {
	rpcServer := new(RPCServer[V])
	rpc.Register(rpcServer)

	listener, err := net.Listen("tcp", ":" + rs.Port)
	if err != nil {
		return errors.New("Error listening")
	}
	defer listener.Close()
	
	rpc.Accept(listener)
	return nil
}