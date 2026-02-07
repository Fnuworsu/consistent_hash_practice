package types

type Args[V any] struct {
	Key string
	Value V
}

type Reply[V any] struct {
	Value V
}