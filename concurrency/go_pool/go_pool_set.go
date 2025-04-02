package go_pool

import "fmt"

// Contains multiple goroutine pools, each with a unique identifier
type PoolSet[T interface{}] struct {
	nameToPool map[string]*Pool[T]
}

// Parameter key is the pool identifier, value is the corresponding pool configuration
func NewPoolSet[T interface{}](poolSet map[string][]Option[T]) *PoolSet[T] {
	ps := PoolSet[T]{
		nameToPool: make(map[string]*Pool[T]),
	}
	for name, opts := range poolSet {
		ps.nameToPool[name] = NewPool(opts...)
	}
	return &ps
}

func (p *PoolSet[T]) panicIfNotExist(name string) {
	_, ok := p.nameToPool[name]
	if !ok {
		panic(fmt.Sprintf("%s not exist", name))
	}
}

func (p *PoolSet[T]) New(name string, t T) {
	p.panicIfNotExist(name)
	p.nameToPool[name].New(t)
}

func (p *PoolSet[T]) Exit() {
	for _, p := range p.nameToPool {
		p.Exit()
	}
}

func (p *PoolSet[T]) IsFull(name string) bool {
	p.panicIfNotExist(name)
	return p.nameToPool[name].IsFull()
}
