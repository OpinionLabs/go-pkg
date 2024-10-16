package go_pool

import "fmt"

// 含多个协程池, 每个协程池有一个标识符
type PoolSet[T interface{}] struct {
	nameToPool map[string]*Pool[T]
}

// 参数的key是连接池的标识, value是对应连接池的配置
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
