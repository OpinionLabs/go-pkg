package go_pool

import (
	"log"
	"time"

	"go.uber.org/atomic"
)

// Implementation of goroutine pool to improve event processing concurrency

type Pool[T interface{}] struct {
	running atomic.Int32  // Number of running goroutines
	taskCh  chan T        // Event channel
	exitCh  chan struct{} // Channel for closing goroutine pool
	options options[T]    // Configuration
}

func NewPool[T interface{}](opts ...Option[T]) *Pool[T] {
	p := Pool[T]{
		taskCh:  make(chan T),
		exitCh:  make(chan struct{}),
		options: options[T]{},
	}
	for _, opt := range opts {
		opt.apply(&p.options)
	}

	if p.options.size <= 0 {
		log.Fatal("size is less then or equal 0")
	}
	if p.options.taskCB == nil {
		log.Fatal("param taskCB is nil")
	}
	for i := 0; i < p.options.size; i++ {
		go func(i int) {
			p.startWorker(i)
		}(i)
	}
	return &p
}

func (p *Pool[T]) startWorker(i int) {
	if p.options.debug {
		log.Println("go_pool start workder ", i)
	}
	for {
		select {
		case _, ok := <-p.exitCh:
			if !ok {
				if p.options.debug {
					log.Println("go_pool exit worker ", i)
				}
				return
			}

		case t, ok := <-p.taskCh:
			if !ok {
				continue
			}
			p.running.Add(1)
			{
				p.options.taskCB(t, i)
				if p.options.doneCB != nil {
					p.options.doneCB(t, i)
				}
			}
			p.running.Add(-1)
		case <-time.After(3 * time.Minute):
    		if p.options.debug {
       			 log.Println("go_pool worker ", i, " timed out")
    		}
    	}
	}
}

func (p *Pool[T]) New(t T) {
	p.taskCh <- t
}

func (p *Pool[T]) Exit() {
	close(p.exitCh)
}

func (p *Pool[T]) IsFull() bool {
	return p.options.size <= int(p.running.Load())
}
