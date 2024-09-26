package channel

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	assert2 "github.com/stretchr/testify/assert"
)

func TestNoBlock(t *testing.T) {
	assert := assert2.New(t)
	ch1 := make(chan interface{})
	ch2 := NoBlock(ch1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	cnt := 10000

	nextExpect := 0
	go func() {
		defer wg.Done()
		for {
			select {
			case e, ok := <-ch2:
				if !ok {
					return
				}
				val, ok := e.(int)
				assert.True(ok)
				assert.Equal(nextExpect, val)
				nextExpect += 1
			}
		}
	}()

	for next := 0; next < cnt; next++ {
		ch1 <- next
	}
	close(ch1)
	wg.Wait()
}