package go_pool

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoolSet(t *testing.T) {
	wg := sync.WaitGroup{}
	calcs := sync.Map{}
	dones := sync.Map{}
	opts := []Option[TaskForT]{
		WithSize[TaskForT](10),
		WithDebug[TaskForT](true),
		WithTaskCB(func(t TaskForT, i int) {
			calcs.Store(t.i, struct{}{})
		}),
		WithDoneCB(func(t TaskForT, i int) {
			dones.Store(t.i, struct{}{})
			wg.Done()
		}),
	}

	nameToPool := make(map[string][]Option[TaskForT])
	nameToPool["test"] = opts
	ps := NewPoolSet(nameToPool)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		ps.New("test", TaskForT{
			i: i,
		})
	}
	wg.Wait()

	assert := require.New(t)
	for i := 0; i < 100; i++ {
		_, ok := calcs.Load(i)
		assert.True(ok)

		_, ok = dones.Load(i)
		assert.True(ok)
	}

	ps.Exit()
	time.Sleep(100 * time.Millisecond)
}
