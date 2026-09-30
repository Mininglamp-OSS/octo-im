package raft

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestApplyRetryConcurrentKeepAlive(t *testing.T) {
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithApplyErrorRetry(true)))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			n.KeepAlive()
		}
	}()
	for i := 0; i < 1000; i++ {
		n.Tick()
	}
	wg.Wait()
}
