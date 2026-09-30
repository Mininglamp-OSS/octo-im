package slot

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type retryTestNode struct{ icluster.Node }

func (retryTestNode) Slot(uint32) *nodetypes.Slot {
	return &nodetypes.Slot{Id: 0, Leader: 1, Term: 1, Replicas: []uint64{1}}
}
func (n retryTestNode) Slots() []*nodetypes.Slot { return []*nodetypes.Slot{n.Slot(0)} }

// Exercise slot.Server -> real raftgroup worker -> PebbleShardLogStorage ->
// OnApply, rather than sending synthetic ApplyResp events directly to a Node.
func TestSlotApplyRetryThroughRealRaftGroup(t *testing.T) {
	var attempts atomic.Int32
	firstFailure := make(chan struct{})
	release := make(chan struct{})
	s := NewServer(NewOptions(WithNodeId(1), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithNode(retryTestNode{}), WithApplyErrorRetry(true),
		WithOnSaveConfig(func(uint32, types.Config) error { return nil }),
		WithOnApply(func(_ uint32, logs []types.Log) error {
			if attempts.Add(1) == 1 {
				close(firstFailure)
				<-release
			}
			if attempts.Load() <= 3 {
				return errors.New("injected temporary disk failure")
			}
			return nil
		})))
	s.raftGroup.Options().TickInterval = 10 * time.Millisecond
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s.AddEvent("0", types.Event{Type: types.Propose, Logs: []types.Log{{Id: 1, Index: 1, Term: 1, Data: []byte("command")}}})
	select {
	case <-firstFailure:
	case <-ctx.Done():
		t.Fatal("slot never reached OnApply")
	}
	index, err := s.AppliedIndex(0)
	require.NoError(t, err)
	require.Zero(t, index, "failed apply must not advance durable progress")
	close(release)
	require.Eventually(t, func() bool {
		index, err := s.AppliedIndex(0)
		return err == nil && index == 1
	}, time.Second, time.Millisecond, "slot did not recover after the fault cleared")
	require.Equal(t, int32(4), attempts.Load())
	index, err = s.AppliedIndex(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	s.AddEvent("0", types.Event{Type: types.Propose, Logs: []types.Log{{Id: 2, Index: 2, Term: 1, Data: []byte("next command")}}})
	require.Eventually(t, func() bool {
		index, err := s.AppliedIndex(0)
		return err == nil && index == 2
	}, time.Second, time.Millisecond, "subsequent proposals must also apply")
}
