package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type localSlotZeroCluster struct{ icluster.ICluster }

func (localSlotZeroCluster) SlotLeaderNodeInfo(uint32) *types.Node { return &types.Node{Id: 1} }

type slotZeroQuarantined struct{}

func (slotZeroQuarantined) SlotLeaderId(uint32) uint64 { return 1 }
func (slotZeroQuarantined) GetSlotId(string) uint32    { return 0 }
func (slotZeroQuarantined) Propose(uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (slotZeroQuarantined) ProposeUntilApplied(uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (slotZeroQuarantined) ProposeUntilAppliedTimeout(context.Context, uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (slotZeroQuarantined) CheckSlotReadable(id uint32) error {
	return fmt.Errorf("%w: slot %d", rafttypes.ErrSlotDiverged, id)
}

// T25 (ext): a quarantined slot-0 replica makes the system-UID load fail; it
// is not taken as an empty set and is retried on the next call.
func TestSystemUidsLoadFailsOnQuarantinedSlotZero(t *testing.T) {
	oldStore, oldCluster, oldOptions := service.Store, service.Cluster, options.G
	t.Cleanup(func() { service.Store, service.Cluster, options.G = oldStore, oldCluster, oldOptions })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = localSlotZeroCluster{}
	service.Store = store.New(store.NewOptions(store.WithSlot(slotZeroQuarantined{})))

	m := NewSystemAccountManager()
	require.ErrorIs(t, m.LoadIfNeed(), rafttypes.ErrSlotDiverged)
	require.False(t, m.loaded.Load(), "a fenced load is not remembered as loaded")
}
