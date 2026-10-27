package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type quarantinedSlot struct{}

func (quarantinedSlot) SlotLeaderId(uint32) uint64 { return 1 }
func (quarantinedSlot) GetSlotId(string) uint32    { return 3 }
func (quarantinedSlot) Propose(uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (quarantinedSlot) ProposeUntilApplied(uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (quarantinedSlot) ProposeUntilAppliedTimeout(context.Context, uint32, []byte) (*rafttypes.ProposeResp, error) {
	return nil, rafttypes.ErrSlotDiverged
}
func (quarantinedSlot) CheckSlotReadable(id uint32) error {
	return fmt.Errorf("%w: slot %d", rafttypes.ErrSlotDiverged, id)
}

// T25 (ext): subscriber fan-out for a channel whose slot is quarantined on
// this node errors; nothing is delivered as if the channel had no
// subscribers.
func TestDistributeFailsOnQuarantinedSubscriberRead(t *testing.T) {
	setupSendResult(t, 1)
	oldStore, oldChannel := service.Store, eventbus.Channel
	t.Cleanup(func() { service.Store, eventbus.Channel = oldStore, oldChannel })
	service.Store = store.New(store.NewOptions(store.WithSlot(quarantinedSlot{})))
	options.G.Cluster.NodeId = 1

	h := NewHandler()
	tag, err := h.makeChannelTag("group", wkproto.ChannelTypeGroup)
	require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
	require.Nil(t, tag, "no tag (and so no recipient set) is made")

	subscribers, err := h.getSubscribers("group", wkproto.ChannelTypeGroup)
	require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
	require.Nil(t, subscribers)
}
