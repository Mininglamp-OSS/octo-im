package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// quarantinedSlot fences every slot, as on a node whose replicas of the
// owning slots are quarantined.
type quarantinedSlot struct{}

func (quarantinedSlot) SlotLeaderId(uint32) uint64 { return 1 }
func (quarantinedSlot) GetSlotId(string) uint32    { return 3 }
func (quarantinedSlot) Propose(uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (quarantinedSlot) ProposeUntilApplied(uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (quarantinedSlot) ProposeUntilAppliedTimeout(context.Context, uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (quarantinedSlot) CheckSlotReadable(id uint32) error {
	return fmt.Errorf("%w: slot %d", types.ErrSlotDiverged, id)
}

// T25f: permission checks fail closed with ReasonSystemError, never allow.
func TestPermissionFailsClosedOnQuarantinedSlot(t *testing.T) {
	oldStore, oldOptions := Store, options.G
	t.Cleanup(func() { Store, options.G = oldStore, oldOptions })
	Store = store.New(store.NewOptions(store.WithSlot(quarantinedSlot{})))
	options.G = options.New()
	p := NewPermissionService(nil)

	reason, err := p.HasPermissionForChannel("g1", wkproto.ChannelTypeGroup)
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	require.Equal(t, wkproto.ReasonSystemError, reason)

	reason, err = p.hasPermissionForCommChannel("g1", wkproto.ChannelTypeGroup, SenderInfo{UID: "u1"})
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	require.Equal(t, wkproto.ReasonSystemError, reason)

	reason, err = p.allowSend("u1", "u2")
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	require.Equal(t, wkproto.ReasonSystemError, reason)
}
