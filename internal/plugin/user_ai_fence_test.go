package plugin

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

type fencedSlot struct{ fenced bool }

func (*fencedSlot) SlotLeaderId(uint32) uint64 { return 1 }
func (*fencedSlot) GetSlotId(string) uint32    { return 0 }
func (*fencedSlot) Propose(uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (*fencedSlot) ProposeUntilApplied(uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (*fencedSlot) ProposeUntilAppliedTimeout(context.Context, uint32, []byte) (*types.ProposeResp, error) {
	return nil, types.ErrSlotDiverged
}
func (f *fencedSlot) CheckSlotReadable(id uint32) error {
	if f.fenced {
		return fmt.Errorf("%w: slot %d", types.ErrSlotDiverged, id)
	}
	return nil
}

func testPluginServer() *Server {
	s := &Server{Log: wklog.NewWKLog("pluginTest"), bucketSize: 2}
	s.userPluginBuckets = []*userPluginBucket{newUserPluginBucket(0), newUserPluginBucket(1)}
	return s
}

// T25d: a fenced plugin-user lookup fails the request and is never cached
// as "not AI"; plugin routing lookups are fenced too.
func TestUserIsAIErrorNotCached(t *testing.T) {
	oldStore := service.Store
	t.Cleanup(func() { service.Store = oldStore })
	service.Store = store.New(store.NewOptions(store.WithSlot(&fencedSlot{fenced: true})))
	s := testPluginServer()

	isAI, err := s.UserIsAI("bot")
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	require.False(t, isAI)
	_, cached := s.isAiFromCache("bot")
	require.False(t, cached, "a failed lookup must not be cached")

	_, err = s.GetUserPluginNo("bot")
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	_, cached = s.getPluginNoFromCache("bot")
	require.False(t, cached)

	_, err = s.searchPluginUsers(wkdbSearchAll())
	require.ErrorIs(t, err, types.ErrSlotDiverged)
}

func wkdbSearchAll() wkdb.SearchPluginUserReq { return wkdb.SearchPluginUserReq{} }
