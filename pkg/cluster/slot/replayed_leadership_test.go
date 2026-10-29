package slot

import (
	"testing"

	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func replayTestSlot(t *testing.T) (*Server, *Slot) {
	s := &Server{opts: NewOptions(WithNodeId(1002), WithRecovering(true), WithOnApply(func(uint32, []types.Log) error { return nil }))}
	s.storage = NewPebbleShardLogStorage(s, t.TempDir(), 1)
	require.NoError(t, s.storage.Open())
	t.Cleanup(func() { require.NoError(t, s.storage.Close()) })
	metadata := &clustertypes.Slot{Id: 1, Leader: 1002, Term: 2, Replicas: []uint64{1001, 1002, 1003}}
	return s, newSlot(metadata, s)
}

func TestReplayedLeadershipCannotReleaseFreshSlotFence(t *testing.T) {
	s, local := replayTestSlot(t)
	for _, metadata := range []*clustertypes.Slot{
		{Id: 1, Leader: 1002, Term: 2, Replicas: []uint64{1001, 1002, 1003}},
		{Id: 1, Leader: 1001, Term: 3, Replicas: []uint64{1001, 1002, 1003}},
		{Id: 1, Leader: 1002, Term: 4, Replicas: []uint64{1001, 1002, 1003}},
	} {
		require.NoError(t, local.Step(types.Event{Type: types.ConfChange, Config: s.slotRuntimeConfig(metadata, local)}))
		local.Ready()
		require.False(t, local.IsLeader(), "replaying another leader and a larger term does not restore lost logs")
	}
}

func TestPartialPrefixCannotReleaseRecreatedSlotFence(t *testing.T) {
	s, local := replayTestSlot(t)
	configure := func(local *Slot, leader uint64, term uint32) {
		metadata := &clustertypes.Slot{Id: 1, Leader: leader, Term: term, Replicas: []uint64{1001, 1002, 1003}}
		require.NoError(t, local.Step(types.Event{Type: types.ConfChange, Config: s.slotRuntimeConfig(metadata, local)}))
		local.Ready()
	}
	persist := func(local *Slot, logs []types.Log, term uint32) {
		require.NoError(t, local.Step(types.Event{Type: types.SyncResp, From: 1001, Term: term,
			CommittedIndex: 4, Reason: types.ReasonOk, Logs: logs}))
		for _, event := range local.Ready() {
			if event.Type == types.StoreReq {
				require.NoError(t, s.storage.AppendLogs("1", event.Logs, event.TermStartIndexInfo))
				require.NoError(t, local.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk,
					Index: event.Logs[len(event.Logs)-1].Index}))
			}
		}
	}
	configure(local, 1001, 3)
	persist(local, []types.Log{{Index: 1, Term: 3}, {Index: 2, Term: 3}}, 3)
	metadata := &clustertypes.Slot{Id: 1, Leader: 1002, Term: 4, Replicas: []uint64{1001, 1002, 1003}}
	local = newSlot(metadata, s)
	configure(local, 1002, 4)
	require.False(t, local.IsLeader(), "a partial persisted prefix is insufficient after Raft recreation")
	configure(local, 1001, 5)
	persist(local, []types.Log{{Index: 3, Term: 3}, {Index: 4, Term: 3}}, 5)
	configure(local, 1002, 6)
	require.True(t, local.IsLeader(), "verified durable catch-up may release the startup fence")
}
