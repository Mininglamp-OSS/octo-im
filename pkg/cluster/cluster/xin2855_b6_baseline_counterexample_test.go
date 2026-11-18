package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

// TestXIN2855_B6_LoadOnlyChannelClusterConfigFencedFastPathAndABA verifies B6:
// 1) When local slotServer/slotRaft is nil or stepped down on the fast path, LoadOnlyChannelClusterConfig fails closed.
// 2) Leadership/term/epoch changes during fast path, apply wait, or DB read (including away-and-back ABA) return ErrConversationReadRetry.
// 3) Stable leader/term/epoch succeeds, and repeated leader churn exits within bounded deadline/attempts.
func TestXIN2855_B6_LoadOnlyChannelClusterConfigFencedFastPathAndABA(t *testing.T) {
	t.Run("FastPathNilSlotServer_FailsClosedAndStableLeaderSucceeds", func(t *testing.T) {
		servers, _ := newConfigRPCServersWithSlots(t, 2, 1)
		leaderNodeID := servers[0].cfgServer.SlotLeaderId(0)
		require.NotZero(t, leaderNodeID)
		sLeader := servers[leaderNodeID-1]

		chID := "b6_ch_0"
		validCfg := wkdb.ChannelClusterConfig{
			ChannelId:   chID,
			ChannelType: 2,
			LeaderId:    leaderNodeID,
			Term:        1,
			ConfVersion: 1,
			Replicas:    []uint64{1, 2},
		}
		require.NoError(t, sLeader.db.SaveChannelClusterConfig(validCfg))

		// Stable leader/term/epoch succeeds:
		loaded, err := sLeader.LoadOnlyChannelClusterConfig(chID, 2)
		require.NoError(t, err)
		require.Equal(t, validCfg.ChannelId, loaded.ChannelId)
		require.Equal(t, validCfg.LeaderId, loaded.LeaderId)

		// Simulate fast-path where AppliedIndex >= CommittedIndex, but local slotServer is unavailable:
		savedSlotServer := sLeader.slotServer
		sLeader.slotServer = nil
		t.Cleanup(func() { sLeader.slotServer = savedSlotServer })

		_, err = sLeader.LoadOnlyChannelClusterConfig(chID, 2)
		require.ErrorIs(t, err, ErrConversationReadRetry, "B6: LoadOnlyChannelClusterConfig must fail closed when local slotServer/slotRaft is unavailable")
	})

	t.Run("AwayAndBackABA_DuringDBRead_And_FastPathChange_Rejected", func(t *testing.T) {
		// Away-and-back ABA: node 1 is leader at Term 2 before DB read, steps down to node 2 at Term 3,
		// and returns as leader on node 1 at Term 4 (or higher slotTerm/cfgVer) before after-read check:
		for _, abaMode := range []string{"raft_term_aba", "slot_epoch_aba"} {
			t.Run(abaMode, func(t *testing.T) {
				calls := 0
				slotTerm := uint32(2)
				cfgVer := uint64(10)
				r := conversationConfigReader{
					nodeID:  1,
					maxWait: 50 * time.Millisecond,
					leader:  func() uint64 { return 1 },
					epoch:   func() (uint32, uint64) { return slotTerm, cfgVer },
					state: func(context.Context) (raftgroup.ReadState, error) {
						calls++
						st := raftgroup.ReadState{
							Exists:         true,
							Ready:          true,
							LeaderID:       1, // same nodeID 1 before and after!
							Term:           2,
							ConfigVersion:  5,
							CommittedIndex: 20,
							AppliedIndex:   20,
						}
						if calls > 1 && abaMode == "raft_term_aba" {
							// Node 1 lost leadership to node 2 (term 3) and won it back at term 4:
							st.Term = 4
						}
						return st, nil
					},
					load: func() (wkdb.ChannelClusterConfig, error) {
						if abaMode == "slot_epoch_aba" {
							slotTerm = 4
							cfgVer = 12
						}
						return wkdb.ChannelClusterConfig{ChannelId: "aba_ch", ChannelType: 2, LeaderId: 1, Term: 2}, nil
					},
				}
				cfg, err := r.read(context.Background())
				require.ErrorIs(t, err, ErrConversationReadRetry, "B6: away-and-back ABA (%s) must return ErrConversationReadRetry", abaMode)
				require.True(t, wkdb.IsEmptyChannelClusterConfig(cfg), "B6: stale config object must never be returned on ABA")
			})
		}
	})

	t.Run("BoundedDeadlineOnRepeatedChurn", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		term := uint32(1)
		r := conversationConfigReader{
			nodeID:  1,
			maxWait: 20 * time.Millisecond,
			leader:  func() uint64 { return 1 },
			state: func(context.Context) (raftgroup.ReadState, error) {
				term++
				return raftgroup.ReadState{
					Exists:         true,
					Ready:          true,
					LeaderID:       1,
					Term:           term,
					CommittedIndex: 10,
					AppliedIndex:   10,
				}, nil
			},
			load: func() (wkdb.ChannelClusterConfig, error) {
				return wkdb.ChannelClusterConfig{ChannelId: "churn", ChannelType: 2, LeaderId: 1}, nil
			},
		}
		start := time.Now()
		for attempt := 0; attempt < maxChannelConfigReadAttempts; attempt++ {
			_, err := r.read(ctx)
			require.ErrorIs(t, err, ErrConversationReadRetry)
		}
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})
}
