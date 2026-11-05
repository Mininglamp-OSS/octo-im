package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func retirementElectionCases(t *testing.T, run func(*testing.T, bool, bool)) {
	for _, tc := range []struct {
		name              string
		destination, lost bool
	}{
		{"source-leader", false, false}, {"destination-leader", true, false}, {"destination-lost-task", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.destination, tc.lost) })
	}
}

func retirementFollowers(leader uint64) (source, other uint64) {
	for _, id := range []uint64{1, 2, 3} {
		if id != leader {
			if source == 0 {
				source = id
			} else {
				other = id
			}
		}
	}
	return
}

// Keep real log replication running while withholding only config-install
// acknowledgements. This holds the four-voter stage across a real election.
func capRetirementFollowers(gates map[uint64]*expansionGroupGate, version uint64) {
	for _, id := range []uint64{1, 2, 3, 4} {
		gates[id].capConfigVersion.Store(version)
	}
}
func releaseRetirementFollowers(gates map[uint64]*expansionGroupGate) {
	for _, gate := range gates {
		gate.pause.Store(false)
		gate.maxConfigVersion.Store(0)
		gate.capConfigVersion.Store(0)
	}
}

func TestChannelRetirementElectionOverTCP(t *testing.T) {
	retirementElectionCases(t, func(t *testing.T, destination, lost bool) {
		f := newExpansionFixture(t)
		f.start(4, fmt.Sprintf("1@%s", f.addresses[1]), false, false)
		f.waitStable([]uint64{1, 2, 3, 4}, 4)
		id := f.channelIDs[0]
		owner := f.servers[1].cfgServer.SlotLeaderId(f.servers[1].getSlotId(id))
		cfg, err := f.servers[owner].loadConversationConfig(f.ctx, id, 2)
		require.NoError(t, err)
		leader := cfg.LeaderId
		source, other := retirementFollowers(leader)
		winner := source
		if destination {
			winner = 4
		}
		f.channels[4].pause.Store(true)
		cfg = cfg.Clone()
		cfg.Learners = []uint64{4}
		cfg.MigrateFrom, cfg.MigrateTo = source, 4
		version, err := f.servers[owner].saveChannelConfig(f.ctx, cfg)
		require.NoError(t, err)
		f.channels[4].maxConfigVersion.Store(version)
		f.channels[4].pause.Store(false)
		var stage wkdb.ChannelClusterConfig
		require.Eventually(t, func() bool {
			stage, err = f.servers[f.live()].loadConversationConfig(f.ctx, id, 2)
			if err != nil || len(stage.Replicas) != 4 || len(stage.Learners) != 0 {
				return false
			}
			for _, peer := range []uint64{1, 2, 3, 4} {
				state, e := f.servers[peer].channelServer.ReadLeaderState(f.ctx, id, 2)
				if e != nil || !state.Exists || state.ConfigVersion < stage.ConfVersion || state.LeaderID != leader {
					return false
				}
			}
			return true
		}, 10*time.Second, 20*time.Millisecond)
		capRetirementFollowers(f.channels, version)
		f.channels[4].maxConfigVersion.Store(0)
		// Only A and the chosen future leader can receive message 5. It is
		// not committed on four voters; freshness must choose that follower.
		var mask uint64
		for _, peer := range []uint64{1, 2, 3, 4} {
			if peer != leader && peer != winner {
				mask |= 1 << peer
			}
		}
		f.channels[leader].holdLogTo.Store(mask)
		message := func(seq int) wkdb.Message {
			return wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: id, ChannelType: 2, FromUID: "expansion-test", MessageID: int64(seq), ClientMsgNo: fmt.Sprintf("%s-%d", id, seq), Payload: []byte(fmt.Sprintf("payload-%s-%d", id, seq))}}
		}
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		pending := make(chan error, 1)
		go func() {
			_, err := f.servers[leader].store.AppendMessages(ctx, id, 2, []wkdb.Message{message(5)})
			pending <- err
		}()
		require.Eventually(t, func() bool {
			for _, peer := range []uint64{1, 2, 3, 4} {
				last, _, err := f.servers[peer].db.GetChannelLastMessageSeq(id, 2)
				want := uint64(4)
				if peer == leader || peer == winner {
					want = 5
				}
				if err != nil || last != want {
					return false
				}
			}
			return true
		}, 3*time.Second, 10*time.Millisecond, "future leader must have a strictly newer persisted tail")
		cancel()
		require.Error(t, <-pending, "two of four votes cannot commit the pending message")
		f.stop(leader)
		f.waitOffline(leader)
		require.Eventually(t, func() bool {
			current, e := f.servers[f.live()].loadConversationConfig(f.ctx, id, 2)
			return e == nil && current.LeaderId == winner && len(current.Replicas) == 4
		}, 10*time.Second, 20*time.Millisecond)
		if lost {
			current, err := f.servers[f.live()].loadConversationConfig(f.ctx, id, 2)
			require.NoError(t, err)
			current = current.Clone()
			current.MigrateFrom, current.MigrateTo = 0, 0
			owner = f.servers[f.live()].cfgServer.SlotLeaderId(f.servers[f.live()].getSlotId(id))
			_, err = f.servers[owner].saveChannelConfig(f.ctx, current)
			require.NoError(t, err)
		}
		releaseRetirementFollowers(f.channels)
		var final wkdb.ChannelClusterConfig
		require.Eventually(t, func() bool {
			final, err = f.servers[f.live()].loadConversationConfig(f.ctx, id, 2)
			return err == nil && final.LeaderId == winner && len(final.Replicas) == 3 && len(final.Learners) == 0 && final.MigrateFrom == 0 && final.MigrateTo == 0 && !wkutil.ArrayContainsUint64(final.Replicas, leader)
		}, 15*time.Second, 25*time.Millisecond)
		survivors := []uint64{source, other, 4}
		f.waitStable(survivors, 3)
		appendCommitted := func(seq int) {
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			_, err := f.servers[winner].store.AppendMessages(ctx, id, 2, []wkdb.Message{message(seq)})
			require.NoError(t, err)
		}
		appendCommitted(6)
		verify := func(peers []uint64, count int) {
			for _, peer := range peers {
				require.Eventually(t, func() bool {
					rows, e := f.servers[peer].db.LoadLastMsgs(id, 2, 20)
					if e != nil || len(rows) != count {
						return false
					}
					for i, row := range rows {
						if row.MessageID != int64(i+1) || row.MessageSeq != uint32(i+1) || string(row.Payload) != fmt.Sprintf("payload-%s-%d", id, i+1) {
							return false
						}
					}
					return true
				}, 5*time.Second, 20*time.Millisecond)
			}
		}
		verify(survivors, 6)
		victim := source
		if victim == winner {
			victim = other
		}
		f.stop(victim)
		f.waitOffline(victim)
		appendCommitted(7)
		live := []uint64{winner}
		for _, peer := range survivors {
			if peer != winner && peer != victim {
				live = append(live, peer)
			}
		}
		verify(live, 7)
		t.Logf("leader %d -> %d; retired %d; then node %d crashed: seven messages committed on %v", leader, winner, leader, victim, live)
	})
}

func TestSlotRetirementElectionOverTCP(t *testing.T) {
	retirementElectionCases(t, func(t *testing.T, destination, lost bool) {
		f := newExpansionFixture(t)
		f.start(4, fmt.Sprintf("1@%s", f.addresses[1]), false, false)
		f.waitStable([]uint64{1, 2, 3, 4}, 4)
		var st *nodetypes.Slot
		for _, s := range f.servers[1].cfgServer.Slots() {
			if !wkutil.ArrayContainsUint64(s.Replicas, 4) {
				st = s.Clone()
				break
			}
		}
		require.NotNil(t, st)
		leader := st.Leader
		source, other := retirementFollowers(leader)
		winner := source
		if destination {
			winner = 4
		}
		uid := ""
		for i := 0; uid == ""; i++ {
			v := fmt.Sprintf("retirement-device-%d", i)
			if f.servers[leader].getSlotId(v) == st.Id {
				uid = v
			}
		}
		device := wkdb.Device{Id: 9001, Uid: uid, DeviceFlag: 1, Token: "before-election"}
		require.NoError(t, f.servers[leader].store.AddDevice(device))
		f.slots[4].pause.Store(true)
		next := st.Clone()
		next.Learners = []uint64{4}
		next.MigrateFrom, next.MigrateTo = source, 4
		require.NoError(t, f.servers[f.configLeader()].cfgServer.ProposeSlotsIfCurrent([]*nodetypes.Slot{st}, []*nodetypes.Slot{next}))
		version := f.servers[f.configLeader()].cfgServer.ConfigVersion()
		f.slots[4].maxConfigVersion.Store(version)
		f.slots[4].pause.Store(false)
		require.Eventually(t, func() bool {
			current := f.servers[f.live()].cfgServer.Slot(st.Id)
			if len(current.Replicas) != 4 || len(current.Learners) != 0 {
				return false
			}
			for _, peer := range []uint64{1, 2, 3, 4} {
				state, e := f.servers[peer].slotServer.ReadLeaderState(f.ctx, st.Id)
				if e != nil || !state.Exists || state.ConfigVersion <= version || state.LeaderID != leader {
					return false
				}
			}
			return true
		}, 10*time.Second, 20*time.Millisecond)
		capRetirementFollowers(f.slots, version)
		f.slots[4].maxConfigVersion.Store(0)
		var mask uint64
		for _, peer := range []uint64{1, 2, 3, 4} {
			if peer != leader && peer != winner {
				mask |= 1 << peer
			}
		}
		f.slots[leader].holdLogTo.Store(mask)
		base, err := f.servers[leader].slotServer.LastIndex(st.Id)
		require.NoError(t, err)
		encode := func(token string) []byte {
			d := device
			d.Token = token
			data, err := store.NewCMD(store.CMDUpdateDevice, store.EncodeCMDDevice(d)).Marshal()
			require.NoError(t, err)
			return data
		}
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		pending := make(chan error, 1)
		data := encode("pending-election")
		go func() {
			_, err := f.servers[leader].slotServer.ProposeUntilAppliedTimeoutForLocal(ctx, st.Id, rafttypes.ProposeReqSet{{Id: 9002, Data: data}})
			pending <- err
		}()
		require.Eventually(t, func() bool {
			for _, peer := range []uint64{1, 2, 3, 4} {
				last, e := f.servers[peer].slotServer.LastIndex(st.Id)
				want := base
				if peer == leader || peer == winner {
					want++
				}
				if e != nil || last != want {
					return false
				}
			}
			return true
		}, 3*time.Second, 10*time.Millisecond, "slot election must select the follower holding the unique durable tail")
		cancel()
		require.Error(t, <-pending)
		f.stop(leader)
		f.waitOffline(leader)
		require.Eventually(t, func() bool {
			current := f.servers[f.live()].cfgServer.Slot(st.Id)
			return current.Leader == winner && len(current.Replicas) == 4 && current.Status == nodetypes.SlotStatus_SlotStatusNormal
		}, 10*time.Second, 20*time.Millisecond)
		if lost {
			current := f.servers[f.live()].cfgServer.Slot(st.Id)
			clean := current.Clone()
			clean.MigrateFrom, clean.MigrateTo = 0, 0
			require.NoError(t, f.servers[f.configLeader()].cfgServer.ProposeSlotsIfCurrent([]*nodetypes.Slot{current}, []*nodetypes.Slot{clean}))
		}
		releaseRetirementFollowers(f.slots)
		survivors := []uint64{source, other, 4}
		f.waitStable(survivors, 3)
		final := f.servers[f.live()].cfgServer.Slot(st.Id)
		require.Equal(t, winner, final.Leader)
		require.NotContains(t, final.Replicas, leader)
		verify := func(peers []uint64, token string) {
			for _, peer := range peers {
				require.Eventually(t, func() bool {
					d, e := f.servers[peer].db.GetDevice(uid, 1)
					return e == nil && d.Token == token && d.Id == device.Id
				}, 5*time.Second, 20*time.Millisecond)
			}
		}
		// Commit a new entry in the elected term to apply the inherited tail.
		propose := func(token string) {
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			_, err := f.servers[winner].slotServer.ProposeUntilAppliedTimeoutForLocal(ctx, st.Id, rafttypes.ProposeReqSet{{Id: f.servers[winner].slotServer.GenLogId(), Data: encode(token)}})
			require.NoError(t, err)
		}
		propose("after-first-failure")
		verify(survivors, "after-first-failure")
		victim := source
		if victim == winner {
			victim = other
		}
		f.stop(victim)
		f.waitOffline(victim)
		propose("after-second-failure")
		live := []uint64{winner}
		for _, peer := range survivors {
			if peer != winner && peer != victim {
				live = append(live, peer)
			}
		}
		verify(live, "after-second-failure")
		t.Logf("slot %d: leader %d -> %d; retired %d; subsequent failure of %d still commits on %v", st.Id, leader, winner, leader, victim, live)
	})
}
