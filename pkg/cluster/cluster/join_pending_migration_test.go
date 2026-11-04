package cluster

import (
	"fmt"
	"slices"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestSeededEmptyLeaderRejoinDuringSlotMigrationOverTCP(t *testing.T) {
	testSeededEmptyLeaderRejoinDuringSlotMigration(t, false)
}

func TestSeededEmptyLeaderRejoinWhileNextNodeJoiningOverTCP(t *testing.T) {
	testSeededEmptyLeaderRejoinDuringSlotMigration(t, true)
}

func testSeededEmptyLeaderRejoinDuringSlotMigration(t *testing.T, nextJoining bool) {
	// Keep the heartbeat grace period longer than the entire recovery so an
	// offline transition cannot accidentally break the admission deadlock.
	f := newExpansionFixtureWithPong(t, 1000)
	seed := f.configLeader()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, true)
	var migrating *nodetypes.Slot
	require.Eventually(t, func() bool {
		n := f.servers[seed].cfgServer.Node(4)
		if n == nil || n.Status != nodetypes.NodeStatus_NodeStatusJoined {
			return false
		}
		for _, st := range f.servers[seed].cfgServer.Snapshot().Slots {
			if st.Leader != seed && st.MigrateTo == 4 && len(st.Learners) == 1 {
				migrating = st
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	victim := migrating.Leader
	uid := "rejoin-migrating-slot"
	for f.servers[seed].getSlotId(uid) != migrating.Id {
		uid += "x"
	}
	device := wkdb.Device{Id: 9101, Uid: uid, DeviceFlag: 1, Token: "before-empty-rejoin"}
	require.NoError(t, f.servers[victim].store.AddDevice(device))
	for _, id := range migrating.Replicas {
		require.Eventually(t, func() bool {
			got, err := f.servers[id].db.GetDevice(uid, 1)
			return err == nil && got.Token == device.Token
		}, 3*time.Second, 10*time.Millisecond)
	}
	if nextJoining {
		f.start(5, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
		require.Eventually(t, func() bool {
			n := f.servers[seed].cfgServer.Node(5)
			return n != nil && n.Status == nodetypes.NodeStatus_NodeStatusJoining && len(f.servers[seed].cfgServer.Snapshot().Learners) == 0
		}, 10*time.Second, 20*time.Millisecond)
	}
	f.stop(victim)
	f.dirs[victim] = t.TempDir()
	f.start(victim, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	defer func() {
		if t.Failed() {
			state, err := f.servers[victim].slotServer.ReadLeaderState(f.ctx, migrating.Id)
			t.Logf("returning leader runtime: %+v, err=%v", state, err)
			f.dump()
		}
	}()
	require.Eventually(t, func() bool {
		s := f.servers[victim]
		n := s.cfgServer.Node(victim)
		st := s.cfgServer.Slot(migrating.Id)
		if n == nil || !n.Online || n.Status != nodetypes.NodeStatus_NodeStatusJoined || st == nil || st.Status != nodetypes.SlotStatus_SlotStatusNormal {
			return false
		}
		owner := f.servers[st.Leader]
		state, err := owner.slotServer.ReadLeaderState(f.ctx, st.Id)
		return err == nil && state.Ready && state.LeaderID == st.Leader
	}, 15*time.Second, 20*time.Millisecond, "joining and recovery elections must finish without waiting for learner catch-up")
	st := f.servers[seed].cfgServer.Slot(migrating.Id)
	require.Equal(t, migrating.Learners, st.Learners)
	require.Equal(t, migrating.MigrateFrom, st.MigrateFrom)
	require.Equal(t, migrating.MigrateTo, st.MigrateTo)
	require.Eventually(t, func() bool {
		got, err := f.servers[victim].db.GetDevice(uid, 1)
		return err == nil && got.Id == device.Id && got.Token == device.Token
	}, 5*time.Second, 10*time.Millisecond, "the empty returning node must replay the committed history")
	device.Token = "after-empty-rejoin"
	require.NoError(t, f.servers[st.Leader].store.UpdateDevice(device), "the recovered slot must commit while the existing learner is still paused")
	f.slots[4].pause.Store(false)
	f.channels[4].pause.Store(false)
	if nextJoining {
		f.waitStable([]uint64{1, 2, 3, 4, 5}, 5)
	} else {
		f.waitStable([]uint64{1, 2, 3, 4}, 4)
	}
	for _, id := range f.servers[seed].cfgServer.Slot(migrating.Id).Replicas {
		require.Eventually(t, func() bool {
			got, err := f.servers[id].db.GetDevice(uid, 1)
			return err == nil && got.Token == device.Token
		}, 5*time.Second, 10*time.Millisecond)
	}
	// A later externally added node must not inherit ErrMembershipBusy from
	// the recovered node's unfinished admission.
	if !nextJoining {
		f.start(5, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
		f.waitStable([]uint64{1, 2, 3, 4, 5}, 5)
	}
	assigned := 0
	for _, slot := range f.servers[seed].cfgServer.Snapshot().Slots {
		for _, id := range slot.Replicas {
			if id == 5 {
				assigned++
			}
		}
	}
	require.Positive(t, assigned, "the waiting new node must receive its ordinary slot allocation")
}

func TestJoiningNodeReceivesSlotsAfterDeadLearnerCleanupOverTCP(t *testing.T) {
	f := newExpansionFixture(t)
	seed := f.configLeader()
	devices := map[uint32]wkdb.Device{}
	for _, slot := range f.servers[seed].cfgServer.Snapshot().Slots {
		uid := fmt.Sprintf("cleanup-slot-%d", slot.Id)
		for f.servers[seed].getSlotId(uid) != slot.Id {
			uid += "x"
		}
		device := wkdb.Device{Id: uint64(9200 + slot.Id), Uid: uid, DeviceFlag: 1, Token: "before-cleanup"}
		require.NoError(t, f.servers[slot.Leader].store.AddDevice(device))
		devices[slot.Id] = device
	}
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, true)
	require.Eventually(t, func() bool {
		n := f.servers[seed].cfgServer.Node(4)
		if n == nil || n.Status != nodetypes.NodeStatus_NodeStatusJoined {
			return false
		}
		for _, slot := range f.servers[seed].cfgServer.Snapshot().Slots {
			if slot.MigrateTo == 4 && slices.Contains(slot.Learners, uint64(4)) {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	f.start(5, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	require.Eventually(t, func() bool {
		n := f.servers[seed].cfgServer.Node(5)
		return n != nil && n.Status == nodetypes.NodeStatus_NodeStatusJoining
	}, 10*time.Second, 20*time.Millisecond)
	f.stop(4)
	f.waitOffline(4)
	f.waitStable([]uint64{1, 2, 3, 5}, 4)
	assigned := 0
	for _, slot := range f.servers[seed].cfgServer.Snapshot().Slots {
		if !slices.Contains(slot.Replicas, uint64(5)) {
			continue
		}
		assigned++
		device := devices[slot.Id]
		require.Eventually(t, func() bool {
			got, err := f.servers[5].db.GetDevice(device.Uid, 1)
			return err == nil && got.Token == device.Token
		}, 5*time.Second, 10*time.Millisecond, "new slot replicas must contain the existing data")
		device.Token = "after-cleanup"
		require.NoError(t, f.servers[5].store.UpdateDevice(device))
		for _, id := range slot.Replicas {
			require.Eventually(t, func() bool {
				got, err := f.servers[id].db.GetDevice(device.Uid, 1)
				return err == nil && got.Token == device.Token
			}, 5*time.Second, 10*time.Millisecond)
		}
	}
	require.Positive(t, assigned, "cleanup must not finish admission before allocating E any slots")
}
