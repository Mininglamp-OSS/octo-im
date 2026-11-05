package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestSingleReplicaExpansionPreservesDataOverTCP(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 1, 4)
	initial := servers[0]
	devices := make(map[uint32]wkdb.Device)
	for slot := uint32(0); slot < 4; slot++ {
		uid := fmt.Sprintf("single-replica-device-%d", slot)
		for initial.getSlotId(uid) != slot {
			uid += "x"
		}
		device := wkdb.Device{Id: uint64(9201 + slot), Uid: uid, DeviceFlag: 1, Token: "before-expansion"}
		require.NoError(t, initial.store.AddDevice(device))
		devices[slot] = device
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	seed := "1@" + initial.cfgServer.Node(1).ClusterAddr
	cfg := newTestOptions(t, 2, nil, clusterconfig.WithSeed(seed), clusterconfig.WithServerAddr(address),
		clusterconfig.WithSlotCount(4), clusterconfig.WithSlotMaxReplicaCount(1))
	joining := New(NewOptions(WithAddr("tcp://"+address), WithServerAddr(address), WithSeed(seed),
		WithConfigOptions(cfg), WithDataDir(t.TempDir()), WithDBWKDbShardNum(1), WithDBSlotShardNum(1),
		WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	require.NoError(t, joining.Start())
	t.Cleanup(joining.Stop)
	joining.configReconciler.stop()
	servers = append(servers, joining)
	require.Eventually(t, func() bool {
		for _, server := range servers {
			node := server.cfgServer.Node(2)
			if node == nil || node.Status != nodetypes.NodeStatus_NodeStatusJoined {
				return false
			}
			counts := map[uint64]int{}
			for _, slot := range server.cfgServer.Snapshot().Slots {
				if len(slot.Replicas) != 1 || len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 || slot.Leader != slot.Replicas[0] {
					return false
				}
				counts[slot.Leader]++
			}
			if counts[1] != 2 || counts[2] != 2 {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond, "a healthy single-replica expansion must migrate half the slots")
	for _, slot := range initial.cfgServer.Snapshot().Slots {
		owner := servers[slot.Leader-1]
		require.Eventually(t, func() bool {
			state, err := owner.slotServer.ReadLeaderState(ctx, slot.Id)
			return err == nil && state.Ready && state.LeaderID == slot.Leader
		}, time.Second, 10*time.Millisecond)
		device := devices[slot.Id]
		got, err := owner.db.GetDevice(device.Uid, 1)
		require.NoError(t, err)
		require.Equal(t, device.Token, got.Token, "history must be applied before the new sole replica serves")
		device.Token = "after-expansion"
		require.NoError(t, owner.store.UpdateDevice(device))
		got, err = owner.db.GetDevice(device.Uid, 1)
		require.NoError(t, err)
		require.Equal(t, device.Token, got.Token)
	}
}
