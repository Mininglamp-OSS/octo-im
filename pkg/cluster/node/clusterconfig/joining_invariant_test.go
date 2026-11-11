package clusterconfig

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestJoiningLogApplicationPublishesCleanMembershipSnapshots(t *testing.T) {
	for _, version := range []uint16{1, 2} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			s := generationTestServer(t, path)
			index := uint64(0)
			apply := func(kind CMDType, data []byte, wireVersion uint16) {
				t.Helper()
				cmd := NewCMD(kind, data)
				cmd.version = wireVersion
				encoded, err := cmd.Marshal()
				require.NoError(t, err)
				index++
				require.NoError(t, s.applyLog(rafttypes.Log{Index: index, Term: 1, Data: encoded}))
			}
			initial := &pb.Config{SlotCount: 64, SlotReplicaCount: 3, ChannelReplicaCount: 3}
			for _, id := range []uint64{1, 2, 3} {
				initial.Nodes = append(initial.Nodes, &pb.Node{Id: id, Online: id != 2, AllowVote: true,
					OfflineCount: 1, Status: pb.NodeStatus_NodeStatusJoined})
			}
			data, err := initial.Marshal()
			require.NoError(t, err)
			apply(CMDTypeConfigChange, data, 3)

			done, stopped, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
			invalid := make(chan error, 1)
			var observations atomic.Uint64
			stop := sync.OnceFunc(func() { close(done); <-stopped })
			t.Cleanup(stop)
			go func() {
				defer close(stopped)
				for {
					select {
					case <-done:
						return
					default:
					}
					cfg := s.Snapshot()
					if observations.Add(1) == 1 {
						close(started)
					}
					for _, node := range cfg.Nodes {
						if node.Status == pb.NodeStatus_NodeStatusJoining &&
							(slices.Contains(cfg.Learners, node.Id) || cfg.MigrateTo == node.Id) {
							invalid <- fmt.Errorf("Joining node %d still owns configuration learner/migration state at version %d", node.Id, cfg.Version)
							return
						}
					}
				}
			}()
			<-started

			data, err = (&pb.Node{Id: 4, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
			require.NoError(t, err)
			apply(CMDTypeNodeJoin, data, version)
			require.Equal(t, pb.NodeStatus_NodeStatusWillJoin, s.Node(4).Status)
			require.Equal(t, []uint64{4}, s.Snapshot().Learners)
			if version == 1 {
				apply(CMDTypeNodeRemove, compatibilityRemoval().Data, 1)
			}
			apply(CMDTypeNodeJoining, compatibilityPromotion(4).Data, version)
			require.Equal(t, pb.NodeStatus_NodeStatusJoining, s.Node(4).Status)
			require.Empty(t, s.Snapshot().Learners)
			require.Zero(t, s.Snapshot().MigrateFrom)
			require.Zero(t, s.Snapshot().MigrateTo)
			data, err = EncodeNodeOnlineStatusChange(4, false)
			require.NoError(t, err)
			apply(CMDTypeNodeOnlineStatusChange, data, 1)
			before := s.Snapshot()
			reloaded := generationTestServer(t, path)
			require.True(t, proto.Equal(before, reloaded.Snapshot()), "persisted offline Joining state must retain completed promotion cleanup")
			joinedData, err := EncodeNodeJoined(4, nil)
			require.NoError(t, err)
			apply(CMDTypeNodeJoined, joinedData, 3)

			// An existing voter can re-enter Joining while another node's
			// admission remains active. Completing the offline voter must leave
			// that other learner and migration intact.
			data, err = (&pb.Node{Id: 5, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
			require.NoError(t, err)
			apply(CMDTypeNodeJoin, data, 1)
			apply(CMDTypeNodeJoining, compatibilityPromotion(4).Data, 1)
			pending := s.Snapshot()
			require.Equal(t, []uint64{5}, pending.Learners)
			require.Equal(t, uint64(5), pending.MigrateTo)
			apply(CMDTypeNodeJoined, joinedData, 3)
			require.Equal(t, pending.Learners, s.Snapshot().Learners)
			require.Equal(t, pending.MigrateFrom, s.Snapshot().MigrateFrom)
			require.Equal(t, pending.MigrateTo, s.Snapshot().MigrateTo)
			stop()
			require.Greater(t, observations.Load(), uint64(0))
			select {
			case err := <-invalid:
				t.Fatal(err)
			default:
			}
		})
	}
}
