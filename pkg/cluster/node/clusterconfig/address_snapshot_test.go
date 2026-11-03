package clusterconfig

import (
	"runtime"
	"sync/atomic"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

func addressSnapshotServer() *Server {
	return &Server{config: &Config{cfg: &pb.Config{
		Version: 7, Term: 2, SlotCount: 1, SlotReplicaCount: 1, ChannelReplicaCount: 1,
		Nodes: []*pb.Node{
			{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
			{Id: 2, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin},
		},
		Slots:    []*pb.Slot{{Id: 0, Leader: 1, Replicas: []uint64{1}, Learners: []uint64{2}}},
		Learners: []uint64{2}, MigrateFrom: 2, MigrateTo: 2,
	}, Log: wklog.NewWKLog("address-snapshot-test")}}
}

func TestAddressReadersKeepSnapshotDuringSingleBackfill(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   uint64
		read func(*Server) *pb.Node
	}{
		{"Node", 1, func(s *Server) *pb.Node { return s.Node(1) }},
		{"Nodes", 1, func(s *Server) *pb.Node { return s.Nodes()[0] }},
		{"GetClusterConfig", 1, func(s *Server) *pb.Node { return s.GetClusterConfig().Nodes[0] }},
		{"AllowVoteNodes", 1, func(s *Server) *pb.Node { return s.AllowVoteNodes()[0] }},
		{"AllowVoteAndJoinedNodes", 1, func(s *Server) *pb.Node { return s.AllowVoteAndJoinedNodes()[0] }},
		{"AllowVoteAndJoinedOnlineNodes", 1, func(s *Server) *pb.Node { return s.AllowVoteAndJoinedOnlineNodes()[0] }},
		{"OnlineNodes", 1, func(s *Server) *pb.Node { return s.config.onlineNodes()[0] }},
		{"WillJoinNodes", 2, func(s *Server) *pb.Node { return s.config.willJoinNodes()[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := addressSnapshotServer()
			require.Nil(t, s.raft, "this regression must not depend on Raft or membership promotion")
			retained := tc.read(s)
			require.Empty(t, retained.ClusterAddr)
			started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var readBytes atomic.Int64
			go func() {
				defer close(done)
				close(started)
				for {
					select {
					case <-stop:
						return
					default:
						readBytes.Add(int64(len(retained.ClusterAddr)))
						runtime.Gosched()
					}
				}
			}()
			<-started
			payload, err := EncodeClusterAddrChange(tc.id, "node.internal:11110")
			require.NoError(t, err)
			err = s.handleClusterAddrChange(NewCMD(CMDTypeConfigClusterAddrChange, payload))
			runtime.Gosched()
			close(stop)
			<-done
			require.NoError(t, err)
			require.Empty(t, retained.ClusterAddr, "a retained snapshot must not change after publication")
			require.Equal(t, "node.internal:11110", tc.read(s).ClusterAddr)
		})
	}
}

func TestConfigSnapshotsCannotMutateLiveState(t *testing.T) {
	s := addressSnapshotServer()
	node := s.Node(1)
	node.ClusterAddr = "changed.invalid:1"
	nodes := s.Nodes()
	nodes[0].ClusterAddr = "changed.invalid:2"
	nodes[0] = &pb.Node{Id: 99}
	cfg := s.GetClusterConfig()
	cfg.Version = 99
	cfg.Nodes[0].ClusterAddr = "changed.invalid:3"
	cfg.Learners[0] = 99
	cfg.Slots[0].Replicas[0] = 99
	currentNode := s.Node(1)
	require.NotNil(t, currentNode)
	require.Empty(t, currentNode.ClusterAddr)
	require.Equal(t, uint64(1), s.Nodes()[0].Id)
	current := s.GetClusterConfig()
	require.Equal(t, uint64(7), current.Version)
	require.Equal(t, []uint64{2}, current.Learners)
	require.Equal(t, []uint64{1}, current.Slots[0].Replicas)
}

func TestConcurrentAddressSnapshotsAndJoinMutation(t *testing.T) {
	s := addressSnapshotServer()
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				cfg := s.GetClusterConfig()
				for _, node := range cfg.Nodes {
					runtime.KeepAlive(node.ClusterAddr)
				}
				runtime.KeepAlive(cfg.Learners)
				runtime.Gosched()
			}
		}
	}()
	<-started
	for id := uint64(3); id < 40; id++ {
		payload, err := (&pb.Node{Id: id, ClusterAddr: "node.internal:11110", AllowVote: true}).Marshal()
		require.NoError(t, err)
		require.NoError(t, s.handleNodeJoin(NewCMD(CMDTypeNodeJoin, payload)))
	}
	close(stop)
	<-done
	require.Len(t, s.Nodes(), 39)
}
