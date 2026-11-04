package clusterconfig

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type rejoinVoteTransport struct{ votes chan types.Event }

func (tr *rejoinVoteTransport) Send(e types.Event) {
	if e.Type == types.VoteResp {
		tr.votes <- e
	}
}

func TestRejoinedVoterRestoresVoteAtRecoveryBoundary(t *testing.T) {
	tr := &rejoinVoteTransport{votes: make(chan types.Event, 4)}
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json"), WithTransport(tr)))
	s.config.cfg = &pb.Config{Version: 9, Nodes: []*pb.Node{
		{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 2, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 3, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
	}}
	s.config.inited = true
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	remote := s.Snapshot()
	remote.Version = 10
	require.NoError(t, s.ResumeJoin(remote, 2, 10))
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, 1)
	data, err := NewCMD(CMDTypeNodeJoining, payload).Marshal()
	require.NoError(t, err)
	require.NoError(t, s.applyLog(types.Log{Index: 10, Term: 1, Data: data}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.raft.StepWait(ctx, types.Event{Type: types.VoteReq, From: 2, To: 1, Term: 100,
		ConfigVersion: 10, Logs: []types.Log{{Index: 10, Term: 1}}}))
	select {
	case vote := <-tr.votes:
		require.Equal(t, types.ReasonOk, vote.Reason)
	case <-ctx.Done():
		t.Fatal("replaying the committed recovery boundary must restore the existing voter's local role")
	}
}
