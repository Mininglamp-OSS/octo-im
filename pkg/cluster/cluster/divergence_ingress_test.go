package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// spoofFrames builds the frames a peer could put on the cluster port: a
// StoreResp and a TruncateResp with each divergence reason, from sender 0 and
// from real node ids, encoded exactly as the transports encode them.
func spoofEvents(to uint64) []rafttypes.Event {
	var out []rafttypes.Event
	for _, typ := range []rafttypes.EventType{rafttypes.StoreResp, rafttypes.TruncateResp} {
		for _, reason := range []rafttypes.Reason{rafttypes.ReasonDiverged, rafttypes.ReasonDivergedFailStop} {
			for _, from := range []uint64{0, 1, 2, 3} {
				out = append(out, rafttypes.Event{Type: typ, From: from, To: to, Reason: reason, Index: 1})
			}
		}
	}
	return out
}

func keyedFrame(t *testing.T, msgType uint32, key string, e rafttypes.Event) *proto.Message {
	t.Helper()
	data, err := e.Marshal()
	require.NoError(t, err)
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteString(key)
	enc.WriteBytes(data)
	return &proto.Message{MsgType: msgType, Content: enc.Bytes()}
}

func nodeFrame(t *testing.T, e rafttypes.Event) *proto.Message {
	t.Helper()
	data, err := e.Marshal()
	require.NoError(t, err)
	return &proto.Message{MsgType: MsgTypeNode, Content: data}
}

// sendFrom sends raw frames over the real cluster connection of node from.
func (c *divCluster) sendFrom(t *testing.T, from, to uint64, msgs ...*proto.Message) {
	t.Helper()
	n := c.nodes[from].s.nodeManager.node(to)
	require.NotNil(t, n)
	for _, m := range msgs {
		require.NoError(t, n.SendWithPriority(m, true))
	}
}

// B1 regression: spoofed local storage results arriving over the network at
// slot, channel and config ingress never quarantine a healthy replica, and
// the replicas keep serving afterwards.
func TestDivergenceSpoofedStorageResultsOverNetwork(t *testing.T) {
	c := newDivCluster(t)
	const S = uint32(1)
	L, F, T := c.roles(S)
	leader := c.nodes[L].s
	ch := c.key("spoof-ch", S)

	// A running channel raft for the channel ingress path.
	require.NoError(t, leader.store.AddSubscribers(ch, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "alice"}}))
	cfg, err := leader.GetOrCreateChannelClusterConfigFromSlotLeader(ch, wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	chLeader := c.nodes[cfg.LeaderId].s
	msgID := time.Now().UnixNano()
	appendMsg := func(s *Server) error {
		msgID++
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s.store.AppendMessages(ctx, ch, wkproto.ChannelTypeGroup, []wkdb.Message{{Term: 1, RecvPacket: wkproto.RecvPacket{
			MessageID: msgID, ChannelID: ch, ChannelType: wkproto.ChannelTypeGroup, FromUID: "alice",
			ClientMsgNo: fmt.Sprintf("spoof-%d", msgID), Payload: []byte("m")}}})
		return err
	}
	require.NoError(t, appendMsg(chLeader))
	chKey := wkutil.ChannelToKey(ch, wkproto.ChannelTypeGroup)
	cfgLeader := leader.cfgServer.LeaderId()
	require.NotZero(t, cfgLeader)

	// Every node attacks every other node on all three ingress paths.
	for attacker := range c.nodes {
		for victim := range c.nodes {
			if attacker == victim {
				continue
			}
			var frames []*proto.Message
			for _, e := range spoofEvents(victim) {
				frames = append(frames,
					keyedFrame(t, MsgTypeSlot, slot.SlotIdToKey(S), e),
					keyedFrame(t, MsgTypeChannel, chKey, e),
					nodeFrame(t, e))
			}
			c.sendFrom(t, attacker, victim, frames...)
		}
	}
	// Let the loops consume the frames, then check through ordinary traffic,
	// which is processed after them on the same connections.
	time.Sleep(time.Second)

	for id, n := range c.nodes {
		require.Equal(t, slot.ReplicaHealthy, n.s.slotServer.ReplicaState(S), "slot replica on node %d", id)
		r := n.s.slotServer.GetSlotRaft(S)
		require.NotNil(t, r)
		require.False(t, r.Diverged(), "slot raft on node %d", id)
		require.Empty(t, n.s.slotServer.DivergenceStats().Slots, "node %d", id)
	}
	// Slot: writes still commit and replicate to every replica.
	require.NoError(t, c.nodes[F].s.store.AddSubscribers(ch, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "bob"}}))
	for _, id := range []uint64{L, F, T} {
		c.waitReplicated(id, subscribed(ch, "bob"))
	}
	m, err := c.nodes[T].s.store.GetSubscribers(ch, wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Len(t, m, 2)
	// Channel: the running channel raft still commits.
	require.NoError(t, appendMsg(chLeader))
	// Config: the config Raft still commits a proposal from every node.
	for id, n := range c.nodes {
		addr := n.api.URL
		require.NoError(t, n.s.cfgServer.ProposeApiServerAddr(id, addr), "config proposal via node %d", id)
	}
	require.EqualValues(t, cfgLeader, leader.cfgServer.LeaderId(), "config leadership unchanged")
}
