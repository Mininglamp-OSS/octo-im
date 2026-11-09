package cluster

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/stretchr/testify/require"
)

func TestMembershipRejoinRetriesAfterBusyAndLeaderChangeOverTCP(t *testing.T) {
	f := newExpansionFixture(t)
	f.stop(3)
	f.waitOffline(3)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	f.waitStable([]uint64{1, 2, 4}, 3)
	// C's original disk has only the earlier history. Its return must recover
	// these additional messages as well as survive the control-plane retry.
	f.send(4, 2)
	f.checkMessages([]uint64{1, 2, 4})
	for _, id := range []uint64{1, 2, 4} {
		require.Nil(t, f.servers[id].cfgServer.Node(3))
	}

	type busyObservation struct {
		peerID uint64
		probe  bool
		uid    string
	}
	busy := make(chan busyObservation, 1)
	var allowJoin atomic.Bool
	var busyCount atomic.Uint64
	var acceptedBy atomic.Uint64
	for _, id := range []uint64{1, 2, 4} {
		server := f.servers[id]
		server.Route("/rpc/cluster/join", func(c *wkserver.Context) {
			var req ClusterJoinReq
			if err := req.Unmarshal(c.Body()); err != nil || req.NodeId != 3 {
				server.rpcServer.handleClusterJoin(c)
				return
			}
			// Reject promptly while the test changes leaders. No RPC callback
			// remains gated across Stop and later accesses the retired server.
			if !allowJoin.Load() {
				c.WriteErr(clusterconfig.ErrMembershipBusy)
				if busyCount.Add(1) == 1 {
					busy <- busyObservation{peerID: server.opts.ConfigOptions.NodeId, probe: req.Probe, uid: c.PeerUID()}
				}
				return
			}
			server.rpcServer.handleClusterJoin(c)
			if server.cfgServer.IsLeader() && server.cfgServer.Node(3) != nil {
				acceptedBy.Store(server.opts.ConfigOptions.NodeId)
			}
		})
	}

	// The original process configuration has no new seed. Its real periodic
	// probe must discover absence and automatically send a separate join.
	f.start(3, "", false, false)
	select {
	case observed := <-busy:
		require.False(t, observed.probe, "discovery must turn into an explicit ordinary join")
		require.Equal(t, "3", observed.uid, "the first join must originate from returning C")
		t.Logf("returning C's explicit join received Busy from peer %d", observed.peerID)
	case <-f.ctx.Done():
		t.Fatal("returning C did not attempt an explicit join after discovering absence")
	}
	for _, id := range []uint64{1, 2, 4} {
		require.Nil(t, f.servers[id].cfgServer.Node(3), "Busy must leave C unadmitted")
	}
	// Choose from the current ABD voters, ignoring C's stale local membership.
	var retiredLeader uint64
	for _, id := range []uint64{1, 2, 4} {
		if f.servers[id].cfgServer.IsLeader() {
			retiredLeader = id
		}
	}
	require.NotZero(t, retiredLeader)
	f.stop(retiredLeader)
	var survivingOld []uint64
	for _, id := range []uint64{1, 2, 4} {
		if id != retiredLeader {
			survivingOld = append(survivingOld, id)
		}
	}
	// Two of ABD's three voters remain, so election and C's admission retain
	// the original quorum rather than depending on C's obsolete voting state.
	var elected uint64
	require.Eventually(t, func() bool {
		elected = 0
		for _, id := range survivingOld {
			if f.servers[id].cfgServer.IsLeader() {
				elected = id
			}
		}
		if elected == 0 {
			return false
		}
		for _, id := range survivingOld {
			if f.servers[id].cfgServer.LeaderId() != elected {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond, "surviving old voters must elect a new leader before accepting C")
	for _, id := range survivingOld {
		require.Nil(t, f.servers[id].cfgServer.Node(3))
	}
	allowJoin.Store(true)
	remaining := append(slices.Clone(survivingOld), uint64(3))
	slices.Sort(remaining)
	f.waitStable(remaining, 3)
	require.Positive(t, busyCount.Load())
	require.NotZero(t, acceptedBy.Load(), "the new leader must process the retried ordinary join")
	require.NotEqual(t, retiredLeader, acceptedBy.Load())
	for _, id := range remaining {
		require.NotNil(t, f.servers[id].cfgServer.Node(3))
		require.Nil(t, f.servers[id].cfgServer.Node(retiredLeader))
	}
	t.Logf("Busy retries=%d retired leader=%d elected successor=%d join accepted by=%d remaining=%v",
		busyCount.Load(), retiredLeader, elected, acceptedBy.Load(), remaining)
	f.checkMessages(remaining)
	f.send(3, 2)
	f.checkMessages(remaining)
}
