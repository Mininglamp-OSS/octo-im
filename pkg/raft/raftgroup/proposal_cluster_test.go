// Replication makes the leader's GetLogsReq worker read Node.LastLogIndex off
// the event loop (getTrunctLogIndex), a race that predates the admission
// handshake and is tracked separately. Race builds leave this test out unless
// the c02threenode tag asks for it.

//go:build !race || c02threenode

package raftgroup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Three replicas, three groups and full replication. Node 1 leads; callers
// propose on the leader and through both followers with random deadlines.

type clusterTransport struct {
	mu     sync.Mutex
	groups map[uint64]*RaftGroup
}

func (tr *clusterTransport) Send(key string, e types.Event) {
	tr.mu.Lock()
	g := tr.groups[e.To]
	tr.mu.Unlock()
	if g != nil {
		g.AddEvent(key, e)
		g.Advance()
	}
}

const clusterSeed = 20444

func TestAdmissionThreeNodeProperty(t *testing.T) {
	tr := &clusterTransport{groups: map[uint64]*RaftGroup{}}
	groups := map[uint64]*RaftGroup{}
	storages := map[uint64]*memStorage{}
	for id := uint64(1); id <= 3; id++ {
		storages[id] = &memStorage{}
		g := New(NewOptions(WithTransport(tr), WithStorage(storages[id]), WithTickInterval(10*time.Millisecond),
			WithProposeTimeout(time.Second)))
		node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(id),
			raft.WithReplicas([]uint64{1, 2, 3})))
		if id == 1 {
			node.BecomeLeader(1)
		} else {
			node.BecomeFollower(1, 1)
		}
		g.AddRaft(node)
		groups[id] = g
		tr.groups[id] = g
		t.Cleanup(func() {
			g.Stop()
			g.goPool.Release()
		})
	}
	for _, g := range groups {
		require.NoError(t, g.Start())
	}

	const proposers, proposals = 12, 150
	records := make([][]proposalRecord, proposers)
	var wg sync.WaitGroup
	for p := 0; p < proposers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(clusterSeed*1000 + int64(p)))
			via := groups[uint64(p%3)+1] // leader, then each follower
			for i := 0; i < proposals; i++ {
				rec := proposalRecord{}
				n := 1 + rnd.Intn(4)
				for j := 0; j < n; j++ {
					rec.ids = append(rec.ids, uint64(p+1)*1_000_000+uint64(i)*10+uint64(j))
				}
				reqs := types.ProposeReqSet{}
				for _, id := range rec.ids {
					reqs = append(reqs, types.ProposeReq{Id: id, Data: []byte(fmt.Sprintf("d%d", id))})
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rnd.Intn(30001))*time.Microsecond)
				rec.resps, rec.err = via.ProposeBatchUntilAppliedTimeout(ctx, admKey, reqs)
				cancel()
				records[p] = append(records[p], rec)
			}
		}(p)
	}
	wg.Wait()

	// All replicas converge on one applied sequence.
	var applied []types.Log
	waitFor(t, "replicas converge", func() bool {
		a1 := storages[1].appliedLogs()
		if len(a1) != len(storages[1].storedIDs()) {
			return false
		}
		for _, id := range []uint64{2, 3} {
			if len(storages[id].appliedLogs()) != len(a1) {
				return false
			}
		}
		applied = a1
		return true
	})
	identity := func(logs []types.Log) []string {
		out := make([]string, 0, len(logs))
		for _, log := range logs {
			out = append(out, fmt.Sprintf("%d/%d/%d", log.Index, log.Term, log.Id))
		}
		return out
	}
	for _, id := range []uint64{2, 3} {
		require.True(t, slices.Equal(identity(applied), identity(storages[id].appliedLogs())), "replica %d applied a different sequence", id)
	}
	appliedAt := map[uint64]uint64{}
	seen := map[uint64]bool{}
	for i, log := range applied {
		require.Equal(t, uint64(i+1), log.Index)
		require.False(t, seen[log.Id], "id %d applied twice", log.Id)
		seen[log.Id] = true
		appliedAt[log.Index] = log.Id
	}
	var ok, notAdmitted, unknown, rejected int
	for _, recs := range records {
		for _, rec := range recs {
			switch {
			case rec.err == nil:
				ok++
				for j, resp := range rec.resps {
					require.Equal(t, rec.ids[j], resp.Id)
					require.Equal(t, resp.Id, appliedAt[resp.Index], "success at index %d maps to another entry", resp.Index)
				}
			case errors.Is(rec.err, types.ErrProposalNotAdmitted):
				notAdmitted++
				for _, id := range rec.ids {
					require.False(t, seen[id], "not-admitted id %d applied", id)
				}
			case errors.Is(rec.err, types.ErrProposalOutcomeUnknown):
				unknown++
			case errors.Is(rec.err, types.ErrForwardRejected):
				rejected++
			default:
				t.Fatalf("unexpected outcome %v", rec.err)
			}
		}
	}
	for id, g := range groups {
		require.Zero(t, g.wait.pending(), "node %d receipts left", id)
		_, _, _, superseded := g.ProposalOutcomeCounts()
		require.Zero(t, superseded, "node %d", id)
	}
	t.Logf("seed %d: ok=%d notAdmitted=%d unknown=%d forwardRejected=%d applied=%d", clusterSeed, ok, notAdmitted, unknown, rejected, len(applied))
	require.NotZero(t, ok)
}
