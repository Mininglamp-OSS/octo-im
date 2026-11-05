package raftgroup

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestAsyncLogReadRetainsRequestTerm(t *testing.T) {
	storage := &lifecycleStorage{entered: make(chan struct{}), release: make(chan struct{})}
	g := New(NewOptions(WithStorage(storage)))
	r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
	g.AddRaft(r)
	t.Cleanup(g.Stop)
	t.Cleanup(storage.unblock)
	g.handleGetLogsReq(r, types.Event{Type: types.GetLogsReq, Term: 7, From: 2, Index: 1, StoredIndex: 1, Reason: types.ReasonOnlySync})
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("read was not submitted")
	}
	storage.unblock()
	g.workers.Wait()
	g.handleReceivedEvents()
	require.Len(t, r.events, 1)
	require.Equal(t, types.GetLogsResp, r.events[0].Type)
	require.Equal(t, uint32(7), r.events[0].Term, "the owner must be able to discard reads from an earlier term")
}
