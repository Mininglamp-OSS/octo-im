package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Uses only APIs that exist before divergence containment.

// An overlap held entirely in memory is compared on (Term, Id, Data): a
// conflicting entry inside a fully covered range is replaced, not ignored.
func TestQueueReplacesConflictingUnstoredOverlap(t *testing.T) {
	q := newQueue("k", 0, 0)
	require.NoError(t, q.append(
		types.Log{Index: 1, Term: 1, Id: 11, Data: []byte("a")},
		types.Log{Index: 2, Term: 1, Id: 12, Data: []byte("b")},
		types.Log{Index: 3, Term: 1, Id: 13, Data: []byte("c")},
	))
	require.NoError(t, q.append(
		types.Log{Index: 2, Term: 2, Id: 22, Data: []byte("B")},
	))
	require.EqualValues(t, 2, q.lastLogIndex)
	require.Len(t, q.logs, 2)
	require.EqualValues(t, 22, q.logs[1].Id)
	require.EqualValues(t, 2, q.logs[1].Term)
}

// An identical overlap only appends the new tail and keeps held entries.
func TestQueueIdenticalOverlapAppendsTailOnly(t *testing.T) {
	q := newQueue("k", 0, 0)
	a := types.Log{Index: 1, Term: 1, Id: 11, Data: []byte("a")}
	b := types.Log{Index: 2, Term: 1, Id: 12, Data: []byte("b")}
	c := types.Log{Index: 3, Term: 1, Id: 13, Data: []byte("c")}
	require.NoError(t, q.append(a, b))
	require.NoError(t, q.append(b, c))
	require.EqualValues(t, 3, q.lastLogIndex)
	require.Len(t, q.logs, 3)
	require.EqualValues(t, 12, q.logs[1].Id)
	require.EqualValues(t, 13, q.logs[2].Id)
	// A fully covered identical overlap changes nothing.
	rev := q.revision
	require.NoError(t, q.append(b))
	require.EqualValues(t, 3, q.lastLogIndex)
	require.Greater(t, q.revision, rev-1)
}
