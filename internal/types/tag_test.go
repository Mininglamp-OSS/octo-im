package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The /tag API encodes Tag directly; its fields must keep their names and
// last_get_time must report the latest access.
func TestTagJSONKeepsShapeWithLiveAccessTime(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tag := &Tag{Key: "k", ChannelId: "g1", ChannelType: 2, CreatedAt: created, LastGetTime: created,
		Nodes: []*Node{{LeaderId: 1, Uids: []string{"a"}, SlotIds: []uint32{3}}}}
	tag.accessState().getCount.Store(7)
	tag.Touch()

	data, err := json.Marshal(tag)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	for _, k := range []string{"key", "channel_id", "channel_type", "nodes", "created_at", "last_get_time", "node_version", "get_count"} {
		require.Contains(t, got, k)
	}
	require.Len(t, got, 8)
	require.EqualValues(t, 7, got["get_count"])
	last, err := time.Parse(time.RFC3339Nano, got["last_get_time"].(string))
	require.NoError(t, err)
	require.True(t, last.After(created))
}

// Callers of GetNodeUsers keep the result; it must not alias the published
// tag that fan-out ranges over.
func TestGetNodeUsersReturnsCopy(t *testing.T) {
	tag := &Tag{Nodes: []*Node{{LeaderId: 1, Uids: []string{"a", "b"}}}}

	got := tag.GetNodeUsers(1)
	got[0] = "x"
	_ = append(got[:1], "y")

	require.Equal(t, []string{"a", "b"}, tag.Nodes[0].Uids)
	require.Nil(t, tag.GetNodeUsers(2))
}

func TestShareAccessSharesTimeAndCount(t *testing.T) {
	src := &Tag{}
	src.Touch()
	src.IncGetCount()

	c := &Tag{}
	c.ShareAccess(src)
	require.Equal(t, src.LastGet(), c.LastGet())
	require.EqualValues(t, 1, c.GetCount())

	src.IncGetCount()
	c.IncGetCount()
	require.EqualValues(t, 3, src.GetCount())
	require.EqualValues(t, 3, c.GetCount())
}

// Expiry uses time.Since(LastGet()), so LastGet must carry a monotonic clock
// reading; otherwise a wall-clock step expires tags early or never.
func TestLastGetUsesMonotonicClock(t *testing.T) {
	tag := &Tag{}
	tag.Touch()
	require.Contains(t, tag.LastGet().String(), "m=")
	require.Less(t, time.Since(tag.LastGet()), time.Second)
}
