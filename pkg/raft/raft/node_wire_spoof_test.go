package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// B1: the divergence reasons are peer-controlled once on the wire. A
// StoreResp or TruncateResp decoded from network bytes, from sender 0 or any
// other sender, must not quarantine a healthy replica.
func TestWireDecodedDivergeReasonDoesNotQuarantine(t *testing.T) {
	for _, typ := range []types.EventType{types.StoreResp, types.TruncateResp} {
		for _, reason := range []types.Reason{types.ReasonDiverged, types.ReasonDivergedFailStop} {
			for _, from := range []uint64{0, 2} {
				n := newTestNode(1, []uint64{1, 2, 3})
				makeFollower(n, 2, 2)
				data, err := types.Event{Type: typ, From: from, To: 1, Reason: reason, Index: 1}.Marshal()
				require.NoError(t, err)
				var got types.Event
				require.NoError(t, got.Unmarshal(data))
				n.Step(got)
				require.False(t, n.Diverged(), "%s %s from=%d", typ, reason, from)
			}
		}
	}
}
