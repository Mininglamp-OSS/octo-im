package raftgroup

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestXIN2857_YP107_RaftGroupMalformedProposeValidation(t *testing.T) {
	rg := New(NewOptions())
	emptyReqs, err := (types.ProposeReqSet{}).Marshal()
	require.NoError(t, err)
	emptyResps, err := (types.ProposeRespSet{}).Marshal()
	require.NoError(t, err)

	require.NotPanics(t, func() {
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendPropose})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: nil}}})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: []byte{0xff, 0x01}}}})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: emptyReqs}}})

		rg.TryAddEvent("k1", types.Event{From: 2, Type: types.SendPropose})
		rg.TryAddEvent("k1", types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: emptyReqs}}})

		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendProposeResp})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: nil}}})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: []byte{0xff, 0x01}}}})
		rg.AddEvent("k1", types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: emptyResps}}})
	})
}
