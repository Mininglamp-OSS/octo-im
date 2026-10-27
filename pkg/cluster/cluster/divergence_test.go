package cluster

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// legacyUnmarshalSlotLogInfo is the decoder of releases without the
// divergence extension, kept verbatim to check the new encoding against it.
func legacyUnmarshalSlotLogInfo(data []byte) (*SlotLogInfoResp, error) {
	s := &SlotLogInfoResp{}
	dec := wkproto.NewDecoder(data)
	var err error
	if s.NodeId, err = dec.Uint64(); err != nil {
		return nil, err
	}
	var slotsLen uint16
	if slotsLen, err = dec.Uint16(); err != nil {
		return nil, err
	}
	s.Slots = make([]SlotInfo, slotsLen)
	for i := uint16(0); i < slotsLen; i++ {
		if s.Slots[i].SlotId, err = dec.Uint32(); err != nil {
			return nil, err
		}
		if s.Slots[i].LogIndex, err = dec.Uint64(); err != nil {
			return nil, err
		}
		if s.Slots[i].LogTerm, err = dec.Uint32(); err != nil {
			return nil, err
		}
		if s.Slots[i].Term, err = dec.Uint32(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// legacyMarshalSlotLogInfo is the encoder of releases without the extension.
func legacyMarshalSlotLogInfo(s *SlotLogInfoResp) []byte {
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteUint64(s.NodeId)
	enc.WriteUint16(uint16(len(s.Slots)))
	for _, slot := range s.Slots {
		enc.WriteUint32(slot.SlotId)
		enc.WriteUint64(slot.LogIndex)
		enc.WriteUint32(slot.LogTerm)
		enc.WriteUint32(slot.Term)
	}
	return enc.Bytes()
}

// T11/T11b: codec in both directions.
func TestSlotLogInfoExtensionCodec(t *testing.T) {
	resp := &SlotLogInfoResp{NodeId: 7, Slots: []SlotInfo{
		{SlotId: 1, LogIndex: 10, LogTerm: 2, Term: 3, Diverged: true, Applied: 9},
		{SlotId: 2, LogIndex: 20, LogTerm: 4, Term: 4, Applied: 20},
	}}
	data, err := resp.Marshal()
	require.NoError(t, err)

	got := &SlotLogInfoResp{}
	require.NoError(t, got.Unmarshal(data))
	require.Equal(t, resp.Slots, got.Slots)
	for _, s := range got.Slots {
		require.Equal(t, SlotInfoExtValid, s.ExtState)
	}

	// An old node decodes the new encoding unchanged (new -> old).
	old, err := legacyUnmarshalSlotLogInfo(data)
	require.NoError(t, err)
	require.Len(t, old.Slots, 2)
	require.EqualValues(t, 10, old.Slots[0].LogIndex)
	require.EqualValues(t, 4, old.Slots[1].Term)

	// A new node decoding an old response marks the extension missing.
	got = &SlotLogInfoResp{}
	require.NoError(t, got.Unmarshal(legacyMarshalSlotLogInfo(resp)))
	for _, s := range got.Slots {
		require.Equal(t, SlotInfoExtMissing, s.ExtState)
		require.False(t, s.Diverged)
	}
}

func TestSlotLogInfoExtensionMalformed(t *testing.T) {
	resp := &SlotLogInfoResp{NodeId: 7, Slots: []SlotInfo{{SlotId: 1, LogIndex: 10, LogTerm: 2, Term: 3, Applied: 9}}}
	base := legacyMarshalSlotLogInfo(resp)
	good, err := resp.Marshal()
	require.NoError(t, err)
	cases := map[string][]byte{
		"wrong version":  append(append([]byte(nil), base...), 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9),
		"count mismatch": append(append([]byte(nil), base...), 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 9),
		"truncated":      good[:len(good)-3],
		"bad flag":       append(append([]byte(nil), base...), 1, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 9),
		"trailing bytes": append(append([]byte(nil), good...), 0xff),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			got := &SlotLogInfoResp{}
			require.NoError(t, got.Unmarshal(data), "a malformed extension never fails the base decode")
			require.Len(t, got.Slots, 1)
			require.Equal(t, SlotInfoExtInvalid, got.Slots[0].ExtState)
			require.False(t, got.Slots[0].Diverged)
		})
	}
}

func testElectionServer(strict bool) *Server {
	return &Server{
		opts: &Options{
			ConfigOptions:              clusterconfig.NewOptions(clusterconfig.WithSlotMaxReplicaCount(3)),
			SlotDivergeStrictExtension: strict,
		},
		Log: wklog.NewWKLog("electionTest"),
	}
}

func info(slotId uint32, logIndex uint64, logTerm, term uint32) SlotInfo {
	return SlotInfo{SlotId: slotId, LogIndex: logIndex, LogTerm: logTerm, Term: term, Applied: logIndex}
}

func elect(t *testing.T, s *Server, slot *types.Slot, resps ...*SlotLogInfoResp) uint64 {
	t.Helper()
	infos, err := s.collectSlotLogInfo(resps)
	require.NoError(t, err)
	return electSlotLeader(infos, slot.Id, slot)
}

// T10: a diverged responder neither counts towards the quorum nor can win,
// even with the newest log.
func TestSlotElectionExcludesDivergedResponder(t *testing.T) {
	s := testElectionServer(true)
	slot := &types.Slot{Id: 3, Leader: 1, Replicas: []uint64{1, 2, 3}}
	diverged := info(3, 100, 9, 9)
	diverged.Diverged = true
	leader := elect(t, s, slot,
		&SlotLogInfoResp{NodeId: 1, Slots: []SlotInfo{diverged}},
		&SlotLogInfoResp{NodeId: 2, Slots: []SlotInfo{info(3, 50, 5, 5)}},
		&SlotLogInfoResp{NodeId: 3, Slots: []SlotInfo{info(3, 49, 5, 5)}},
	)
	require.EqualValues(t, 2, leader)

	// One diverged, one offline: no healthy quorum, no leader (T7).
	leader = elect(t, s, slot,
		&SlotLogInfoResp{NodeId: 1, Slots: []SlotInfo{diverged}},
		&SlotLogInfoResp{NodeId: 2, Slots: []SlotInfo{info(3, 50, 5, 5)}},
	)
	require.Zero(t, leader)
}

func TestSlotElectionExcludesAppliedBeyondLog(t *testing.T) {
	s := testElectionServer(true)
	slot := &types.Slot{Id: 3, Replicas: []uint64{1, 2, 3}}
	beyond := info(3, 100, 9, 9)
	beyond.Applied = 101
	leader := elect(t, s, slot,
		&SlotLogInfoResp{NodeId: 1, Slots: []SlotInfo{beyond}},
		&SlotLogInfoResp{NodeId: 2, Slots: []SlotInfo{info(3, 50, 5, 5)}},
	)
	require.Zero(t, leader, "a single healthy responder is not a quorum")
}

// A diverged ExpectLeader is skipped; the healthy responders elect.
func TestSlotElectionSkipsDivergedExpectLeader(t *testing.T) {
	s := testElectionServer(true)
	slot := &types.Slot{Id: 3, Leader: 2, ExpectLeader: 1, Replicas: []uint64{1, 2, 3}}
	expect := info(3, 10, 2, 2)
	expect.Diverged = true
	leader := elect(t, s, slot,
		&SlotLogInfoResp{NodeId: 1, Slots: []SlotInfo{expect}},
		&SlotLogInfoResp{NodeId: 2, Slots: []SlotInfo{info(3, 10, 2, 2)}},
		&SlotLogInfoResp{NodeId: 3, Slots: []SlotInfo{info(3, 9, 2, 2)}},
	)
	require.EqualValues(t, 2, leader)

	expect.Diverged = false
	leader = elect(t, s, slot,
		&SlotLogInfoResp{NodeId: 1, Slots: []SlotInfo{expect}},
		&SlotLogInfoResp{NodeId: 2, Slots: []SlotInfo{info(3, 10, 2, 2)}},
		&SlotLogInfoResp{NodeId: 3, Slots: []SlotInfo{info(3, 9, 2, 2)}},
	)
	require.EqualValues(t, 1, leader, "control: a healthy ExpectLeader is preferred")
}

// T11b: strict mode (default) treats a missing or malformed extension as
// unknown; lenient mode uses defaults.
func TestSlotElectionStrictExtension(t *testing.T) {
	slot := &types.Slot{Id: 3, Replicas: []uint64{1, 2, 3}}
	missing := info(3, 100, 9, 9)
	missing.ExtState = SlotInfoExtMissing
	invalid := info(3, 99, 9, 9)
	invalid.ExtState = SlotInfoExtInvalid
	resps := func() []*SlotLogInfoResp {
		return []*SlotLogInfoResp{
			{NodeId: 1, Slots: []SlotInfo{missing}},
			{NodeId: 2, Slots: []SlotInfo{invalid}},
			{NodeId: 3, Slots: []SlotInfo{info(3, 50, 5, 5)}},
		}
	}
	before := slotLogInfoExtInvalid.Load()
	require.Zero(t, elect(t, testElectionServer(true), slot, resps()...), "strict: only one known responder")
	require.EqualValues(t, before+1, slotLogInfoExtInvalid.Load())
	require.EqualValues(t, 1, elect(t, testElectionServer(false), slot, resps()...), "lenient: defaults")
	require.True(t, NewOptions().SlotDivergeStrictExtension, "strict is the default")
}

// T25e-3: a node excluded from channel election info leaves the election to
// the other replicas: it succeeds with their quorum and fails closed without.
func TestChannelElectionWithoutExcludedReplica(t *testing.T) {
	cfg := &wkdb.ChannelClusterConfig{ChannelId: "g", ChannelType: 2, Term: 3, Replicas: []uint64{1, 2, 3}}
	quorum := map[uint64]*ChannelLastLogInfoResponse{
		2: {LogTerm: 3, LogIndex: 10, Term: 3},
		3: {LogTerm: 3, LogIndex: 9, Term: 3},
	}
	ok, err := electChannelLeader(cfg, quorum)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 2, cfg.LeaderId)
	require.EqualValues(t, 4, cfg.Term)

	cfg = &wkdb.ChannelClusterConfig{ChannelId: "g", ChannelType: 2, Term: 3, Replicas: []uint64{1, 2, 3}}
	_, err = electChannelLeader(cfg, map[uint64]*ChannelLastLogInfoResponse{2: {LogTerm: 3, LogIndex: 10, Term: 3}})
	require.ErrorIs(t, err, ErrConversationReadRetry)
	require.Zero(t, cfg.LeaderId)
}

// T25e-7: the fence error is never a not-found.
func TestSlotDivergedIsNotNotFound(t *testing.T) {
	require.False(t, errors.Is(rafttypes.ErrSlotDiverged, wkdb.ErrNotFound))
	require.False(t, errors.Is(wkdb.ErrNotFound, rafttypes.ErrSlotDiverged))
	require.False(t, errors.Is(rafttypes.ErrSlotDiverged, ErrChannelClusterConfigNotFound))
}

// D6: responses from concurrent requests are collected without a shared
// append. Run with -race.
func TestRequestSlotInfosConcurrentCollection(t *testing.T) {
	s := testElectionServer(true)
	s.opts.ConfigOptions = clusterconfig.NewOptions(clusterconfig.WithNodeId(1), clusterconfig.WithSlotMaxReplicaCount(3))
	wait := map[uint64][]uint32{}
	for id := uint64(2); id <= 64; id++ {
		wait[id] = []uint32{1, 2}
	}
	fetch := func(nodeId uint64, req *SlotLogInfoReq) (*SlotLogInfoResp, error) {
		if nodeId%7 == 0 {
			return nil, errors.New("unreachable")
		}
		slots := make([]SlotInfo, 0, len(req.SlotIds))
		for _, id := range req.SlotIds {
			slots = append(slots, info(id, nodeId, 1, 1))
		}
		return &SlotLogInfoResp{NodeId: nodeId, Slots: slots}, nil
	}
	// wklog configures itself lazily on first use, which races on its own;
	// configure it before the concurrent part.
	s.Warn("warm up logger")
	for i := 0; i < 20; i++ {
		resps, err := s.requestSlotInfosWith(wait, fetch)
		require.NoError(t, err)
		require.Len(t, resps, 63-9, "every answering node exactly once")
		seen := map[uint64]bool{}
		for _, r := range resps {
			require.False(t, seen[r.NodeId])
			seen[r.NodeId] = true
		}
	}
}
