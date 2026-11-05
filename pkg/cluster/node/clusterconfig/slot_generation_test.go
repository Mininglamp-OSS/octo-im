package clusterconfig

import (
	"encoding/binary"
	"os"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func generationTestServer(t *testing.T, path string) *Server {
	t.Helper()
	if path == "" {
		path = t.TempDir() + "/config.json"
	}
	s := New(NewOptions(WithNodeId(1), WithConfigPath(path)))
	t.Cleanup(func() { _ = s.config.cfgFile.Close() })
	return s
}

func generationSlot(id uint32) *pb.Slot {
	return &pb.Slot{Id: id, Leader: 1, Term: 1, Replicas: []uint64{1}}
}

func generationConfig(slots ...*pb.Slot) *pb.Config {
	return &pb.Config{SlotCount: 64, SlotReplicaCount: 3, ChannelReplicaCount: 3, Nodes: []*pb.Node{{Id: 1}}, Slots: slots}
}

// Encode versions explicitly so historical replay tests do not depend on the
// current writer's defaults or on CMD.Marshal accepting an explicit version.
func generationLog(index uint64, version uint16, kind CMDType, data []byte) rafttypes.Log {
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteUint16(version)
	enc.WriteUint16(kind.Uint16())
	enc.WriteBytes(data)
	return rafttypes.Log{Index: index, Term: 1, Data: enc.Bytes()}
}

func generationSlots(t *testing.T, slots ...*pb.Slot) []byte {
	t.Helper()
	data, err := pb.SlotSet(slots).Marshal()
	require.NoError(t, err)
	return data
}

func generationCAS(t *testing.T, before, after []*pb.Slot) []byte {
	t.Helper()
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteBinary(generationSlots(t, before...))
	enc.WriteBinary(generationSlots(t, after...))
	return enc.Bytes()
}

func TestSlotGenerationCommandVersions(t *testing.T) {
	versioned := map[CMDType]bool{CMDTypeConfigChange: true, CMDTypeSlotUpdate: true, CMDTypeNodeJoined: true,
		CMDTypeSlotMigrate: true, CMDTypeSlotStatusChange: true, CMDTypeSlotUpdateIfCurrent: true}
	for kind := CMDTypeConfigChange; kind <= CMDTypeSlotUpdateIfCurrent; kind++ {
		cmd := NewCMD(kind, nil)
		data, err := cmd.Marshal()
		require.NoError(t, err)
		want := uint16(1)
		if versioned[kind] {
			want = 3
		}
		require.Equal(t, want, binary.BigEndian.Uint16(data), kind.String())
	}
	for _, version := range []uint16{1, 2, 3} {
		cmd := NewCMD(CMDTypeSlotUpdate, nil)
		cmd.version = version
		data, err := cmd.Marshal()
		require.NoError(t, err)
		require.Equal(t, version, binary.BigEndian.Uint16(data), "explicit historical versions must survive re-encoding")
	}
}

func TestSlotGenerationCoversEveryMutationAndPreservesNoops(t *testing.T) {
	for _, kind := range []CMDType{CMDTypeConfigChange, CMDTypeSlotUpdate, CMDTypeNodeJoined, CMDTypeSlotMigrate, CMDTypeSlotStatusChange, CMDTypeSlotUpdateIfCurrent} {
		t.Run(kind.String(), func(t *testing.T) {
			s := generationTestServer(t, "")
			old, untouched := generationSlot(1), generationSlot(2)
			s.config.update(generationConfig(old, untouched))
			next := old.Clone()
			next.MigrateFrom, next.MigrateTo, next.Learners, next.Generation = 1, 4, []uint64{4}, 99999
			var data []byte
			var err error
			switch kind {
			case CMDTypeConfigChange:
				data, err = generationConfig(next, untouched).Marshal()
			case CMDTypeSlotUpdate:
				data = generationSlots(t, next)
			case CMDTypeNodeJoined:
				data, err = EncodeNodeJoined(1, []*pb.Slot{next})
			case CMDTypeSlotMigrate:
				data, err = EncodeMigrateSlot(1, 1, 4)
			case CMDTypeSlotStatusChange:
				data, err = EncodeSlotStatusChange(1, pb.SlotStatus_SlotStatusCandidate)
			case CMDTypeSlotUpdateIfCurrent:
				data = generationCAS(t, []*pb.Slot{old}, []*pb.Slot{next})
			}
			require.NoError(t, err)
			log := generationLog(10, 3, kind, data)
			require.NoError(t, s.applyLog(log))
			require.Equal(t, uint64(10), s.Slot(1).Generation, "the applied index, not the supplied generation, owns the new assignment")
			require.Zero(t, s.Slot(2).Generation)
			require.NoError(t, s.applyLog(log), "retrying an applied log must preserve generation")
			log.Index = 11
			require.NoError(t, s.applyLog(log), "repeating a no-op command at a later index must preserve generation")
			require.Equal(t, uint64(10), s.Slot(1).Generation)
			require.Zero(t, s.Slot(2).Generation)
		})
	}
}

func TestSlotGenerationCASRejectsABAAfterProposalWasPrepared(t *testing.T) {
	s := generationTestServer(t, "")
	s.config.update(generationConfig(generationSlot(1), generationSlot(2)))
	start, err := EncodeMigrateSlot(1, 1, 4)
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(10, 3, CMDTypeSlotMigrate, start)))
	origin := s.Slot(1)
	completed := generationSlot(1)
	completed.Leader, completed.Replicas, completed.Generation = 4, []uint64{4}, 99999
	// This is the command queued after SaveConfig's pre-check. Configuration
	// changes before Apply must still reject it even when the shape returns.
	queued := generationLog(13, 3, CMDTypeSlotUpdateIfCurrent, generationCAS(t, []*pb.Slot{origin}, []*pb.Slot{completed}))
	require.NoError(t, s.applyLog(generationLog(11, 3, CMDTypeSlotUpdate, generationSlots(t, generationSlot(1)))))
	require.NoError(t, s.applyLog(generationLog(12, 3, CMDTypeSlotMigrate, start)))
	require.True(t, s.Slot(1).Equal(origin), "the new task intentionally has the old assignment shape")
	require.NoError(t, s.applyLog(queued))
	require.Equal(t, uint64(1), s.Slot(1).Leader)
	require.Equal(t, uint64(12), s.Slot(1).Generation)

	// Batch CAS retains the existing per-slot semantics: skip a stale entry,
	// but apply the independent entry whose source generation still matches.
	second, secondNext := s.Slot(2), generationSlot(2)
	secondNext.Status = pb.SlotStatus_SlotStatusCandidate
	batch := generationCAS(t, []*pb.Slot{origin, second}, []*pb.Slot{completed, secondNext})
	require.NoError(t, s.applyLog(generationLog(14, 3, CMDTypeSlotUpdateIfCurrent, batch)))
	require.Equal(t, uint64(12), s.Slot(1).Generation)
	require.Equal(t, uint64(1), s.Slot(1).Leader)
	require.Equal(t, uint64(14), s.Slot(2).Generation)
	current := s.Slot(1)
	require.NoError(t, s.applyLog(generationLog(15, 3, CMDTypeSlotUpdateIfCurrent, generationCAS(t, []*pb.Slot{current}, []*pb.Slot{completed}))))
	require.Equal(t, uint64(4), s.Slot(1).Leader)
	require.Equal(t, uint64(15), s.Slot(1).Generation)
}

func TestSlotGenerationUnrelatedLogsDoNotInvalidatePreparedCAS(t *testing.T) {
	s := generationTestServer(t, "")
	s.config.update(generationConfig(generationSlot(1), generationSlot(2)))
	start, err := EncodeMigrateSlot(1, 1, 4)
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(10, 3, CMDTypeSlotMigrate, start)))
	origin := s.Slot(1)
	addr, err := EncodeApiServerAddrChange(1, "http://127.0.0.1:5001")
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(11, 1, CMDTypeConfigApiServerAddrChange, addr)))
	status, err := EncodeSlotStatusChange(2, pb.SlotStatus_SlotStatusCandidate)
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(12, 3, CMDTypeSlotStatusChange, status)))
	require.Equal(t, origin.Generation, s.Slot(1).Generation)
	completed := generationSlot(1)
	completed.Leader, completed.Replicas = 4, []uint64{4}
	require.NoError(t, s.applyLog(generationLog(13, 3, CMDTypeSlotUpdateIfCurrent, generationCAS(t, []*pb.Slot{origin}, []*pb.Slot{completed}))))
	require.Equal(t, uint64(4), s.Slot(1).Leader)
	require.Equal(t, uint64(13), s.Slot(1).Generation)
}

func TestSlotGenerationLegacySnapshotAndReplayConverge(t *testing.T) {
	fromLog := generationTestServer(t, "")
	initial, err := generationConfig(generationSlot(1)).Marshal()
	require.NoError(t, err)
	start, err := EncodeMigrateSlot(1, 1, 4)
	require.NoError(t, err)
	origin := generationSlot(1)
	origin.MigrateFrom, origin.MigrateTo, origin.Learners = 1, 4, []uint64{4}
	legacyCancel := generationCAS(t, []*pb.Slot{origin}, []*pb.Slot{generationSlot(1)})
	legacy := []rafttypes.Log{generationLog(1, 1, CMDTypeConfigChange, initial), generationLog(2, 1, CMDTypeSlotMigrate, start),
		generationLog(3, 1, CMDTypeSlotUpdateIfCurrent, legacyCancel)}
	for _, log := range legacy {
		require.NoError(t, fromLog.applyLog(log))
	}
	require.Zero(t, fromLog.Slot(1).Generation, "pure historical replay must match an old snapshot without the field")
	data, err := os.ReadFile(fromLog.opts.ConfigPath)
	require.NoError(t, err)
	require.NotContains(t, string(data), "generation")
	fromSnapshot := generationTestServer(t, fromLog.opts.ConfigPath)
	require.True(t, proto.Equal(fromLog.Snapshot(), fromSnapshot.Snapshot()))
	for _, s := range []*Server{fromLog, fromSnapshot} {
		require.NoError(t, s.applyLog(generationLog(4, 3, CMDTypeSlotMigrate, start)))
		require.Equal(t, uint64(4), s.Slot(1).Generation)
		// Historical conditional updates compare shape, even after a slot has
		// acquired a generation. Their real mutation must not erase that fence.
		require.NoError(t, s.applyLog(generationLog(5, 1, CMDTypeSlotUpdateIfCurrent, legacyCancel)))
		require.Equal(t, uint64(5), s.Slot(1).Generation)
		require.Zero(t, s.Slot(1).MigrateTo)
	}
	require.True(t, proto.Equal(fromLog.Snapshot(), fromSnapshot.Snapshot()))
	reloaded := generationTestServer(t, fromSnapshot.opts.ConfigPath)
	require.True(t, proto.Equal(fromSnapshot.Snapshot(), reloaded.Snapshot()), "new snapshots must persist the assignment generation")
}

func TestSlotGenerationLegacyFullConfigCannotEraseFence(t *testing.T) {
	s := generationTestServer(t, "")
	s.config.update(generationConfig(generationSlot(1)))
	status, err := EncodeSlotStatusChange(1, pb.SlotStatus_SlotStatusCandidate)
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(10, 3, CMDTypeSlotStatusChange, status)))
	for _, version := range []uint16{1, 3} {
		cfg := s.Snapshot()
		cfg.Slots[0].Generation = 99999
		data, err := cfg.Marshal()
		require.NoError(t, err)
		index := s.ConfigVersion() + 1
		require.NoError(t, s.applyLog(generationLog(index, version, CMDTypeConfigChange, data)))
		require.Equal(t, uint64(10), s.Slot(1).Generation, "unchanged shapes cannot import a caller-supplied generation")
	}
	cfg := s.Snapshot()
	cfg.Slots[0].Generation, cfg.Slots[0].Term = 0, 2
	data, err := cfg.Marshal()
	require.NoError(t, err)
	require.NoError(t, s.applyLog(generationLog(13, 1, CMDTypeConfigChange, data)))
	require.Equal(t, uint64(13), s.Slot(1).Generation, "legacy full replacement must advance an existing generation when its shape changes")
}
