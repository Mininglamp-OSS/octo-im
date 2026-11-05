package clusterconfig

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"time"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// Snapshot is immutable to callers, including while the apply worker advances.
func (s *Server) Snapshot() *pb.Config { return s.config.config() }

// A retired process may still be campaigning in an obsolete, higher term.
// A successful join RPC authorizes passive catch-up from the current leader;
// only replay of the committed application membership may restore its vote.
func (s *Server) ResumeJoin(remote *pb.Config, leader, joinVersion uint64) error {
	if joinVersion == 0 || remote.Version < joinVersion || leader == 0 || s.Snapshot().Version >= joinVersion {
		return nil
	}
	cfg := rafttypes.Config{Version: joinVersion, Leader: leader}
	for _, n := range remote.Nodes {
		if n.Id != s.opts.NodeId && n.AllowVote && n.Role == pb.NodeRole_NodeRoleReplica && !wkutil.ArrayContainsUint64(remote.Learners, n.Id) {
			cfg.Replicas = append(cfg.Replicas, n.Id)
		}
	}
	cfg.Learners = append(slices.Clone(remote.Learners), s.opts.NodeId)
	slices.Sort(cfg.Learners)
	cfg.Learners = slices.Compact(cfg.Learners)
	cfg.MigrateFrom, cfg.MigrateTo = remote.MigrateFrom, remote.MigrateTo
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.raft.StepWait(ctx, rafttypes.Event{Type: rafttypes.ConfChange, Config: cfg})
	if errors.Is(err, raft.ErrConfigVersionStale) {
		return nil // replay/config delivery already installed newer membership
	}
	return err
}

// Admission retires an offline voter only after an external learner catches up.
// The old voting set must still have quorum.
func offlineVoterForJoin(cfg *pb.Config, joining uint64) uint64 {
	if !wkutil.ArrayContainsUint64(cfg.Learners, joining) || cfg.MigrateTo != joining {
		return 0
	}
	eligible := false
	for _, n := range cfg.Nodes {
		if n.Id == joining {
			eligible = n.AllowVote && n.Role == pb.NodeRole_NodeRoleReplica
			break
		}
	}
	if !eligible {
		return 0
	}
	voters, online := 0, 0
	var offline uint64
	for _, n := range cfg.Nodes {
		if !n.AllowVote || n.Role != pb.NodeRole_NodeRoleReplica || wkutil.ArrayContainsUint64(cfg.Learners, n.Id) {
			continue
		}
		voters++
		if n.Online {
			online++
		} else if offline == 0 || n.Id < offline {
			offline = n.Id
		}
	}
	if online < voters/2+1 {
		return 0
	}
	return offline
}

// D can finish joining before the heartbeat timeout observes C's crash.
// Revisit that ordering after admission too, but never shrink an ordinary
// three-voter cluster to two merely because its third voter is offline.
func offlineVoterWithSpareCapacity(cfg *pb.Config) uint64 {
	if len(cfg.Learners) != 0 || cfg.MigrateFrom != 0 || cfg.MigrateTo != 0 {
		return 0
	}
	voters, online := 0, 0
	var offline uint64
	for _, n := range cfg.Nodes {
		if n.Status != pb.NodeStatus_NodeStatusJoined {
			return 0
		}
		if !n.AllowVote || n.Role != pb.NodeRole_NodeRoleReplica {
			continue
		}
		voters++
		if n.Online {
			online++
		} else if offline == 0 || n.Id < offline {
			offline = n.Id
		}
	}
	target := max(cfg.SlotReplicaCount, cfg.ChannelReplicaCount, uint32(1))
	if online < int(target) || online < voters/2+1 {
		return 0
	}
	return offline
}

func (s *Server) ReconcileExpansionMembership() error {
	cfg := s.Snapshot()
	for _, n := range cfg.Nodes {
		if !n.Online && n.Status == pb.NodeStatus_NodeStatusWillJoin && wkutil.ArrayContainsUint64(cfg.Learners, n.Id) {
			return s.proposeNodeRemove(n.Id, n.Id)
		}
	}
	if id := offlineVoterWithSpareCapacity(cfg); id != 0 {
		return s.proposeNodeRemove(id, 0)
	}
	return nil
}

func (s *Server) proposeNodeRemove(id, joining uint64) error {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data, id)
	binary.BigEndian.PutUint64(data[8:], joining)
	cmd, err := NewCMD(CMDTypeNodeRemove, data).Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), cmd)
	return err
}

func (s *Server) handleNodeRemove(cmd *CMD) error {
	if len(cmd.Data) != 16 {
		return errors.New("invalid node removal payload")
	}
	id, joining := binary.BigEndian.Uint64(cmd.Data), binary.BigEndian.Uint64(cmd.Data[8:])
	s.config.mu.Lock()
	defer s.config.mu.Unlock()
	if id != 0 && id == joining && wkutil.ArrayContainsUint64(s.config.cfg.Learners, id) {
		for _, n := range s.config.cfg.Nodes {
			if n.Id == id && !n.Online && n.Status == pb.NodeStatus_NodeStatusWillJoin {
				s.config.cfg.Nodes = slices.DeleteFunc(s.config.cfg.Nodes, func(n *pb.Node) bool { return n.Id == id })
				s.config.cfg.Learners = wkutil.RemoveUint64(s.config.cfg.Learners, id)
				if s.config.cfg.MigrateTo == id {
					s.config.cfg.MigrateFrom, s.config.cfg.MigrateTo = 0, 0
				}
				return nil
			}
		}
		return nil
	}
	// A reconnect or competing membership update can win before admission.
	// A stale removal is a committed no-op, never a deletion of an online node.
	candidate := offlineVoterForJoin(s.config.cfg, joining)
	if joining == 0 {
		candidate = offlineVoterWithSpareCapacity(s.config.cfg)
	}
	if id == 0 || candidate != id {
		return nil
	}
	s.config.cfg.Nodes = slices.DeleteFunc(s.config.cfg.Nodes, func(n *pb.Node) bool { return n.Id == id })
	return nil
}

// ProposeSlotsIfCurrent prevents a delayed promotion or planner from restoring
// an assignment superseded by failover, another learner, or another leader.
func (s *Server) ProposeSlotsIfCurrent(before, after []*pb.Slot) error {
	if len(before) != len(after) {
		return errors.New("slot transition length mismatch")
	}
	oldData, err := pb.SlotSet(before).Marshal()
	if err != nil {
		return err
	}
	newData, err := pb.SlotSet(after).Marshal()
	if err != nil {
		return err
	}
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteBinary(oldData)
	enc.WriteBinary(newData)
	data, err := NewCMD(CMDTypeSlotUpdateIfCurrent, enc.Bytes()).Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), data)
	return err
}

func (s *Server) handleSlotUpdateIfCurrent(cmd *CMD) error {
	dec := wkproto.NewDecoder(cmd.Data)
	oldData, err := dec.Binary()
	if err != nil {
		return err
	}
	newData, err := dec.Binary()
	if err != nil {
		return err
	}
	var before, after pb.SlotSet
	if err := before.Unmarshal(oldData); err != nil {
		return err
	}
	if err := after.Unmarshal(newData); err != nil {
		return err
	}
	if len(before) != len(after) || dec.Len() != 0 {
		return errors.New("invalid conditional slot transition")
	}
	s.config.mu.Lock()
	defer s.config.mu.Unlock()
	for i, old := range before {
		if old.Id != after[i].Id {
			return errors.New("slot transition id mismatch")
		}
	}
	for i, old := range before {
		for j, current := range s.config.cfg.Slots {
			if current.Id == old.Id && current.Equal(old) {
				s.config.cfg.Slots[j] = after[i]
				break
			}
		}
	}
	return nil
}
