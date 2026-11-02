package event

import (
	"slices"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func (s *Server) handleNodeJoining() error {
	cfg := s.cfgServer.Snapshot()
	if len(cfg.Slots) == 0 {
		return nil
	}
	var joining *types.Node
	for _, node := range cfg.Nodes {
		if node.Status == types.NodeStatus_NodeStatusJoining {
			joining = node
			break
		}
	}
	if joining != nil && !joining.Online {
		// Its configuration vote was already committed. Finish admission
		// without assigning data so another external node can join and heal it.
		return s.cfgServer.ProposeJoined(joining.Id, nil)
	}
	// Use the same planner after a restart or an interrupted migration. Its
	// durable journal is the slot assignment, not an in-memory join callback.
	planned := planSlotExpansion(cfg)
	if joining != nil && len(planned) == 0 {
		for _, slot := range cfg.Slots {
			if len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 || slot.Status != types.SlotStatus_SlotStatusNormal {
				return nil // finish the existing migration before ordinary balance
			}
		}
		planned = balanceJoiningSlots(cfg, joining.Id)
	}
	if joining != nil {
		// An explicitly rejoined ID may have lost its slot logs. Preserve the
		// existing recovery election even when no replica migration is needed.
		for _, slot := range cfg.Slots {
			if slot.Leader != joining.Id {
				continue
			}
			var updated *types.Slot
			for _, next := range planned {
				if next.Id == slot.Id {
					updated = next
					break
				}
			}
			if updated == nil {
				updated = slot.Clone()
				planned = append(planned, updated)
			}
			updated.Status = types.SlotStatus_SlotStatusCandidate
			updated.ExpectLeader = slot.Leader
		}
	}
	if len(planned) > 0 {
		before := make([]*types.Slot, 0, len(planned))
		for _, next := range planned {
			for _, old := range cfg.Slots {
				if next.Id == old.Id {
					before = append(before, old)
					break
				}
			}
		}
		if err := s.cfgServer.ProposeSlotsIfCurrent(before, planned); err != nil {
			return err
		}
	}
	if joining != nil {
		return s.cfgServer.ProposeJoined(joining.Id, nil)
	}
	return nil
}

func expansionNodes(cfg *types.Config) ([]uint64, map[uint64]bool) {
	var eligible []uint64
	online := make(map[uint64]bool, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		online[n.Id] = n.Online
		if n.Online && n.AllowVote && n.Role == types.NodeRole_NodeRoleReplica &&
			(n.Status == types.NodeStatus_NodeStatusJoining || n.Status == types.NodeStatus_NodeStatusJoined) {
			eligible = append(eligible, n.Id)
		}
	}
	slices.Sort(eligible)
	return eligible, online
}

// Desired capacity counts healthy replicas; voting quorum still counts the
// committed replicas. Learners first catch up through the existing Raft path.
func planSlotExpansion(cfg *types.Config) []*types.Slot {
	eligible, online := expansionNodes(cfg)
	var planned []*types.Slot
	for _, slot := range cfg.Slots {
		live := 0
		var offline uint64
		for _, id := range slot.Replicas {
			if online[id] {
				live++
			} else if offline == 0 {
				offline = id
			}
		}
		if live < len(slot.Replicas)/2+1 {
			continue
		}
		if !online[slot.Leader] {
			if slot.Status != types.SlotStatus_SlotStatusCandidate {
				next := slot.Clone()
				next.Status = types.SlotStatus_SlotStatusCandidate
				for _, id := range slot.Replicas {
					if online[id] {
						next.ExpectLeader = id
						break
					}
				}
				planned = append(planned, next)
			}
			continue
		}
		if slot.Status != types.SlotStatus_SlotStatusNormal {
			continue
		}
		if slot.MigrateTo != 0 || slot.MigrateFrom != 0 || len(slot.Learners) > 0 {
			// A dead destination cannot finish. Cancel only an actual learner,
			// never a live destination or a leader transfer between voters.
			if slot.MigrateTo != 0 && !online[slot.MigrateTo] && wkutil.ArrayContainsUint64(slot.Learners, slot.MigrateTo) {
				next := slot.Clone()
				next.Learners = wkutil.RemoveUint64(next.Learners, next.MigrateTo)
				next.MigrateFrom, next.MigrateTo = 0, 0
				planned = append(planned, next)
			} else if slot.MigrateTo != 0 && !online[slot.MigrateTo] && slot.MigrateFrom != slot.Leader &&
				online[slot.MigrateFrom] && wkutil.ArrayContainsUint64(slot.Replicas, slot.MigrateTo) && live >= int(cfg.SlotReplicaCount) {
				// D failed after the add-first stage. The old follower still
				// holds its place; retire only D and cancel this migration.
				next := slot.Clone()
				next.Replicas = wkutil.RemoveUint64(next.Replicas, next.MigrateTo)
				next.MigrateFrom, next.MigrateTo = 0, 0
				planned = append(planned, next)
			}
			continue
		}
		if live >= int(cfg.SlotReplicaCount) {
			continue
		}
		for _, id := range eligible {
			if wkutil.ArrayContainsUint64(slot.Replicas, id) {
				continue
			}
			next := slot.Clone()
			next.Learners = append(next.Learners, id)
			next.MigrateFrom, next.MigrateTo = id, id
			if offline != 0 {
				next.MigrateFrom = offline
			}
			planned = append(planned, next)
			break // one persisted migration per group
		}
	}
	return planned
}

// Normal healthy expansion keeps the existing target count and moves only the
// share needed by the joining node. Never overwrite another group's migration.
func balanceJoiningSlots(cfg *types.Config, joining uint64) []*types.Slot {
	eligible, online := expansionNodes(cfg)
	if len(eligible) == 0 {
		return nil
	}
	counts := map[uint64]int{}
	for _, slot := range cfg.Slots {
		for _, id := range slot.Replicas {
			counts[id]++
		}
	}
	target := (len(cfg.Slots)*int(cfg.SlotReplicaCount) + len(eligible) - 1) / len(eligible)
	var planned []*types.Slot
	for _, slot := range cfg.Slots {
		if counts[joining] >= target {
			break
		}
		if slot.Status != types.SlotStatus_SlotStatusNormal || !online[slot.Leader] ||
			len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 || wkutil.ArrayContainsUint64(slot.Replicas, joining) {
			continue
		}
		var from uint64
		for _, id := range slot.Replicas {
			// Replace followers first. Once D is a full replica, the existing
			// leader balancer transfers leadership between established voters.
			if id == slot.Leader || !online[id] || counts[id] <= target {
				continue
			}
			if from == 0 || counts[id] > counts[from] {
				from = id
			}
		}
		if from == 0 {
			continue
		}
		next := slot.Clone()
		next.MigrateFrom, next.MigrateTo = from, joining
		next.Learners = append(next.Learners, joining)
		planned = append(planned, next)
		counts[from]--
		counts[joining]++
	}
	return planned
}
