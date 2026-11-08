package event

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// planOfflineSlotReplacements adds at most one learner per idle slot. Existing
// voters remain intact until the normal durable-catchup promotion replaces the
// failed member. A zero target selects any eligible, joined spare node.
func planOfflineSlotReplacements(cfg *types.Config, target uint64) []*types.Slot {
	nodes := make(map[uint64]*types.Node, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		nodes[node.Id] = node
	}
	var plans []*types.Slot
	for _, slot := range cfg.Slots {
		leader := nodes[slot.Leader]
		if leader == nil || !leader.Online || slot.Status != types.SlotStatus_SlotStatusNormal ||
			len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 {
			continue
		}
		var failed uint64
		online := 0
		for _, id := range slot.Replicas {
			node := nodes[id]
			if node == nil {
				continue
			}
			if node.Online {
				online++
			}
			if id != slot.Leader && (!node.AllowVote || !node.Online && node.OfflineCount > 0) {
				failed = id
			}
		}
		if failed == 0 || online < len(slot.Replicas)/2+1 {
			continue
		}
		for _, node := range cfg.Nodes {
			if target != 0 && node.Id != target || !node.AllowVote || !node.Online ||
				node.Role != types.NodeRole_NodeRoleReplica || wkutil.ArrayContainsUint64(slot.Replicas, node.Id) ||
				node.Status != types.NodeStatus_NodeStatusJoined && !(node.Id == target && node.Status == types.NodeStatus_NodeStatusJoining) {
				continue
			}
			next := slot.Clone()
			next.MigrateFrom, next.MigrateTo = failed, node.Id
			next.Learners = []uint64{node.Id}
			plans = append(plans, next)
			break
		}
	}
	return plans
}
