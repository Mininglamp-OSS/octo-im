package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

// A surviving voter can have applied a promotion that another voter only
// stored before the old leader failed. In the crash-fault model a known peer
// may share its applied membership even without a leader. Do not use a vote
// or a bare version hint itself as membership/commit evidence.
func (n *Node) stepElectionMembership(e types.Event) error {
	if !n.opts.ElectionOn || e.Term != 0 || (!n.isVoter(e.From) && !n.isLearner(e.From)) {
		return nil
	}
	switch e.Type {
	case types.ConfigReq:
		// Election configs are versioned by the application log index. Do not
		// relay a config received ahead of our own durable apply boundary.
		if !n.isVoter(n.opts.NodeId) || n.cfg.Version < e.ConfigVersion || n.cfg.Version > n.queue.appliedIndex {
			return nil
		}
		cfg := n.cfg.Clone()
		cfg.Leader, cfg.Term, cfg.Role = None, 0, types.RoleUnknown
		n.events = append(n.events, types.Event{Type: types.ConfigResp, From: n.opts.NodeId, To: e.From,
			Reason: types.ReasonOnlySync, Config: cfg})
	case types.ConfigResp:
		version, requested := n.membershipRequests[e.From]
		if !requested || e.Config.Version < version || e.Config.Version <= n.cfg.Version ||
			!isVoter(e.Config, e.From) || e.Config.Leader != None || e.Config.Term != 0 {
			return nil
		}
		if err := n.switchRemoteConfig(e.Config); err != nil {
			return err
		}
		delete(n.membershipRequests, e.From)
	}
	return nil
}
