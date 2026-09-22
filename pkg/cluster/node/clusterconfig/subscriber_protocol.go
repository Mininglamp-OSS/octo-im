package clusterconfig

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
)

// SubscriberNodes returns an immutable snapshot for capability decisions.
func (s *Server) SubscriberNodes() []*types.Node {
	s.config.mu.RLock()
	defer s.config.mu.RUnlock()
	nodes := make([]*types.Node, 0, len(s.config.cfg.Nodes))
	for _, n := range s.config.cfg.Nodes {
		nodes = append(nodes, n.Clone())
	}
	return nodes
}

// Confirmations are ordered and persisted by config Raft, so every slot leader
// (including a newly elected one) inherits them without contacting offline peers.
func (s *Server) ProposeSubscriberProtocols(ctx context.Context, nodes []*types.Node) error {
	// Old binaries may have skipped earlier capability commands. Re-broadcast
	// the complete proof set when another node upgrades, so its first understood
	// command also repairs the confirmations it missed before the upgrade.
	all := s.SubscriberNodes()
	confirmed := make([]*types.Node, 0, len(all))
	for _, current := range all {
		for _, proof := range nodes {
			if current.Id == proof.Id && current.ClusterAddr == proof.ClusterAddr && current.CreatedAt == proof.CreatedAt {
				current.SubscriberProtocol = max(current.SubscriberProtocol, proof.SubscriberProtocol)
			}
		}
		if current.SubscriberProtocol > 0 {
			confirmed = append(confirmed, current)
		}
	}
	data, err := (&types.Config{Nodes: confirmed}).Marshal()
	if err != nil {
		return err
	}
	cmd, err := NewCMD(CMDTypeSubscriberProtocols, data).Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilAppliedTimeout(ctx, s.genConfigId(), cmd)
	return err
}

func (c *Config) confirmSubscriberProtocols(confirmed []*types.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, proof := range confirmed {
		for i, node := range c.cfg.Nodes {
			// A delayed confirmation must never authorize a replacement identity.
			if node.Id == proof.Id && node.ClusterAddr == proof.ClusterAddr && node.CreatedAt == proof.CreatedAt && proof.SubscriberProtocol > node.SubscriberProtocol {
				next := node.Clone()
				next.SubscriberProtocol = proof.SubscriberProtocol
				c.cfg.Nodes[i] = next
				break
			}
		}
	}
}
