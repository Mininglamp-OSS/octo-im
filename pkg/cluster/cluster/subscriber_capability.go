package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"golang.org/x/sync/errgroup"
)

// Protocol 3 adds replicated capability confirmations and paged pending work.
// An activated data directory must not be downgraded to a protocol-2 binary.
const subscriberProtocolVersion uint32 = 3
const subscriberRevisionProtocolVersion uint32 = 4
const subscriberRevisionCapabilityPath = "/rpc/cluster/subscriber-revision-capability"
const subscriberRevisionActivationPath = "/rpc/cluster/subscriber-revision-activation"
const subscriberAtomicProtocolVersion = wkdb.SubscriberAtomicProtocolVersion
const subscriberAtomicCapabilityPath = "/rpc/cluster/subscriber-atomic-capability"
const subscriberAtomicActivationPath = "/rpc/cluster/subscriber-atomic-activation"
const subscriberCapabilityPath = "/rpc/cluster/subscriber-capability"
const subscriberActivationPath = "/rpc/cluster/subscriber-activation"

func (s *Server) checkSubscriberProposal(ctx context.Context, _ uint32, reqs rafttypes.ProposeReqSet) error {
	needed, err := subscriberProposalProtocol(reqs)
	if err != nil || needed == 0 {
		return err
	}
	return s.ensureSubscriberProtocol(ctx, needed)
}

func subscriberProposalProtocol(reqs rafttypes.ProposeReqSet) (uint32, error) {
	needed := uint32(0)
	for _, req := range reqs {
		cmd := &store.CMD{}
		if err := cmd.Unmarshal(req.Data); err != nil {
			return 0, err
		}
		switch cmd.CmdType {
		case store.CMDSubscriberOperation, store.CMDConversationEffects, store.CMDSubscriberCheckpoint:
			needed = max(needed, subscriberProtocolVersion)
		case store.CMDSubscriberReconcile:
			needed = max(needed, subscriberRevisionProtocolVersion)
		}
		if cmd.CmdType == store.CMDSubscriberOperation || cmd.CmdType == store.CMDSubscriberReconcile {
			var o wkdb.SubscriberOperation
			if err := json.Unmarshal(cmd.Data, &o); err != nil {
				return 0, err
			}
			if o.SourceProtocol != 0 {
				if o.SourceProtocol != subscriberAtomicProtocolVersion {
					return 0, errors.New("unsupported subscriber source protocol")
				}
				needed = max(needed, subscriberAtomicProtocolVersion)
			}
		}
	}
	return needed, nil
}

func (s *Server) ensureSubscriberProtocol(ctx context.Context, required uint32) error {
	nodes := s.cfgServer.SubscriberNodes()
	if subscriberNodesConfirmedAt(nodes, required) {
		return ctx.Err()
	}
	// A lagging/new slot leader obtains the committed proof from the config
	// leader, rather than probing a now-offline replica again.
	leader := s.cfgServer.LeaderId()
	if leader == s.opts.ConfigOptions.NodeId {
		_, err := s.activateSubscriberProtocolAt(ctx, required)
		return err
	}
	if leader == 0 {
		return errors.New("cluster configuration leader unavailable")
	}
	activationPath := subscriberActivationPath
	if required == subscriberRevisionProtocolVersion {
		activationPath = subscriberRevisionActivationPath
	} else if required == subscriberAtomicProtocolVersion {
		activationPath = subscriberAtomicActivationPath
	}
	response, err := s.RequestWithContext(ctx, leader, activationPath, nil)
	if err != nil {
		return err
	}
	if response.Status != proto.StatusOK {
		return fmt.Errorf("subscriber activation: %s", response.Body)
	}
	proof := &types.Config{}
	if err := proof.Unmarshal(response.Body); err != nil {
		return err
	}
	if !subscriberProofMatchesAt(s.cfgServer.SubscriberNodes(), proof.Nodes, required) {
		return errors.New("cluster membership changed during subscriber activation")
	}
	return ctx.Err()
}

// An individual capability advertisement is not cluster activation. While a
// rolling upgrade is incomplete, ordinary protocol-3 replacements can still
// join. Protocol 4 can first be emitted only with a proof covering every
// current identity; after that, replacing a node cannot lower the requirement.
func requiredSubscriberJoinProtocol(nodes []*types.Node) uint32 {
	if subscriberNodesConfirmedAt(nodes, subscriberAtomicProtocolVersion) {
		return subscriberAtomicProtocolVersion
	}
	if subscriberNodesConfirmedAt(nodes, subscriberRevisionProtocolVersion) {
		return subscriberRevisionProtocolVersion
	}
	return subscriberProtocolVersion
}

func subscriberNodesConfirmed(nodes []*types.Node) bool {
	return subscriberNodesConfirmedAt(nodes, subscriberProtocolVersion)
}

func subscriberNodesConfirmedAt(nodes []*types.Node, required uint32) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if node.SubscriberProtocol < required {
			return false
		}
	}
	return true
}

func subscriberProofMatches(nodes, proof []*types.Node) bool {
	return subscriberProofMatchesAt(nodes, proof, subscriberProtocolVersion)
}

func subscriberProofMatchesAt(nodes, proof []*types.Node, required uint32) bool {
	if len(nodes) != len(proof) || !subscriberNodesConfirmedAt(proof, required) {
		return false
	}
	confirmed := make(map[uint64]*types.Node, len(proof))
	for _, node := range proof {
		confirmed[node.Id] = node
	}
	for _, node := range nodes {
		known := confirmed[node.Id]
		if known == nil || known.ClusterAddr != node.ClusterAddr || known.CreatedAt != node.CreatedAt {
			return false
		}
	}
	return true
}

// Probe only identities never confirmed by config Raft. Successful proofs are
// retained even when a different peer is unavailable during a rolling upgrade.
func probeSubscriberNodes(ctx context.Context, nodes []*types.Node, local uint64, probe func(context.Context, uint64) (bool, error)) ([]*types.Node, error) {
	return probeSubscriberNodesAt(ctx, nodes, local, subscriberProtocolVersion, probe)
}

func probeSubscriberNodesAt(ctx context.Context, nodes []*types.Node, local uint64, required uint32, probe func(context.Context, uint64) (bool, error)) ([]*types.Node, error) {
	var confirmed []*types.Node
	var mu sync.Mutex
	var g errgroup.Group
	g.SetLimit(8)
	for _, node := range nodes {
		if node.SubscriberProtocol >= required {
			continue
		}
		node := node
		g.Go(func() error {
			if node.Id != local {
				supported, err := probe(ctx, node.Id)
				if err != nil {
					return fmt.Errorf("node %d subscriber capability unconfirmed: %w", node.Id, err)
				}
				if !supported {
					return fmt.Errorf("node %d must be upgraded before subscriber recovery can activate", node.Id)
				}
			}
			proof := node.Clone()
			proof.SubscriberProtocol = required
			mu.Lock()
			confirmed = append(confirmed, proof)
			mu.Unlock()
			return nil
		})
	}
	err := g.Wait()
	sort.Slice(confirmed, func(i, j int) bool { return confirmed[i].Id < confirmed[j].Id })
	return confirmed, err
}

func (s *Server) activateSubscriberProtocol(ctx context.Context) ([]*types.Node, error) {
	return s.activateSubscriberProtocolAt(ctx, subscriberProtocolVersion)
}

func (s *Server) activateSubscriberProtocolAt(ctx context.Context, required uint32) ([]*types.Node, error) {
	if !s.cfgServer.IsLeader() {
		return nil, errors.New("subscriber activation requires the config leader")
	}
	nodes := s.cfgServer.SubscriberNodes()
	if subscriberNodesConfirmedAt(nodes, required) {
		return nodes, ctx.Err()
	}
	select {
	case s.subscriberCapabilityGate <- struct{}{}:
		defer func() { <-s.subscriberCapabilityGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	nodes = s.cfgServer.SubscriberNodes()
	capabilityPath := subscriberCapabilityPath
	if required == subscriberRevisionProtocolVersion {
		capabilityPath = subscriberRevisionCapabilityPath
	} else if required == subscriberAtomicProtocolVersion {
		capabilityPath = subscriberAtomicCapabilityPath
	}
	confirmed, probeErr := probeSubscriberNodesAt(ctx, nodes, s.opts.ConfigOptions.NodeId, required, func(ctx context.Context, id uint64) (bool, error) {
		response, err := s.RequestWithContext(ctx, id, capabilityPath, nil)
		if err != nil {
			return false, err
		}
		return response.Status == proto.StatusOK && string(response.Body) == fmt.Sprint(required), nil
	})
	if len(confirmed) > 0 {
		if err := s.cfgServer.ProposeSubscriberProtocols(ctx, confirmed); err != nil {
			return nil, err
		}
	}
	if probeErr != nil {
		return nil, probeErr
	}
	nodes = s.cfgServer.SubscriberNodes()
	if !subscriberNodesConfirmedAt(nodes, required) {
		return nil, errors.New("subscriber capability confirmation pending or membership changed")
	}
	return nodes, ctx.Err()
}

func (r *rpcServer) handleSubscriberActivation(c *wkserver.Context) {
	r.handleSubscriberActivationAt(c, subscriberProtocolVersion)
}

func (r *rpcServer) handleSubscriberRevisionActivation(c *wkserver.Context) {
	r.handleSubscriberActivationAt(c, subscriberRevisionProtocolVersion)
}

func (r *rpcServer) handleSubscriberAtomicActivation(c *wkserver.Context) {
	r.handleSubscriberActivationAt(c, subscriberAtomicProtocolVersion)
}

func (r *rpcServer) handleSubscriberActivationAt(c *wkserver.Context, required uint32) {
	ctx, cancel := context.WithTimeout(r.s.cancelCtx, 5*time.Second)
	defer cancel()
	nodes, err := r.s.activateSubscriberProtocolAt(ctx, required)
	if err != nil {
		c.WriteErr(err)
		return
	}
	data, err := (&types.Config{Nodes: nodes}).Marshal()
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

// Pre-confirm after startup, independently of membership traffic. Once all
// identities are confirmed this loop performs no network or storage work.
func (s *Server) subscriberCapabilityLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.cfgServer.IsLeader() && !subscriberNodesConfirmed(s.cfgServer.SubscriberNodes()) {
				probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, _ = s.activateSubscriberProtocol(probeCtx)
				cancel()
			}
		}
	}
}
