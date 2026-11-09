package cluster

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"

	"go.uber.org/zap"
)

func (s *Server) joinLoop() {
	seedNodeId, _, _ := seedNode(s.opts.Seed)
	req := &ClusterJoinReq{
		NodeId:     s.opts.ConfigOptions.NodeId,
		ServerAddr: s.joinServerAddr(s.cfgServer.Snapshot()),
		Role:       s.opts.Role,
	}
	for {
		select {
		case <-time.After(time.Second * 2):
			resp, err := s.rpcClient.RequestClusterJoin(seedNodeId, req)
			if err != nil {
				s.Error("requestClusterJoin failed", zap.Error(err), zap.Uint64("seedNodeId", seedNodeId))
				continue
			}
			if err := s.resumeClusterJoin(resp); err != nil {
				continue
			}
			s.joinReady.Store(true)
			return
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

// A persisted bootstrap node can have been removed while it was down. Discover
// that absence first, then explicitly join without a new seed or node ID.
func (s *Server) membershipLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var cursor int
	for {
		select {
		case <-s.stopper.ShouldStop():
			return
		case <-ticker.C:
			if !s.joinReady.Load() {
				continue
			}
			cfg := s.cfgServer.Snapshot()
			peers := s.membershipProbePeers(cfg)
			if len(peers) == 0 {
				continue
			}
			peer := peers[cursor%len(peers)]
			cursor++
			_ = s.probeMembership(peer)
		}
	}
}

func (s *Server) probeMembership(peer uint64) error {
	req := &ClusterJoinReq{NodeId: s.opts.ConfigOptions.NodeId,
		ServerAddr: s.joinServerAddr(s.cfgServer.Snapshot()), Role: s.opts.Role, Probe: true}
	resp, err := s.rpcClient.RequestClusterJoin(peer, req)
	if err != nil {
		return err // Transport/route errors never establish absence.
	}
	if resp.JoinVersion != 0 {
		return errors.New("membership probe returned a mutating join response")
	}
	if resp.Config == nil {
		return nil // An existing formal member needs no recovery action.
	}
	if resp.Config.Version == 0 || resp.LeaderID == 0 {
		return errors.New("membership probe returned an incomplete configuration")
	}
	var existing *types.Node
	leaderKnown := false
	for _, node := range resp.Config.Nodes {
		if node.Id == resp.LeaderID {
			leaderKnown = true
		}
		if node.Id == req.NodeId {
			existing = node
		}
	}
	if !leaderKnown {
		return errors.New("membership probe leader is absent from its configuration")
	}
	if existing == nil {
		// A delayed absence response must not rejoin a member whose newer
		// configuration has already arrived. A newer local deletion still joins.
		current := s.cfgServer.Snapshot()
		if current.Version >= resp.Config.Version {
			for _, node := range current.Nodes {
				if node.Id == req.NodeId {
					return nil
				}
			}
		}
		select {
		case <-s.stopper.ShouldStop():
			return nil
		default:
		}
		req.Probe = false
		joined, err := s.rpcClient.RequestClusterJoin(peer, req)
		if err != nil {
			return err
		}
		return s.resumeClusterJoin(joined)
	}
	// A WillJoin node is already admitted. Resume its passive catch-up without
	// creating another membership command or changing a formal member's role.
	if existing.Status != types.NodeStatus_NodeStatusWillJoin || s.cfgServer.Snapshot().Version >= resp.Config.Version {
		return nil
	}
	if err := s.cfgServer.ResumeJoin(resp.Config, resp.LeaderID, resp.Config.Version); err != nil {
		return err
	}
	s.addJoinPeers(resp.Nodes)
	return nil
}

func (s *Server) resumeClusterJoin(resp *ClusterJoinResp) error {
	if resp == nil || resp.Config == nil || resp.JoinVersion == 0 || resp.LeaderID == 0 || resp.Config.Version < resp.JoinVersion {
		return errors.New("join response does not establish a committed recovery boundary")
	}
	found, leaderKnown := false, false
	for _, node := range resp.Config.Nodes {
		if node.Id == resp.LeaderID {
			leaderKnown = true
		}
		if node.Id == s.opts.ConfigOptions.NodeId {
			found = true
		}
	}
	if !found || !leaderKnown {
		return errors.New("join response no longer contains this member and its leader")
	}
	if err := s.cfgServer.ResumeJoin(resp.Config, resp.LeaderID, resp.JoinVersion); err != nil {
		return err
	}
	if s.cfgServer.Snapshot().Version < resp.JoinVersion {
		s.addJoinPeers(resp.Nodes)
	}
	// Only the actual join command, never a discovery snapshot, can release
	// the startup recovery fence after its subsequent Joined configuration.
	s.slotServer.SetRecoveryJoinVersion(resp.JoinVersion)
	return nil
}

func (s *Server) addJoinPeers(nodes []*NodeInfo) {
	peers := make(map[uint64]string, len(nodes))
	for _, node := range nodes {
		peers[node.NodeId] = node.ServerAddr
	}
	s.addOrUpdateNodes(peers)
}

// Bootstrap deployments may advertise through InitNodes and leave ServerAddr
// empty or set to an unused legacy bind address. Rejoining must use a dialable
// self address, with the committed address preferred over the bootstrap fallback.
func (s *Server) joinServerAddr(cfg *types.Config) string {
	if types.ValidateJoinAddr(s.opts.ServerAddr) == nil {
		return s.opts.ServerAddr
	}
	for _, node := range cfg.Nodes {
		if node.Id == s.opts.ConfigOptions.NodeId && types.ValidateJoinAddr(node.ClusterAddr) == nil {
			return node.ClusterAddr
		}
	}
	if addr := s.opts.ConfigOptions.InitNodes[s.opts.ConfigOptions.NodeId]; types.ValidateJoinAddr(addr) == nil {
		return addr
	}
	// Preserve the original validation error when no advertised source is usable.
	return s.opts.ServerAddr
}

// A seeded learner can be retired before applying its first config log. Keep
// its original discovery route so releasing a temporary outage can rejoin it.
func (s *Server) membershipProbePeers(cfg *types.Config) []uint64 {
	var peers []uint64
	for _, node := range cfg.Nodes {
		if node.Id != s.opts.ConfigOptions.NodeId {
			peers = append(peers, node.Id)
		}
	}
	if len(peers) == 0 {
		for id := range s.opts.ConfigOptions.InitNodes {
			if id != s.opts.ConfigOptions.NodeId {
				peers = append(peers, id)
			}
		}
	}
	if len(peers) == 0 {
		if id, _, err := seedNode(s.opts.Seed); err == nil && id != 0 && id != s.opts.ConfigOptions.NodeId {
			peers = append(peers, id)
		}
	}
	return peers
}

// 是否需要加入集群
func (s *Server) needJoin() (bool, error) {
	if strings.TrimSpace(s.opts.Seed) == "" {
		return false, nil
	}
	seedNodeId, _, err := seedNode(s.opts.Seed)
	if err != nil {
		return false, err
	}
	if s.freshSeededStart {
		return true, nil
	}
	seedNode := s.cfgServer.Node(seedNodeId)
	return seedNode == nil, nil
}

func seedNode(seed string) (uint64, string, error) {
	seedArray := strings.Split(seed, "@")
	if len(seedArray) < 2 {
		return 0, "", errors.New("seed format error")
	}
	seedNodeIDStr := seedArray[0]
	seedAddr := seedArray[1]
	seedNodeID, err := strconv.ParseUint(seedNodeIDStr, 10, 64)
	if err != nil {
		return 0, "", err
	}
	return seedNodeID, seedAddr, nil
}
