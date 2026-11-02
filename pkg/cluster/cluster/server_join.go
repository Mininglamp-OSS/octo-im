package cluster

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func (s *Server) joinLoop() {
	seedNodeId, _, _ := seedNode(s.opts.Seed)
	req := &ClusterJoinReq{
		NodeId:     s.opts.ConfigOptions.NodeId,
		ServerAddr: s.opts.ServerAddr,
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
			if resp.Config != nil {
				if err := s.cfgServer.ResumeJoin(resp.Config, resp.LeaderID, resp.JoinVersion); err != nil {
					continue
				}
			}
			if len(resp.Nodes) > 0 && s.cfgServer.Snapshot().Version < resp.JoinVersion {
				nodeMap := make(map[uint64]string)
				for _, n := range resp.Nodes {
					nodeMap[n.NodeId] = n.ServerAddr
				}
				s.addOrUpdateNodes(nodeMap)
			}
			// The response may race ahead of or behind configuration replay. The
			// slot server waits for Joined after this exact fresh command in either case.
			s.slotServer.SetRecoveryJoinVersion(resp.JoinVersion)
			s.joinReady.Store(true)
			return
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

// A persisted bootstrap node can have been removed while it was down. Probe
// through the existing join RPC so it can reconnect without a new seed/config
// or an operator choosing a replacement ID. Existing membership is a no-op.
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
			var peers []uint64
			for _, node := range cfg.Nodes {
				if node.Id != s.opts.ConfigOptions.NodeId {
					peers = append(peers, node.Id)
				}
			}
			if len(peers) == 0 {
				// A retired bootstrap node may have lost its local config too.
				// Its original peer addresses can still find the current leader.
				for id := range s.opts.ConfigOptions.InitNodes {
					if id != s.opts.ConfigOptions.NodeId {
						peers = append(peers, id)
					}
				}
			}
			if len(peers) == 0 {
				continue
			}
			peer := peers[cursor%len(peers)]
			cursor++
			resp, err := s.rpcClient.RequestClusterJoin(peer, &ClusterJoinReq{
				NodeId: s.opts.ConfigOptions.NodeId, ServerAddr: s.opts.ServerAddr, Role: s.opts.Role, Probe: true,
			})
			if err != nil {
				continue
			}
			boundary := resp.JoinVersion
			if boundary == 0 && resp.Config != nil {
				boundary = resp.Config.Version
			}
			if boundary == 0 || s.cfgServer.Snapshot().Version >= boundary {
				continue
			}
			if resp.Config != nil {
				if err := s.cfgServer.ResumeJoin(resp.Config, resp.LeaderID, boundary); err != nil {
					continue
				}
			}
			nodes := make(map[uint64]string, len(resp.Nodes))
			for _, node := range resp.Nodes {
				nodes[node.NodeId] = node.ServerAddr
			}
			s.addOrUpdateNodes(nodes)
		}
	}
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
