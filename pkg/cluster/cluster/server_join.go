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
			if len(resp.Nodes) > 0 {
				nodeMap := make(map[uint64]string)
				for _, n := range resp.Nodes {
					nodeMap[n.NodeId] = n.ServerAddr
				}
				s.addOrUpdateNodes(nodeMap)
			}
			// The response may race ahead of or behind configuration replay. The
			// slot server waits for Joined after this exact fresh command in either case.
			s.slotServer.SetRecoveryJoinVersion(resp.JoinVersion)
			return
		case <-s.stopper.ShouldStop():
			return
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
