package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/sendgrid/rest"
	"go.uber.org/zap"
)

// RPCClient 定义RPC客户端接口，用于跨节点权限检查
type RPCClient interface {
	RequestAllowSendForPerson(toNodeId uint64, from, to string) (*proto.Response, error)
}

// PermissionService 权限服务，提供统一的权限检查功能
type PermissionService struct {
	wklog.Log
	rpcClient     RPCClient // RPC客户端，用于跨节点权限检查
	boundNodeId   uint64
	boundCluster  icluster.ICluster
	boundStore    *store.Store
	peerMu        sync.RWMutex
	legacyPeers   map[uint64]time.Time
	upgradedPeers map[uint64]time.Time
}

// NewPermissionService 创建权限服务实例
func NewPermissionService(client RPCClient) *PermissionService {
	return &PermissionService{
		Log:           wklog.NewWKLog("PermissionService"),
		rpcClient:     client,
		legacyPeers:   make(map[uint64]time.Time),
		upgradedPeers: make(map[uint64]time.Time),
	}
}

func (p *PermissionService) BindDeps(nodeId uint64, cl icluster.ICluster, st *store.Store) {
	if p == nil {
		return
	}
	p.boundNodeId = nodeId
	p.boundCluster = cl
	p.boundStore = st
}

func (p *PermissionService) localNodeId() uint64 {
	if p != nil && p.boundNodeId != 0 {
		return p.boundNodeId
	}
	if options.G != nil {
		return options.G.Cluster.NodeId
	}
	return 0
}

func (p *PermissionService) isLocalNode(nodeId uint64) bool {
	if p != nil && p.boundNodeId != 0 {
		return p.boundNodeId == nodeId
	}
	return options.G != nil && options.G.IsLocalNode(nodeId)
}

func (p *PermissionService) cluster() icluster.ICluster {
	if p != nil && p.boundCluster != nil {
		return p.boundCluster
	}
	return Cluster
}

func (p *PermissionService) store() *store.Store {
	if p != nil && p.boundStore != nil {
		return p.boundStore
	}
	return Store
}

func normalizePermissionChannelId(channelId string) string {
	if options.G != nil && options.G.IsCmdChannel(channelId) {
		return options.G.CmdChannelConvertOrginalChannel(channelId)
	}
	return channelId
}

func (p *PermissionService) isLocalNodeSlotReplica(channelId string, channelType uint8) bool {
	cl := p.cluster()
	if cl == nil {
		return false
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)
	var (
		slotId   uint32
		slots    []*types.Slot
		panicked bool
	)
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		slotId = cl.GetSlotId(realFakeChannelId)
		slots = cl.Slots()
	}()
	if panicked || len(slots) == 0 {
		return false
	}
	localId := p.localNodeId()
	if localId == 0 {
		return false
	}
	for _, st := range slots {
		if st != nil && st.Id == slotId {
			if len(st.Replicas) == 0 {
				return st.Leader != 0 && st.Leader == localId
			}
			return wkutil.ArrayContainsUint64(st.Replicas, localId)
		}
	}
	return false
}

func (p *PermissionService) peerNodeInfo(nodeId uint64, channelId string, channelType uint8) *types.Node {
	cl := p.cluster()
	if cl == nil || nodeId == 0 {
		return nil
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)
	var node *types.Node
	func() {
		defer func() { _ = recover() }()
		node = cl.NodeInfoById(nodeId)
	}()
	if node != nil && node.Id == nodeId && strings.TrimSpace(node.ApiServerAddr) != "" {
		return node
	}
	func() {
		defer func() { _ = recover() }()
		if ln, err := cl.SlotLeaderOfChannel(realFakeChannelId, channelType); err == nil && ln != nil && ln.Id == nodeId {
			node = ln
		}
	}()
	return node
}

func (p *PermissionService) requestLegacyPeerJSON(ctx context.Context, leaderNode *types.Node, fullURL string, queryParams map[string]string) (*rest.Response, error) {
	cl := p.cluster()
	if !IsTrustedClusterPeerURLOnCluster(cl, fullURL) || (leaderNode != nil && !IsTrustedClusterPeerOnCluster(cl, leaderNode)) {
		return nil, fmt.Errorf("refusing legacy permission request to untrusted peer URL %s", fullURL)
	}
	headers := TrustedPeerURLHeadersOnCluster(cl, fullURL, nil)
	return SendPeerRequestWithContext(ctx, rest.Request{
		Method:      rest.Get,
		BaseURL:     fullURL,
		QueryParams: queryParams,
		Headers:     headers,
	})
}

func (p *PermissionService) requestChannelPermissionFromLegacyHTTP(leaderNode *types.Node, channelId string, channelType uint8) (wkproto.ReasonCode, bool, error) {
	if leaderNode == nil || strings.TrimSpace(leaderNode.ApiServerAddr) == "" {
		return wkproto.ReasonSuccess, false, nil
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)
	baseAddr := strings.TrimRight(strings.TrimSpace(leaderNode.ApiServerAddr), "/")
	fullURL := fmt.Sprintf("%s/cluster/channels", baseAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	resp, err := p.requestLegacyPeerJSON(ctx, leaderNode, fullURL, map[string]string{
		"node_id":      fmt.Sprintf("%d", leaderNode.Id),
		"channel_id":   realFakeChannelId,
		"channel_type": fmt.Sprintf("%d", channelType),
	})
	if err != nil {
		return wkproto.ReasonSystemError, true, err
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		return wkproto.ReasonSystemError, true, fmt.Errorf("legacy channel permission HTTP status %d", statusCode)
	}
	var payload struct {
		Data []struct {
			ChannelId   string `json:"channel_id"`
			ChannelType uint8  `json:"channel_type"`
			Ban         int    `json:"ban"`
			Disband     int    `json:"disband"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(resp.Body), &payload); err != nil {
		return wkproto.ReasonSystemError, true, err
	}
	for _, item := range payload.Data {
		if item.ChannelId == realFakeChannelId && item.ChannelType == channelType {
			if item.Ban != 0 {
				return wkproto.ReasonBan, true, nil
			}
			if item.Disband != 0 {
				return wkproto.ReasonDisband, true, nil
			}
		}
	}
	return wkproto.ReasonSuccess, true, nil
}

func (p *PermissionService) requestCommPermissionFromLegacyHTTP(leaderNode *types.Node, realFakeChannelId string, channelType uint8, fromUid string) (wkproto.ReasonCode, bool, error) {
	if leaderNode == nil || strings.TrimSpace(leaderNode.ApiServerAddr) == "" {
		return wkproto.ReasonSuccess, false, nil
	}
	realFakeChannelId = normalizePermissionChannelId(realFakeChannelId)
	baseAddr := strings.TrimRight(strings.TrimSpace(leaderNode.ApiServerAddr), "/")
	escapedCh := url.PathEscape(realFakeChannelId)

	denyURL := fmt.Sprintf("%s/cluster/channels/%s/%d/denylist", baseAddr, escapedCh, channelType)
	denyCtx, denyCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	denyResp, denyErr := p.requestLegacyPeerJSON(denyCtx, leaderNode, denyURL, nil)
	denyCancel()
	if denyErr != nil {
		return wkproto.ReasonSystemError, true, denyErr
	}
	if denyResp == nil || denyResp.StatusCode != http.StatusOK {
		statusCode := 0
		if denyResp != nil {
			statusCode = denyResp.StatusCode
		}
		return wkproto.ReasonSystemError, true, fmt.Errorf("legacy denylist HTTP status %d", statusCode)
	}
	var denyUids []string
	if err := json.Unmarshal([]byte(denyResp.Body), &denyUids); err != nil {
		return wkproto.ReasonSystemError, true, err
	}
	for _, uid := range denyUids {
		if uid == fromUid {
			return wkproto.ReasonInBlacklist, true, nil
		}
	}

	if channelType != wkproto.ChannelTypePerson {
		allowURL := fmt.Sprintf("%s/cluster/channels/%s/%d/allowlist", baseAddr, escapedCh, channelType)
		allowCtx, allowCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		allowResp, allowErr := p.requestLegacyPeerJSON(allowCtx, leaderNode, allowURL, nil)
		allowCancel()
		if allowErr != nil {
			return wkproto.ReasonSystemError, true, allowErr
		}
		if allowResp == nil || allowResp.StatusCode != http.StatusOK {
			statusCode := 0
			if allowResp != nil {
				statusCode = allowResp.StatusCode
			}
			return wkproto.ReasonSystemError, true, fmt.Errorf("legacy allowlist HTTP status %d", statusCode)
		}
		var allowUids []string
		if err := json.Unmarshal([]byte(allowResp.Body), &allowUids); err != nil {
			return wkproto.ReasonSystemError, true, err
		}
		if len(allowUids) > 0 {
			inAllowlist := false
			for _, uid := range allowUids {
				if uid == fromUid {
					inAllowlist = true
					break
				}
			}
			if !inAllowlist {
				return wkproto.ReasonNotInWhitelist, true, nil
			}
		}
	}

	return wkproto.ReasonSuccess, true, nil
}

func (p *PermissionService) markLegacyPeer(nodeId uint64) {
	if p == nil || nodeId == 0 {
		return
	}
	p.peerMu.Lock()
	if p.legacyPeers == nil {
		p.legacyPeers = make(map[uint64]time.Time)
	}
	p.legacyPeers[nodeId] = time.Now().Add(5 * time.Second)
	delete(p.upgradedPeers, nodeId)
	p.peerMu.Unlock()
}

func (p *PermissionService) markUpgradedPeer(nodeId uint64) {
	if p == nil || nodeId == 0 {
		return
	}
	p.peerMu.Lock()
	if p.upgradedPeers == nil {
		p.upgradedPeers = make(map[uint64]time.Time)
	}
	p.upgradedPeers[nodeId] = time.Now().Add(5 * time.Second)
	delete(p.legacyPeers, nodeId)
	p.peerMu.Unlock()
}

func (p *PermissionService) IsLegacyPeer(nodeId uint64) bool {
	if p == nil || nodeId == 0 {
		return false
	}
	now := time.Now()
	p.peerMu.RLock()
	exp, ok := p.legacyPeers[nodeId]
	p.peerMu.RUnlock()
	return ok && now.Before(exp)
}

func (p *PermissionService) isUpgradedPeer(nodeId uint64) bool {
	if p == nil || nodeId == 0 {
		return false
	}
	now := time.Now()
	p.peerMu.RLock()
	exp, ok := p.upgradedPeers[nodeId]
	p.peerMu.RUnlock()
	return ok && now.Before(exp)
}

func isLegacyAllowSendDecodeError(body []byte) bool {
	msg := string(body)
	return strings.Contains(msg, "size is less than 0") ||
		strings.Contains(msg, "Decoder couldn't read expect bytes") ||
		strings.Contains(msg, "EOF")
}

// CheckOrProbeLegacyPeer returns true when nodeId is a legacy 21cd8f88 node that does not
// implement the extended channel permission / pre-persisted event protocol.
func (p *PermissionService) CheckOrProbeLegacyPeer(nodeId uint64, channelId string, channelType uint8) bool {
	cl := p.cluster()
	if p == nil || nodeId == 0 || cl == nil {
		return false
	}
	if p.IsLegacyPeer(nodeId) {
		return true
	}
	if p.isUpgradedPeer(nodeId) {
		return false
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteBytes([]byte{0xff, 0xff, 1})
	enc.WriteString(realFakeChannelId)
	enc.WriteUint8(channelType)

	probeCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	resp, err := cl.RequestWithContext(probeCtx, nodeId, "/wk/ingress/allowSend", enc.Bytes())
	cancel()
	if err == nil && resp != nil {
		if resp.Status == proto.StatusError && isLegacyAllowSendDecodeError(resp.Body) {
			p.markLegacyPeer(nodeId)
			return true
		}
		if resp.Status == proto.StatusOK && len(resp.Body) == 2 && resp.Body[0] == 1 {
			p.markUpgradedPeer(nodeId)
			return false
		}
	}
	return false
}

// SenderInfo 发送者信息，用于权限检查
type SenderInfo struct {
	UID      string // 发送者UID
	DeviceID string // 设备ID
}

// IsPublicChannelType 检查是否为公开频道类型，这些频道类型允许直接通过权限检查
// 返回 true 表示是公开频道，可以直接通过；false 表示需要进一步权限检查
func (p *PermissionService) IsPublicChannelType(channelType uint8) bool {
	// 资讯频道是公开的，直接通过
	if channelType == wkproto.ChannelTypeInfo {
		return true
	}
	// 客服频道，直接通过
	if channelType == wkproto.ChannelTypeCustomerService ||
		channelType == wkproto.ChannelTypeVisitors ||
		channelType == wkproto.ChannelTypeAgent {
		return true
	}
	return false
}

// HasPermissionForChannel 检查频道级别的权限
// 主要检查频道是否被封禁或解散（集群模式下向频道所属槽领导节点查询权威状态）
func (p *PermissionService) HasPermissionForChannel(channelId string, channelType uint8) (wkproto.ReasonCode, error) {
	// 检查是否为公开频道类型
	if p.IsPublicChannelType(channelType) {
		return wkproto.ReasonSuccess, nil
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)

	cl := p.cluster()
	if options.G != nil && options.G.ClusterOn() && cl != nil {
		var (
			leaderNode *types.Node
			slotErr    error
			panicked   bool
		)
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			leaderNode, slotErr = cl.SlotLeaderOfChannel(realFakeChannelId, channelType)
		}()
		if panicked {
			return wkproto.ReasonSystemError, errors.New("slot leader lookup panicked")
		}
		if slotErr != nil {
			p.Error("HasPermissionForChannel: SlotLeaderOfChannel error", zap.Error(slotErr), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType))
			return wkproto.ReasonSystemError, slotErr
		}
		if leaderNode == nil || leaderNode.Id == 0 {
			return wkproto.ReasonSystemError, errors.New("slot leader not found")
		}
		if !p.isLocalNode(leaderNode.Id) {
			return p.requestChannelPermissionFromNode(leaderNode.Id, realFakeChannelId, channelType)
		}
	}

	return p.HasPermissionForChannelLocal(realFakeChannelId, channelType)
}

// HasPermissionForChannelLocal 检查本节点上的频道级别权限（仅在槽领导节点或单机模式下直接读取本地存储）
func (p *PermissionService) HasPermissionForChannelLocal(channelId string, channelType uint8) (wkproto.ReasonCode, error) {
	if p.IsPublicChannelType(channelType) {
		return wkproto.ReasonSuccess, nil
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)

	// 查询频道基本信息
	channelInfo, err := p.store().GetChannel(realFakeChannelId, channelType)
	if err != nil {
		p.Error("HasPermissionForChannel: GetChannel error", zap.Error(err))
		return wkproto.ReasonSystemError, err
	}

	// 频道被封禁
	if channelInfo.Ban {
		return wkproto.ReasonBan, nil
	}
	// 频道已解散
	if channelInfo.Disband {
		return wkproto.ReasonDisband, nil
	}
	return wkproto.ReasonSuccess, nil
}

func (p *PermissionService) requestChannelPermissionFromNode(toNodeId uint64, channelId string, channelType uint8) (wkproto.ReasonCode, error) {
	cl := p.cluster()
	if cl == nil {
		return wkproto.ReasonSystemError, errors.New("cluster not initialized")
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)

	if !p.IsLegacyPeer(toNodeId) {
		probeEnc := wkproto.NewEncoder()
		defer probeEnc.End()
		probeEnc.WriteBytes([]byte{0xff, 0xff, 1})
		probeEnc.WriteString(realFakeChannelId)
		probeEnc.WriteUint8(channelType)

		probeCtx, probeCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		probeResp, probeErr := cl.RequestWithContext(probeCtx, toNodeId, "/wk/ingress/allowSend", probeEnc.Bytes())
		probeCancel()
		if probeErr != nil {
			p.Error("HasPermissionForChannel probe RPC failed", zap.Error(probeErr), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType), zap.Uint64("nodeId", toNodeId))
			return wkproto.ReasonSystemError, probeErr
		}
		if probeResp == nil {
			return wkproto.ReasonSystemError, errors.New("empty permission probe response")
		}
		if probeResp.Status == proto.StatusOK && len(probeResp.Body) == 2 && probeResp.Body[0] == 1 {
			p.markUpgradedPeer(toNodeId)
			return wkproto.ReasonCode(probeResp.Body[1]), nil
		}
		if probeResp.Status == proto.StatusError && isLegacyAllowSendDecodeError(probeResp.Body) {
			p.markLegacyPeer(toNodeId)
		} else {
			enc := wkproto.NewEncoder()
			defer enc.End()
			enc.WriteString(realFakeChannelId)
			enc.WriteUint8(channelType)

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			resp, err := cl.RequestWithContext(timeoutCtx, toNodeId, "/wk/ingress/hasPermissionForChannel", enc.Bytes())
			if err != nil {
				p.Error("HasPermissionForChannel RPC call failed", zap.Error(err), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType), zap.Uint64("nodeId", toNodeId))
				return wkproto.ReasonSystemError, err
			}
			if resp == nil {
				return wkproto.ReasonSystemError, errors.New("empty permission response")
			}
			p.markUpgradedPeer(toNodeId)
			if resp.Status == proto.StatusOK {
				return wkproto.ReasonSuccess, nil
			}
			if resp.Status == proto.StatusError {
				return wkproto.ReasonSystemError, errors.New(string(resp.Body))
			}
			return wkproto.ReasonCode(resp.Status), nil
		}
	}

	// Legacy 21cd8f88 peer: query authoritative :5001 /cluster/channels on the legacy slot leader if ApiServerAddr is configured.
	leaderNode := p.peerNodeInfo(toNodeId, realFakeChannelId, channelType)
	if rc, handled, err := p.requestChannelPermissionFromLegacyHTTP(leaderNode, realFakeChannelId, channelType); handled {
		return rc, err
	}
	if !p.isLocalNodeSlotReplica(realFakeChannelId, channelType) {
		return wkproto.ReasonSystemError, errors.New("legacy slot leader channel permission unavailable from non-replica node")
	}
	return p.HasPermissionForChannelLocal(realFakeChannelId, channelType)
}

// HasPermissionForSender 检查发送者权限
// 检查发送者是否有权限向指定频道发送消息
func (p *PermissionService) HasPermissionForSender(channelId string, channelType uint8, sender SenderInfo) (wkproto.ReasonCode, error) {
	realFakeChannelId := normalizePermissionChannelId(channelId)
	// 检查是否为公开频道类型的特殊处理
	if channelType == wkproto.ChannelTypeInfo {
		return wkproto.ReasonSuccess, nil
	}
	if channelType == wkproto.ChannelTypeCustomerService {
		return wkproto.ReasonSuccess, nil
	}

	// Agent频道特殊处理
	if channelType == wkproto.ChannelTypeAgent {
		uid, agentUID := options.GetUidAndAgentUIDWith(realFakeChannelId)
		if sender.UID == uid || sender.UID == agentUID {
			return wkproto.ReasonSuccess, nil
		}
	}

	// 访客频道只有自己和客服可以发消息
	if channelType == wkproto.ChannelTypeVisitors {
		// 因为访客频道的id就是访客的uid，所以这里发送者是自己直接通过
		// 否则需要走普通频道的流程：判断是否是订阅者，白名单，黑名单这些
		if sender.UID == realFakeChannelId {
			return wkproto.ReasonSuccess, nil
		}
	}

	// 系统发的消息直接通过
	if options.G != nil && options.G.IsSystemDevice(sender.DeviceID) {
		return wkproto.ReasonSuccess, nil
	}

	// 系统账号，直接通过
	if SystemAccountManager != nil && SystemAccountManager.IsSystemAccount(sender.UID) {
		return wkproto.ReasonSuccess, nil
	}

	// 个人频道,需要判断接收者是否允许
	if channelType == wkproto.ChannelTypePerson {
		return p.hasPermissionForPerson(realFakeChannelId, channelType, sender)
	}

	return p.hasPermissionForCommChannel(realFakeChannelId, channelType, sender)
}

// hasPermissionForCommChannel 通用频道权限判断
func (p *PermissionService) hasPermissionForCommChannel(channelId string, channelType uint8, sender SenderInfo) (wkproto.ReasonCode, error) {
	realFakeChannelId := normalizePermissionChannelId(channelId)

	cl := p.cluster()
	if options.G != nil && options.G.ClusterOn() && cl != nil {
		var (
			leaderNode *types.Node
			slotErr    error
			panicked   bool
		)
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			leaderNode, slotErr = cl.SlotLeaderOfChannel(realFakeChannelId, channelType)
		}()
		if panicked {
			return wkproto.ReasonSystemError, errors.New("slot leader lookup panicked")
		}
		if slotErr != nil {
			p.Error("hasPermissionForCommChannel: SlotLeaderOfChannel error", zap.Error(slotErr), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType))
			return wkproto.ReasonSystemError, slotErr
		}
		if leaderNode == nil || leaderNode.Id == 0 {
			return wkproto.ReasonSystemError, errors.New("slot leader not found")
		}
		if !p.isLocalNode(leaderNode.Id) {
			return p.requestCommChannelPermissionFromNode(leaderNode.Id, realFakeChannelId, channelType, sender)
		}
	}

	return p.HasPermissionForCommChannelLocal(realFakeChannelId, channelType, sender)
}

func (p *PermissionService) requestCommChannelPermissionFromNode(toNodeId uint64, channelId string, channelType uint8, sender SenderInfo) (wkproto.ReasonCode, error) {
	cl := p.cluster()
	if cl == nil {
		return wkproto.ReasonSystemError, errors.New("cluster not initialized")
	}
	realFakeChannelId := normalizePermissionChannelId(channelId)

	if !p.IsLegacyPeer(toNodeId) {
		enc := wkproto.NewEncoder()
		defer enc.End()
		enc.WriteBytes([]byte{0xff, 0xff, 2})
		enc.WriteString(realFakeChannelId)
		enc.WriteUint8(channelType)
		enc.WriteString(sender.UID)
		enc.WriteString(sender.DeviceID)

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		resp, err := cl.RequestWithContext(timeoutCtx, toNodeId, "/wk/ingress/allowSend", enc.Bytes())
		cancel()
		if err != nil {
			p.Error("hasPermissionForCommChannel RPC call failed", zap.Error(err), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType), zap.Uint64("nodeId", toNodeId))
			return wkproto.ReasonSystemError, err
		}
		if resp == nil {
			return wkproto.ReasonSystemError, errors.New("empty comm channel permission response")
		}
		if resp.Status == proto.StatusOK && len(resp.Body) == 2 && resp.Body[0] == 1 {
			p.markUpgradedPeer(toNodeId)
			return wkproto.ReasonCode(resp.Body[1]), nil
		}
		if resp.Status == proto.StatusError && isLegacyAllowSendDecodeError(resp.Body) {
			p.markLegacyPeer(toNodeId)
		} else if resp.Status == proto.StatusError {
			return wkproto.ReasonSystemError, errors.New(string(resp.Body))
		} else {
			return wkproto.ReasonCode(resp.Status), nil
		}
	}

	// Legacy 21cd8f88 peer:
	// 1. Authoritatively verify subscriber membership via /wk/ingress/getSubscribers on the slot leader (fail-closed on RPC error or non-member).
	fromUid := sender.UID

	subEnc := wkproto.NewEncoder()
	defer subEnc.End()
	subEnc.WriteString(realFakeChannelId)
	subEnc.WriteUint8(channelType)
	subCtx, subCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	subResp, subErr := cl.RequestWithContext(subCtx, toNodeId, "/wk/ingress/getSubscribers", subEnc.Bytes())
	subCancel()
	if subErr != nil {
		p.Error("legacy getSubscribers RPC call failed", zap.Error(subErr), zap.String("channelId", realFakeChannelId), zap.Uint8("channelType", channelType), zap.Uint64("nodeId", toNodeId))
		return wkproto.ReasonSystemError, subErr
	}
	if subResp == nil || subResp.Status != proto.StatusOK || len(subResp.Body) < 4 {
		return wkproto.ReasonSystemError, errors.New("legacy getSubscribers returned invalid response")
	}
	dec := wkproto.NewDecoder(subResp.Body)
	count, decErr := dec.Uint32()
	if decErr != nil {
		return wkproto.ReasonSystemError, decErr
	}
	found := false
	for i := 0; i < int(count); i++ {
		uid, uErr := dec.String()
		if uErr != nil {
			return wkproto.ReasonSystemError, uErr
		}
		if uid == fromUid {
			found = true
			break
		}
	}
	if !found {
		return wkproto.ReasonSubscriberNotExist, nil
	}

	// 2. Authoritatively verify denylist and allowlist on the legacy slot leader via :5001 /cluster/channels/:id/:type/{denylist,allowlist}.
	leaderNode := p.peerNodeInfo(toNodeId, realFakeChannelId, channelType)
	if rc, handled, err := p.requestCommPermissionFromLegacyHTTP(leaderNode, realFakeChannelId, channelType, fromUid); handled {
		return rc, err
	}
	if !p.isLocalNodeSlotReplica(realFakeChannelId, channelType) {
		return wkproto.ReasonSystemError, errors.New("legacy slot leader denylist/allowlist unavailable from non-replica node")
	}

	isDenylist, err := p.store().ExistDenylist(realFakeChannelId, channelType, fromUid)
	if err != nil {
		return wkproto.ReasonSystemError, err
	}
	if isDenylist {
		return wkproto.ReasonInBlacklist, nil
	}

	if channelType != wkproto.ChannelTypePerson {
		hasAllowlist, aErr := p.store().HasAllowlist(realFakeChannelId, channelType)
		if aErr != nil {
			return wkproto.ReasonSystemError, aErr
		}
		if hasAllowlist {
			isAllowlist, eErr := p.store().ExistAllowlist(realFakeChannelId, channelType, fromUid)
			if eErr != nil {
				return wkproto.ReasonSystemError, eErr
			}
			if !isAllowlist {
				return wkproto.ReasonNotInWhitelist, nil
			}
		}
	}
	return wkproto.ReasonSuccess, nil
}

// HasPermissionForCommChannelLocal 检查本节点上的通用频道发送者权限
func (p *PermissionService) HasPermissionForCommChannelLocal(channelId string, channelType uint8, sender SenderInfo) (wkproto.ReasonCode, error) {
	realFakeChannelId := normalizePermissionChannelId(channelId)
	fromUid := sender.UID

	st := p.store()
	// 判断是否是黑名单内
	isDenylist, err := st.ExistDenylist(realFakeChannelId, channelType, fromUid)
	if err != nil {
		p.Error("ExistDenylist error", zap.Error(err))
		return wkproto.ReasonSystemError, err
	}
	if isDenylist {
		return wkproto.ReasonInBlacklist, nil
	}

	// 判断是否是订阅者
	isSubscriber, err := st.ExistSubscriber(realFakeChannelId, channelType, fromUid)
	if err != nil {
		p.Error("ExistSubscriber error", zap.Error(err))
		return wkproto.ReasonSystemError, err
	}
	if !isSubscriber {
		return wkproto.ReasonSubscriberNotExist, nil
	}

	if channelType != wkproto.ChannelTypePerson {
		hasAllowlist, err := st.HasAllowlist(realFakeChannelId, channelType)
		if err != nil {
			p.Error("HasAllowlist error", zap.Error(err))
			return wkproto.ReasonSystemError, err
		}

		if hasAllowlist { // 如果频道有白名单，则判断是否在白名单内
			isAllowlist, err := st.ExistAllowlist(realFakeChannelId, channelType, fromUid)
			if err != nil {
				p.Error("ExistAllowlist error", zap.Error(err))
				return wkproto.ReasonSystemError, err
			}
			if !isAllowlist {
				return wkproto.ReasonNotInWhitelist, nil
			}
		}
	}
	return wkproto.ReasonSuccess, nil
}

// hasPermissionForPerson 个人频道权限判断
func (p *PermissionService) hasPermissionForPerson(channelId string, _ uint8, sender SenderInfo) (wkproto.ReasonCode, error) {
	realFakeChannel := normalizePermissionChannelId(channelId)
	fromUid := sender.UID

	uid1, uid2 := options.GetFromUIDAndToUIDWith(realFakeChannel)
	toUid := ""
	if uid1 == fromUid {
		toUid = uid2
	} else {
		toUid = uid1
	}

	// 如果接收者是系统账号，则直接通过
	if SystemAccountManager != nil && SystemAccountManager.IsSystemAccount(toUid) {
		return wkproto.ReasonSuccess, nil
	}

	// 请求个人频道是否允许发送
	reasonCode, err := p.requestAllowSend(fromUid, toUid)
	if err != nil {
		return wkproto.ReasonSystemError, err
	}
	return reasonCode, nil
}

// HasPermissionForPersonLocal 检查个人频道权限（仅本地检查，适用于API层）
// 这个方法只进行本地权限检查，不涉及跨节点RPC调用
func (p *PermissionService) HasPermissionForPersonLocal(from, to string) (wkproto.ReasonCode, error) {
	return p.allowSend(from, to)
}

// requestAllowSend 请求是否允许发送个人消息（支持跨节点RPC调用）
func (p *PermissionService) requestAllowSend(from, to string) (wkproto.ReasonCode, error) {
	leaderNode, err := p.cluster().SlotLeaderOfChannel(to, wkproto.ChannelTypePerson)
	if err != nil {
		return wkproto.ReasonSystemError, err
	}
	if p.isLocalNode(leaderNode.Id) {
		return p.allowSend(from, to)
	}

	// 对于非本地节点，使用RPC客户端进行跨节点调用
	resp, err := p.rpcClient.RequestAllowSendForPerson(leaderNode.Id, from, to)
	if err != nil {
		p.Error("RequestAllowSendForPerson RPC call failed", zap.Error(err), zap.String("from", from), zap.String("to", to), zap.Uint64("nodeId", leaderNode.Id))
		return wkproto.ReasonSystemError, err
	}
	if resp.Status == proto.StatusOK {
		return wkproto.ReasonSuccess, nil
	}
	if resp.Status == proto.StatusError {
		return wkproto.ReasonSystemError, errors.New(string(resp.Body))
	}
	return wkproto.ReasonCode(resp.Status), nil
}

// allowSend 检查是否允许发送个人消息（本地检查）
func (p *PermissionService) allowSend(from, to string) (wkproto.ReasonCode, error) {
	st := p.store()
	// 判断是否是黑名单内
	isDenylist, err := st.ExistDenylist(to, wkproto.ChannelTypePerson, from)
	if err != nil {
		p.Error("ExistDenylist error", zap.String("from", from), zap.String("to", to), zap.Error(err))
		return wkproto.ReasonSystemError, err
	}
	if isDenylist {
		return wkproto.ReasonInBlacklist, nil
	}

	if !options.G.WhitelistOffOfPerson {
		// 判断是否在白名单内
		isAllowlist, err := st.ExistAllowlist(to, wkproto.ChannelTypePerson, from)
		if err != nil {
			p.Error("ExistAllowlist error", zap.Error(err))
			return wkproto.ReasonSystemError, err
		}
		if !isAllowlist {
			return wkproto.ReasonNotInWhitelist, nil
		}
	}

	return wkproto.ReasonSuccess, nil
}

// AllowSendForPersonLocal 本地检查是否允许个人消息发送（替代原有的AllowSendForPerson）
func (p *PermissionService) AllowSendForPersonLocal(from, to string) (wkproto.ReasonCode, error) {
	return p.allowSend(from, to)
}

// 全局权限服务实例
var Permission *PermissionService
