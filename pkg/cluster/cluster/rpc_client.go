package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type rpcClient struct {
	s *Server
}

func newRpcClient(s *Server) *rpcClient {
	return &rpcClient{
		s: s,
	}
}

// RequestChannelProposeBatchUntilApplied 向指定节点请求频道提案
func (r *rpcClient) RequestChannelProposeBatchUntilApplied(ctx context.Context, nodeId uint64, channelId string, channelType uint8, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	if r.s != nil && nodeId != 0 && nodeId != r.s.opts.ConfigOptions.NodeId && !r.s.isPeerRPCAuthed(nodeId) {
		waitEnd := time.Now().Add(90 * time.Millisecond)
		for !r.s.isPeerRPCAuthed(nodeId) && time.Now().Before(waitEnd) && ctx.Err() == nil {
			time.Sleep(30 * time.Millisecond)
		}
		if !r.s.isPeerRPCAuthed(nodeId) {
			mCtx, mCancel := context.WithTimeout(ctx, 1200*time.Millisecond)
			r.s.MaintainExistingChannelLeader(mCtx, channelId, channelType, nodeId)
			mCancel()
			return nil, fmt.Errorf("%w: peer %d rpc not authed", channel.ErrSendUnavailable, nodeId)
		}
	}

	req := &channelProposeReq{
		channelId:   channelId,
		channelType: channelType,
		reqs:        reqs,
	}
	data, err := req.encode()
	if err != nil {
		return nil, err
	}
	resp, err := r.s.RequestWithContext(ctx, nodeId, "/rpc/channel/propose/v2", data)
	if err != nil {
		if isPreWireChannelRPCError(err) {
			if r.s != nil {
				mCtx, mCancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
				r.s.MaintainExistingChannelLeader(mCtx, channelId, channelType, nodeId)
				mCancel()
			}
			return nil, fmt.Errorf("%w: %v", channel.ErrSendUnavailable, err)
		}
		return nil, fmt.Errorf("%w: %w", channel.ErrSendUnavailable, err)
	}
	if err := channelProposalResponseError(resp); err != nil {
		if errors.Is(err, channel.ErrSendUnavailable) && r.s != nil {
			mCtx, mCancel := context.WithTimeout(ctx, 1200*time.Millisecond)
			r.s.MaintainExistingChannelLeader(mCtx, channelId, channelType, nodeId)
			mCancel()
		}
		return nil, err
	}
	body := resp.Body

	var response channelProposeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Version != 2 {
		return nil, fmt.Errorf("unsupported channel proposal response")
	}
	if err := channel.ValidateMessageResults(reqs, response.Results); err != nil {
		return nil, err
	}
	return response.Results, nil
}

func isPreWireChannelRPCError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connect not authed") ||
		strings.Contains(msg, "conn is nil") ||
		strings.Contains(msg, "connect is not connected") ||
		strings.Contains(msg, "not exist") ||
		strings.Contains(msg, "connection refused")
}

func isLegacyPreAdmissionUnavailableBody(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(string(raw)))
	if strings.Contains(msg, "outcome unknown") || strings.Contains(msg, "invalid channel message") || strings.Contains(msg, "content") {
		return false
	}
	return strings.Contains(msg, "stopped") ||
		strings.Contains(msg, "connect not authed") ||
		strings.Contains(msg, "conn is nil") ||
		strings.Contains(msg, "connect is not connected") ||
		strings.Contains(msg, "node not found") ||
		strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "leader changed") ||
		strings.Contains(msg, "not ready") ||
		strings.Contains(msg, "conf version") ||
		strings.Contains(msg, "term is lower")
}

func channelProposalResponseError(resp *proto.Response) error {
	if resp == nil || resp.Status == channelProposalOutcomeUnknown {
		return channel.ErrSendOutcomeUnknown
	}
	if resp.Status == channelProposalUnavailable {
		return channel.ErrSendUnavailable
	}
	if resp.Status != proto.StatusOK {
		if isLegacyPreAdmissionUnavailableBody(resp.Body) {
			return fmt.Errorf("%w: %s", channel.ErrSendUnavailable, strings.TrimSpace(string(resp.Body)))
		}
		return fmt.Errorf("channel proposal rejected")
	}
	return nil
}

// RequestSlotProposeBatchUntilApplied 向指定节点请求槽提案
func (r *rpcClient) RequestSlotProposeBatchUntilApplied(nodeId uint64, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	return r.RequestSlotProposeBatchUntilAppliedWithContext(context.Background(), nodeId, slotId, reqs)
}

// RequestSlotProposeBatchUntilAppliedWithContext 向指定节点请求槽提案（受调用方 ctx 截止时间约束）
func (r *rpcClient) RequestSlotProposeBatchUntilAppliedWithContext(ctx context.Context, nodeId uint64, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	req := &slotProposeReq{
		SlotId: slotId,
		reqs:   reqs,
	}
	data, err := req.encode()
	if err != nil {
		return nil, err
	}
	body, err := r.requestWithContext(ctx, nodeId, "/rpc/slot/propose", data)
	if err != nil {
		return nil, err
	}

	resps := types.ProposeRespSet{}
	if err := resps.Unmarshal(body); err != nil {
		return nil, err
	}
	return resps, nil
}

func (r *rpcClient) RequestGetOrCreateChannelClusterConfig(nodeId uint64, channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {

	req := &channelReq{
		channelId:   channelId,
		channelType: channelType,
	}
	data, err := req.encode()
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	body, err := r.requestWithContext(ctx, nodeId, "/rpc/channel/getOrCreateConfig", data)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}

	config := wkdb.ChannelClusterConfig{}
	if err := config.Unmarshal(body); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	return config, nil
}

func (r *rpcClient) RequestGetChannelClusterConfig(nodeId uint64, channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {

	req := &channelReq{
		channelId:   channelId,
		channelType: channelType,
	}
	data, err := req.encode()
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	body, err := r.request(nodeId, "/rpc/channel/getConfig", data)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}

	config := wkdb.ChannelClusterConfig{}
	if err := config.Unmarshal(body); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	return config, nil
}

func (r *rpcClient) RequestSlotLastLogInfo(nodeId uint64, req *SlotLogInfoReq) (*SlotLogInfoResp, error) {

	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	body, err := r.request(nodeId, "/rpc/slot/lastLogInfo", data)
	if err != nil {
		return nil, err
	}

	resp := &SlotLogInfoResp{}
	if err := resp.Unmarshal(body); err != nil {
		return nil, err
	}
	return resp, nil

}

// RequestWakeLeaderIfNeed 向指定节点请求唤醒领导
func (r *rpcClient) RequestWakeLeaderIfNeed(nodeId uint64, config wkdb.ChannelClusterConfig) error {

	data, err := config.Marshal()
	if err != nil {
		return err
	}
	_, err = r.request(nodeId, "/rpc/channel/wakeLeaderIfNeed", data)
	return err
}

// RequestChannelSwitchConfig 请求切换频道配置
func (r *rpcClient) RequestChannelSwitchConfig(nodeId uint64, config wkdb.ChannelClusterConfig) error {
	data, err := config.Marshal()
	if err != nil {
		return err
	}
	_, err = r.request(nodeId, "/rpc/channel/switchConfig", data)
	return err
}

// RequestChannelLastLogInfo 请求频道最后日志信息
func (r *rpcClient) RequestChannelLastLogInfo(ctx context.Context, nodeId uint64, channelId string, channelType uint8) (*ChannelLastLogInfoResponse, error) {
	req := &channelReq{
		channelId:   channelId,
		channelType: channelType,
	}
	data, err := req.encode()
	if err != nil {
		return nil, err
	}
	result, err := r.s.RequestWithContext(ctx, nodeId, "/rpc/channel/lastLogInfo", data)
	if err != nil {
		return nil, err
	}

	if result == nil || result.Status != proto.StatusOK {
		return nil, ErrConversationReadRetry
	}
	resp := &ChannelLastLogInfoResponse{}
	if err := resp.Unmarshal(result.Body); err != nil {
		return nil, err
	}
	return resp, nil
}

// 节点请求加入
func (r *rpcClient) RequestClusterJoin(nodeId uint64, req *ClusterJoinReq) (*ClusterJoinResp, error) {
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	body, err := r.request(nodeId, "/rpc/cluster/join", data)
	if err != nil {
		return nil, err
	}
	resp := &ClusterJoinResp{}
	if err := resp.Unmarshal(body); err != nil {
		return nil, err
	}
	return resp, nil
}

// RequestClusterLogs 请求分布式日志
func (r *rpcClient) RequestClusterLogs(nodeId uint64, req *clusterLogsReq) ([]rafttypes.Log, error) {
	data, err := req.encode()
	if err != nil {
		return nil, err
	}
	body, err := r.request(nodeId, "/rpc/cluster/logs", data)
	if err != nil {
		return nil, err
	}
	logSet := logSet{}
	if err := logSet.decode(body); err != nil {
		return nil, err
	}
	return logSet, nil
}

func (r *rpcClient) request(nodeId uint64, path string, body []byte) ([]byte, error) {
	return r.requestWithContext(context.Background(), nodeId, path, body)
}

func (r *rpcClient) requestWithContext(ctx context.Context, nodeId uint64, path string, body []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	node := r.s.nodeManager.node(nodeId)
	if node == nil {
		return nil, fmt.Errorf("node[%d] not exist", nodeId)
	}

	timeout := time.Second * 5
	if r.s != nil && r.s.opts != nil && r.s.opts.ReqTimeout > 0 {
		timeout = r.s.opts.ReqTimeout
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := node.RequestWithContext(timeoutCtx, path, body)
	if err != nil {
		return nil, err
	}

	if resp.Status == proto.StatusOK {
		return resp.Body, nil
	}
	if errMsg := strings.TrimSpace(string(resp.Body)); errMsg != "" {
		return nil, fmt.Errorf("rpc request failed, status: %d, err: %s", resp.Status, errMsg)
	}

	return nil, fmt.Errorf("rpc request failed, status: %d", resp.Status)

}

type channelReq struct {
	channelId   string
	channelType uint8
}

func (ch *channelReq) encode() ([]byte, error) {
	enc := wkproto.NewEncoder()
	defer enc.End()

	enc.WriteString(ch.channelId)
	enc.WriteUint8(ch.channelType)

	return enc.Bytes(), nil
}

func (ch *channelReq) decode(data []byte) error {
	dec := wkproto.NewDecoder(data)
	var err error
	if ch.channelId, err = dec.String(); err != nil {
		return err
	}
	if ch.channelType, err = dec.Uint8(); err != nil {
		return err
	}
	return nil
}
