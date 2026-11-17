package slot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
)

var ErrEmptySlotProposeResponse = errors.New("empty slot propose response")

type slotProposeBatchWithContextRPC interface {
	RequestSlotProposeBatchUntilAppliedWithContext(ctx context.Context, nodeId uint64, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error)
}

func isTransientSlotHandoffError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "proposal dropped") || strings.Contains(msg, "not leader")
}

func (s *Server) requestSlotProposeBatchWithContext(ctx context.Context, leaderId uint64, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.opts == nil || s.opts.RPC == nil {
		return nil, errors.New("slot RPC not initialized")
	}
	if ctxRPC, ok := s.opts.RPC.(slotProposeBatchWithContextRPC); ok {
		return ctxRPC.RequestSlotProposeBatchUntilAppliedWithContext(ctx, leaderId, slotId, reqs)
	}
	type rpcResult struct {
		resps types.ProposeRespSet
		err   error
	}
	ch := make(chan rpcResult, 1)
	go func() {
		resps, err := s.opts.RPC.RequestSlotProposeBatchUntilApplied(leaderId, slotId, reqs)
		ch <- rpcResult{resps: resps, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.resps, res.err
	}
}

func (s *Server) GetSlotId(v string) uint32 {

	return s.getSlotId(v)
}

func (s *Server) SlotLeaderId(slotId uint32) uint64 {
	slotConfig := s.opts.Node.Slot(slotId)
	if slotConfig != nil {
		return slotConfig.Leader
	}
	return 0

}

func (s *Server) CheckWriteHealth() error {
	if s == nil || s.storage == nil {
		return errors.New("slot storage not initialized")
	}
	return s.storage.CheckWriteHealth()
}

func (s *Server) Propose(slotId uint32, data []byte) (*types.ProposeResp, error) {
	shardNo := SlotIdToKey(slotId)
	logId := s.GenLogId()
	return s.raftGroup.Propose(shardNo, logId, data)
}

func (s *Server) ProposeUntilApplied(slotId uint32, data []byte) (*types.ProposeResp, error) {

	start := time.Now()
	defer func() {
		if trace.GlobalTrace != nil {
			trace.GlobalTrace.Metrics.Cluster().ProposeLatencyAdd(trace.ClusterKindSlot, time.Since(start).Milliseconds())
			if err := recover(); err != nil {
				trace.GlobalTrace.Metrics.Cluster().ProposeFailedCountAdd(trace.ClusterKindSlot, 1)
			}
		}
	}()

	shardNo := SlotIdToKey(slotId)
	logId := s.GenLogId()
	deadline := time.Now().Add(150 * time.Millisecond)
	delay := 15 * time.Millisecond
	for {
		var (
			resp *types.ProposeResp
			err  error
		)
		if s.opts != nil && s.opts.Node != nil {
			if slotConfig := s.opts.Node.Slot(slotId); slotConfig != nil && slotConfig.Leader != 0 && slotConfig.Leader != s.opts.NodeId && s.opts.RPC != nil {
				resps, rpcErr := s.requestSlotProposeBatchWithContext(context.Background(), slotConfig.Leader, slotId, types.ProposeReqSet{
					{
						Id:   logId,
						Data: data,
					},
				})
				err = rpcErr
				if err == nil && len(resps) > 0 {
					resp = resps[0]
				}
			} else {
				resp, err = s.raftGroup.ProposeUntilApplied(shardNo, logId, data)
			}
		} else {
			resp, err = s.raftGroup.ProposeUntilApplied(shardNo, logId, data)
		}
		if err == nil {
			if resp == nil {
				return nil, ErrEmptySlotProposeResponse
			}
			return resp, nil
		}
		if !isTransientSlotHandoffError(err) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(delay)
		delay = min(delay*2, 50*time.Millisecond)
	}
}

// ProposeUntilAppliedForLocal proposes data to slotId only when the local node is
// currently the authoritative slot leader. It never forwards pre-encoded local
// read-modify-write state to a remote slot leader.
func (s *Server) ProposeUntilAppliedForLocal(slotId uint32, data []byte) (*types.ProposeResp, error) {
	if s.opts != nil && s.opts.Node != nil {
		if slotConfig := s.opts.Node.Slot(slotId); slotConfig != nil && slotConfig.Leader != 0 && slotConfig.Leader != s.opts.NodeId {
			return nil, types.ErrNotLeader
		}
	}
	shardNo := SlotIdToKey(slotId)
	if r := s.raftGroup.GetRaft(shardNo); r == nil || !r.IsLeader() {
		return nil, types.ErrNotLeader
	}
	logId := s.GenLogId()
	resp, err := s.raftGroup.ProposeUntilApplied(shardNo, logId, data)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrEmptySlotProposeResponse
	}
	return resp, nil
}

func (s *Server) ProposeUntilAppliedTimeout(ctx context.Context, slotId uint32, data []byte) (*types.ProposeResp, error) {

	start := time.Now()
	defer func() {
		if trace.GlobalTrace != nil {
			trace.GlobalTrace.Metrics.Cluster().ProposeLatencyAdd(trace.ClusterKindSlot, time.Since(start).Milliseconds())
			if err := recover(); err != nil {
				trace.GlobalTrace.Metrics.Cluster().ProposeFailedCountAdd(trace.ClusterKindSlot, 1)
			}
		}
	}()

	if ctx == nil {
		ctx = context.Background()
	}
	logId := s.GenLogId()
	reqs := types.ProposeReqSet{
		{
			Id:   logId,
			Data: data,
		},
	}

	delay := 15 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		slotConfig := s.opts.Node.Slot(slotId)
		if slotConfig == nil {
			return nil, fmt.Errorf("slot[%d] config not found", slotId)
		}

		var (
			resps types.ProposeRespSet
			err   error
		)
		// 如果当前节点不是槽的领导节点，则向槽的领导节点请求提案
		if slotConfig.Leader != s.opts.NodeId {
			resps, err = s.requestSlotProposeBatchWithContext(ctx, slotConfig.Leader, slotId, reqs)
		} else {
			// 如果当前节点是槽的领导节点，则本地提案
			resps, err = s.ProposeUntilAppliedTimeoutForLocal(ctx, slotId, reqs)
		}
		if err == nil {
			if len(resps) == 0 || resps[0] == nil {
				return nil, ErrEmptySlotProposeResponse
			}
			return resps[0], nil
		}
		if !isTransientSlotHandoffError(err) || ctx.Err() != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 60*time.Millisecond)
	}
}

func (s *Server) ProposeUntilAppliedTimeoutForLocal(ctx context.Context, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	shardNo := SlotIdToKey(slotId)

	resps, err := s.raftGroup.ProposeBatchUntilAppliedTimeout(ctx, shardNo, reqs)
	if err != nil {
		return nil, err
	}
	if len(resps) == 0 {
		return nil, ErrEmptySlotProposeResponse
	}
	return resps, nil
}

func (s *Server) MustWaitAllSlotsReady(timeout time.Duration) {
	tk := time.NewTicker(time.Millisecond * 10)
	defer tk.Stop()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-tk.C:
			slots := s.opts.Node.Slots()
			if len(slots) > 0 {
				notReady := false
				for _, st := range slots {
					if st.Leader == 0 {
						notReady = true
						break
					}
				}
				if !notReady {
					return
				}

			}
		case <-timeoutCtx.Done():
			s.Panic("wait all slots ready timeout")
			return
		}
	}
}
