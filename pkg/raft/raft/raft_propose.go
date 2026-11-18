package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

// Propose 提案
func (r *Raft) Propose(id uint64, data []byte) (*types.ProposeResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), r.opts.ProposeTimeout)
	defer cancel()
	resps, err := r.proposeBatch(timeoutCtx, []types.ProposeReq{
		{
			Id:   id,
			Data: data,
		},
	}, nil)
	if err != nil {
		return nil, err
	}
	return resps[0], nil
}

// ProposeUntilApplied 提案直到应用（提案后等待日志被应用）
func (r *Raft) ProposeUntilAppliedTimeout(ctx context.Context, id uint64, data []byte) (*types.ProposeResp, error) {
	resps, err := r.ProposeBatchUntilAppliedTimeout(ctx, []types.ProposeReq{
		{
			Id:   id,
			Data: data,
		},
	})
	if err != nil {
		return nil, err
	}
	return resps[0], nil
}

func (r *Raft) ProposeUntilApplied(id uint64, data []byte) (*types.ProposeResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), r.opts.ProposeTimeout)
	defer cancel()
	return r.ProposeUntilAppliedTimeout(timeoutCtx, id, data)
}

// ProposeBatchTimeout 批量提案
func (r *Raft) ProposeBatchTimeout(ctx context.Context, reqs []types.ProposeReq) ([]*types.ProposeResp, error) {
	return r.proposeBatch(ctx, reqs, nil)
}

// ProposeBatchUntilAppliedTimeout 批量提案（等待应用）
func (r *Raft) ProposeBatchUntilAppliedTimeout(ctx context.Context, reqs []types.ProposeReq) ([]*types.ProposeResp, error) {
	// 等待应用
	var (
		applyProcess *progress
		resps        []*types.ProposeResp
		err          error
		needWait     = true
	)

	if r.node.LeaderId() == 0 {
		return nil, types.ErrNotLeader
	}

	if !r.node.IsLeader() {
		// 如果不是leader，则转发给leader
		resps, err = r.fowardPropose(ctx, reqs)
		if err != nil {
			return nil, err
		}
		if len(resps) == 0 || resps[len(resps)-1] == nil {
			return nil, errors.New("raft: empty forwarded proposal response")
		}
		maxLogIndex := resps[len(resps)-1].Index
		// 如果最大的日志下标大于已应用的日志下标，则不需要等待
		if r.node.AppliedIndex() >= maxLogIndex {
			needWait = false
		}
		if needWait {
			applyProcess = r.wait.waitApply(maxLogIndex)
		}
	} else {
		resps, err = r.proposeBatch(ctx, reqs, func(logs []types.Log) {
			maxLogIndex := logs[len(logs)-1].Index
			// 如果最大的日志下标大于已应用的日志下标，则不需要等待
			if r.node.AppliedIndex() >= maxLogIndex {
				needWait = false
			}
			if needWait {
				applyProcess = r.wait.waitApply(maxLogIndex)
			}

		})
		if err != nil {
			return nil, err
		}
	}

	if needWait {
		select {
		case <-applyProcess.waitC:
			return resps, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.stopper.ShouldStop():
			return nil, types.ErrStopped
		}
	} else {
		return resps, nil
	}
}

func (r *Raft) proposeBatch(ctx context.Context, reqs types.ProposeReqSet, stepBefore func(logs []types.Log)) ([]*types.ProposeResp, error) {
	if len(reqs) == 0 {
		return nil, errors.New("raft: empty proposal batch")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.node.Lock()
	defer r.node.Unlock()
	lastLogIndex := r.node.LastLogIndex()
	term := r.node.LastTerm()
	logs := make([]types.Log, 0, len(reqs))
	for i, req := range reqs {
		logIndex := lastLogIndex + 1 + uint64(i)
		logs = append(logs, types.Log{
			Id:    req.Id,
			Term:  term,
			Index: logIndex,
			Data:  req.Data,
		})
	}

	release, err := r.node.AdmitProposalBatch(logs)
	if err != nil {
		return nil, err
	}
	defer release()

	if stepBefore != nil {
		stepBefore(logs)
	}

	err = r.stepWaitWithComplete(ctx, types.Event{
		Type: types.Propose,
		Logs: logs,
	}, release)
	if err != nil {
		return nil, err
	}

	resps := make([]*types.ProposeResp, 0, len(reqs))
	for i, req := range reqs {
		logIndex := lastLogIndex + 1 + uint64(i)
		resps = append(resps, &types.ProposeResp{
			Id:    req.Id,
			Index: logIndex,
		})
	}
	return resps, nil
}

func (r *Raft) validateAndDecodeProposeReqs(e types.Event) (types.ProposeReqSet, bool) {
	if len(e.Logs) == 0 || len(e.Logs[0].Data) == 0 {
		r.Error("handleSendPropose: empty proposal event logs", zap.Uint64("from", e.From))
		return nil, false
	}
	var reqs types.ProposeReqSet
	if err := reqs.Unmarshal(e.Logs[0].Data); err != nil {
		r.Error("unmarshal propose req failed", zap.Error(err), zap.Uint64("from", e.From))
		return nil, false
	}
	if len(reqs) == 0 {
		r.Error("handleSendPropose: decoded proposal request batch is empty", zap.Uint64("from", e.From))
		return nil, false
	}
	return reqs, true
}

func (r *Raft) handleSendPropose(e types.Event) {
	reqs, ok := r.validateAndDecodeProposeReqs(e)
	if !ok {
		return
	}
	r.handleSendProposeDecoded(e, reqs)
}

func (r *Raft) handleSendProposeDecoded(e types.Event, reqs types.ProposeReqSet) {
	if len(reqs) == 0 {
		return
	}
	if !r.node.IsLeader() {
		r.Error("handleSendPropose: node is leader, but receive propose from other node", zap.Uint64("leaderId", r.node.LeaderId()), zap.Uint64("from", e.From))
		r.sendProposeRespError(reqs, e.From)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), r.opts.ProposeTimeout)
	defer cancel()
	resps, err := r.ProposeBatchUntilAppliedTimeout(timeoutCtx, reqs)
	if err != nil {
		r.Error("handleSendPropose: propose batch failed", zap.Error(err))
		r.sendProposeRespError(reqs, e.From)
		return
	}
	data, err := types.ProposeRespSet(resps).Marshal()
	if err != nil {
		r.Error("marshal propose resp failed", zap.Error(err))
		r.sendProposeRespError(reqs, e.From)
		return
	}

	r.opts.Transport.Send(types.Event{
		From: r.opts.NodeId,
		To:   e.From,
		Type: types.SendProposeResp,
		Logs: []types.Log{
			{
				Data: data,
			},
		},
		Reason: types.ReasonOk,
	})
}

func (r *Raft) sendProposeRespError(reqs types.ProposeReqSet, to uint64) {
	if len(reqs) == 0 || r == nil || r.opts == nil || r.opts.Transport == nil {
		return
	}
	resps := types.ProposeRespSet{}
	for _, req := range reqs {
		resps = append(resps, &types.ProposeResp{
			Id:    req.Id,
			Index: 0,
		})
	}
	data, err := resps.Marshal()
	if err != nil {
		r.Error("marshal propose resp failed", zap.Error(err))
		return
	}
	r.opts.Transport.Send(types.Event{
		From:   r.opts.NodeId,
		To:     to,
		Type:   types.SendProposeResp,
		Reason: types.ReasonError,
		Logs: []types.Log{
			{
				Data: data,
			},
		},
	})
}

func (r *Raft) handleSendProposeResp(e types.Event) {
	if len(e.Logs) == 0 || len(e.Logs[0].Data) == 0 {
		r.Error("handleSendProposeResp: empty proposal response event logs", zap.Uint64("from", e.From))
		return
	}
	var resps types.ProposeRespSet
	err := resps.Unmarshal(e.Logs[0].Data)
	if err != nil {
		r.Error("unmarshal propose resp failed", zap.Error(err))
		return
	}
	if len(resps) == 0 || resps[len(resps)-1] == nil {
		r.Error("handleSendProposeResp: decoded proposal response batch is empty", zap.Uint64("from", e.From))
		return
	}

	key := fmt.Sprintf("%d", resps[len(resps)-1].Id)
	if e.Reason == types.ReasonError {
		r.Error("handleSendProposeResp: receive propose resp error", zap.Uint64("from", e.From))
		r.fowardProposeWait.Trigger(key, errors.New("receive propose resp error"))
		return
	}
	r.fowardProposeWait.Trigger(key, resps)
}

func (r *Raft) fowardPropose(ctx context.Context, reqs types.ProposeReqSet) ([]*types.ProposeResp, error) {
	if len(reqs) == 0 {
		return nil, errors.New("raft: empty proposal batch")
	}
	data, err := reqs.Marshal()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%d", reqs[len(reqs)-1].Id)
	waitC := r.fowardProposeWait.Register(key)

	r.opts.Transport.Send(types.Event{
		From: r.node.opts.NodeId,
		To:   r.node.LeaderId(),
		Type: types.SendPropose,
		Logs: []types.Log{
			{
				Data: data,
			},
		},
	})

	select {
	case result := <-waitC:
		if result == nil {
			return nil, errors.New("foward propose failed")
		}
		err, ok := result.(error)
		if ok {
			return nil, err
		}
		return result.(types.ProposeRespSet), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopper.ShouldStop():
		return nil, types.ErrStopped
	}
}
