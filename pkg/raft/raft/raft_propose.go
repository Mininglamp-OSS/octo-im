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
func (r *Raft) ProposeBatchUntilAppliedTimeout(ctx context.Context, reqs []types.ProposeReq) (resps []*types.ProposeResp, err error) {
	defer func() { r.recordProposalOutcome(err) }()
	return r.proposeUntilApplied(ctx, reqs)
}

func (r *Raft) proposeUntilApplied(ctx context.Context, reqs []types.ProposeReq) ([]*types.ProposeResp, error) {
	if r.IsLeader() {
		return r.submitProposal(ctx, reqs, true, nil)
	}
	route := r.view.Load().(nodeView)
	resps, err := r.fowardPropose(ctx, reqs)
	if err != nil {
		return nil, err
	}
	logs := make([]types.Log, len(reqs))
	for i, req := range reqs {
		logs[i] = types.Log{Id: req.Id, Index: resps[i].Index, Term: route.term, Data: append([]byte(nil), req.Data...)}
	}
	r.forwardMu.Lock()
	if route != r.view.Load().(nodeView) {
		r.forwardMu.Unlock()
		return nil, proposalUnknown(types.ErrNotLeader)
	}
	p := r.wait.registerReceipt(logs, true)
	r.forwardMu.Unlock()
	select {
	case <-p.waitC:
		return resps, p.err
	case <-ctx.Done():
		if err, done := r.wait.detach(p); done {
			return resps, err
		}
		return nil, proposalUnknown(ctx.Err())
	case <-r.stopper.ShouldStop():
		if err, done := r.wait.detach(p); done {
			return resps, err
		}
		return nil, proposalUnknown(types.ErrStopped)
	}
}

func (r *Raft) proposeBatch(ctx context.Context, reqs types.ProposeReqSet, before func([]types.Log)) (resps []*types.ProposeResp, err error) {
	defer func() { r.recordProposalOutcome(err) }()
	return r.submitProposal(ctx, reqs, false, before)
}

func (r *Raft) handleSendPropose(e types.Event) {
	if len(e.Logs) != 1 {
		return
	}
	var reqs types.ProposeReqSet
	err := reqs.Unmarshal(e.Logs[0].Data)
	if err != nil {
		r.Error("unmarshal propose req failed", zap.Error(err))
		r.sendProposeRespError(reqs, e.From)
		return
	}
	if len(reqs) == 0 {
		return
	}
	if !r.IsLeader() {
		r.Error("handleSendPropose: node is leader, but receive propose from other node", zap.Uint64("leaderId", r.LeaderId()), zap.Uint64("from", e.From))
		r.sendProposeRespError(reqs, e.From)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), r.opts.ProposeTimeout)
	defer cancel()
	resps, err := r.submitProposal(timeoutCtx, reqs, true, nil)
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

// The unchanged wire response carries only request IDs and indexes. Forwarded
// correlation keys therefore cannot be reused while delayed packets may exist.
// Keep bounded lifetime tombstones and fail closed at exhaustion; never evict
// them and allow an old packet to complete a new registration.
const configForwardIdentityLimit = 65536

func (r *Raft) handleSendProposeResp(e types.Event) {
	if len(e.Logs) != 1 {
		return
	}
	var resps types.ProposeRespSet
	if err := resps.Unmarshal(e.Logs[0].Data); err != nil || len(resps) == 0 {
		return
	}
	key := fmt.Sprintf("%d", resps[len(resps)-1].Id)
	r.forwardMu.Lock()
	defer r.forwardMu.Unlock()
	leader, ok := r.forwardPending[key]
	if !ok || leader != e.From {
		return
	}
	delete(r.forwardPending, key)
	if e.Reason != types.ReasonOk {
		r.fowardProposeWait.Trigger(key, proposalUnknown(errors.New("raft: forwarded proposal rejected")))
		return
	}
	r.fowardProposeWait.Trigger(key, resps)
}

func (r *Raft) fowardPropose(ctx context.Context, reqs types.ProposeReqSet) ([]*types.ProposeResp, error) {
	if len(reqs) == 0 {
		return nil, proposalNotAdmitted(errors.New("raft: empty proposal"))
	}
	if err := ctx.Err(); err != nil {
		return nil, proposalNotAdmitted(err)
	}
	data, err := reqs.Marshal()
	if err != nil {
		return nil, proposalNotAdmitted(err)
	}
	key := fmt.Sprintf("%d", reqs[len(reqs)-1].Id)
	r.forwardMu.Lock()
	v := r.view.Load().(nodeView)
	if r.stopped.Load() {
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(types.ErrStopped)
	}
	if v.leader == 0 {
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(types.ErrNotLeader)
	}
	if _, used := r.forwardUsed[key]; used {
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(errors.New("raft: forwarded request ID already used"))
	}
	if len(r.forwardUsed) >= configForwardIdentityLimit {
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(errors.New("raft: forwarding identity capacity exhausted"))
	}
	r.forwardUsed[key] = struct{}{}
	r.forwardPending[key] = v.leader
	waitC := r.fowardProposeWait.Register(key)
	r.forwardMu.Unlock()
	defer func() {
		r.forwardMu.Lock()
		defer r.forwardMu.Unlock()
		if _, ok := r.forwardPending[key]; ok {
			delete(r.forwardPending, key)
			r.fowardProposeWait.Trigger(key, nil)
		}
	}()
	r.opts.Transport.Send(types.Event{From: r.opts.NodeId, To: v.leader, Type: types.SendPropose, Logs: []types.Log{{Data: data}}})
	select {
	case result := <-waitC:
		if err, ok := result.(error); ok {
			return nil, err
		}
		resps, ok := result.(types.ProposeRespSet)
		if !ok || len(resps) != len(reqs) {
			return nil, proposalUnknown(errors.New("raft: invalid forwarded response"))
		}
		for i, resp := range resps {
			if resp == nil || resp.Id != reqs[i].Id || resp.Index == 0 || (i > 0 && resp.Index != resps[i-1].Index+1) {
				return nil, proposalUnknown(errors.New("raft: mismatched forwarded response"))
			}
		}
		return resps, nil
	case <-ctx.Done():
		return nil, proposalUnknown(ctx.Err())
	case <-r.stopper.ShouldStop():
		return nil, proposalUnknown(types.ErrStopped)
	}
}
