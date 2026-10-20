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
	// The leader assigns the term and the response does not carry it; the
	// route's term is only this node's view. Term 0 leaves it unverified, as
	// in raftgroup: Id, Index and Data identify the entry.
	logs := make([]types.Log, len(reqs))
	for i, req := range reqs {
		logs[i] = types.Log{Id: req.Id, Index: resps[i].Index, Data: append([]byte(nil), req.Data...)}
	}
	r.forwardMu.Lock()
	// Commit progress is not a routing change. Generation changes only
	// when leader, term or local role changes.
	if route.generation != r.view.Load().(nodeView).generation {
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
		r.sendProposeRespError(reqs, e)
		return
	}
	if len(reqs) == 0 {
		return
	}
	if !r.IsLeader() {
		r.Error("handleSendPropose: node is leader, but receive propose from other node", zap.Uint64("leaderId", r.LeaderId()), zap.Uint64("from", e.From))
		r.sendProposeRespError(reqs, e)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), r.opts.ProposeTimeout)
	defer cancel()
	resps, err := r.submitProposal(timeoutCtx, reqs, true, nil)
	if err != nil {
		r.Error("handleSendPropose: propose batch failed", zap.Error(err))
		r.sendProposeRespError(reqs, e)
		return
	}
	data, err := types.ProposeRespSet(resps).Marshal()
	if err != nil {
		r.Error("marshal propose resp failed", zap.Error(err))
		r.sendProposeRespError(reqs, e)
		return
	}

	r.opts.Transport.Send(types.Event{
		From: r.opts.NodeId,
		To:   e.From,
		Type: types.SendProposeResp,
		Logs: []types.Log{
			echoForwardToken(e, data),
		},
		Reason: types.ReasonOk,
	})
}

func (r *Raft) sendProposeRespError(reqs types.ProposeReqSet, req types.Event) {
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
		To:     req.From,
		Type:   types.SendProposeResp,
		Reason: types.ReasonError,
		Logs: []types.Log{
			echoForwardToken(req, data),
		},
	})
}

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
	switch {
	case e.Logs[0].Term == forwardTokenVersion:
		token := forwardToken{incarnation: e.Logs[0].Index, attempt: e.Logs[0].Id}
		reg, ok := r.forwardPending[key]
		if !ok || reg.token != token {
			// A stale echo may predate a term change, so it proves nothing
			// about the leader of the current term.
			r.forwardStats.stale++
			return
		}
		if reg.leader != e.From {
			r.forwardStats.senderMismatch++
			return
		}
		// A live registration is retired on every route change, so this
		// echo comes from the current term's leader.
		r.markTokenAwareLocked(e.From)
	case e.Logs[0].Term == 0 && e.Logs[0].Index == 0 && e.Logs[0].Id == 0:
		// Zero header: a leader that predates the token; see forward_token.go.
		if !r.acceptLegacyResponseLocked(key, e.From) {
			return
		}
	default:
		r.forwardStats.unechoed++
		r.warnForwardLocked(e.From, warnUnknownVersion, "forwarding response with an unknown token version dropped", zap.Uint32("version", e.Logs[0].Term))
		return
	}
	if e.Reason != types.ReasonOk {
		r.retireForwardLocked(key, proposalUnknown(types.ErrForwardRejected))
		return
	}
	r.retireForwardLocked(key, resps)
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
	if _, live := r.forwardPending[key]; live {
		r.forwardStats.duplicateRejected++
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(errors.New("raft: forwarded request ID already pending"))
	}
	if len(r.forwardPending) >= configForwardLiveLimit {
		r.forwardStats.limitRejected++
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(errors.New("raft: forwarded registration limit reached"))
	}
	token, err := r.forwardToken.issue()
	if err != nil {
		if errors.Is(err, errForwardIncarnationUnset) {
			r.forwardStats.unreservedRejected++
		} else {
			r.forwardStats.limitRejected++
		}
		r.forwardMu.Unlock()
		return nil, proposalNotAdmitted(err)
	}
	r.forwardPending[key] = forwardRegistration{leader: v.leader, token: token}
	r.forwardStats.registered++
	r.forwardStats.highWater = max(r.forwardStats.highWater, len(r.forwardPending))
	waitC := r.fowardProposeWait.Register(key)
	r.forwardMu.Unlock()
	defer func() {
		r.forwardMu.Lock()
		defer r.forwardMu.Unlock()
		// The key may already belong to a newer registration.
		if reg, ok := r.forwardPending[key]; ok && reg.token == token {
			r.retireForwardLocked(key, nil)
		}
	}()
	r.opts.Transport.Send(types.Event{From: r.opts.NodeId, To: v.leader, Type: types.SendPropose, Logs: []types.Log{token.log(data)}})
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
