package raftgroup

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/raft/track"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

func (rg *RaftGroup) Propose(raftKey string, id uint64, data []byte) (*types.ProposeResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), rg.opts.ProposeTimeout)
	defer cancel()
	return rg.ProposeTimeout(timeoutCtx, raftKey, id, data)
}

func (rg *RaftGroup) ProposeBatch(raftKey string, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rg.opts.ProposeTimeout)
	defer cancel()
	return rg.ProposeBatchTimeout(ctx, raftKey, reqs)
}

func (rg *RaftGroup) ProposeUntilApplied(raftKey string, id uint64, data []byte) (*types.ProposeResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), rg.opts.ProposeTimeout)
	defer cancel()

	resps, err := rg.ProposeBatchUntilAppliedTimeout(timeoutCtx, raftKey, types.ProposeReqSet{
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

func (rg *RaftGroup) ProposeTimeout(ctx context.Context, raftKey string, id uint64, data []byte) (*types.ProposeResp, error) {
	resps, err := rg.ProposeBatchTimeout(ctx, raftKey, types.ProposeReqSet{
		{
			Id:   id,
			Data: data,
		},
	})
	if err != nil {
		return nil, err
	}

	if len(resps) != 0 {
		return resps[0], nil
	}
	return nil, fmt.Errorf("propose failed")
}

// ProposeBatchTimeout waits for leader admission, not replication or business
// Apply. ErrProposalNotAdmitted means the proposal definitely did not enter
// the log. Once the leader admitted it, a local caller gets success even if
// ctx expires meanwhile; a forwarding caller whose ctx expires gets
// ErrProposalOutcomeUnknown. Callers must not blindly replay such proposals.
func (rg *RaftGroup) ProposeBatchTimeout(ctx context.Context, raftKey string, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, proposalNotAdmitted(err)
	}
	select {
	case <-rg.stopper.ShouldStop():
		return nil, proposalNotAdmitted(ErrGroupStopped)
	default:
	}
	raft := rg.raftList.get(raftKey)
	if raft == nil {
		return nil, fmt.Errorf("raft not found, key:%s", raftKey)
	}
	if !raft.IsLeader() {
		resps, err := rg.fowardPropose(ctx, raft, reqs)
		rg.recordOutcome(raftKey, receipt{forwarded: true}, err)
		return resps, err
	}
	resps, _, err := rg.proposeBatchTimeout(ctx, raft, reqs, false)
	rg.recordOutcome(raftKey, receipt{}, err)
	return resps, err
}

// ProposeBatchUntilAppliedTimeout 批量提案（等待应用）
//
// Success means the entries applied at the returned indexes are this
// proposal's entries. ErrProposalNotAdmitted means they were never admitted;
// ErrProposalOutcomeUnknown and ErrProposalSuperseded are returned for
// admitted proposals whose application could not be confirmed.
func (rg *RaftGroup) ProposeBatchUntilAppliedTimeout(ctx context.Context, raftKey string, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	var (
		resps  []*types.ProposeResp
		waiter *receiptWaiter
		err    error
	)
	raft := rg.raftList.get(raftKey)
	if raft == nil {
		return nil, fmt.Errorf("raft not found, key:%s", raftKey)
	}

	raft.KeepAlive()

	if !raft.IsLeader() {
		// 如果不是leader，则转发给leader
		resps, err = rg.fowardPropose(ctx, raft, reqs)
		if err != nil {
			rg.recordOutcome(raftKey, receipt{forwarded: true}, err)
			return nil, err
		}
		var appliedBefore bool
		waiter, appliedBefore = rg.wait.register(receipt{
			raftKey:   raftKey,
			seq:       rg.proposalSeq.Add(1),
			first:     resps[0].Index,
			last:      resps[len(resps)-1].Index,
			lastLogId: resps[len(resps)-1].Id,
			forwarded: true,
		})
		// The entry may have been applied before the waiter existed. An
		// applied index alone does not prove it is this entry, so read it.
		if appliedBefore {
			rg.wait.verifyStored(waiter)
		}
	} else {
		resps, waiter, err = rg.proposeBatchTimeout(ctx, raft, reqs, true)
		if err != nil {
			rg.recordOutcome(raftKey, receipt{}, err)
			return nil, err
		}
	}

	select {
	case err = <-waiter.doneC:
	case <-ctx.Done():
		err = rg.detachWaiter(waiter, ctx.Err())
	case <-rg.stopper.ShouldStop():
		err = rg.detachWaiter(waiter, ErrGroupStopped)
	}
	rg.recordOutcome(raftKey, waiter.receipt, err)
	if err != nil {
		return nil, err
	}
	return resps, nil
}

// detachWaiter stops waiting for w. A result already published in the same
// round is kept, so an applied proposal is not reported as timed out.
func (rg *RaftGroup) detachWaiter(w *receiptWaiter, cause error) error {
	if result, completed := rg.wait.detach(w); completed {
		return result
	}
	return proposalOutcomeUnknown(cause)
}

// admissionSem returns the per-raft admission semaphore. It keeps at most one
// local admission per raft in flight, like the node lock it replaces, but a
// caller can give up while waiting for it.
func (rg *RaftGroup) admissionSem(raftKey string) chan struct{} {
	if sem, ok := rg.admissionSems.Load(raftKey); ok {
		return sem.(chan struct{})
	}
	sem, _ := rg.admissionSems.LoadOrStore(raftKey, make(chan struct{}, 1))
	return sem.(chan struct{})
}

// proposeBatchTimeout hands reqs to the event loop for admission. The loop
// assigns the indexes and term; nothing here reads them from the node. When
// waitApplied is set, the returned waiter was registered by the loop before
// the admitted range could be stored or applied.
func (rg *RaftGroup) proposeBatchTimeout(ctx context.Context, raft IRaft, reqs []types.ProposeReq, waitApplied bool) ([]*types.ProposeResp, *receiptWaiter, error) {
	if len(reqs) == 0 {
		return nil, nil, errors.New("raftgroup: empty proposal batch")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, proposalNotAdmitted(err)
	}

	sem := rg.admissionSem(raft.Key())
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, proposalNotAdmitted(ctx.Err())
	case <-rg.stopper.ShouldStop():
		return nil, nil, proposalNotAdmitted(ErrGroupStopped)
	}
	defer func() { <-sem }()
	if err := ctx.Err(); err != nil {
		return nil, nil, proposalNotAdmitted(err)
	}
	select {
	case <-rg.stopper.ShouldStop():
		return nil, nil, proposalNotAdmitted(ErrGroupStopped)
	default:
	}

	raft.KeepAlive()

	logs := make([]types.Log, 0, len(reqs))
	for _, req := range reqs {
		logs = append(logs, types.Log{
			Id:   req.Id,
			Data: req.Data,
		})
	}
	p := newProposal(rg.proposalSeq.Add(1), waitApplied)
	rg.mq.MustAdd(Event{
		RaftKey: raft.Key(),
		Event: types.Event{
			Type: types.Propose,
			Logs: logs,
		},
		Proposal: p,
	})

	rg.Advance()

	var result admitResult
	select {
	case result = <-p.admitC:
	case <-ctx.Done():
		if p.state.CompareAndSwap(proposalPending, proposalAbandoned) {
			return nil, nil, proposalNotAdmitted(ctx.Err())
		}
		// The loop is admitting it: the result is published without IO.
		result = <-p.admitC
	case <-rg.stopper.ShouldStop():
		if p.state.CompareAndSwap(proposalPending, proposalAbandoned) {
			return nil, nil, proposalNotAdmitted(ErrGroupStopped)
		}
		result = <-p.admitC
	}
	if result.err != nil {
		return nil, nil, result.err
	}
	resps := make([]*types.ProposeResp, 0, len(reqs))
	for i, req := range reqs {
		resps = append(resps, &types.ProposeResp{
			Id:    req.Id,
			Index: result.adm.FirstIndex + uint64(i),
		})
	}
	return resps, result.waiter, nil
}

// admitProposal runs on the event loop. A proposal its caller abandoned is
// skipped without stepping the node or allocating an index.
func (rg *RaftGroup) admitProposal(raft IRaft, e Event) {
	p := e.Proposal
	if !p.state.CompareAndSwap(proposalPending, proposalAdmitting) {
		return
	}
	if rg.beforeAdmit != nil {
		rg.beforeAdmit(p.seq)
	}
	if raft == nil {
		p.state.Store(proposalRejected)
		p.admitC <- admitResult{err: proposalNotAdmitted(ErrRaftNotExist)}
		return
	}
	for i := range e.Logs {
		e.Logs[i].Record.Add(track.PositionPropose)
	}
	adm, err := raft.Admit(e.Logs)
	if err != nil {
		p.state.Store(proposalRejected)
		p.admitC <- admitResult{err: proposalNotAdmitted(err)}
		return
	}
	var waiter *receiptWaiter
	if p.waitApplied {
		waiter, _ = rg.wait.register(receipt{
			raftKey:   raft.Key(),
			seq:       p.seq,
			term:      adm.Term,
			first:     adm.FirstIndex,
			last:      adm.LastIndex,
			lastLogId: e.Logs[len(e.Logs)-1].Id,
		})
		// RemoveRaft completes the receipts it finds after unlisting the
		// raft. A receipt registered after that is completed here.
		if rg.raftList.get(raft.Key()) == nil {
			rg.wait.complete(waiter, proposalOutcomeUnknown(ErrRaftNotExist))
		}
	}
	p.state.Store(proposalAdmitted)
	p.admitC <- admitResult{adm: adm, waiter: waiter}
}

func (rg *RaftGroup) fowardPropose(ctx context.Context, raft IRaft, reqs types.ProposeReqSet) ([]*types.ProposeResp, error) {
	if len(reqs) == 0 {
		return nil, errors.New("raftgroup: empty proposal batch")
	}
	if err := ctx.Err(); err != nil {
		return nil, proposalNotAdmitted(err)
	}
	leaderID := raft.LeaderId()
	if leaderID == 0 {
		return nil, proposalNotAdmitted(types.ErrNotLeader)
	}
	data, err := reqs.Marshal()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%d", reqs[len(reqs)-1].Id)
	waitC := rg.fowardProposeWait.Register(key)

	rg.opts.Transport.Send(raft.Key(), types.Event{
		From: raft.NodeId(),
		To:   leaderID,
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
			return nil, proposalOutcomeUnknown(errors.New("raftgroup: foward propose failed"))
		}
		err, ok := result.(error)
		if ok {
			return nil, err
		}
		resps := result.(types.ProposeRespSet)
		if !forwardRespsMatch(reqs, resps) {
			return nil, proposalOutcomeUnknown(errors.New("raftgroup: forwarded propose response does not match the request"))
		}
		return resps, nil
	case <-ctx.Done():
		// The leader may already have admitted it; this caller cannot know.
		rg.fowardProposeWait.Trigger(key, nil)
		rg.Error("foward propose timeout", zap.String("raftKey", raft.Key()))
		return nil, proposalOutcomeUnknown(ctx.Err())
	case <-rg.stopper.ShouldStop():
		rg.fowardProposeWait.Trigger(key, nil)
		return nil, proposalOutcomeUnknown(ErrGroupStopped)
	}
}

// forwardRespsMatch checks that a leader response is for reqs and carries
// one contiguous admitted range.
func forwardRespsMatch(reqs types.ProposeReqSet, resps types.ProposeRespSet) bool {
	if len(resps) != len(reqs) {
		return false
	}
	for i, resp := range resps {
		if resp == nil || resp.Id != reqs[i].Id || resp.Index == 0 || (i > 0 && resp.Index != resps[i-1].Index+1) {
			return false
		}
	}
	return true
}

func (rg *RaftGroup) handleSendPropose(raftKey string, e types.Event) {

	raft := rg.raftList.get(raftKey)
	if raft == nil {
		rg.Error("raftgroup: raft not exist", zap.String("raftKey", raftKey))
		return
	}

	var reqs types.ProposeReqSet
	err := reqs.Unmarshal(e.Logs[0].Data)
	if err != nil {
		rg.Error("raftgroup: unmarshal propose req failed", zap.Error(err))
		rg.sendProposeRespError(raft, reqs, e.From)
		return
	}

	if !raft.IsLeader() {
		rg.Error("raftgroup: handleSendPropose: node is leader, but receive propose from other node", zap.Uint64("leaderId", raft.LeaderId()), zap.Uint64("from", e.From))
		rg.sendProposeRespError(raft, reqs, e.From)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), rg.opts.ProposeTimeout)
	defer cancel()
	// An admitted proposal is answered with its indexes even if timeoutCtx
	// expired meanwhile; ReasonError always means it was not admitted.
	resps, _, err := rg.proposeBatchTimeout(timeoutCtx, raft, reqs, false)
	rg.recordOutcome(raftKey, receipt{}, err)
	if err != nil {
		rg.Error("handleSendPropose: propose batch failed", zap.Error(err))
		rg.sendProposeRespError(raft, reqs, e.From)
		return
	}
	data, err := types.ProposeRespSet(resps).Marshal()
	if err != nil {
		rg.Error("raftgroup: marshal propose resp failed", zap.Error(err))
		rg.sendProposeRespError(raft, reqs, e.From)
		return
	}

	rg.opts.Transport.Send(raftKey, types.Event{
		From: raft.NodeId(),
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

func (rg *RaftGroup) sendProposeRespError(r IRaft, reqs types.ProposeReqSet, to uint64) {
	resps := types.ProposeRespSet{}
	for _, req := range reqs {
		resps = append(resps, &types.ProposeResp{
			Id:    req.Id,
			Index: 0,
		})
	}
	data, err := resps.Marshal()
	if err != nil {
		rg.Error("marshal propose resp failed", zap.Error(err))
		return
	}
	rg.opts.Transport.Send(r.Key(), types.Event{
		From:   r.NodeId(),
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

func (rg *RaftGroup) handleSendProposeResp(e types.Event) {

	var resps types.ProposeRespSet
	err := resps.Unmarshal(e.Logs[0].Data)
	if err != nil {
		rg.Error("unmarshal propose resp failed", zap.Error(err))
		return
	}

	key := fmt.Sprintf("%d", resps[len(resps)-1].Id)

	if e.Reason == types.ReasonError {
		rg.Error("handleSendProposeResp: receive propose resp error", zap.Uint64("from", e.From))
		rg.fowardProposeWait.Trigger(key, types.ErrForwardRejected)
		return
	}
	rg.fowardProposeWait.Trigger(key, resps)
}
