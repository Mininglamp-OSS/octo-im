package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func proposalNotAdmitted(cause error) error {
	return fmt.Errorf("%w: %w", types.ErrProposalNotAdmitted, cause)
}
func proposalUnknown(cause error) error {
	return fmt.Errorf("%w: %w", types.ErrProposalOutcomeUnknown, cause)
}

type configProposal struct {
	mu          sync.Mutex
	ctx         context.Context
	logs        []types.Log
	waitApplied bool
	abandoned   bool
	handled     bool
	waiter      *progress
	err         error
	done        chan struct{}
}

// handleProposal is called only by the goroutine stepping the node. Holding
// p.mu is the admission/cancellation linearization point: cancellation that
// wins it leaves no admitted logs; cancellation that loses cannot roll back.
func (r *Raft) handleProposal(p *configProposal) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.abandoned {
		return
	}
	p.handled = true
	switch {
	case p.ctx.Err() != nil:
		p.err = proposalNotAdmitted(p.ctx.Err())
	case r.stopped.Load():
		p.err = proposalNotAdmitted(types.ErrStopped)
	default:
		_, err := r.node.Admit(p.logs)
		if err != nil {
			p.err = proposalNotAdmitted(err)
		} else if p.waitApplied {
			p.waiter = r.wait.registerReceipt(p.logs, false)
		}
	}
	close(p.done)
}
func (r *Raft) cancelProposal(p *configProposal, cause error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.handled {
		p.abandoned = true
		return proposalNotAdmitted(cause)
	}
	if p.err != nil {
		return p.err
	}
	if p.waiter != nil {
		if err, done := r.wait.detach(p.waiter); done {
			return err
		}
	}
	return proposalUnknown(cause)
}
func (r *Raft) submitProposal(ctx context.Context, reqs types.ProposeReqSet, applied bool, before func([]types.Log)) ([]*types.ProposeResp, error) {
	if len(reqs) == 0 {
		return nil, proposalNotAdmitted(fmt.Errorf("raft: empty proposal"))
	}
	if err := ctx.Err(); err != nil {
		return nil, proposalNotAdmitted(err)
	}
	if r.pause.Load() {
		return nil, proposalNotAdmitted(types.ErrPaused)
	}
	if r.stopped.Load() {
		return nil, proposalNotAdmitted(types.ErrStopped)
	}
	p := &configProposal{ctx: ctx, waitApplied: applied, done: make(chan struct{}), logs: make([]types.Log, len(reqs))}
	for i, req := range reqs {
		p.logs[i] = types.Log{Id: req.Id, Data: append([]byte(nil), req.Data...)}
	}
	select {
	case r.stepC <- stepReq{proposal: p}:
	case <-ctx.Done():
		return nil, proposalNotAdmitted(ctx.Err())
	case <-r.stopper.ShouldStop():
		return nil, proposalNotAdmitted(types.ErrStopped)
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		if err := r.cancelProposal(p, ctx.Err()); err != nil {
			return nil, err
		}
	case <-r.stopper.ShouldStop():
		if err := r.cancelProposal(p, types.ErrStopped); err != nil {
			return nil, err
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	if before != nil {
		before(p.logs)
	}
	if applied {
		select {
		case <-p.waiter.waitC:
			if p.waiter.err != nil {
				return nil, p.waiter.err
			}
		case <-ctx.Done():
			if err := r.cancelProposal(p, ctx.Err()); err != nil {
				return nil, err
			}
		case <-r.stopper.ShouldStop():
			if err := r.cancelProposal(p, types.ErrStopped); err != nil {
				return nil, err
			}
		}
	}
	resps := make([]*types.ProposeResp, len(p.logs))
	for i, log := range p.logs {
		resps[i] = &types.ProposeResp{Id: log.Id, Index: log.Index}
	}
	return resps, nil
}
