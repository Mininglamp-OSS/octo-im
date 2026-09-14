package raftgroup

import "context"

type ownerRequest struct {
	ctx    context.Context
	key    string
	fn     func(IRaft) error
	result chan error
}

// Do runs a short operation on the Raft owner, serialized with Step and Tick.
// The callback must not call blocking RaftGroup methods or retain mutable Raft
// state. Once accepted, Do waits for completion even if ctx expires, so callers
// can safely use callback results without a callback outliving their stack.
func (rg *RaftGroup) Do(ctx context.Context, key string, fn func(IRaft) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	req := ownerRequest{ctx: ctx, key: key, fn: fn, result: make(chan error, 1)}
	select {
	case rg.ownerC <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-rg.stopper.ShouldStop():
		return ErrGroupStopped
	}
	return <-req.result
}
