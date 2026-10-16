package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	syncatomic "sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	wt "github.com/WuKongIM/WuKongIM/pkg/wait"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Raft struct {
	stopper  *syncutil.Stopper
	opts     *Options
	node     *Node
	advanceC chan struct{}
	stepC    chan stepReq
	wklog.Log
	pause     atomic.Bool // 是否暂停
	pauseCond *sync.Cond

	wait *wait

	fowardProposeWait wt.Wait // 转发提按给领导等待领导的回应

	pool           *ants.Pool
	workerMu       sync.Mutex
	outcomes       proposalCounters
	started        syncatomic.Bool
	stopped        syncatomic.Bool
	stopOnce       sync.Once
	stopBegin      sync.Once
	view           syncatomic.Value
	forwardMu      sync.Mutex
	forwardPending map[string]forwardRegistration
	forwardToken   forwardTokenSource
	forwardStats   forwardStats
	forwardPeers   map[uint64]forwardPeer
}

func New(opts *Options) *Raft {
	opts.SaveHardState = opts.Storage.SaveHardState

	raftState, err := opts.Storage.GetState()
	if err != nil {
		panic(fmt.Sprintf("get state failed, err:%v", err))
	}

	lastTermStartLogIndex, err := opts.Storage.GetTermStartIndex(raftState.LastTerm)
	if err != nil {
		panic(fmt.Sprintf("get term start index failed, err:%v", err))
	}

	pool, err := ants.NewPool(opts.GoPoolSize, ants.WithNonblocking(true))
	if err != nil {
		panic(err)
	}

	r := &Raft{
		stopper:           syncutil.NewStopper(),
		opts:              opts,
		node:              NewNode(lastTermStartLogIndex, raftState, opts),
		advanceC:          make(chan struct{}, 1),
		stepC:             make(chan stepReq, 1024),
		Log:               wklog.NewWKLog("raft"),
		pauseCond:         sync.NewCond(&sync.Mutex{}),
		wait:              newWait("raft"),
		fowardProposeWait: wt.New(),
		pool:              pool,
		forwardPending:    make(map[string]forwardRegistration),
		forwardPeers:      make(map[uint64]forwardPeer),
		forwardToken:      newForwardTokenSource(opts.ForwardIncarnation),
	}
	r.wait.applied = raftState.AppliedIndex
	r.wait.readLogs = opts.Storage.GetLogs
	r.wait.verifyStored = func(p *progress) {
		if err := r.submitWorker(func() { r.wait.readReceipt(p) }); err != nil {
			r.wait.complete(p, proposalUnknown(err))
		}
	}
	r.publishView()
	opts.Advance = r.advance

	return r
}

func (r *Raft) Start() error {
	r.workerMu.Lock()
	defer r.workerMu.Unlock()
	if r.stopped.Load() {
		return types.ErrStopped
	}
	if !r.started.CompareAndSwap(false, true) {
		return nil
	}
	r.stopper.RunWorker(r.loop)

	return nil
}

// BeginStop synchronously fences admission and settles owned receipts without
// joining an event loop that may be inside durable storage I/O. Storage must
// remain open until Stop and WaitQuiescence finish.
func (r *Raft) BeginStop() {
	r.stopBegin.Do(func() {
		r.workerMu.Lock()
		r.stopped.Store(true)
		r.pool.Release()
		r.workerMu.Unlock()
		r.wait.stop()
		r.forwardMu.Lock()
		r.settleForwardsLocked(proposalUnknown(types.ErrStopped))
		r.forwardMu.Unlock()
		r.Resume()
	})
}

func (r *Raft) Stop() {
	r.BeginStop()
	r.stopOnce.Do(func() {
		r.stopper.Stop()
		r.drainStoppedQueue()
	})
}

// Pause 暂停服务
func (r *Raft) Pause() {
	r.pause.Store(true)

}

// Resume 恢复服务
func (r *Raft) Resume() {
	r.pause.Store(false)

	// 唤醒等待中的 loop
	r.pauseCond.L.Lock()
	r.pauseCond.Signal() // 唤醒一个等待的 Goroutine
	r.pauseCond.L.Unlock()
}

func (r *Raft) Step(e types.Event) {
	if r.stopped.Load() {
		return
	}
	if r.pause.Load() {
		r.Info("raft is paused, ignore event", zap.String("event", e.String()))
		return
	}
	if e.Type == types.SendPropose {
		r.stopper.RunWorker(func() { r.handleSendPropose(e) })
		return
	} else if e.Type == types.SendProposeResp {
		r.handleSendProposeResp(e)
		return
	}
	select {
	case r.stepC <- stepReq{event: e}:
	case <-r.stopper.ShouldStop():
		return
	}
}

func (r *Raft) StepWait(ctx context.Context, e types.Event) error {
	if r.stopped.Load() {
		return types.ErrStopped
	}
	if r.pause.Load() {
		r.Info("raft is paused, ignore event", zap.String("event", e.String()))
		return types.ErrPaused
	}
	// 处理其他副本发过来的提案
	if e.Type == types.SendPropose {
		r.handleSendPropose(e)
		return nil
	} else if e.Type == types.SendProposeResp {
		r.handleSendProposeResp(e)
		return nil
	}

	resp := make(chan error, 1)
	select {
	case r.stepC <- stepReq{event: e, resp: resp}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopper.ShouldStop():
		return types.ErrStopped
	}

	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopper.ShouldStop():
		return types.ErrStopped
	}
}

func (r *Raft) IsLeader() bool {
	return r.view.Load().(nodeView).isLeader
}

func (r *Raft) LeaderId() uint64 {
	return r.view.Load().(nodeView).leader
}

func (r *Raft) GetReplicaLastLogIndex(replicaId uint64) uint64 {
	return r.node.GetReplicaLastLogIndex(replicaId)
}

func (r *Raft) Options() *Options {
	return r.opts
}

// WaitUtilCommit 等待日志提交到指定的下标
func (r *Raft) WaitUtilCommit(ctx context.Context, index uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if r.view.Load().(nodeView).committed >= index {
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (r *Raft) BecomeLeader(term uint32) {
	r.changeRole(func() { r.node.BecomeLeader(term) })
}

func (r *Raft) BecomeFollower(term uint32, leader uint64) {
	r.changeRole(func() { r.node.BecomeFollower(term, leader) })
}

func (r *Raft) KeepAlive() {
	r.node.KeepAlive()
}

func (r *Raft) loop() {
	tk := time.NewTicker(r.opts.TickInterval)
	defer tk.Stop()
	for {
		if r.pause.Load() {
			r.pauseCond.L.Lock()
			for r.pause.Load() && !r.stopped.Load() {
				r.pauseCond.Wait()
			}
			r.pauseCond.L.Unlock()
			if r.stopped.Load() {
				return
			}
			continue
		}

		r.readyEvents()

		select {
		case <-tk.C:
			r.node.Tick()
			r.publishView()
		case <-r.advanceC:
		case req := <-r.stepC:
			r.handleStep(req)
		case <-r.stopper.ShouldStop():
			return
		}
	}
}

func (r *Raft) readyEvents() {
	events := r.node.Ready()
	for _, e := range events {
		switch e.Type {
		case types.StoreReq: // 处理存储请求
			r.node.KeepAlive()
			r.handleStoreReq(e)
			continue
		case types.GetLogsReq: // 处理获取日志请求
			r.node.KeepAlive()
			r.handleGetLogsReq(e)
			continue
		case types.TruncateReq: // 截断请求
			r.node.KeepAlive()
			r.handleTruncateReq(e)
			continue
		case types.ApplyReq: // 处理应用请求
			r.node.KeepAlive()
			r.handleApplyReq(e)
			continue
			// 角色转换
		case types.LearnerToFollowerReq,
			types.LearnerToLeaderReq,
			types.FollowerToLeaderReq:
			r.handleRoleChangeReq(e)
			continue
		case types.ProposalFence:
			r.wait.fence(e.Term)
			continue
		}

		if e.To == None {
			r.Foucs("none node event", zap.Any("event", e))
			continue
		}

		if e.To == types.LocalNode || e.To == r.opts.NodeId {
			err := r.node.Step(e)
			r.publishView()
			if err != nil {
				r.node.Error("step error", zap.Error(err))
			}
			continue
		}
		r.opts.Transport.Send(e)
	}
}

func (r *Raft) handleStoreReq(e types.Event) {

	err := r.submitWorker(func() {
		// 追加消息
		err := r.opts.Storage.AppendLogs(e.Logs, e.TermStartIndexInfo)
		if err != nil {
			r.Error("append logs failed", zap.Error(err))
		}
		reason := types.ReasonOk
		if err != nil {
			reason = types.ReasonError
		}
		r.enqueue(types.Event{
			Type:   types.StoreResp,
			Index:  e.Logs[len(e.Logs)-1].Index,
			Logs:   e.Logs,
			Reason: reason,
		})
	})

	if err != nil {
		r.Error("submit append logs failed", zap.Error(err))
		r.enqueue(types.Event{
			Type:   types.StoreResp,
			Reason: types.ReasonError,
		})
	}

}

func (r *Raft) handleGetLogsReq(e types.Event) {

	var leaderLastLogTerm = r.node.lastTermStartIndex.Term

	err := r.submitWorker(func() {
		// 获取裁断日志下标
		var (
			trunctIndex uint64
		)
		if e.Reason != types.ReasonOnlySync {
			var treason types.Reason
			trunctIndex, treason = r.getTrunctLogIndex(e, leaderLastLogTerm)
			if treason != types.ReasonOk {
				r.enqueue(types.Event{
					To:     e.From,
					Type:   types.GetLogsResp,
					Index:  e.Index,
					Reason: treason,
				})
				return
			}
		}

		// 需要裁剪
		if trunctIndex > 0 {
			r.enqueue(types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Index:  trunctIndex,
				Reason: types.ReasonTruncate,
			})
			return
		}

		// 获取日志数据
		logs, err := r.opts.Storage.GetLogs(e.Index, e.StoredIndex+1, r.opts.MaxLogCountPerBatch)
		if err != nil {
			r.node.Error("get logs failed", zap.Error(err))
			r.enqueue(types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Index:  e.Index,
				Reason: types.ReasonError,
			})
			return
		}
		r.enqueue(types.Event{
			To:     e.From,
			Type:   types.GetLogsResp,
			Index:  e.Index,
			Logs:   logs,
			Reason: types.ReasonOk,
		})
	})
	if err != nil {
		r.Error("submit get logs failed", zap.Error(err))
		r.enqueue(types.Event{
			To:     e.From,
			Type:   types.GetLogsResp,
			Index:  e.Index,
			Reason: types.ReasonError,
		})
	}
}

func (r *Raft) handleTruncateReq(e types.Event) {
	err := r.submitWorker(func() {
		r.Info("truncate log to index", zap.Uint64("index", e.Index))
		err := r.opts.Storage.TruncateLogTo(e.Index)
		if err != nil {
			r.Error("truncate logs failed", zap.Error(err))
			r.enqueue(types.Event{
				Type:   types.TruncateResp,
				Reason: types.ReasonError,
			})
			return
		}
		// 删除本地的leader term start index
		err = r.opts.Storage.DeleteLeaderTermStartIndexGreaterThanTerm(e.LastLogTerm)
		if err != nil {
			r.Error("delete leader term start index failed", zap.Error(err), zap.Uint32("LastLogTerm", e.LastLogTerm))
			r.enqueue(types.Event{
				Type:   types.TruncateResp,
				Reason: types.ReasonError,
			})
			return
		}
		r.enqueue(types.Event{
			Type:   types.TruncateResp,
			Index:  e.Index,
			Reason: types.ReasonOk,
		})
	})
	if err != nil {
		r.Error("submit truncate logs failed", zap.Error(err))
		r.enqueue(types.Event{
			Type:   types.TruncateResp,
			Reason: types.ReasonError,
		})
	}
}

func (r *Raft) handleApplyReq(e types.Event) {
	err := r.submitWorker(func() {

		// 已提交
		r.wait.didCommit(e.EndIndex - 1)

		logs, err := r.opts.Storage.GetLogs(e.StartIndex, e.EndIndex, 0)
		if err != nil {
			r.Error("apply logs failed", zap.Error(err))
			r.enqueue(types.Event{
				Type:   types.ApplyResp,
				Reason: types.ReasonError,
			})
			return
		}
		if len(logs) == 0 {
			r.Error("logs is empty", zap.Uint64("startIndex", e.StartIndex), zap.Uint64("endIndex", e.EndIndex))
			r.enqueue(types.Event{
				Type:   types.ApplyResp,
				Reason: types.ReasonError,
			})
			return
		}
		err = r.opts.Storage.Apply(logs)
		if err != nil {
			r.Error("apply logs failed", zap.Error(err))
			r.enqueue(types.Event{
				Type:   types.ApplyResp,
				Reason: types.ReasonError,
			})
			return
		}
		lastLogIndex := logs[len(logs)-1].Index
		r.enqueue(types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonOk,
			Index:  lastLogIndex,
		})
		// Successful apply publishes identity and frontier under one lock.
		r.wait.appliedLogs(logs)
	})
	if err != nil {
		r.Error("submit apply logs failed", zap.Error(err))
		r.enqueue(types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonError,
		})
	}
}

// 根据副本的同步数据，来获取副本的需要裁剪的日志下标，如果不需要裁剪，则返回0
func (r *Raft) getTrunctLogIndex(e types.Event, leaderLastLogTerm uint32) (uint64, types.Reason) {

	// 副本的最新日志任期为0，说明副本没有日志，不需要裁剪
	if e.LastLogTerm == 0 {
		return 0, types.ReasonOk
	}
	// 如果副本的最新日志任期等于当前领导的最新日志任期
	//  如果副本的最新日志下标大于领导者最新日志下标，则需要裁剪至领导者最新日志下标
	if e.LastLogTerm == leaderLastLogTerm {
		if e.Index > 0 {
			replicaLastLogIndex := e.Index - 1 // 副本日志下标等于同步下标-1
			if replicaLastLogIndex > r.node.LastLogIndex() {
				return r.node.LastLogIndex(), types.ReasonOk
			}
		}
		return 0, types.ReasonOk
	}

	// 如果副本的最新日志任期小于当前领导的最新日志任期，则需要裁剪
	if e.LastLogTerm < leaderLastLogTerm {

		term, err := r.opts.Storage.LeaderTermGreaterEqThan(e.LastLogTerm + 1)
		if err != nil {
			r.Error("LeaderTermGreaterEqThan: get leader last term failed", zap.Error(err))
			return 0, types.ReasonError
		}

		// 获取副本的最新日志任期+1的开始日志下标
		termStartIndex, err := r.opts.Storage.GetTermStartIndex(term)
		if err != nil {
			r.Error("get term start index failed", zap.Error(err))
			return 0, types.ReasonError
		}
		if termStartIndex > 0 {
			return termStartIndex - 1, types.ReasonOk
		}
		return termStartIndex, types.ReasonOk
	} else {
		if e.LastLogTerm <= 1 {
			return 0, types.ReasonOk
		}
		term, err := r.opts.Storage.LeaderLastTerm()
		if err != nil {
			r.Error("LeaderLastTerm: get leader last term failed", zap.Error(err))
			return 0, types.ReasonError
		}
		termStartIndex, err := r.opts.Storage.GetTermStartIndex(term)
		if err != nil {
			r.Error("get term start index failed", zap.Error(err))
			return 0, types.ReasonError
		}
		r.Foucs("getTrunctLogIndex: lastLogTerm > leaderLastLogTerm", zap.Uint64("from", e.From), zap.Uint32("e.LastLogTerm", e.LastLogTerm), zap.Uint32("leaderLastLogTerm", leaderLastLogTerm), zap.Uint64("termStartIndex", termStartIndex))
		if termStartIndex > 0 {
			return termStartIndex - 1, types.ReasonOk
		}
		return termStartIndex, types.ReasonOk
	}
}

func (r *Raft) advance() {
	select {
	case r.advanceC <- struct{}{}:
	default:
	}
}

func (r *Raft) handleRoleChangeReq(e types.Event) {

	err := r.submitWorker(func() {

		var (
			newCfg        types.Config
			err           error
			respEventType types.EventType
		)
		switch e.Type {
		case types.LearnerToLeaderReq,
			types.LearnerToFollowerReq:
			newCfg, err = r.learnTo(e.From)
			if err != nil {
				r.Error("learn switch failed", zap.Error(err), zap.String("type", e.Type.String()), zap.String("cfg", r.node.Config().String()))
			}
			if e.Type == types.LearnerToFollowerReq {
				respEventType = types.LearnerToFollowerResp
			} else if e.Type == types.LearnerToLeaderReq {
				respEventType = types.LearnerToLeaderResp
			}
		case types.FollowerToLeaderReq:
			newCfg, err = r.followerToLeader(e.From)
			if err != nil {
				r.Error("follower switch to leader failed", zap.Error(err), zap.String("cfg", r.node.Config().String()))
			}
			respEventType = types.FollowerToLeaderResp
		default:
			err = errors.New("unknown role switch")
		}

		if err != nil {

			r.enqueue(types.Event{
				Type:   respEventType,
				From:   e.From,
				Reason: types.ReasonError,
			})
			return
		}

		err = r.opts.Storage.SaveConfig(newCfg)
		if err != nil {
			r.Error("change role failed", zap.Error(err))
			r.enqueue(types.Event{
				Type:   respEventType,
				From:   e.From,
				Reason: types.ReasonError,
			})
			return
		}
		r.enqueue(types.Event{
			Type:   respEventType,
			From:   e.From,
			Reason: types.ReasonOk,
			Config: newCfg,
		})
	})
	if err != nil {
		r.Error("submit role change req failed", zap.Error(err))

		var respEventType types.EventType

		switch e.Type {
		case types.LearnerToLeaderReq:
			respEventType = types.LearnerToLeaderResp
		case types.LearnerToFollowerReq:
			respEventType = types.LearnerToFollowerResp
		case types.FollowerToLeaderReq:
			respEventType = types.FollowerToLeaderResp
		}
		r.enqueue(types.Event{
			Type:   respEventType,
			From:   e.From,
			Reason: types.ReasonError,
		})
	}
}

// 学习者转换
func (r *Raft) learnTo(learnerId uint64) (types.Config, error) {
	if !wkutil.ArrayContainsUint64(r.node.Config().Learners, learnerId) {
		return types.Config{}, errors.New("promotion target is not a learner")
	}
	cfg := r.node.Config().Clone()

	// orphan learner: 前次迁移已完成（MigrateFrom/MigrateTo 都为 0），
	// 但 learnerId 仍残留在 cfg.Learners 中。此时既无迁移源也无迁移目标，
	// 不能走常规迁移路径，否则会被 learnerId != cfg.MigrateTo 误拒绝。
	orphan := cfg.MigrateTo == 0 && cfg.MigrateFrom == 0 &&
		wkutil.ArrayContainsUint64(cfg.Learners, learnerId)

	if !orphan && learnerId != cfg.MigrateTo {
		r.Warn("learnerId not equal migrateTo", zap.Uint64("learnerId", learnerId), zap.Uint64("migrateTo", cfg.MigrateTo))
		return types.Config{}, errors.New("learnerId not equal migrateTo")
	}

	cfg.Learners = wkutil.RemoveUint64(cfg.Learners, learnerId)

	if orphan {
		// orphan: 把 learnerId 直接加入 Replicas，不切换 leader，不移除旧节点。
		if !wkutil.ArrayContainsUint64(cfg.Replicas, learnerId) {
			cfg.Replicas = append(cfg.Replicas, learnerId)
		}
	} else {
		// 正常迁移路径
		if !wkutil.ArrayContainsUint64(cfg.Replicas, cfg.MigrateTo) {
			cfg.Replicas = append(cfg.Replicas, cfg.MigrateTo)
		}

		if cfg.MigrateFrom != cfg.MigrateTo {
			cfg.Replicas = wkutil.RemoveUint64(cfg.Replicas, cfg.MigrateFrom)
		}

		if cfg.MigrateFrom == cfg.Leader { // 学习者转领导
			cfg.Leader = cfg.MigrateTo
		}
	}

	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil
}

// follower转换成leader
func (r *Raft) followerToLeader(followerId uint64) (types.Config, error) {
	cfg := r.node.Config().Clone()
	if !wkutil.ArrayContainsUint64(cfg.Replicas, followerId) {
		r.Error("followerToLeader: follower not in replicas", zap.Uint64("followerId", followerId))
		return types.Config{}, fmt.Errorf("follower not in replicas")
	}

	cfg.Leader = followerId
	cfg.Term = cfg.Term + 1
	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil

}

type nodeView struct {
	committed  uint64
	leader     uint64
	term       uint32
	isLeader   bool
	generation uint64
}

// Caller routing uses an immutable snapshot, never mutable node fields.
func (r *Raft) publishView() {
	v := nodeView{committed: r.node.queue.committedIndex, leader: r.node.LeaderId(), term: r.node.cfg.Term, isLeader: r.node.IsLeader(), generation: 1}
	r.forwardMu.Lock()
	defer r.forwardMu.Unlock()
	old := r.view.Load()
	if old != nil {
		v.generation = old.(nodeView).generation
	}
	if old != nil && (old.(nodeView).leader != v.leader || old.(nodeView).term != v.term || old.(nodeView).isLeader != v.isLeader) {
		v.generation++
		r.wait.fenceForward()
		r.settleForwardsLocked(proposalUnknown(types.ErrNotLeader))
	}
	r.view.Store(v)
}
func (r *Raft) changeRole(change func()) {
	if !r.started.Load() {
		change()
		r.publishView()
		r.readyFences()
		return
	}
	resp := make(chan error, 1)
	select {
	case r.stepC <- stepReq{change: change, resp: resp}:
	case <-r.stopper.ShouldStop():
		return
	}
	select {
	case <-resp:
	case <-r.stopper.ShouldStop():
	}
}

// Direct role setters before Start are initialization/test-only. Consume their
// fences immediately without dispatching storage work.
func (r *Raft) readyFences() {
	for _, e := range r.node.events {
		if e.Type == types.ProposalFence {
			r.wait.fence(e.Term)
		}
	}
}
func (r *Raft) handleStep(req stepReq) error {
	var err error
	if req.proposal != nil {
		r.handleProposal(req.proposal)
	} else if req.change != nil {
		req.change()
	} else {
		err = r.node.Step(req.event)
	}
	r.publishView()
	r.readyFences()
	if req.resp != nil {
		req.resp <- err
	}
	return err
}

// Worker replies must survive Pause and must not block forever after the loop
// stops consuming. Network Step retains its existing pause policy.
func (r *Raft) enqueue(e types.Event) {
	if r.stopped.Load() {
		return
	}
	select {
	case r.stepC <- stepReq{event: e}:
	case <-r.stopper.ShouldStop():
	}
}
