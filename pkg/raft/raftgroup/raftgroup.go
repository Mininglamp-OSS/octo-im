package raftgroup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/track"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type RaftGroup struct {
	raftList   *linkedList
	stopper    *syncutil.Stopper
	opts       *Options
	advanceC   chan struct{}
	readStateC chan readStateRequest
	ownerC     chan ownerRequest

	tmpRafts []IRaft
	stopped  bool

	goPool *ants.Pool
	wklog.Log

	mq *EventQueue

	wait *wait

	// Forwarded proposals awaiting the leader's response; see forward_token.go.
	forwardMu          sync.Mutex
	forwardPending     map[string]*forwardRegistration
	forwardIncarnation uint64
	forwardAttempt     uint64
	forwardStats       forwardStats
	forwardWarned      map[uint64]uint8
	// forwardObserve, if set by a test, sees correlation events under forwardMu.
	forwardObserve func(key, phase, detail string)

	proposalSeq   atomic.Uint64 // receipt identity, unique within the group
	admissionSems sync.Map      // raftKey -> chan struct{}, see admissionSem
	proposalStats proposalStats
	stopOnce      sync.Once
	stopBegun     atomic.Bool
	// loopDone closes once the shutdown lifecycle joined the event loop,
	// fenced pool submission and settled receipts.
	loopDone       chan struct{}
	verificationMu sync.Mutex
	// verificationStopped fences pool submission. BeginStop sets it before it
	// joins anything, so no storage task is accepted once shutdown began.
	verificationStopped bool
	// loopJoined is set once the event loop has exited.
	loopJoined          bool
	verificationWorkers atomic.Int64
	// poolTasks counts accepted pool tasks, all of which may touch Storage.
	// poolIdle closes once the loop is joined and the count is zero.
	poolTasks int
	poolIdle  chan struct{}
	// beforeAdmit, if set by a test, runs on the event loop after a proposal
	// left pending and before it is admitted.
	beforeAdmit func(seq uint64)
}

func New(opts *Options) *RaftGroup {
	rg := &RaftGroup{
		raftList:           newLinkedList(),
		stopper:            syncutil.NewStopper(),
		opts:               opts,
		advanceC:           make(chan struct{}, 1),
		readStateC:         make(chan readStateRequest),
		ownerC:             make(chan ownerRequest),
		Log:                wklog.NewWKLog(fmt.Sprintf("RaftGroup[%s]", opts.LogPrefix)),
		mq:                 NewEventQueue(opts.ReceiveQueueLength, false, 0, 0),
		forwardPending:     make(map[string]*forwardRegistration),
		forwardIncarnation: newForwardIncarnation(),
		forwardWarned:      make(map[uint64]uint8),
		poolIdle:           make(chan struct{}),
		loopDone:           make(chan struct{}),
	}
	rg.wait = newWait(rg.readLog)
	var err error
	rg.goPool, err = ants.NewPool(opts.GoPoolSize, ants.WithNonblocking(true), ants.WithPanicHandler(func(i interface{}) {
		rg.Panic("raftgroup:go pool panic", zap.Any("panic", i), zap.Stack("stack"))
	}))
	if err != nil {
		rg.Panic("create go pool failed", zap.Error(err))
	}
	rg.wait.dispatchVerification = rg.verifyReceiptAsync
	return rg
}

func (rg *RaftGroup) Options() *Options {
	return rg.opts
}

func (rg *RaftGroup) AddRaft(r IRaft) {
	rg.raftList.push(r)
	if rg.opts.Event != nil {
		rg.opts.Event.OnAddRaft(r)
	}
}

func (rg *RaftGroup) RemoveRaft(r IRaft) {
	rg.raftList.remove(r.Key())
	// Nothing will apply or fence this raft's receipts any more.
	rg.wait.removeKey(r.Key(), proposalOutcomeUnknown(ErrRaftNotExist))
	rg.admissionSems.Delete(r.Key())
	if rg.opts.Event != nil {
		rg.opts.Event.OnRemoveRaft(r)
	}
}

func (rg *RaftGroup) GetRaft(raftKey string) IRaft {
	return rg.raftList.get(raftKey)
}

func (rg *RaftGroup) GetRafts() []IRaft {

	return rg.raftList.all()
}

func (rg *RaftGroup) GetRaftCount() int {
	return rg.raftList.count()
}

func (rg *RaftGroup) IsLeader(raftKey string) bool {
	raft := rg.raftList.get(raftKey)
	if raft == nil {
		return false
	}
	return raft.IsLeader()
}

func (rg *RaftGroup) Start() error {
	rg.stopper.RunWorker(rg.loopEvent)
	return nil
}

// Stop runs the shutdown lifecycle and waits until it joined the event loop
// and settled receipts. Submission is fenced from BeginStop on. Stop does not
// wait for pool tasks accepted before the fence, which may still be inside
// Storage: a caller that releases Storage must use WaitQuiescence. Because Stop
// never joins pool tasks, a task that hands its result to the loop cannot
// deadlock it.
func (rg *RaftGroup) Stop() {
	rg.BeginStop()
	<-rg.loopDone
}

// BeginStop starts the single shutdown lifecycle without waiting for it.
// On return no pool task can be accepted any more and AddEventWait refuses,
// so no storage task starts after this point; tasks accepted before stay
// owned until WaitQuiescence. Callers blocked on the group observe the stop
// signal immediately; the event loop may still be inside synchronous Storage
// I/O such as SaveHardState, so its join runs on the lifecycle goroutine and
// WaitQuiescence bounds it.
func (rg *RaftGroup) BeginStop() {
	rg.stopOnce.Do(func() {
		rg.verificationMu.Lock()
		rg.verificationStopped = true
		rg.stopBegun.Store(true)
		rg.verificationMu.Unlock()
		go func() {
			// Storage work the loop requests from here on is refused and
			// answered with ReasonError, which it never steps: it exits at its
			// next iteration.
			rg.stopper.Stop()
			rg.verificationMu.Lock()
			rg.loopJoined = true
			if rg.poolTasks == 0 {
				close(rg.poolIdle)
			}
			rg.verificationMu.Unlock()
			// Proposals still queued are abandoned by their callers; receipts of
			// admitted ones can no longer complete.
			rg.wait.completeAll(proposalOutcomeUnknown(ErrGroupStopped))
			close(rg.loopDone)
		}()
	})
}

// readLog point-reads one stored log for receipt verification.
func (rg *RaftGroup) readLog(raftKey string, index uint64) (types.Log, bool, error) {
	logs, err := rg.opts.Storage.GetLogs(raftKey, index, index+1, 0)
	if err != nil {
		return types.Log{}, false, err
	}
	for _, log := range logs {
		if log.Index == index {
			return log, true, nil
		}
	}
	return types.Log{}, false, nil
}

func (rg *RaftGroup) Advance() {
	select {
	case rg.advanceC <- struct{}{}:
	default:
	}
}

// AddEvent 添加事件
func (rg *RaftGroup) AddEvent(raftKey string, e types.Event) {

	if e.Type == types.SendPropose {
		err := rg.submit(func() {
			rg.handleSendPropose(raftKey, e)
		})
		if err != nil {
			rg.Error("submit go pool failed", zap.Error(err))
		}
		return
	} else if e.Type == types.SendProposeResp {
		rg.handleSendProposeResp(raftKey, e)
		return
	}

	rg.mq.MustAdd(Event{
		RaftKey: raftKey,
		Event:   e,
	})
}

// AddEventWait steps e on the event loop and returns its result. It returns
// ErrGroupStopped, and e is never stepped, if shutdown began before the loop
// took the event; a result the loop already produced is kept.
func (rg *RaftGroup) AddEventWait(raftKey string, e types.Event) error {
	if rg.stopBegun.Load() {
		return ErrGroupStopped
	}
	waitC := make(chan error, 1)
	claim := new(atomic.Bool)
	rg.mq.MustAdd(Event{
		RaftKey: raftKey,
		Event:   e,
		WaitC:   waitC,
		claim:   claim,
	})
	rg.Advance()
	select {
	case err := <-waitC:
		return err
	case <-rg.stopper.ShouldStop():
	}
	if claim.CompareAndSwap(false, true) {
		// The loop never took the event and now never will.
		return ErrGroupStopped
	}
	// The loop is handling it in memory; its result follows.
	return <-waitC
}

func (rg *RaftGroup) TryAddEvent(raftKey string, e types.Event) {
	if e.Type == types.SendPropose {
		rg.handleSendPropose(raftKey, e)
		return
	} else if e.Type == types.SendProposeResp {
		rg.handleSendProposeResp(raftKey, e)
		return
	}

	rg.mq.Add(Event{
		RaftKey: raftKey,
		Event:   e,
	})
}

func (rg *RaftGroup) loopEvent() {
	tk := time.NewTicker(rg.opts.TickInterval)
	for !rg.stopped {
		if rg.stopBegun.Load() {
			// Storage submission is fenced; do not step its refusals.
			return
		}
		rg.readyEvents()
		select {
		case <-tk.C:
			rg.ticks()
		case <-rg.advanceC:
		case req := <-rg.ownerC:
			if err := req.ctx.Err(); err != nil {
				req.result <- err
			} else {
				req.result <- req.fn(rg.GetRaft(req.key))
			}
		case req := <-rg.readStateC:
			rg.handleReadState(req)
		case <-rg.stopper.ShouldStop():
			return
		}
	}
}

func (rg *RaftGroup) readyEvents() {
	hasEvent := false
	// 处理收到的事件
	has := rg.handleReceivedEvents()
	if has {
		hasEvent = true
	}
	// 处理产生的事件
	has = rg.handleReadys()
	if has {
		hasEvent = true
	}

	if hasEvent {
		rg.Advance()
	}
}

// 处理收到的事件
func (rg *RaftGroup) handleReceivedEvents() bool {
	events := rg.mq.Get()
	if len(events) == 0 {
		return false
	}
	hasEvent := false
	for _, e := range events {
		if e.claim != nil && !e.claim.CompareAndSwap(false, true) {
			continue // abandoned by its waiter after shutdown began
		}
		raft := rg.raftList.get(e.RaftKey)
		if e.Proposal != nil {
			if raft == nil {
				rg.Warn("raft not found", zap.String("raftKey", e.RaftKey), zap.String("event", e.Event.Type.String()))
			}
			rg.admitProposal(raft, e)
			hasEvent = true
			continue
		}
		if raft == nil {
			rg.Warn("raft not found", zap.String("raftKey", e.RaftKey), zap.String("event", e.Event.Type.String()))
			if e.WaitC != nil {
				e.WaitC <- ErrRaftNotExist
			}
			continue
		}

		// 轨迹记录
		if e.Type == types.Propose {
			for i := range e.Logs {
				e.Logs[i].Record.Add(track.PositionPropose)
			}
		}
		hasEvent = true
		err := raft.Step(e.Event)
		if err != nil {
			rg.Error("step failed", zap.Error(err), zap.String("handleKey", raft.Key()))
		}
		if e.WaitC != nil {
			e.WaitC <- err
		}
	}
	return hasEvent
}

func (rg *RaftGroup) handleReadys() bool {
	rg.raftList.readHandlers(&rg.tmpRafts)
	hasEvent := false
	for _, r := range rg.tmpRafts {
		if r.HasReady() {
			has := rg.handleReady(r)
			if has {
				hasEvent = true
			}
		}
	}
	rg.tmpRafts = rg.tmpRafts[:0]
	return hasEvent
}

func (rg *RaftGroup) ticks() {
	rg.raftList.readHandlers(&rg.tmpRafts)
	for _, r := range rg.tmpRafts {
		r.Tick()
	}
	rg.tmpRafts = rg.tmpRafts[:0]
}

func (rg *RaftGroup) handleReady(r IRaft) bool {
	events := r.Ready()
	if len(events) == 0 {
		return false
	}
	for _, e := range events {
		switch e.Type {
		case types.StoreReq: // 处理存储请求
			r.KeepAlive()
			rg.handleStoreReq(r, e)
			continue
		case types.GetLogsReq: // 处理获取日志请求
			r.KeepAlive()
			rg.handleGetLogsReq(r, e)
			continue
		case types.TruncateReq: // 处理截断请求
			r.KeepAlive()
			rg.handleTruncateReq(r, e)
		case types.ApplyReq: // 处理应用请求
			r.KeepAlive()
			rg.handleApplyReq(r, e)
			continue
		case types.Destory: // 处理销毁请求
			rg.handleDestory(r)
			continue
		case types.ProposalFence: // 领导任期结束，旧回执不能再按本地日志身份完成
			rg.wait.fence(r.Key(), e.Term)
			continue

			// 角色转换
		case types.LearnerToFollowerReq,
			types.LearnerToLeaderReq,
			types.FollowerToLeaderReq:
			r.KeepAlive()
			rg.handleRoleChangeReq(r, e)
			continue
		}
		if e.To == 0 {
			rg.Foucs("none node event", zap.Any("event", e))
			continue
		}
		if e.To == types.LocalNode {
			err := r.Step(e)
			if err != nil {
				rg.Error("step error", zap.Error(err))
			}
			continue
		}
		rg.opts.Transport.Send(r.Key(), e)
	}
	return true
}
