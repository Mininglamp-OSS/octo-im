package slot

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/bwmarrin/snowflake"
	"go.uber.org/zap"
)

func (s *Server) ReadLeaderState(ctx context.Context, slotID uint32) (raftgroup.ReadState, error) {
	return s.raftGroup.ReadLeaderState(ctx, SlotIdToKey(slotID))
}

type Server struct {
	raftGroup *raftgroup.RaftGroup
	storage   *PebbleShardLogStorage
	opts      *Options
	wklog.Log
	genLogId *snowflake.Node

	slotUpdateLock sync.Mutex
	divergence     divergence
	stopOnce       sync.Once
	stopDone       chan struct{}
	stopErr        error
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts:     opts,
		Log:      wklog.NewWKLog("slot.Server"),
		stopDone: make(chan struct{}),
	}
	s.storage = NewPebbleShardLogStorage(s, path.Join(opts.DataDir, "logdb"), uint32(opts.SlotDbShardNum))
	s.raftGroup = raftgroup.New(raftgroup.NewOptions(raftgroup.WithLogPrefix("slot"), raftgroup.WithStorage(s.storage), raftgroup.WithTransport(opts.Transport)))
	var err error
	s.genLogId, err = snowflake.NewNode(int64(opts.NodeId))
	if err != nil {
		s.Panic("snowflake.NewNode failed", zap.Error(err))
	}
	return s
}

func (s *Server) Start() error {

	err := s.storage.Open()
	if err != nil {
		return err
	}

	err = s.raftGroup.Start()
	if err != nil {
		return err
	}
	s.registerDivergenceMetrics()

	slots := s.opts.Node.Slots()
	for _, slot := range slots {
		s.AddOrUpdateSlotRaft(slot)
	}

	return nil
}

// quiescenceWarnInterval paces the warning while Stop waits for workers.
var quiescenceWarnInterval = 10 * time.Second

// StopWithContext starts the single shutdown lifecycle and waits for it within
// ctx. The lifecycle joins the RaftGroup event loop and every pool task that
// may read or write storage, then closes storage once. On a deadline storage
// stays open and owned by the pending lifecycle; a later call awaits the same
// handoff. Once the lifecycle has finished, every call returns its result.
func (s *Server) StopWithContext(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.raftGroup.BeginStop()
		go func() {
			defer close(s.stopDone)
			// The lifecycle owns storage, so it waits without a deadline.
			err := wkutil.WaitReportingSlow(s.raftGroup.WaitQuiescence, quiescenceWarnInterval, func(elapsed time.Duration) {
				s.Warn("slot shutdown still waiting for workers to quiesce; storage stays open", zap.Duration("elapsed", elapsed))
			})
			if err != nil {
				s.stopErr = err
				return
			}
			s.stopErr = s.storage.Close()
			if s.stopErr != nil {
				s.Error("storage close failed", zap.Error(s.stopErr))
			}
		}()
	})
	select {
	case <-s.stopDone:
		return s.stopErr
	default:
	}
	select {
	case <-s.stopDone:
		return s.stopErr
	case <-ctx.Done():
		return fmt.Errorf("slot storage handoff: %w: %w", raftgroup.ErrIncompleteQuiescence, ctx.Err())
	}
}

func (s *Server) AddEvent(shardNo string, event types.Event) {
	s.raftGroup.AddEvent(shardNo, event)
	s.raftGroup.Advance()
}

func SlotIdToKey(slotId uint32) string {
	return strconv.FormatUint(uint64(slotId), 10)
}

func KeyToSlotId(key string) uint32 {
	slotId, _ := strconv.ParseUint(key, 10, 32)
	return uint32(slotId)
}

func (s *Server) WaitAllSlotReady(ctx context.Context, slotCount int) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ready := true
		rafts := s.raftGroup.GetRafts()
		if slotCount == len(rafts) {
			for _, raft := range rafts {
				if raft.LeaderId() == 0 {
					ready = false
					break
				}
			}
		} else {
			ready = false
		}

		if ready {
			return nil
		}
	}
}

func (s *Server) GenLogId() uint64 {
	return uint64(s.genLogId.Generate().Int64())
}

// 获取频道所在的slotId
func (s *Server) getSlotId(v string) uint32 {
	var slotCount uint32 = s.opts.Node.SlotCount()
	if slotCount == 0 {
		slotCount = s.opts.SlotCount
	}
	return wkutil.GetSlotNum(int(slotCount), v)
}

func (s *Server) AppliedIndex(slotId uint32) (uint64, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.AppliedIndex(shardNo)
}

func (s *Server) LastIndex(slotId uint32) (uint64, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.LastIndex(shardNo)
}

func (s *Server) LastLog(slotId uint32) (types.Log, error) {
	return s.storage.LastLog(SlotIdToKey(slotId))
}

func (s *Server) GetSlotRaft(slotId uint32) *Slot {
	raft := s.raftGroup.GetRaft(SlotIdToKey(slotId))
	if raft == nil {
		return nil
	}
	return raft.(*Slot)
}

func (s *Server) GetLogsInReverseOrder(slotId uint32, startLogIndex uint64, endLogIndex uint64, limit int) ([]types.Log, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.GetLogsInReverseOrder(shardNo, startLogIndex, endLogIndex, limit)
}
