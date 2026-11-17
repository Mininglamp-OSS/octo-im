package slot

import (
	"context"
	"errors"
	"math"
	"path"
	"strconv"
	"sync"

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
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts: opts,
		Log:  wklog.NewWKLog("slot.Server"),
	}
	s.storage = NewPebbleShardLogStorage(s, path.Join(opts.DataDir, "logdb"), uint32(opts.SlotDbShardNum))
	rgOpts := []raftgroup.Option{
		raftgroup.WithLogPrefix("slot"),
		raftgroup.WithStorage(s.storage),
		raftgroup.WithTransport(opts.Transport),
	}
	if opts.MaxLogSizePerBatch > 0 {
		rgOpts = append(rgOpts, raftgroup.WithMaxLogSizePerBatch(opts.MaxLogSizePerBatch))
	}
	if opts.MaxLogCountPerBatch > 0 {
		rgOpts = append(rgOpts, raftgroup.WithMaxLogCountPerBatch(opts.MaxLogCountPerBatch))
	}
	s.raftGroup = raftgroup.New(raftgroup.NewOptions(rgOpts...))
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

	slots := s.opts.Node.Slots()
	for _, slot := range slots {
		s.AddOrUpdateSlotRaft(slot)
	}

	return nil
}

func (s *Server) Stop() {
	s.raftGroup.Stop()

	err := s.storage.Close()
	if err != nil {
		s.Error("storage close failed", zap.Error(err))
	}
}

func (s *Server) AddEvent(shardNo string, event types.Event) {
	s.raftGroup.AddEvent(shardNo, event)
	s.raftGroup.Advance()
}

func (s *Server) Storage() *PebbleShardLogStorage {
	if s == nil {
		return nil
	}
	return s.storage
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

const (
	MaxSlotLogsRangeCount = 10000
	MaxSlotLogsRangeBytes = 32 * 1024 * 1024
)

var ErrSlotLogsRangeTooLarge = errors.New("requested slot log range exceeds maximum limit")

func (s *Server) WalkLogsRange(slotId uint32, fromLogIndex uint64, toLogIndex uint64, maxEntries int, fn func(types.Log) error) (int, error) {
	if fromLogIndex == 0 {
		fromLogIndex = 1
	}
	if toLogIndex < fromLogIndex {
		return 0, nil
	}
	if maxEntries <= 0 || maxEntries > MaxSlotLogsRangeCount {
		maxEntries = MaxSlotLogsRangeCount
	}
	if toLogIndex == math.MaxUint64 {
		lastIdx, err := s.LastIndex(slotId)
		if err != nil {
			return 0, err
		}
		if lastIdx < fromLogIndex {
			return 0, nil
		}
		toLogIndex = lastIdx
		if toLogIndex == math.MaxUint64 {
			toLogIndex = math.MaxUint64 - 1
		}
	}
	if toLogIndex-fromLogIndex >= uint64(maxEntries) {
		return 0, ErrSlotLogsRangeTooLarge
	}

	batchBytes := uint64(2 * 1024 * 1024)
	if s.opts != nil && s.opts.MaxLogSizePerBatch > 0 {
		batchBytes = s.opts.MaxLogSizePerBatch
	}

	shardNo := SlotIdToKey(slotId)
	cur := fromLogIndex
	totalCount := 0
	var totalBytes uint64
	for cur <= toLogIndex {
		logs, err := s.storage.GetLogs(shardNo, cur, toLogIndex+1, batchBytes)
		if err != nil {
			return totalCount, err
		}
		if len(logs) == 0 {
			break
		}
		for _, lg := range logs {
			if lg.Index > toLogIndex {
				return totalCount, nil
			}
			totalCount++
			totalBytes += uint64(lg.LogSize())
			if totalCount > maxEntries || totalBytes > MaxSlotLogsRangeBytes {
				return totalCount, ErrSlotLogsRangeTooLarge
			}
			if fn != nil {
				if err := fn(lg); err != nil {
					return totalCount, err
				}
			}
		}
		last := logs[len(logs)-1].Index
		if last < cur || last >= toLogIndex || last == math.MaxUint64 {
			break
		}
		cur = last + 1
	}
	return totalCount, nil
}

func (s *Server) GetLogsRange(slotId uint32, fromLogIndex uint64, toLogIndex uint64) ([]types.Log, error) {
	var all []types.Log
	_, err := s.WalkLogsRange(slotId, fromLogIndex, toLogIndex, MaxSlotLogsRangeCount, func(lg types.Log) error {
		all = append(all, lg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return all, nil
}
