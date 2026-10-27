package channel

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

type storage struct {
	db wkdb.DB
	s  *Server
	wklog.Log

	// Diagnostics for GetLogs emission. Entries whose ChannelId or
	// ChannelType column differs from the key are emitted as stored.
	formatErrors            sync.Map // reason -> *atomic.Uint64
	inconsistentChannelID   atomic.Uint64
	inconsistentChannelType atomic.Uint64
	formatLogMu             sync.Mutex
	formatLogged            map[formatLogKey]*formatLogState
}

type formatLogKey struct {
	key    string
	index  uint64
	reason string
}

type formatLogState struct {
	last       time.Time
	suppressed uint64
}

const (
	formatLogInterval = time.Minute
	formatLogMaxKeys  = 1024
)

func newStorage(db wkdb.DB, s *Server) *storage {
	return &storage{db: db, s: s, Log: wklog.NewWKLog("channel_storage")}
}

// reportFormatError counts every emission rejection and logs it at most once
// per (key, index, reason) and interval, because each follower retry of a
// stalled channel hits the same entry again.
func (s *storage) reportFormatError(e *wkdb.ChannelLogFormatError) {
	c, _ := s.formatErrors.LoadOrStore(e.Reason, new(atomic.Uint64))
	c.(*atomic.Uint64).Add(1)

	now := time.Now()
	k := formatLogKey{key: e.Key, index: e.Index, reason: e.Reason}
	s.formatLogMu.Lock()
	if s.formatLogged == nil || len(s.formatLogged) >= formatLogMaxKeys {
		s.formatLogged = make(map[formatLogKey]*formatLogState)
	}
	st := s.formatLogged[k]
	if st != nil && now.Sub(st.last) < formatLogInterval {
		st.suppressed++
		s.formatLogMu.Unlock()
		return
	}
	var suppressed uint64
	if st != nil {
		suppressed = st.suppressed
	}
	s.formatLogged[k] = &formatLogState{last: now}
	s.formatLogMu.Unlock()
	s.Error("GetLogs: stored message cannot be emitted without loss", zap.String("key", e.Key), zap.Uint64("index", e.Index), zap.String("reason", e.Reason), zap.Uint64("suppressed", suppressed))
}

// formatErrorCount returns how many emissions were rejected for reason.
func (s *storage) formatErrorCount(reason string) uint64 {
	c, ok := s.formatErrors.Load(reason)
	if !ok {
		return 0
	}
	return c.(*atomic.Uint64).Load()
}

func (s *storage) GetState(channelId string, channelType uint8) (types.RaftState, error) {
	lastMsg, err := s.db.GetLastMsg(channelId, channelType)
	if err != nil {
		return types.RaftState{}, err
	}

	applied, err := s.db.GetChannelAppliedIndex(channelId, channelType)
	if err != nil {
		return types.RaftState{}, err
	}
	if applied > uint64(lastMsg.MessageSeq) {
		return types.RaftState{}, fmt.Errorf("channel applied index exceeds log tail")
	}
	state, err := s.db.RaftHardState(wkutil.ChannelToKey(channelId, channelType))
	if err != nil {
		return types.RaftState{}, err
	}
	return types.RaftState{
		HardState:    state,
		LastLogIndex: uint64(lastMsg.MessageSeq),
		LastTerm:     uint32(lastMsg.Term),
		AppliedIndex: applied,
	}, nil
}

func (s *storage) AppendLogs(key string, logs []types.Log, termStartIndexInfo *types.TermStartIndexInfo) error {

	channelId, channelType := wkutil.ChannelFromlKey(key)

	messages := make([]wkdb.Message, 0, len(logs))
	for _, log := range logs {
		var msg wkdb.Message
		err := msg.Unmarshal(log.Data)
		if err != nil {
			s.Error("AppendLogs: unmarshal message error", zap.Error(err))
			return err
		}
		msg.MessageSeq = uint32(log.Index)
		msg.Term = uint64(log.Term)
		messages = append(messages, msg)
	}
	err := s.db.AppendMessages(channelId, channelType, messages)
	if err != nil {
		return err
	}
	if termStartIndexInfo != nil {
		err = s.db.SetLeaderTermStartIndex(key, termStartIndexInfo.Term, termStartIndexInfo.Index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) GetTermStartIndex(key string, term uint32) (uint64, error) {

	return s.db.LeaderTermStartIndex(key, term)
}

func (s *storage) LeaderLastLogTerm(key string) (uint32, error) {
	return s.db.LeaderLastTerm(key)
}

func (s *storage) LeaderTermGreaterEqThan(key string, term uint32) (uint32, error) {
	return s.db.LeaderLastTermGreaterEqThan(key, term)
}

func (s *storage) GetLogs(key string, startLogIndex uint64, endLogIndex uint64, limitSize uint64) ([]types.Log, error) {
	channelID, channelType := wkutil.ChannelFromlKey(key)
	var (
		messages []wkdb.Message
		err      error
	)

	lastIdx, err := s.LastIndex(key)
	if err != nil {
		return nil, err
	}

	if endLogIndex == 0 || endLogIndex > lastIdx+1 {
		endLogIndex = lastIdx + 1
	}

	messages, err = s.db.LoadNextRangeMsgsForSize(channelID, channelType, startLogIndex, endLogIndex, limitSize)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, nil
	}
	logs := make([]types.Log, len(messages))
	for i := range messages {
		msg := &messages[i]
		if msg.ChannelID != channelID {
			s.inconsistentChannelID.Add(1)
		}
		if msg.ChannelType != channelType {
			s.inconsistentChannelType.Add(1)
		}
		// Fail the whole call on a format error: no partial batch and no
		// lossy encoding. The caller answers the follower with ReasonError.
		data, err := wkdb.EncodeChannelLogData(msg)
		if err != nil {
			var formatErr *wkdb.ChannelLogFormatError
			if errors.As(err, &formatErr) {
				formatErr.Key = key
				s.reportFormatError(formatErr)
			}
			return nil, err
		}
		logs[i] = types.Log{
			Id:    uint64(msg.MessageID),
			Index: uint64(msg.MessageSeq),
			Term:  uint32(msg.Term),
			Data:  data,
		}
	}
	return logs, nil
}

func (s *storage) Apply(key string, logs []types.Log) error {
	if len(logs) == 0 {
		return nil
	}
	id, typ := wkutil.ChannelFromlKey(key)
	return s.db.UpdateChannelAppliedIndex(id, typ, logs[len(logs)-1].Index)
}

// AppendLogs already writes the message state. Once Raft confirms commitment,
// applying it only needs a durable boundary, including on a legacy channel
// with no marker. Do not deserialize its entire history on the ACK path.
func (s *storage) ApplyCommittedRange(key string, start, end uint64) error {
	if start == 0 || end <= start {
		return fmt.Errorf("invalid committed channel range [%d,%d)", start, end)
	}
	last, err := s.LastIndex(key)
	if err != nil {
		return err
	}
	if end-1 > last {
		return fmt.Errorf("committed channel range exceeds stored tail")
	}
	id, typ := wkutil.ChannelFromlKey(key)
	return s.db.UpdateChannelAppliedIndex(id, typ, end-1)
}

func (s *storage) SaveConfig(key string, cfg types.Config) error {
	if s.s.opts.OnSaveConfig != nil {
		channelId, channelType := wkutil.ChannelFromlKey(key)
		err := s.s.opts.OnSaveConfig(channelId, channelType, cfg)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *storage) TruncateLogTo(key string, index uint64) error {
	channelId, channelType := wkutil.ChannelFromlKey(key)
	return s.db.TruncateLogTo(channelId, channelType, index)
}

// 最后一条日志的索引
func (s *storage) LastIndex(key string) (uint64, error) {
	channelId, channelType := wkutil.ChannelFromlKey(key)

	lastMsgSeq, _, err := s.db.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return 0, err
	}
	return uint64(lastMsgSeq), nil
}

// DeleteLeaderTermStartIndexGreaterThanTerm 删除大于term的领导任期和开始索引
func (s *storage) DeleteLeaderTermStartIndexGreaterThanTerm(key string, term uint32) error {

	return s.db.DeleteLeaderTermStartIndexGreaterThanTerm(key, term)
}

func (s *storage) LastIndexAndAppendTime(shardNo string) (uint64, uint64, error) {
	channelId, channelType := wkutil.ChannelFromlKey(shardNo)
	lastMsgSeq, appendTime, err := s.db.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return 0, 0, err
	}
	return uint64(lastMsgSeq), appendTime, nil
}

func (s *storage) getLastMessage(channelId string, channelType uint8) (wkdb.Message, error) {

	return s.db.GetLastMsg(channelId, channelType)
}
