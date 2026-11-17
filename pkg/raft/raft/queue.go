package raft

import (
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type termRange struct {
	startIndex uint64
	endIndex   uint64
	term       uint32
}

type queue struct {
	mu         sync.RWMutex
	logs       []types.Log
	revision   uint64 // owner-only generation of appended/replaced/truncated log content
	storeEpoch uint64 // owner-only generation incremented on truncation to fence in-flight StoreResp
	termRanges []termRange
	wklog.Log

	// 存储日志偏移下标，比如日志 1 2 3 4 5 6 7 8 9，存储的日志偏移是6 表示1 2 3 4 5 6已经存储
	storedIndex         uint64
	lastLogIndex        uint64 // 最后一条日志下标
	committedIndex      uint64 // 已提交的日志下标
	appending           bool   // 是否在追加中
	appendingStoreEpoch uint64 // storeEpoch captured when current in-flight StoreReq started
	applying            bool   // 是否在应用中
	appliedIndex        uint64
}

func (r *queue) LastLogIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastLogIndex
}

func (r *queue) CommittedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.committedIndex
}

func (r *queue) setCommittedIndex(idx uint64) {
	r.mu.Lock()
	r.committedIndex = idx
	r.mu.Unlock()
}

func (r *queue) AppliedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.appliedIndex
}

func (r *queue) StoredIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.storedIndex
}

func (r *queue) IsApplying() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.applying
}

func (r *queue) setApplying(v bool) {
	r.mu.Lock()
	r.applying = v
	r.mu.Unlock()
}

func newQueue(key string, appliedLogIndex, lastLogIndex uint64) *queue {

	q := &queue{
		Log:            wklog.NewWKLog(fmt.Sprintf("queue[%s]", key)),
		storedIndex:    lastLogIndex,
		lastLogIndex:   lastLogIndex,
		committedIndex: appliedLogIndex,
		appliedIndex:   appliedLogIndex,
	}
	return q
}

func (r *queue) recordTermRange(startIndex, endIndex uint64, term uint32) {
	if startIndex == 0 || endIndex < startIndex || term == 0 {
		return
	}
	r.truncateTermRangesTo(startIndex - 1)
	if n := len(r.termRanges); n > 0 && r.termRanges[n-1].term == term && r.termRanges[n-1].endIndex+1 >= startIndex {
		if endIndex > r.termRanges[n-1].endIndex {
			r.termRanges[n-1].endIndex = endIndex
		}
		return
	}
	r.termRanges = append(r.termRanges, termRange{
		startIndex: startIndex,
		endIndex:   endIndex,
		term:       term,
	})
	if len(r.termRanges) > 64 {
		r.termRanges = append([]termRange(nil), r.termRanges[len(r.termRanges)-64:]...)
	}
}

func (r *queue) recordTermRanges(logs []types.Log) {
	for _, lg := range logs {
		r.recordTermRange(lg.Index, lg.Index, lg.Term)
	}
}

func (r *queue) truncateTermRangesTo(logIndex uint64) {
	if logIndex == 0 {
		r.termRanges = nil
		return
	}
	for len(r.termRanges) > 0 {
		last := &r.termRanges[len(r.termRanges)-1]
		if last.startIndex > logIndex {
			r.termRanges = r.termRanges[:len(r.termRanges)-1]
			continue
		}
		if last.endIndex > logIndex {
			last.endIndex = logIndex
		}
		break
	}
}

func (r *queue) knownTermAt(index uint64) (uint32, bool) {
	if index == 0 || index > r.lastLogIndex {
		return 0, false
	}
	if index > r.storedIndex {
		offset := index - r.storedIndex - 1
		if offset < uint64(len(r.logs)) && r.logs[offset].Index == index {
			return r.logs[offset].Term, true
		}
	}
	for i := len(r.termRanges) - 1; i >= 0; i-- {
		tr := r.termRanges[i]
		if index >= tr.startIndex && index <= tr.endIndex {
			return tr.term, true
		}
	}
	return 0, false
}

func (r *queue) append(incomingLogs ...types.Log) error {
	if len(incomingLogs) > 0 {
		r.revision++
	}
	// 1. 参数验证：检查是否有日志需要追加
	if len(incomingLogs) == 0 {
		r.Debug("append operation skipped: no logs provided")
		return nil
	}

	if incomingLogs[0].Index == 0 {
		return types.ErrLogIndexNotContinuous
	}

	// 2. 验证输入日志序列的连续性
	for i := 1; i < len(incomingLogs); i++ {
		previousLogIndex := incomingLogs[i-1].Index
		currentLogIndex := incomingLogs[i].Index

		if currentLogIndex != previousLogIndex+1 {
			r.Error("incoming log sequence is not continuous",
				zap.Uint64("previousLogIndex", previousLogIndex),
				zap.Uint64("currentLogIndex", currentLogIndex),
				zap.Uint64("expectedIndex", previousLogIndex+1),
				zap.Int("brokenAtPosition", i),
				zap.Int("totalIncomingLogs", len(incomingLogs)))
			return fmt.Errorf("incoming log sequence broken at position %d: expected index %d, got %d",
				i, previousLogIndex+1, currentLogIndex)
		}
	}

	// 3. 提取关键索引信息
	incomingFirstIndex := incomingLogs[0].Index
	incomingLastIndex := incomingLogs[len(incomingLogs)-1].Index
	incomingLogCount := len(incomingLogs)
	expectedPrevIndex := incomingFirstIndex - 1 // 新日志序列期望的前一个索引

	r.Debug("attempting to append logs",
		zap.Uint64("queueLastIndex", r.lastLogIndex),
		zap.Uint64("incomingFirstIndex", incomingFirstIndex),
		zap.Uint64("incomingLastIndex", incomingLastIndex),
		zap.Int("incomingLogCount", incomingLogCount),
		zap.Uint64("expectedPrevIndex", expectedPrevIndex))

	// 4. 场景1：正常的连续追加
	if expectedPrevIndex == r.lastLogIndex {
		r.mu.Lock()
		r.logs = append(r.logs, incomingLogs...)
		r.lastLogIndex = incomingLastIndex
		r.mu.Unlock()
		r.recordTermRanges(incomingLogs)

		r.Debug("logs appended successfully",
			zap.Uint64("newQueueLastIndex", r.lastLogIndex),
			zap.Uint64("appendedRangeFrom", incomingFirstIndex),
			zap.Uint64("appendedRangeTo", incomingLastIndex),
			zap.Int("appendedCount", incomingLogCount),
			zap.Int("totalQueueSize", len(r.logs)))
		return nil
	}

	// 5. 场景2：日志重叠或冲突处理（先比较任期与前缀，保留合法同任期重复日志，仅截断冲突的未提交后缀）
	if incomingFirstIndex <= r.lastLogIndex {
		overlapSize := r.lastLogIndex - incomingFirstIndex + 1

		r.Warn("detected log overlap or conflict",
			zap.Uint64("queueLastIndex", r.lastLogIndex),
			zap.Uint64("incomingFirstIndex", incomingFirstIndex),
			zap.Uint64("incomingLastIndex", incomingLastIndex),
			zap.Uint64("overlapSize", overlapSize),
			zap.Int("incomingLogCount", incomingLogCount))

		var (
			hasConflict      bool
			firstConflictIdx uint64
			hasKnownMatch    bool
			firstUnknownIdx  uint64
		)
		for _, inLog := range incomingLogs {
			if inLog.Index > r.lastLogIndex {
				break
			}
			if knownTerm, ok := r.knownTermAt(inLog.Index); ok {
				if knownTerm != inLog.Term {
					hasConflict = true
					firstConflictIdx = inLog.Index
					break
				}
				hasKnownMatch = true
			} else if firstUnknownIdx == 0 {
				firstUnknownIdx = inLog.Index
			}
		}

		// 5a. 同任期重复或合法已提交前缀重叠（无任期冲突）：保留已有前缀，仅追加超出 lastLogIndex 的新后缀
		if !hasConflict && (hasKnownMatch || incomingFirstIndex <= r.committedIndex) {
			if incomingLastIndex <= r.lastLogIndex {
				r.Info("ignoring completely duplicate same-term logs",
					zap.Uint64("incomingRangeFrom", incomingFirstIndex),
					zap.Uint64("incomingRangeTo", incomingLastIndex),
					zap.Uint64("queueRangeFrom", r.storedIndex+1),
					zap.Uint64("queueRangeTo", r.lastLogIndex),
					zap.String("action", "no_change_needed"))
				return nil
			}
			newSuffix := incomingLogs[r.lastLogIndex-incomingFirstIndex+1:]
			r.mu.Lock()
			r.logs = append(r.logs, newSuffix...)
			r.lastLogIndex = incomingLastIndex
			r.mu.Unlock()
			r.recordTermRanges(newSuffix)
			return nil
		}

		// 5b. 存在任期冲突（或未记录任期的未提交重叠区间）：计算截断边界
		truncateFrom := incomingFirstIndex
		if hasConflict {
			if firstUnknownIdx != 0 && firstUnknownIdx < firstConflictIdx {
				truncateFrom = firstUnknownIdx
			} else {
				truncateFrom = firstConflictIdx
			}
		}
		truncateToIndex := truncateFrom - 1
		if truncateToIndex < r.committedIndex {
			return types.ErrCommittedLogConflict
		}
		originalQueueSize := len(r.logs)

		r.Warn("resolving log conflict by truncation",
			zap.Uint64("truncateToIndex", truncateToIndex),
			zap.Uint64("originalQueueLastIndex", r.lastLogIndex),
			zap.Int("originalQueueSize", originalQueueSize),
			zap.String("action", "truncate_and_reappend"))

		r.truncateLogTo(truncateToIndex)

		suffixToAppend := incomingLogs[truncateFrom-incomingFirstIndex:]
		r.mu.Lock()
		r.logs = append(r.logs, suffixToAppend...)
		r.lastLogIndex = incomingLastIndex
		r.mu.Unlock()
		r.recordTermRanges(suffixToAppend)
		return nil
	}

	// 6. 场景3：日志间隙检测
	if incomingFirstIndex > r.lastLogIndex+1 {
		gapSize := incomingFirstIndex - r.lastLogIndex - 1

		r.Error("detected log gap - cannot append non-continuous logs",
			zap.Uint64("queueLastIndex", r.lastLogIndex),
			zap.Uint64("expectedNextIndex", r.lastLogIndex+1),
			zap.Uint64("incomingFirstIndex", incomingFirstIndex),
			zap.Uint64("gapSize", gapSize),
			zap.Uint64("missingRangeFrom", r.lastLogIndex+1),
			zap.Uint64("missingRangeTo", incomingFirstIndex-1),
			zap.Int("incomingLogCount", incomingLogCount))

		return fmt.Errorf("log gap detected: queue ends at %d, incoming starts at %d (missing %d logs from %d to %d)",
			r.lastLogIndex, incomingFirstIndex, gapSize, r.lastLogIndex+1, incomingFirstIndex-1)
	}

	// 7. 不应该到达的代码路径
	r.Error("unexpected append scenario encountered",
		zap.Uint64("queueLastIndex", r.lastLogIndex),
		zap.Uint64("expectedPrevIndex", expectedPrevIndex),
		zap.Uint64("incomingFirstIndex", incomingFirstIndex),
		zap.Uint64("incomingLastIndex", incomingLastIndex),
		zap.Int("incomingLogCount", incomingLogCount),
		zap.String("error", "logic_error_please_investigate"))

	return fmt.Errorf("unexpected append scenario: queue_last=%d, incoming_first=%d, incoming_last=%d, count=%d",
		r.lastLogIndex, incomingFirstIndex, incomingLastIndex, incomingLogCount)
}

func (r *queue) hasNextStoreLogs() bool {
	if r.appending {
		return false
	}
	return r.storedIndex < r.lastLogIndex
}

func (r *queue) nextStoreLogs(maxSize int) []types.Log {
	if len(r.logs) == 0 {
		return nil
	}
	endIndex := r.lastLogIndex - r.storedIndex
	if endIndex > uint64(len(r.logs)) {
		r.Panic("nextAppendLogs endIndex is out of bound", zap.Uint64("endIndex", endIndex), zap.Int("logsLen", len(r.logs)))
	}
	if maxSize != 0 && endIndex > uint64(maxSize) {
		endIndex = uint64(maxSize)
	}
	// Storage accepts one term-start record per append. Historical catch-up
	// can contain several terms; persist each boundary before acknowledging
	// the next one, so LastLogTerm cannot describe only the first term while
	// storedIndex already includes later terms.
	for i := uint64(1); i < endIndex; i++ {
		if r.logs[i].Term != r.logs[0].Term {
			endIndex = i
			break
		}
	}
	r.mu.Lock()
	r.appending = true
	r.appendingStoreEpoch = r.storeEpoch
	batch := append([]types.Log(nil), r.logs[:endIndex]...)
	r.mu.Unlock()
	return batch
}

func (r *queue) hasNextApplyLogs() bool {
	if r.applying {
		return false
	}
	return r.appliedIndex < r.committedIndex
}

func (r *queue) nextApplyLogs() (uint64, uint64) {
	if r.applying {
		return 0, 0
	}
	r.mu.Lock()
	r.applying = true
	r.mu.Unlock()
	if r.appliedIndex > r.committedIndex {
		return 0, 0
	}

	return r.appliedIndex + 1, r.committedIndex + 1
}

// storeTo 存储日志到指定日志下标，比如storeTo(6)，如果日志是1 2 3 4 5 6 7 8 9，截取后是7 8 9
func (r *queue) storeTo(logIndex uint64) {
	r.mu.Lock()
	r.appending = false
	if logIndex <= r.storedIndex {
		r.mu.Unlock()
		return
	}
	if logIndex > r.lastLogIndex {
		r.mu.Unlock()
		r.Panic("appendTo logIndex is out of bound", zap.Uint64("logIndex", logIndex), zap.Uint64("lastLogIndex", r.lastLogIndex))
	}
	consumed := logIndex - r.storedIndex
	for i := uint64(0); i < consumed && i < uint64(len(r.logs)); i++ {
		r.logs[i] = types.Log{}
	}
	r.logs = r.logs[consumed:]
	if len(r.logs) == 0 {
		r.logs = nil
	}
	r.storedIndex = logIndex
	r.mu.Unlock()
}

func (r *queue) appliedTo(index uint64) {
	r.mu.Lock()
	r.applying = false
	if index < r.appliedIndex {
		r.mu.Unlock()
		r.Warn("applied index less than applappliedIndexiedLogIndex", zap.Uint64("index", index), zap.Uint64("appliedIndex", r.appliedIndex))
		return
	}
	r.appliedIndex = index
	r.mu.Unlock()
}

// truncateLogTo 截取日志到指定日志下标，比如truncateLogTo(6)，如果日志是1 2 3 4 5 6 7 8 9，截取后是1 2 3 4 5 6
func (r *queue) truncateLogTo(logIndex uint64) {
	r.revision++
	r.storeEpoch++

	if logIndex < r.committedIndex {
		r.Panic("truncate log index must be greater than or equal to committedIndex", zap.Uint64("logIndex", logIndex), zap.Uint64("committedIndex", r.committedIndex))
	}
	if logIndex > r.lastLogIndex {
		r.Panic("truncate log index is out of bound", zap.Uint64("logIndex", logIndex), zap.Uint64("lastLogIndex", r.lastLogIndex))
	}
	r.mu.Lock()
	if logIndex <= r.storedIndex {
		for i := range r.logs {
			r.logs[i] = types.Log{}
		}
		r.logs = nil
		r.storedIndex = logIndex
	} else {
		keep := logIndex - r.storedIndex
		for i := keep; i < uint64(len(r.logs)); i++ {
			r.logs[i] = types.Log{}
		}
		r.logs = r.logs[:keep]
	}

	r.lastLogIndex = logIndex
	r.mu.Unlock()
	r.truncateTermRangesTo(logIndex)

	r.Warn("truncate log to", zap.Uint64("logIndex", logIndex), zap.Uint64("lastLogIndex", r.lastLogIndex))
}
