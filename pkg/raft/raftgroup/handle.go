package raftgroup

import (
	"errors"
	"fmt"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

// 处理存储请求
func (rg *RaftGroup) handleStoreReq(r IRaft, e types.Event) {

	err := rg.submitWork(r, true, func() {
		err := rg.opts.Storage.AppendLogs(r.Key(), e.Logs, e.TermStartIndexInfo)
		if err != nil {
			rg.Panic("append logs failed", zap.Error(err))
		}
		reason := types.ReasonOk
		if err != nil {
			reason = types.ReasonError
		}

		rg.AddEventFor(r, types.Event{
			Type:   types.StoreResp,
			Index:  e.Logs[len(e.Logs)-1].Index,
			Logs:   e.Logs,
			Reason: reason,
		})
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit append logs failed", zap.Error(err))
		rg.AddEventFor(r, types.Event{
			Type:   types.StoreResp,
			Reason: types.ReasonError,
		})
		rg.Advance()
	}
}

func (rg *RaftGroup) handleGetLogsReq(r IRaft, e types.Event) {
	err := rg.submitWork(r, true, func() {
		if e.Reason != types.ReasonOnlySync {
			index, reason := rg.getTrunctLogIndex(r.Key(), e.StoredIndex, e)
			if reason != types.ReasonOk {
				rg.AddEventFor(r, types.Event{To: e.From, Type: types.GetLogsResp,
					Term: e.Term, Index: index, Reason: reason})
				return
			}
		}
		if e.Reason != types.ReasonOnlySync && e.Index > e.StoredIndex {
			rg.Debug("index is greater than stored index", zap.String("key", r.Key()), zap.Uint64("index", e.Index), zap.Uint64("storedIndex", e.StoredIndex))
			rg.AddEventFor(r, types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Term:   e.Term,
				Index:  e.Index,
				Reason: types.ReasonOk,
			})
			rg.Advance()
			return
		}
		// 获取日志数据
		logs, err := rg.opts.Storage.GetLogs(r.Key(), e.Index, e.StoredIndex+1, rg.opts.MaxLogSizePerBatch)
		if err != nil {
			rg.Error("get logs failed", zap.Error(err))
			rg.AddEventFor(r, types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Term:   e.Term,
				Index:  e.Index,
				Reason: types.ReasonError,
			})
			return
		}
		if len(logs) == 0 {
			rg.Warn("logs is empty", zap.String("key", r.Key()), zap.Uint64("index", e.Index), zap.Uint64("storedIndex", e.StoredIndex))
		}
		rg.AddEventFor(r, types.Event{
			To:     e.From,
			Type:   types.GetLogsResp,
			Term:   e.Term,
			Index:  e.Index,
			Logs:   logs,
			Reason: types.ReasonOk,
		})
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit get logs failed", zap.Error(err))
		rg.AddEventFor(r, types.Event{
			Type:   types.GetLogsResp,
			Term:   e.Term,
			Reason: types.ReasonError,
		})
	}
}

func (rg *RaftGroup) handleTruncateReq(r IRaft, e types.Event) {
	err := rg.submitWork(r, true, func() {
		err := rg.opts.Storage.TruncateLogTo(r.Key(), e.Index)
		if err != nil {
			rg.Error("truncate logs failed", zap.Error(err))
			rg.AddEventFor(r, types.Event{
				Type:       types.TruncateResp,
				Term:       e.Term,
				TruncateID: e.TruncateID,
				Index:      e.Index,
				Reason:     types.ReasonError,
			})
			return
		}
		info, err := types.RetainedTermStart(e.Index,
			func(start, end, size uint64) ([]types.Log, error) {
				return rg.opts.Storage.GetLogs(r.Key(), start, end, size)
			},
			func(term uint32) (uint64, error) { return rg.opts.Storage.GetTermStartIndex(r.Key(), term) })
		if err == nil {
			err = rg.opts.Storage.DeleteLeaderTermStartIndexGreaterThanTerm(r.Key(), info.Term)
		}
		if err != nil {
			rg.Error("restore retained log term failed", zap.Error(err))
			rg.AddEventFor(r, types.Event{Type: types.TruncateResp, Term: e.Term, TruncateID: e.TruncateID, Index: e.Index, Reason: types.ReasonError})
			return
		}
		rg.AddEventFor(r, types.Event{
			Type:               types.TruncateResp,
			Term:               e.Term,
			TruncateID:         e.TruncateID,
			Index:              e.Index,
			TermStartIndexInfo: info,
			Reason:             types.ReasonOk,
		})
	})
	if err != nil {
		rg.Error("submit truncate req failed", zap.Error(err))
		rg.AddEventFor(r, types.Event{
			Type:       types.TruncateResp,
			Term:       e.Term,
			TruncateID: e.TruncateID,
			Index:      e.Index,
			Reason:     types.ReasonError,
		})
	}
}

func (rg *RaftGroup) handleApplyReq(r IRaft, e types.Event) {
	err := rg.submitWork(r, true, func() {
		// 已提交
		// rg.wait.didCommit(r.Key(), e.EndIndex-1)
		var lastLogIndex uint64
		if storage, ok := rg.opts.Storage.(CommittedRangeApplier); ok && !rg.opts.NotNeedApplied {
			if err := storage.ApplyCommittedRange(r.Key(), e.StartIndex, e.EndIndex); err != nil {
				rg.Error("apply committed range failed", zap.Error(err))
				rg.AddEventFor(r, types.Event{Type: types.ApplyResp, Reason: types.ReasonError})
				return
			}
			lastLogIndex = e.EndIndex - 1
		} else if !rg.opts.NotNeedApplied {
			logs, err := rg.opts.Storage.GetLogs(r.Key(), e.StartIndex, min(e.EndIndex, e.StartIndex+1000), rg.opts.MaxLogSizePerBatch)
			if err != nil {
				rg.Error("get logs failed", zap.Error(err))
				rg.AddEventFor(r, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			if len(logs) == 0 {
				rg.Error("logs is empty", zap.String("key", r.Key()), zap.Uint64("startIndex", e.StartIndex), zap.Uint64("endIndex", e.EndIndex))
				rg.AddEventFor(r, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			err = rg.opts.Storage.Apply(r.Key(), logs)
			if err != nil {
				rg.Error("apply logs failed", zap.Error(err))
				rg.AddEventFor(r, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			lastLogIndex = logs[len(logs)-1].Index
		} else {
			lastLogIndex = e.EndIndex - 1
		}

		rg.AddEventFor(r, types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonOk,
			Index:  lastLogIndex,
		})
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit apply req failed", zap.Error(err))
		rg.AddEventFor(r, types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonError,
		})
	}
}

// getTrunctLogIndex returns ReasonTruncate even when the common prefix is empty.
func (rg *RaftGroup) getTrunctLogIndex(key string, stored uint64, e types.Event) (uint64, types.Reason) {
	index, reason, err := types.CheckLogPrefix(e.Index, stored, e.LastLogTerm,
		func(start, end, size uint64) ([]types.Log, error) {
			return rg.opts.Storage.GetLogs(key, start, end, size)
		},
		func(term uint32) (uint64, error) { return rg.opts.Storage.GetTermStartIndex(key, term) })
	if err != nil {
		rg.Error("check log prefix failed", zap.Error(err))
	}
	return index, reason
}

func (rg *RaftGroup) handleRoleChangeReq(r IRaft, e types.Event) {
	// Snapshot mutable membership on the owner, before asynchronous SaveConfig.
	current := r.Config()
	if !r.IsLeader() || e.Term != 0 && (e.Term != current.Term || e.ConfigVersion != current.Version) {
		return
	}
	var (
		newCfg        types.Config
		err           error
		respEventType types.EventType
	)
	switch e.Type {
	case types.LearnerToLeaderReq, types.LearnerToFollowerReq:
		newCfg, err = rg.learnTo(r, e.From)
		respEventType = types.LearnerToFollowerResp
		if e.Type == types.LearnerToLeaderReq {
			respEventType = types.LearnerToLeaderResp
		}
	case types.FollowerToLeaderReq:
		newCfg, err = rg.followerToLeader(r, e.From)
		respEventType = types.FollowerToLeaderResp
	default:
		err = errors.New("unknown role switch")
	}
	respond := func(reason types.Reason, cfg types.Config) {
		rg.AddEventFor(r, types.Event{Type: respEventType, From: e.From, Term: e.Term,
			ConfigVersion: e.ConfigVersion, RoleChangeID: e.RoleChangeID, Reason: reason, Config: cfg})
	}
	if err != nil {
		rg.Error("prepare role change failed", zap.Error(err), zap.String("key", r.Key()))
		respond(types.ReasonError, types.Config{})
		return
	}
	err = rg.submitWork(r, false, func() {
		err := rg.opts.Storage.SaveConfig(r.Key(), newCfg)
		if err != nil {
			if err != types.ErrNotLeader {
				rg.Error("change role failed", zap.Error(err), zap.String("key", r.Key()), zap.String("cfg", newCfg.String()))
			}
			respond(types.ReasonError, types.Config{})
			return
		}
		respond(types.ReasonOk, newCfg)
	})
	if err != nil {
		rg.Error("submit role change req failed", zap.Error(err))
		respond(types.ReasonError, types.Config{})
	}
}

// 学习者转换
func (rg *RaftGroup) learnTo(r IRaft, learnerId uint64) (types.Config, error) {
	cfg := r.Config().Clone()
	completing := cfg.MigrateTo == learnerId && cfg.MigrateFrom != learnerId && cfg.MigrateFrom != r.LeaderId() &&
		wkutil.ArrayContainsUint64(cfg.Replicas, learnerId) && wkutil.ArrayContainsUint64(cfg.Replicas, cfg.MigrateFrom)
	if !wkutil.ArrayContainsUint64(cfg.Learners, learnerId) && !completing {
		return types.Config{}, errors.New("promotion target is not a learner")
	}

	// orphan learner: 前次迁移已完成（MigrateFrom/MigrateTo 都为 0），
	// 但 learnerId 仍残留在 cfg.Learners 中。此时既无迁移源也无迁移目标，
	// 不能走常规迁移路径，否则会被 learnerId != cfg.MigrateTo 误拒绝。
	orphan := cfg.MigrateTo == 0 && cfg.MigrateFrom == 0 &&
		wkutil.ArrayContainsUint64(cfg.Learners, learnerId)

	if !orphan && learnerId != cfg.MigrateTo {
		rg.Warn("learnerId not equal migrateTo", zap.Uint64("learnerId", learnerId), zap.Uint64("migrateTo", cfg.MigrateTo))
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

		if cfg.MigrateFrom == r.LeaderId() { // 学习者转领导
			if cfg.Term == math.MaxUint32 {
				return types.Config{}, errors.New("leader term exhausted")
			}
			cfg.Term++
			cfg.Leader = cfg.MigrateTo
		}
	}

	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil
}

// follower转换成leader
func (rg *RaftGroup) followerToLeader(r IRaft, followerId uint64) (types.Config, error) {
	cfg := r.Config().Clone()
	if !wkutil.ArrayContainsUint64(cfg.Replicas, followerId) {
		rg.Error("followerToLeader: follower not in replicas", zap.Uint64("followerId", followerId))
		return types.Config{}, fmt.Errorf("follower not in replicas")
	}

	if cfg.Term == math.MaxUint32 {
		return types.Config{}, errors.New("leader term exhausted")
	}
	cfg.Leader = followerId
	cfg.Term = cfg.Term + 1
	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil

}

func (rg *RaftGroup) handleDestory(r IRaft) {
	rg.RemoveRaft(r)
}
