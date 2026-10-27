package slot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot/key"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// ReplicaState is the containment state of the local replica of one slot.
type ReplicaState uint32

const (
	// ReplicaHealthy means no divergence was detected. It is not a proof that
	// the replica's state matches its log.
	ReplicaHealthy ReplicaState = iota
	// ReplicaQuarantined means divergence was detected and the durable marker
	// is set (or the replica opened with applied > last). The replica never
	// proposes, leads, replicates, applies or serves slot-owned reads again.
	ReplicaQuarantined
	// ReplicaFailStopped means divergence was detected but the marker could
	// not be persisted. It is fenced like ReplicaQuarantined for this process
	// lifetime and, in addition, does not answer slot election info. After a
	// restart it is uncontained until its next refused truncation.
	ReplicaFailStopped
)

func (r ReplicaState) String() string {
	switch r {
	case ReplicaHealthy:
		return "healthy"
	case ReplicaQuarantined:
		return "quarantined"
	case ReplicaFailStopped:
		return "fail_stopped"
	default:
		return fmt.Sprintf("unknown[%d]", uint32(r))
	}
}

// Divergence marker causes, stored in the marker value.
const (
	divergeCauseUnknown        byte = 0
	divergeCauseTruncate       byte = 1
	divergeCauseAppend         byte = 2
	divergeCauseAppliedBeyond  byte = 3
	divergedMarkerValueVersion byte = 1
)

func divergeCause(cause error) byte {
	switch {
	case errors.Is(cause, types.ErrTruncateBelowApplied):
		return divergeCauseTruncate
	case errors.Is(cause, types.ErrAppendBelowApplied):
		return divergeCauseAppend
	default:
		return divergeCauseUnknown
	}
}

// divergence holds the in-memory quarantine registry and counters of one
// slot Server. The registry is sticky for the process lifetime: removing a
// slot raft does not clear it.
type divergence struct {
	states sync.Map // slotId(uint32) -> *atomic.Uint32 (ReplicaState)

	truncateRefused       atomic.Int64
	appendOverlapConflict atomic.Int64
	sameTermConflict      atomic.Int64
	persistFailed         atomic.Int64

	registerMetrics sync.Once
}

func (d *divergence) state(slotId uint32) ReplicaState {
	v, ok := d.states.Load(slotId)
	if !ok {
		return ReplicaHealthy
	}
	return ReplicaState(v.(*atomic.Uint32).Load())
}

// raise moves a slot to at least the given state and reports whether the
// state changed. States only increase.
func (d *divergence) raise(slotId uint32, to ReplicaState) bool {
	v, _ := d.states.LoadOrStore(slotId, &atomic.Uint32{})
	st := v.(*atomic.Uint32)
	for {
		cur := st.Load()
		if cur >= uint32(to) {
			return false
		}
		if st.CompareAndSwap(cur, uint32(to)) {
			return true
		}
	}
}

func (d *divergence) snapshot() map[uint32]ReplicaState {
	out := make(map[uint32]ReplicaState)
	d.states.Range(func(k, v any) bool {
		if st := ReplicaState(v.(*atomic.Uint32).Load()); st != ReplicaHealthy {
			out[k.(uint32)] = st
		}
		return true
	})
	return out
}

// detachedDivergence serves storage opened without a Server (tools, tests).
var detachedDivergence divergence

func (p *PebbleShardLogStorage) divergence() *divergence {
	if p.s == nil {
		return &detachedDivergence
	}
	return &p.s.divergence
}

// DivergenceStats is a point-in-time copy of the divergence counters.
type DivergenceStats struct {
	TruncateRefused       int64
	AppendOverlapConflict int64
	SameTermConflict      int64
	PersistFailed         int64
	Slots                 map[uint32]ReplicaState
}

// DivergenceStats returns the counters and every non-healthy local replica.
func (s *Server) DivergenceStats() DivergenceStats {
	return DivergenceStats{
		TruncateRefused:       s.divergence.truncateRefused.Load(),
		AppendOverlapConflict: s.divergence.appendOverlapConflict.Load(),
		SameTermConflict:      s.divergence.sameTermConflict.Load(),
		PersistFailed:         s.divergence.persistFailed.Load(),
		Slots:                 s.divergence.snapshot(),
	}
}

// ReplicaState reports the containment state of the local replica of slotId.
// It is safe to call from any goroutine.
func (s *Server) ReplicaState(slotId uint32) ReplicaState {
	return s.divergence.state(slotId)
}

// CheckSlotReadable is the read fence for slot-owned rows of the local
// business database. It returns types.ErrSlotDiverged while the local
// replica of slotId is quarantined or fail-stopped. Callers check it before
// and again after reading (check, read, re-check) and discard the rows when
// either check fails.
func (s *Server) CheckSlotReadable(slotId uint32) error {
	if st := s.divergence.state(slotId); st != ReplicaHealthy {
		return fmt.Errorf("%w: slot %d is %s on node %d", types.ErrSlotDiverged, slotId, st, s.opts.NodeId)
	}
	return nil
}

// quarantine records the in-memory fence for a slot. It runs before the
// durable marker is written, so every read that starts its check afterwards
// is refused.
func (s *Server) quarantine(slotId uint32, to ReplicaState, cause error) {
	if s.divergence.raise(slotId, to) {
		s.Error("slot replica fenced",
			zap.Uint32("slotId", slotId),
			zap.Uint64("nodeId", s.opts.NodeId),
			zap.String("state", to.String()),
			zap.Error(cause))
	}
}

// MarkDiverged quarantines the local replica of a slot: first in memory, then
// with a synced single-key marker write. When the write fails the replica is
// fail-stopped instead and the error is returned. The process keeps running
// for every other slot.
func (p *PebbleShardLogStorage) MarkDiverged(shardNo string, cause error) error {
	slotId := KeyToSlotId(shardNo)
	if p.s != nil {
		p.s.quarantine(slotId, ReplicaQuarantined, cause)
	}

	value := make([]byte, 10)
	value[0] = divergedMarkerValueVersion
	value[1] = divergeCause(cause)
	binary.BigEndian.PutUint64(value[2:], uint64(time.Now().UnixNano()))
	if cause != nil {
		value = append(value, cause.Error()...)
	}
	err := p.setDivergedMarker(shardNo, value)
	if err != nil {
		p.divergence().persistFailed.Add(1)
		if p.s != nil {
			p.s.quarantine(slotId, ReplicaFailStopped, err)
		}
		p.Error("slot_diverged_persist_failed: divergence marker not persisted; replica fail-stopped",
			zap.String("shardNo", shardNo), zap.Error(err))
		return fmt.Errorf("persist divergence marker for slot %s: %w", shardNo, err)
	}
	return nil
}

// setDivergedMarker is replaced in tests to inject a marker write failure.
var setDivergedMarkerHook func(shardNo string) error

func (p *PebbleShardLogStorage) setDivergedMarker(shardNo string, value []byte) error {
	if hook := setDivergedMarkerHook; hook != nil {
		if err := hook(shardNo); err != nil {
			return err
		}
	}
	return p.shardDB(shardNo).Set(key.NewDivergedMarkerKey(shardNo), value, pebble.Sync)
}

// DivergedMarker reports whether the durable divergence marker is set.
func (p *PebbleShardLogStorage) DivergedMarker(shardNo string) (bool, error) {
	return divergedMarker(p.shardDB(shardNo), shardNo)
}

func divergedMarker(db *pebble.DB, shardNo string) (bool, error) {
	_, closer, err := db.Get(key.NewDivergedMarkerKey(shardNo))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	closer.Close()
	return true, nil
}

// registerDivergenceMetrics exports the divergence counters once per Server.
func (s *Server) registerDivergenceMetrics() {
	s.divergence.registerMetrics.Do(func() {
		truncateRefused := trace.NewInt64ObservableCounter("slot_truncate_refused_total")
		appendConflict := trace.NewInt64ObservableCounter("slot_append_overlap_conflict_total")
		sameTerm := trace.NewInt64ObservableCounter("slot_same_term_conflict_total")
		persistFailed := trace.NewInt64ObservableCounter("slot_diverged_persist_failed")
		diverged := trace.NewInt64ObservableGauge("slot_diverged")
		node := attribute.String("node", strconv.FormatUint(s.opts.NodeId, 10))
		trace.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
			st := s.DivergenceStats()
			attrs := metric.WithAttributes(node)
			obs.ObserveInt64(truncateRefused, st.TruncateRefused, attrs)
			obs.ObserveInt64(appendConflict, st.AppendOverlapConflict, attrs)
			obs.ObserveInt64(sameTerm, st.SameTermConflict, attrs)
			obs.ObserveInt64(persistFailed, st.PersistFailed, attrs)
			for slotId, state := range st.Slots {
				obs.ObserveInt64(diverged, int64(state), metric.WithAttributes(node,
					attribute.String("slot", strconv.FormatUint(uint64(slotId), 10))))
			}
			return nil
		}, truncateRefused, appendConflict, sameTerm, persistFailed, diverged)
	})
}
