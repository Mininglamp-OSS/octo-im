package slot

import (
	"fmt"
	"path"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/cockroachdb/pebble"
)

// SlotScanResult is the pre-upgrade view of one local slot replica.
type SlotScanResult struct {
	SlotId       uint32 `json:"slot_id"`
	AppliedIndex uint64 `json:"applied_index"`
	LastLogIndex uint64 `json:"last_log_index"`
	// Marker reports a durable divergence marker.
	Marker bool `json:"marker"`
	// AppliedBeyondLog reports applied_index > last_log_index.
	AppliedBeyondLog bool `json:"applied_beyond_log"`
}

// Quarantined reports whether this replica opens quarantined after the
// upgrade.
func (r SlotScanResult) Quarantined() bool {
	return r.Marker || r.AppliedBeyondLog
}

// SlotLogDBPath is the slot log storage directory under a cluster data dir.
func SlotLogDBPath(clusterDataDir string) string {
	return path.Join(clusterDataDir, "logdb")
}

// ScanDivergence opens the slot log storage of a stopped node read-only and
// reports every slot in [0, slotCount) whose replica would open quarantined.
// It writes nothing; the Pebble directory lock makes it fail on a running
// node. It does not detect divergence that left applied == last (see the
// runbook): a clean scan is not a proof that the state matches the log.
func ScanDivergence(logDBPath string, shardNum uint32, slotCount uint32) ([]SlotScanResult, error) {
	if shardNum == 0 {
		return nil, fmt.Errorf("slot scan: shard count must be positive")
	}
	p := &PebbleShardLogStorage{shardNum: shardNum, path: logDBPath, Log: wklog.NewWKLog("slotScan")}
	defer func() {
		for _, db := range p.dbs {
			_ = db.Close()
		}
	}()
	for i := 0; i < int(shardNum); i++ {
		db, err := pebble.Open(fmt.Sprintf("%s/shard%03d", logDBPath, i), &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
		if err != nil {
			return nil, fmt.Errorf("slot scan: open shard %d: %w", i, err)
		}
		p.dbs = append(p.dbs, db)
	}
	var flagged []SlotScanResult
	for slotId := uint32(0); slotId < slotCount; slotId++ {
		shardNo := SlotIdToKey(slotId)
		applied, err := p.AppliedIndex(shardNo)
		if err != nil {
			return nil, fmt.Errorf("slot scan: slot %d applied index: %w", slotId, err)
		}
		last, err := p.LastIndex(shardNo)
		if err != nil {
			return nil, fmt.Errorf("slot scan: slot %d last index: %w", slotId, err)
		}
		marker, err := p.DivergedMarker(shardNo)
		if err != nil {
			return nil, fmt.Errorf("slot scan: slot %d marker: %w", slotId, err)
		}
		r := SlotScanResult{SlotId: slotId, AppliedIndex: applied, LastLogIndex: last, Marker: marker, AppliedBeyondLog: applied > last}
		if r.Quarantined() {
			flagged = append(flagged, r)
		}
	}
	return flagged, nil
}
