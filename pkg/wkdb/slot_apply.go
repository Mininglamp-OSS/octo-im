package wkdb

// SlotApplyDB records the legacy-command replay boundary independently of the
// Raft storage batch. Recovery commands carry their own transactional fences,
// so this boundary may lag the Raft applied index. Callers must serialize one
// slot and must not advance past an unsuccessful command. The current command
// still needs to be idempotent: a crash can occur between its commit and this
// checkpoint.
type SlotApplyDB interface {
	SlotAppliedIndex(slot uint32) (uint64, error)
	SetSlotAppliedIndex(slot uint32, index uint64) error
	ClusterCapability(identity string) (uint32, error)
	SetClusterCapability(identity string, version uint32) error
}

const clusterCapability byte = 10

func (wk *wukongDB) ClusterCapability(identity string) (uint32, error) {
	var version uint32
	_, err := recoveryRead(wk.defaultShardDB(), recoveryKey(clusterCapability, identity), &version)
	return version, err
}

func (wk *wukongDB) SetClusterCapability(identity string, version uint32) error {
	b := wk.defaultShardDB().NewBatch()
	defer b.Close()
	if err := recoverySet(b, recoveryKey(clusterCapability, identity), version); err != nil {
		return err
	}
	return b.Commit(wk.sync)
}

// Keep progress outside the lifecycle range: applying a legacy command must
// not activate subscriber recovery on a deployment which has disabled it.
const slotApplyProgress byte = 7

func (wk *wukongDB) SlotAppliedIndex(slot uint32) (uint64, error) {
	var index uint64
	_, err := recoveryRead(wk.dbs[slot%wk.shardNum], recoverySlotKey(slotApplyProgress, slot), &index)
	return index, err
}

func (wk *wukongDB) SetSlotAppliedIndex(slot uint32, index uint64) error {
	db := wk.dbs[slot%wk.shardNum]
	b := db.NewBatch()
	defer b.Close()
	if err := recoverySet(b, recoverySlotKey(slotApplyProgress, slot), index); err != nil {
		return err
	}
	return b.Commit(wk.sync)
}
