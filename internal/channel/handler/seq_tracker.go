package handler

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
)

type TrackStage uint8

const (
	StagePluginPersistAfter TrackStage = 0
	StageWebhookNotify      TrackStage = 1
	StageChannelDistribute  TrackStage = 2
	StageLocalDistribute    TrackStage = 3
	StagePersistEnqueued    TrackStage = 4
	numTrackStages                     = 5

	seqTrackerShards        = 64
	MaxChannelsPerShard     = 256 // 64 * 256 = 16,384 max tracked channels globally
	MaxGlobalOOOWindows     = 8192
	MaxGlobalSlotExts       = 2048
	MaxInFlightPerShard     = 1024
	oooWindowBits           = 2048
	oooWindowWords          = oooWindowBits / 64 // 32 uint64s = 256 B
	maxHistIntervals        = 4
	maxExtraHistIntervals   = 16384
	sparsePageBits          = 4096
	sparsePageWords         = sparsePageBits / 64 // 64 uint64s = 512 B
	sparseRunThreshold      = 64
	maxPartialSeqsPerExt    = 8
	maxOverflowPartialSeqs  = 1016
	MaxExtraSlotMaskBytes   = int64(8 * 1024 * 1024)
	histIntervalExtraBytes  = int64(16)
	histPageExtraBytes      = int64(544)
	partialSeqEntryOverhead = int64(48)
)

// StageBeginStatus explicitly distinguishes already-committed, in-flight, newly leased, and capacity-backpressured outcomes (A1 + B2).
type StageBeginStatus uint8

const (
	StageBeginAlreadyCommitted StageBeginStatus = iota + 1
	StageBeginInFlight
	StageBeginLeased
	StageBeginCapacityRetry
)

// ChannelWatermarkKey is a fixed-size 40-byte value key that retains zero heap string slices
// and supports arbitrary non-empty channel IDs of any byte length without truncation or rejection.
type ChannelWatermarkKey struct {
	Digest      [32]byte
	ByteLen     uint32
	ChannelType uint8
}

func MakeChannelWatermarkKey(channelID string, channelType uint8) (ChannelWatermarkKey, bool) {
	if len(channelID) == 0 {
		return ChannelWatermarkKey{}, false
	}
	var prefix [5]byte
	prefix[0] = channelType
	binary.LittleEndian.PutUint32(prefix[1:5], uint32(len(channelID)))
	h := sha256.New()
	_, _ = h.Write(prefix[:])
	_, _ = h.Write([]byte(channelID))
	var k ChannelWatermarkKey
	h.Sum(k.Digest[:0])
	k.ByteLen = uint32(len(channelID))
	k.ChannelType = channelType
	return k, true
}

func (k ChannelWatermarkKey) shardIndex() uint32 {
	return binary.LittleEndian.Uint32(k.Digest[:4]) & (seqTrackerShards - 1)
}

type seqInterval struct {
	lo uint64
	hi uint64
}

type OOOWindow struct {
	base        uint64
	bits        [oooWindowWords]uint64
	hist        [maxHistIntervals]seqInterval
	extraHist   []seqInterval
	sparsePages map[uint64]*[sparsePageWords]uint64
	extraBytes  int64
}

func (w *OOOWindow) isBitsEmpty() bool {
	if w == nil {
		return true
	}
	for i := range w.bits {
		if w.bits[i] != 0 {
			return false
		}
	}
	return true
}

func (w *OOOWindow) isHistEmpty() bool {
	if w == nil {
		return true
	}
	if len(w.sparsePages) > 0 {
		return false
	}
	for i := range w.hist {
		if w.hist[i].hi != 0 {
			return false
		}
	}
	for i := range w.extraHist {
		if w.extraHist[i].hi != 0 {
			return false
		}
	}
	return true
}

func (w *OOOWindow) isEmpty() bool {
	return w.isBitsEmpty() && w.isHistEmpty()
}

func (w *OOOWindow) countSetBits() int {
	if w == nil {
		return 0
	}
	n := 0
	for _, word := range w.bits {
		n += bits.OnesCount64(word)
	}
	return n
}

func (w *OOOWindow) histCount() int {
	if w == nil {
		return 0
	}
	n := len(w.sparsePages)
	for i := range w.hist {
		if w.hist[i].hi != 0 {
			n++
		}
	}
	for i := range w.extraHist {
		if w.extraHist[i].hi != 0 {
			n++
		}
	}
	return n
}

func (w *OOOWindow) hasSparseBit(seq uint64) bool {
	if w == nil || seq == 0 || len(w.sparsePages) == 0 {
		return false
	}
	pg := w.sparsePages[seq/sparsePageBits]
	if pg == nil {
		return false
	}
	off := seq % sparsePageBits
	return (pg[off/64] & (uint64(1) << (off % 64))) != 0
}

func (w *OOOWindow) clearSparseBit(t *StageSeqTracker, seq uint64) {
	if w == nil || seq == 0 || len(w.sparsePages) == 0 {
		return
	}
	pIdx := seq / sparsePageBits
	pg := w.sparsePages[pIdx]
	if pg == nil {
		return
	}
	off := seq % sparsePageBits
	pg[off/64] &^= uint64(1) << (off % 64)
	for i := range pg {
		if pg[i] != 0 {
			return
		}
	}
	delete(w.sparsePages, pIdx)
	w.extraBytes -= histPageExtraBytes
	if t != nil {
		t.extraSlotBytes.Add(-histPageExtraBytes)
	}
}

func (w *OOOWindow) setSparseBit(t *StageSeqTracker, seq uint64) bool {
	if w == nil || seq == 0 {
		return false
	}
	pIdx := seq / sparsePageBits
	pg := w.sparsePages[pIdx]
	if pg == nil {
		if t != nil && !t.tryReserveExtraBytes(histPageExtraBytes) {
			return false
		}
		if w.sparsePages == nil {
			w.sparsePages = make(map[uint64]*[sparsePageWords]uint64, 4)
		}
		pg = &[sparsePageWords]uint64{}
		w.sparsePages[pIdx] = pg
		w.extraBytes += histPageExtraBytes
	}
	off := seq % sparsePageBits
	pg[off/64] |= uint64(1) << (off % 64)

	// If all 4096 bits in this page are now committed (except seq 0 on page 0), promote to a compact 16B interval!
	full := true
	for i := range pg {
		expected := ^uint64(0)
		if pIdx == 0 && i == 0 {
			expected = ^uint64(1)
		}
		if pg[i] != expected {
			full = false
			break
		}
	}
	if full {
		lo := pIdx * sparsePageBits
		if lo == 0 {
			lo = 1
		}
		hi := pIdx*sparsePageBits + (sparsePageBits - 1)
		delete(w.sparsePages, pIdx)
		w.extraBytes -= histPageExtraBytes
		if t != nil {
			t.extraSlotBytes.Add(-histPageExtraBytes)
		}
		if !w.storeContiguousInterval(t, lo, hi) {
			// Restore page if interval list could not grow
			if t == nil || t.tryReserveExtraBytes(histPageExtraBytes) {
				w.sparsePages[pIdx] = pg
				w.extraBytes += histPageExtraBytes
			}
		}
	}
	return true
}

func (w *OOOWindow) hasInHist(seq uint64) bool {
	if w == nil || seq == 0 {
		return false
	}
	if w.hasSparseBit(seq) {
		return true
	}
	for i := range w.hist {
		iv := w.hist[i]
		if iv.hi != 0 && seq >= iv.lo && seq <= iv.hi {
			return true
		}
	}
	for i := range w.extraHist {
		iv := w.extraHist[i]
		if iv.hi != 0 && seq >= iv.lo && seq <= iv.hi {
			return true
		}
	}
	return false
}

func (w *OOOWindow) allHistIntervals() []seqInterval {
	if w == nil {
		return nil
	}
	out := make([]seqInterval, 0, len(w.hist)+len(w.extraHist)+1)
	for i := range w.hist {
		if w.hist[i].hi != 0 {
			out = append(out, w.hist[i])
		}
	}
	for i := range w.extraHist {
		if w.extraHist[i].hi != 0 {
			out = append(out, w.extraHist[i])
		}
	}
	return out
}

func (w *OOOWindow) intervalExtraBytes() int64 {
	if len(w.extraHist) == 0 {
		return 0
	}
	return int64(len(w.extraHist)) * histIntervalExtraBytes
}

func (w *OOOWindow) storeHistIntervals(t *StageSeqTracker, intervals []seqInterval) bool {
	// Compact adjacent/overlapping intervals without ever bridging uncommitted gaps (hi+1 < next.lo).
	for i := 0; i < len(intervals); i++ {
		for j := i + 1; j < len(intervals); {
			a, b := intervals[i], intervals[j]
			if a.hi+1 >= b.lo && a.lo <= b.hi+1 {
				if b.lo < a.lo {
					a.lo = b.lo
				}
				if b.hi > a.hi {
					a.hi = b.hi
				}
				intervals[i] = a
				intervals = append(intervals[:j], intervals[j+1:]...)
				j = i + 1
				continue
			}
			j++
		}
	}
	needExtra := 0
	if len(intervals) > maxHistIntervals {
		needExtra = len(intervals) - maxHistIntervals
	}
	if needExtra > maxExtraHistIntervals {
		return false
	}
	oldIvBytes := w.intervalExtraBytes()
	newIvBytes := int64(needExtra) * histIntervalExtraBytes
	delta := newIvBytes - oldIvBytes
	if delta > 0 && t != nil && !t.tryReserveExtraBytes(delta) {
		return false
	}
	if delta < 0 && t != nil {
		t.extraSlotBytes.Add(delta)
	}
	w.extraBytes += delta
	for i := range w.hist {
		if i < len(intervals) {
			w.hist[i] = intervals[i]
		} else {
			w.hist[i] = seqInterval{}
		}
	}
	if needExtra > 0 {
		w.extraHist = append(w.extraHist[:0], intervals[maxHistIntervals:]...)
	} else {
		w.extraHist = nil
	}
	return true
}

func (w *OOOWindow) storeContiguousInterval(t *StageSeqTracker, lo, hi uint64) bool {
	all := w.allHistIntervals()
	all = append(all, seqInterval{lo: lo, hi: hi})
	return w.storeHistIntervals(t, all)
}

// recordHistInterval records [lo, hi] strictly without bridging uncommitted gaps and without retiring/forgetting committed intervals (A1).
// Long contiguous runs (>= sparseRunThreshold) are stored as 16B intervals; short/sparse runs (such as multi-node round-robin sequences)
// are stored in 4096-bit sparse bitmap pages (O(1) lookup, 1 bit/seq, zero interval fragmentation).
func (w *OOOWindow) recordHistInterval(t *StageSeqTracker, lo, hi uint64) bool {
	if w == nil || hi == 0 || lo > hi {
		return false
	}
	if hi-lo >= sparseRunThreshold {
		return w.storeContiguousInterval(t, lo, hi)
	}
	for s := lo; s <= hi; s++ {
		if !w.setSparseBit(t, s) {
			return false
		}
	}
	return true
}

func (w *OOOWindow) absorbAdjacentToFrontier(t *StageSeqTracker, frontierLo, frontierHi uint64) (uint64, uint64) {
	if w == nil || frontierHi == 0 {
		return frontierLo, frontierHi
	}
	all := w.allHistIntervals()
	hasIv := len(all) > 0
	changed := true
	for changed {
		changed = false
		for len(w.sparsePages) > 0 && w.hasSparseBit(frontierHi+1) {
			frontierHi++
			w.clearSparseBit(t, frontierHi)
			changed = true
		}
		for len(w.sparsePages) > 0 && frontierLo > 1 && w.hasSparseBit(frontierLo-1) {
			frontierLo--
			w.clearSparseBit(t, frontierLo)
			changed = true
		}
		for i := 0; i < len(all); {
			iv := all[i]
			if iv.hi+1 >= frontierLo && iv.lo <= frontierHi+1 {
				if iv.lo < frontierLo {
					frontierLo = iv.lo
				}
				if iv.hi > frontierHi {
					frontierHi = iv.hi
				}
				all = append(all[:i], all[i+1:]...)
				changed = true
				continue
			}
			i++
		}
	}
	if hasIv {
		_ = w.storeHistIntervals(t, all)
	}
	// Clear any sparse bits now subsumed inside [frontierLo, frontierHi]:
	if len(w.sparsePages) > 0 {
		startPg := frontierLo / sparsePageBits
		endPg := frontierHi / sparsePageBits
		if endPg-startPg <= uint64(len(w.sparsePages)) {
			for pIdx := startPg; pIdx <= endPg; pIdx++ {
				if w.sparsePages[pIdx] == nil {
					continue
				}
				loSeq := max(frontierLo, pIdx*sparsePageBits)
				hiSeq := min(frontierHi, pIdx*sparsePageBits+(sparsePageBits-1))
				for s := loSeq; s <= hiSeq; s++ {
					w.clearSparseBit(t, s)
				}
			}
		} else {
			for pIdx := range w.sparsePages {
				if pIdx < startPg || pIdx > endPg {
					continue
				}
				loSeq := max(frontierLo, pIdx*sparsePageBits)
				hiSeq := min(frontierHi, pIdx*sparsePageBits+(sparsePageBits-1))
				for s := loSeq; s <= hiSeq; s++ {
					w.clearSparseBit(t, s)
				}
			}
		}
	}
	return frontierLo, frontierHi
}

func (w *OOOWindow) maxOffsetSet() (int, bool) {
	if w == nil {
		return 0, false
	}
	for i := oooWindowWords - 1; i >= 0; i-- {
		if w.bits[i] != 0 {
			bit := 63 - bits.LeadingZeros64(w.bits[i])
			return i*64 + bit, true
		}
	}
	return 0, false
}

func (w *OOOWindow) countBitRuns() int {
	if w == nil {
		return 0
	}
	runs := 0
	for i := 0; i < oooWindowBits; {
		word := w.bits[i/64] >> uint(i%64)
		if (word & 1) == 0 {
			i++
			continue
		}
		runs++
		for i < oooWindowBits && ((w.bits[i/64]>>uint(i%64))&1) != 0 {
			i++
		}
	}
	return runs
}

func (w *OOOWindow) shiftLeftBits(delta uint64) {
	if delta == 0 {
		return
	}
	if delta >= oooWindowBits {
		for i := range w.bits {
			w.bits[i] = 0
		}
		w.base += delta
		return
	}
	wordShift := int(delta / 64)
	bitShift := uint(delta % 64)
	if wordShift > 0 {
		for i := 0; i < oooWindowWords-wordShift; i++ {
			w.bits[i] = w.bits[i+wordShift]
		}
		for i := oooWindowWords - wordShift; i < oooWindowWords; i++ {
			w.bits[i] = 0
		}
	}
	if bitShift > 0 {
		for i := 0; i < oooWindowWords-1; i++ {
			w.bits[i] = (w.bits[i] >> bitShift) | (w.bits[i+1] << (64 - bitShift))
		}
		w.bits[oooWindowWords-1] >>= bitShift
	}
	w.base += delta
}

func (w *OOOWindow) rebaseDown(newBase uint64) bool {
	if newBase >= w.base {
		return true
	}
	shift := w.base - newBase
	if shift >= oooWindowBits {
		if !w.isBitsEmpty() {
			return false
		}
		w.base = newBase
		return true
	}
	if maxOff, ok := w.maxOffsetSet(); ok {
		if uint64(maxOff)+shift >= oooWindowBits {
			return false
		}
	}
	wordShift := int(shift / 64)
	bitShift := uint(shift % 64)
	if wordShift > 0 {
		for i := oooWindowWords - 1; i >= wordShift; i-- {
			w.bits[i] = w.bits[i-wordShift]
		}
		for i := 0; i < wordShift; i++ {
			w.bits[i] = 0
		}
	}
	if bitShift > 0 {
		for i := oooWindowWords - 1; i > 0; i-- {
			w.bits[i] = (w.bits[i] << bitShift) | (w.bits[i-1] >> (64 - bitShift))
		}
		w.bits[0] <<= bitShift
	}
	w.base = newBase
	return true
}

type slotSeqMask struct {
	seq       uint64
	doneSlots eventbus.SlotBitset
}

type SlotHandoffExt struct {
	entries       [maxPartialSeqsPerExt]slotSeqMask
	overflow      []slotSeqMask
	countedInPool bool
}

func (ext *SlotHandoffExt) isEmpty() bool {
	if ext == nil {
		return true
	}
	for i := range ext.entries {
		if ext.entries[i].seq != 0 {
			return false
		}
	}
	for i := range ext.overflow {
		if ext.overflow[i].seq != 0 {
			return false
		}
	}
	return true
}

func (ext *SlotHandoffExt) findDoneSlots(seq uint64) (eventbus.SlotBitset, bool) {
	if ext == nil || seq == 0 {
		return eventbus.SlotBitset{}, false
	}
	for i := range ext.entries {
		if ext.entries[i].seq == seq {
			return ext.entries[i].doneSlots, true
		}
	}
	for i := range ext.overflow {
		if ext.overflow[i].seq == seq {
			return ext.overflow[i].doneSlots, true
		}
	}
	return eventbus.SlotBitset{}, false
}

func (ext *SlotHandoffExt) countActiveEntries() int {
	if ext == nil {
		return 0
	}
	n := 0
	for i := range ext.entries {
		if ext.entries[i].seq != 0 {
			n++
		}
	}
	for i := range ext.overflow {
		if ext.overflow[i].seq != 0 {
			n++
		}
	}
	return n
}

type ChannelWatermark struct {
	key        ChannelWatermarkKey
	lastAccess uint64
	retirable  bool
	frontierLo [numTrackStages]uint64
	frontierHi [numTrackStages]uint64
	ooo        [numTrackStages]*OOOWindow
	slotExt    [2]*SlotHandoffExt // [0]: StageChannelDistribute, [1]: StageLocalDistribute
}

func (wm *ChannelWatermark) hasUnclosedStateLocked() bool {
	if wm == nil {
		return false
	}
	if wm.slotExt[0] != nil || wm.slotExt[1] != nil {
		return true
	}
	for st := 0; st < numTrackStages; st++ {
		if wm.ooo[st] != nil && !wm.ooo[st].isEmpty() {
			return true
		}
		if wm.frontierHi[st] > 0 && wm.frontierLo[st] > 1 {
			return true
		}
	}
	return false
}

func (wm *ChannelWatermark) hasAnyCommittedLocked() bool {
	if wm == nil {
		return false
	}
	for st := 0; st < numTrackStages; st++ {
		if wm.frontierHi[st] > 0 || (wm.ooo[st] != nil && !wm.ooo[st].isEmpty()) {
			return true
		}
	}
	return wm.slotExt[0] != nil || wm.slotExt[1] != nil
}

// isPinnedLocked reports whether wm must remain pinned in the shard map (A1 + B2).
// A channel watermark is pinned whenever it has in-flight leases, partial slot extensions,
// out-of-order gaps, or active committed watermarks that have not been explicitly marked retirable.
func (wm *ChannelWatermark) isPinnedLocked(s *seqTrackerShard, k ChannelWatermarkKey) bool {
	if wm == nil {
		return false
	}
	if s.hasInFlightKeyLocked(k) || wm.hasUnclosedStateLocked() {
		return true
	}
	if wm.hasAnyCommittedLocked() && !wm.retirable {
		return true
	}
	return false
}

type shardInFlightSlot struct {
	inUse         bool
	key           ChannelWatermarkKey
	seq           uint64
	stage         TrackStage
	hasMask       bool
	mask          eventbus.SlotBitset
	extraBytes    int64
	reservedBytes int64
}

type seqTrackerShard struct {
	mu           sync.Mutex
	accessClock  uint64
	channels     map[ChannelWatermarkKey]*ChannelWatermark
	inFlight     [MaxInFlightPerShard]shardInFlightSlot
	inFlightUsed int
}

type StageSeqTracker struct {
	shards              [seqTrackerShards]seqTrackerShard
	activeOOOWindows    atomic.Int32
	activeSlotExts      atomic.Int32
	extraSlotBytes      atomic.Int64
	shardSaturatedTotal atomic.Uint64
	evictedChannels     atomic.Uint64
}

type SeqTrackerMetrics struct {
	ActiveOOOWindows    int32
	ActiveSlotExts      int32
	ExtraSlotBytes      int64
	ShardSaturatedTotal uint64
	EvictedChannels     uint64
}

func (t *StageSeqTracker) Metrics() SeqTrackerMetrics {
	if t == nil {
		return SeqTrackerMetrics{}
	}
	return SeqTrackerMetrics{
		ActiveOOOWindows:    t.activeOOOWindows.Load(),
		ActiveSlotExts:      t.activeSlotExts.Load(),
		ExtraSlotBytes:      t.extraSlotBytes.Load(),
		ShardSaturatedTotal: t.shardSaturatedTotal.Load(),
		EvictedChannels:     t.evictedChannels.Load(),
	}
}

func (t *StageSeqTracker) tryReserveExtraBytes(delta int64) bool {
	if delta <= 0 {
		if delta < 0 {
			t.extraSlotBytes.Add(delta)
		}
		return true
	}
	for {
		cur := t.extraSlotBytes.Load()
		if cur+delta > MaxExtraSlotMaskBytes {
			return false
		}
		if t.extraSlotBytes.CompareAndSwap(cur, cur+delta) {
			return true
		}
	}
}

func (s *seqTrackerShard) hasInFlightKeyLocked(k ChannelWatermarkKey) bool {
	if s.inFlightUsed == 0 {
		return false
	}
	for i := range s.inFlight {
		if s.inFlight[i].inUse && s.inFlight[i].key == k {
			return true
		}
	}
	return false
}

func (s *seqTrackerShard) hasExactInFlightLocked(k ChannelWatermarkKey, stage TrackStage, seq uint64) bool {
	if s.inFlightUsed == 0 {
		return false
	}
	for i := range s.inFlight {
		it := &s.inFlight[i]
		if it.inUse && it.key == k && it.stage == stage && it.seq == seq && !it.hasMask {
			return true
		}
	}
	return false
}

func (s *seqTrackerShard) allocInFlightLocked(
	t *StageSeqTracker,
	k ChannelWatermarkKey,
	stage TrackStage,
	seq uint64,
	hasMask bool,
	mask eventbus.SlotBitset,
	reservedCommitBytes int64,
) bool {
	if s.inFlightUsed >= MaxInFlightPerShard {
		return false
	}
	clonedMask := mask.Clone()
	extra := clonedMask.ExtraHeapBytes()
	totalReserve := extra + reservedCommitBytes
	if totalReserve > 0 && !t.tryReserveExtraBytes(totalReserve) {
		return false
	}
	for i := range s.inFlight {
		if !s.inFlight[i].inUse {
			s.inFlight[i] = shardInFlightSlot{
				inUse:         true,
				key:           k,
				seq:           seq,
				stage:         stage,
				hasMask:       hasMask,
				mask:          clonedMask,
				extraBytes:    extra,
				reservedBytes: reservedCommitBytes,
			}
			s.inFlightUsed++
			return true
		}
	}
	if totalReserve > 0 {
		t.extraSlotBytes.Add(-totalReserve)
	}
	return false
}

func (s *seqTrackerShard) releaseInFlightLocked(t *StageSeqTracker, k ChannelWatermarkKey, stage TrackStage, seq uint64, matchMask *eventbus.SlotBitset) int64 {
	if s.inFlightUsed == 0 {
		return 0
	}
	for i := range s.inFlight {
		it := &s.inFlight[i]
		if !it.inUse || it.key != k || it.stage != stage || it.seq != seq {
			continue
		}
		if matchMask == nil {
			if it.hasMask {
				continue
			}
		} else {
			if !it.hasMask || !it.mask.Equal(*matchMask) {
				continue
			}
		}
		if it.extraBytes > 0 {
			t.extraSlotBytes.Add(-it.extraBytes)
		}
		reserved := it.reservedBytes
		*it = shardInFlightSlot{}
		s.inFlightUsed--
		return reserved
	}
	return 0
}

func (t *StageSeqTracker) freeOOOWindowLocked(wm *ChannelWatermark, stage TrackStage) {
	if wm == nil || wm.ooo[stage] == nil {
		return
	}
	w := wm.ooo[stage]
	if w.extraBytes > 0 {
		t.extraSlotBytes.Add(-w.extraBytes)
		w.extraBytes = 0
	}
	wm.ooo[stage] = nil
	t.activeOOOWindows.Add(-1)
}

func (t *StageSeqTracker) freeSlotExtLocked(wm *ChannelWatermark, idx int) {
	if wm == nil || wm.slotExt[idx] == nil {
		return
	}
	ext := wm.slotExt[idx]
	var extra int64
	for i := range ext.entries {
		extra += ext.entries[i].doneSlots.ExtraHeapBytes()
	}
	for i := range ext.overflow {
		extra += ext.overflow[i].doneSlots.ExtraHeapBytes() + partialSeqEntryOverhead
	}
	if extra > 0 {
		t.extraSlotBytes.Add(-extra)
	}
	counted := ext.countedInPool
	wm.slotExt[idx] = nil
	if counted {
		t.activeSlotExts.Add(-1)
	}
}

func (t *StageSeqTracker) freeWatermarkResourcesLocked(wm *ChannelWatermark) {
	if wm == nil {
		return
	}
	for st := TrackStage(0); st < numTrackStages; st++ {
		t.freeOOOWindowLocked(wm, st)
	}
	for idx := 0; idx < 2; idx++ {
		t.freeSlotExtLocked(wm, idx)
	}
}

// ReleaseChannelWatermarkIfSafe marks a channel watermark retirable (or removes it) ONLY if it has
// no in-flight leases, no partial slot extensions, no out-of-order holes, and has committed up to upToSeq (A1 + B2).
func (t *StageSeqTracker) ReleaseChannelWatermarkIfSafe(k ChannelWatermarkKey, upToSeq uint64) bool {
	if t == nil {
		return false
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels == nil {
		return false
	}
	wm := s.channels[k]
	if wm == nil {
		return false
	}
	if s.hasInFlightKeyLocked(k) || wm.hasUnclosedStateLocked() {
		return false
	}
	if upToSeq > 0 {
		hasCoveredStage := false
		for st := 0; st < numTrackStages; st++ {
			if wm.frontierHi[st] > 0 {
				if wm.frontierLo[st] > 1 || wm.frontierHi[st] < upToSeq {
					return false
				}
				hasCoveredStage = true
			}
		}
		if !hasCoveredStage {
			return false
		}
	}
	wm.retirable = true
	return true
}

func (t *StageSeqTracker) getOrCreateWatermarkLocked(s *seqTrackerShard, k ChannelWatermarkKey) *ChannelWatermark {
	if s.channels == nil {
		s.channels = make(map[ChannelWatermarkKey]*ChannelWatermark, 64)
	}
	if wm := s.channels[k]; wm != nil {
		s.accessClock++
		wm.lastAccess = s.accessClock
		wm.retirable = false
		return wm
	}
	if len(s.channels) >= MaxChannelsPerShard {
		var (
			evictKey ChannelWatermarkKey
			oldest   uint64
			found    bool
		)
		for ck, cwm := range s.channels {
			if cwm.isPinnedLocked(s, ck) {
				continue
			}
			if !found || cwm.lastAccess < oldest {
				evictKey = ck
				oldest = cwm.lastAccess
				found = true
			}
		}
		if !found {
			t.shardSaturatedTotal.Add(1)
			return nil
		}
		evicted := s.channels[evictKey]
		t.freeWatermarkResourcesLocked(evicted)
		delete(s.channels, evictKey)
		t.evictedChannels.Add(1)
	}
	s.accessClock++
	wm := &ChannelWatermark{
		key:        k,
		lastAccess: s.accessClock,
	}
	s.channels[k] = wm
	return wm
}

func isSeqCommittedInWindow(wm *ChannelWatermark, stage TrackStage, seq uint64) bool {
	if wm == nil || seq == 0 {
		return false
	}
	if wm.frontierHi[stage] > 0 && seq >= wm.frontierLo[stage] && seq <= wm.frontierHi[stage] {
		return true
	}
	w := wm.ooo[stage]
	if w == nil {
		return false
	}
	if w.hasInHist(seq) {
		return true
	}
	if w.base > 0 && seq >= w.base {
		off := seq - w.base
		if off < oooWindowBits {
			return (w.bits[off/64] & (uint64(1) << (off % 64))) != 0
		}
	}
	return false
}

func (t *StageSeqTracker) allocOOOWindowLocked(wm *ChannelWatermark, stage TrackStage, base uint64) *OOOWindow {
	if wm.ooo[stage] != nil {
		return wm.ooo[stage]
	}
	for {
		cur := t.activeOOOWindows.Load()
		if cur >= MaxGlobalOOOWindows {
			return nil
		}
		if t.activeOOOWindows.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	w := &OOOWindow{base: base}
	wm.ooo[stage] = w
	return w
}

// precheckSeqCommitCapacityLocked verifies and pre-allocates OOOWindow / worst-case hist interval capacity
// BEFORE granting a stage lease so CommitStage never silently fails after side effects have executed (A1 + B2).
func (t *StageSeqTracker) precheckSeqCommitCapacityLocked(wm *ChannelWatermark, stage TrackStage, seq uint64) (int64, bool) {
	if wm == nil || seq == 0 {
		return 0, false
	}
	if isSeqCommittedInWindow(wm, stage, seq) {
		return 0, true
	}
	if wm.frontierHi[stage] == 0 || seq == wm.frontierHi[stage]+1 || seq+1 == wm.frontierLo[stage] {
		return 0, true
	}
	w := t.allocOOOWindowLocked(wm, stage, wm.frontierHi[stage]+2)
	if w == nil {
		return 0, false
	}
	if seq < wm.frontierLo[stage] {
		if w.sparsePages != nil && w.sparsePages[seq/sparsePageBits] != nil {
			return 0, true
		}
		return histPageExtraBytes, true
	}
	if w.base > 0 && !w.isBitsEmpty() && seq >= w.base+oooWindowBits {
		var worstBytes int64
		if wm.frontierHi[stage]-wm.frontierLo[stage] >= sparseRunThreshold {
			curIv := len(w.allHistIntervals())
			if curIv+1 > maxHistIntervals+maxExtraHistIntervals {
				return 0, false
			}
			if curIv >= maxHistIntervals {
				worstBytes += histIntervalExtraBytes
			}
		} else {
			pStart := wm.frontierLo[stage] / sparsePageBits
			pEnd := wm.frontierHi[stage] / sparsePageBits
			for p := pStart; p <= pEnd; p++ {
				if w.sparsePages == nil || w.sparsePages[p] == nil {
					worstBytes += histPageExtraBytes
				}
			}
		}
		bStart := w.base / sparsePageBits
		bEnd := (w.base + oooWindowBits - 1) / sparsePageBits
		fStart := wm.frontierLo[stage] / sparsePageBits
		fEnd := wm.frontierHi[stage] / sparsePageBits
		hasFrontierSparse := wm.frontierHi[stage]-wm.frontierLo[stage] < sparseRunThreshold
		for p := bStart; p <= bEnd; p++ {
			if hasFrontierSparse && p >= fStart && p <= fEnd {
				continue
			}
			if w.sparsePages == nil || w.sparsePages[p] == nil {
				worstBytes += histPageExtraBytes
			}
		}
		return worstBytes, true
	}
	return 0, true
}

// drainOOOBitsIntoFrontierLocked loops "absorb adjacent hist intervals -> drain contiguous bitmap bits"
// until a fixed point is reached, ensuring no contiguous bit at w.base is stranded after hist absorption (A1).
func (t *StageSeqTracker) drainOOOBitsIntoFrontierLocked(wm *ChannelWatermark, stage TrackStage) {
	w := wm.ooo[stage]
	if w == nil {
		return
	}
	for {
		prevLo, prevHi := wm.frontierLo[stage], wm.frontierHi[stage]
		wm.frontierLo[stage], wm.frontierHi[stage] = w.absorbAdjacentToFrontier(t, wm.frontierLo[stage], wm.frontierHi[stage])
		if w.base > 0 && !w.isBitsEmpty() {
			if wm.frontierHi[stage]+1 > w.base {
				w.shiftLeftBits((wm.frontierHi[stage] + 1) - w.base)
			}
			for w.base == wm.frontierHi[stage]+1 && (w.bits[0]&1) != 0 {
				adv := uint64(bits.TrailingZeros64(^w.bits[0]))
				wm.frontierHi[stage] += adv
				w.shiftLeftBits(adv)
			}
		}
		if wm.frontierLo[stage] == prevLo && wm.frontierHi[stage] == prevHi {
			break
		}
	}
	if w.isEmpty() {
		t.freeOOOWindowLocked(wm, stage)
	}
}

func (t *StageSeqTracker) commitFullSeqLocked(wm *ChannelWatermark, stage TrackStage, seq uint64) bool {
	if wm == nil || seq == 0 {
		return false
	}
	wm.retirable = false
	if isSeqCommittedInWindow(wm, stage, seq) {
		return true
	}

	// First sequence observed on this channel (supports both seq=1 and post-restart/handoff seq > 1 without allocating OOOWindow).
	if wm.frontierHi[stage] == 0 {
		wm.frontierLo[stage] = seq
		wm.frontierHi[stage] = seq
		return true
	}

	// Fast path 1: contiguous advance at upper edge.
	if seq == wm.frontierHi[stage]+1 {
		wm.frontierHi[stage] = seq
		t.drainOOOBitsIntoFrontierLocked(wm, stage)
		return true
	}

	// Fast path 2: contiguous extension at lower edge (e.g. delayed pre-handoff seq = frontierLo - 1).
	if seq+1 == wm.frontierLo[stage] {
		wm.frontierLo[stage] = seq
		t.drainOOOBitsIntoFrontierLocked(wm, stage)
		return true
	}

	// Historical out-of-order sequence below frontierLo - 1: record in exact hist intervals without gap-bridging or eviction.
	if seq < wm.frontierLo[stage] {
		w := t.allocOOOWindowLocked(wm, stage, wm.frontierHi[stage]+2)
		if w == nil {
			return false
		}
		if !w.recordHistInterval(t, seq, seq) {
			if w.isEmpty() {
				t.freeOOOWindowLocked(wm, stage)
			}
			return false
		}
		t.drainOOOBitsIntoFrontierLocked(wm, stage)
		return true
	}

	// Above frontierHi + 1: record in 2048-bit bitmap (or slide frontier up if >= base+2048 while preserving old frontier and runs in hist).
	w := t.allocOOOWindowLocked(wm, stage, wm.frontierHi[stage]+2)
	if w == nil {
		return false
	}
	if w.base == 0 || w.isBitsEmpty() {
		base := wm.frontierHi[stage] + 2
		if seq >= oooWindowBits && seq-(oooWindowBits-1) > base {
			base = seq - (oooWindowBits - 1)
		}
		if seq < base {
			base = seq
		}
		w.base = base
	} else if seq < w.base {
		if !w.rebaseDown(seq) {
			if !w.recordHistInterval(t, seq, seq) {
				return false
			}
			t.drainOOOBitsIntoFrontierLocked(wm, stage)
			return true
		}
	}

	if seq >= w.base && seq-w.base < oooWindowBits {
		off := seq - w.base
		w.bits[off/64] |= uint64(1) << (off % 64)
		t.drainOOOBitsIntoFrontierLocked(wm, stage)
		return true
	}

	// seq exceeds w.base + 2048 while older bits exist: promote current frontier and all bitmap runs into hist
	// without bridging any uncommitted gap and without retiring any committed interval.
	if !w.recordHistInterval(t, wm.frontierLo[stage], wm.frontierHi[stage]) {
		return false
	}
	for i := 0; i < oooWindowBits; {
		word := w.bits[i/64] >> uint(i%64)
		if (word & 1) == 0 {
			i++
			continue
		}
		runLo := w.base + uint64(i)
		runHi := runLo
		for i < oooWindowBits && ((w.bits[i/64]>>uint(i%64))&1) != 0 {
			runHi = w.base + uint64(i)
			i++
		}
		if !w.recordHistInterval(t, runLo, runHi) {
			return false
		}
	}
	for i := range w.bits {
		w.bits[i] = 0
	}
	wm.frontierLo[stage] = seq
	wm.frontierHi[stage] = seq
	w.base = seq + 2
	t.drainOOOBitsIntoFrontierLocked(wm, stage)
	return true
}

func stageSlotExtIndex(stage TrackStage) (int, bool) {
	switch stage {
	case StageChannelDistribute:
		return 0, true
	case StageLocalDistribute:
		return 1, true
	default:
		return 0, false
	}
}

func (t *StageSeqTracker) HasCommitted(k ChannelWatermarkKey, stage TrackStage, seq uint64) bool {
	if t == nil || seq == 0 {
		return false
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels == nil {
		return false
	}
	return isSeqCommittedInWindow(s.channels[k], stage, seq)
}

// BeginStageChecked enforces fail-closed admission (A1 + B2):
// 1. Returns StageBeginAlreadyCommitted if already committed.
// 2. Returns StageBeginInFlight if another worker already holds the in-flight lease.
// 3. Obtains/pins ChannelWatermark and pre-reserves worst-case completion state BEFORE granting StageBeginLeased.
// 4. Returns StageBeginCapacityRetry if shard/watermark/OOO/inFlight capacity is unavailable (zero side effects run).
func (t *StageSeqTracker) BeginStageChecked(k ChannelWatermarkKey, stage TrackStage, seq uint64) StageBeginStatus {
	if t == nil || seq == 0 {
		return StageBeginCapacityRetry
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels != nil {
		if wm := s.channels[k]; isSeqCommittedInWindow(wm, stage, seq) {
			s.accessClock++
			wm.lastAccess = s.accessClock
			return StageBeginAlreadyCommitted
		}
	}
	if s.hasExactInFlightLocked(k, stage, seq) {
		return StageBeginInFlight
	}
	wm := t.getOrCreateWatermarkLocked(s, k)
	if wm == nil {
		return StageBeginCapacityRetry
	}
	reserveBytes, okCap := t.precheckSeqCommitCapacityLocked(wm, stage, seq)
	if !okCap {
		if wm.ooo[stage] != nil && wm.ooo[stage].isEmpty() {
			t.freeOOOWindowLocked(wm, stage)
		}
		return StageBeginCapacityRetry
	}
	if !s.allocInFlightLocked(t, k, stage, seq, false, eventbus.SlotBitset{}, reserveBytes) {
		if wm.ooo[stage] != nil && wm.ooo[stage].isEmpty() {
			t.freeOOOWindowLocked(wm, stage)
		}
		return StageBeginCapacityRetry
	}
	return StageBeginLeased
}

func (t *StageSeqTracker) BeginStage(k ChannelWatermarkKey, stage TrackStage, seq uint64) (skip bool, leased bool) {
	status := t.BeginStageChecked(k, stage, seq)
	switch status {
	case StageBeginAlreadyCommitted, StageBeginInFlight:
		return true, false
	case StageBeginLeased:
		return false, true
	default:
		return false, false
	}
}

func (t *StageSeqTracker) CommitStage(k ChannelWatermarkKey, stage TrackStage, seq uint64) {
	if t == nil || seq == 0 {
		return
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	reserved := s.releaseInFlightLocked(t, k, stage, seq, nil)
	if reserved > 0 {
		t.extraSlotBytes.Add(-reserved)
	}
	wm := t.getOrCreateWatermarkLocked(s, k)
	if wm != nil {
		_ = t.commitFullSeqLocked(wm, stage, seq)
	}
}

func (t *StageSeqTracker) AbortStage(k ChannelWatermarkKey, stage TrackStage, seq uint64) {
	if t == nil || seq == 0 {
		return
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	reserved := s.releaseInFlightLocked(t, k, stage, seq, nil)
	if reserved > 0 {
		t.extraSlotBytes.Add(-reserved)
	}
	if s.channels != nil {
		if wm := s.channels[k]; wm != nil && wm.ooo[stage] != nil && wm.ooo[stage].isEmpty() {
			t.freeOOOWindowLocked(wm, stage)
		}
	}
}

func (t *StageSeqTracker) RecordCommittedForce(k ChannelWatermarkKey, stage TrackStage, seq uint64) bool {
	if t == nil || seq == 0 {
		return false
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	wm := t.getOrCreateWatermarkLocked(s, k)
	if wm == nil {
		return false
	}
	if isSeqCommittedInWindow(wm, stage, seq) {
		return true
	}
	return t.commitFullSeqLocked(wm, stage, seq)
}

func (t *StageSeqTracker) BeginSlotStageChecked(
	k ChannelWatermarkKey,
	stage TrackStage,
	seq uint64,
	requestedMask eventbus.SlotBitset,
) (deliverMask eventbus.SlotBitset, status StageBeginStatus) {
	if t == nil || seq == 0 || requestedMask.IsZero() {
		return eventbus.SlotBitset{}, StageBeginCapacityRetry
	}
	extIdx, ok := stageSlotExtIndex(stage)
	if !ok {
		return eventbus.SlotBitset{}, StageBeginCapacityRetry
	}

	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()

	var wm *ChannelWatermark
	if s.channels != nil {
		wm = s.channels[k]
	}
	if isSeqCommittedInWindow(wm, stage, seq) {
		s.accessClock++
		wm.lastAccess = s.accessClock
		return eventbus.SlotBitset{}, StageBeginAlreadyCommitted
	}

	todo := requestedMask.Clone()
	var existingDone eventbus.SlotBitset
	hasExistingEntry := false
	if wm != nil && wm.slotExt[extIdx] != nil {
		if done, found := wm.slotExt[extIdx].findDoneSlots(seq); found {
			existingDone = done
			hasExistingEntry = true
			todo = todo.AndNot(done)
		}
	}
	if todo.IsZero() {
		return eventbus.SlotBitset{}, StageBeginAlreadyCommitted
	}
	if s.inFlightUsed > 0 {
		for i := range s.inFlight {
			it := &s.inFlight[i]
			if !it.inUse || it.key != k || it.stage != stage || it.seq != seq {
				continue
			}
			if !it.hasMask {
				return eventbus.SlotBitset{}, StageBeginInFlight
			}
			todo = todo.AndNot(it.mask)
		}
	}
	if todo.IsZero() {
		return eventbus.SlotBitset{}, StageBeginInFlight
	}

	wm = t.getOrCreateWatermarkLocked(s, k)
	if wm == nil {
		return eventbus.SlotBitset{}, StageBeginCapacityRetry
	}

	reserveCommitBytes, okCap := t.precheckSeqCommitCapacityLocked(wm, stage, seq)
	if !okCap {
		if wm.ooo[stage] != nil && wm.ooo[stage].isEmpty() {
			t.freeOOOWindowLocked(wm, stage)
		}
		return eventbus.SlotBitset{}, StageBeginCapacityRetry
	}

	// Pre-calculate worst-case partial-slot mask storage requirement so FinishSlotStage never drops succeededMask.
	worstMerged := existingDone.Or(todo)
	deltaMaskBytes := worstMerged.ExtraHeapBytes() - existingDone.ExtraHeapBytes()
	if deltaMaskBytes < 0 {
		deltaMaskBytes = 0
	}
	ext := wm.slotExt[extIdx]
	if !hasExistingEntry && ext != nil {
		activeEntries := ext.countActiveEntries()
		if activeEntries >= maxPartialSeqsPerExt+maxOverflowPartialSeqs {
			return eventbus.SlotBitset{}, StageBeginCapacityRetry
		}
		if activeEntries >= maxPartialSeqsPerExt {
			deltaMaskBytes += partialSeqEntryOverhead
		}
	}
	totalReserve := reserveCommitBytes + deltaMaskBytes
	if !s.allocInFlightLocked(t, k, stage, seq, true, todo, totalReserve) {
		if wm.ooo[stage] != nil && wm.ooo[stage].isEmpty() {
			t.freeOOOWindowLocked(wm, stage)
		}
		return eventbus.SlotBitset{}, StageBeginCapacityRetry
	}
	return todo, StageBeginLeased
}

func (t *StageSeqTracker) BeginSlotStage(k ChannelWatermarkKey, stage TrackStage, seq uint64, requestedMask eventbus.SlotBitset) (deliverMask eventbus.SlotBitset, leased bool) {
	deliv, status := t.BeginSlotStageChecked(k, stage, seq, requestedMask)
	return deliv, status == StageBeginLeased
}

// FinishSlotStage writes succeededMask BEFORE releasing the in-flight lease within the same shard lock (A1 + B2).
// Because BeginSlotStageChecked already pinned wm and pre-reserved worst-case completion state bytes,
// FinishSlotStage never silently drops partial or full completion state.
func (t *StageSeqTracker) FinishSlotStage(
	k ChannelWatermarkKey,
	stage TrackStage,
	seq uint64,
	leasedMask eventbus.SlotBitset,
	succeededMask eventbus.SlotBitset,
	allRecipientSlots eventbus.SlotBitset,
) {
	if t == nil || seq == 0 {
		return
	}
	extIdx, ok := stageSlotExtIndex(stage)
	if !ok {
		return
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()

	if succeededMask.IsZero() {
		reserved := s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
		if reserved > 0 {
			t.extraSlotBytes.Add(-reserved)
		}
		return
	}

	wm := t.getOrCreateWatermarkLocked(s, k)
	if wm == nil || isSeqCommittedInWindow(wm, stage, seq) {
		reserved := s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
		if reserved > 0 {
			t.extraSlotBytes.Add(-reserved)
		}
		return
	}

	// Credit back the in-flight pre-reservation right before recording the actual committed state under the same shard lock,
	// then release the inFlight slot after the state update completes.
	for i := range s.inFlight {
		it := &s.inFlight[i]
		if it.inUse && it.key == k && it.stage == stage && it.seq == seq && it.hasMask && it.mask.Equal(leasedMask) {
			if it.reservedBytes > 0 {
				t.extraSlotBytes.Add(-it.reservedBytes)
				it.reservedBytes = 0
			}
			break
		}
	}

	mergedDone := succeededMask
	ext := wm.slotExt[extIdx]
	inlineIdx := -1
	overflowIdx := -1
	if ext != nil {
		for i := range ext.entries {
			if ext.entries[i].seq == seq {
				inlineIdx = i
				mergedDone = ext.entries[i].doneSlots.Or(succeededMask)
				break
			}
		}
		if inlineIdx < 0 {
			for i := range ext.overflow {
				if ext.overflow[i].seq == seq {
					overflowIdx = i
					mergedDone = ext.overflow[i].doneSlots.Or(succeededMask)
					break
				}
			}
		}
	}

	fullyCovered := !allRecipientSlots.IsZero() && allRecipientSlots.AndNot(mergedDone).IsZero()
	if fullyCovered {
		if t.commitFullSeqLocked(wm, stage, seq) {
			if ext != nil {
				if inlineIdx >= 0 {
					oldExtra := ext.entries[inlineIdx].doneSlots.ExtraHeapBytes()
					if oldExtra > 0 {
						t.extraSlotBytes.Add(-oldExtra)
					}
					ext.entries[inlineIdx] = slotSeqMask{}
				} else if overflowIdx >= 0 {
					oldExtra := ext.overflow[overflowIdx].doneSlots.ExtraHeapBytes() + partialSeqEntryOverhead
					if oldExtra > 0 {
						t.extraSlotBytes.Add(-oldExtra)
					}
					ext.overflow = append(ext.overflow[:overflowIdx], ext.overflow[overflowIdx+1:]...)
				}
				if ext.isEmpty() {
					t.freeSlotExtLocked(wm, extIdx)
				}
			}
			_ = s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
			return
		}
	}

	if ext == nil {
		counted := false
		for {
			cur := t.activeSlotExts.Load()
			if cur >= MaxGlobalSlotExts {
				break
			}
			if t.activeSlotExts.CompareAndSwap(cur, cur+1) {
				counted = true
				break
			}
		}
		ext = &SlotHandoffExt{countedInPool: counted}
		wm.slotExt[extIdx] = ext
	}

	if inlineIdx >= 0 {
		oldExtra := ext.entries[inlineIdx].doneSlots.ExtraHeapBytes()
		newExtra := mergedDone.ExtraHeapBytes()
		if newExtra != oldExtra {
			t.extraSlotBytes.Add(newExtra - oldExtra)
		}
		ext.entries[inlineIdx].doneSlots = mergedDone
		_ = s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
		return
	}
	if overflowIdx >= 0 {
		oldExtra := ext.overflow[overflowIdx].doneSlots.ExtraHeapBytes()
		newExtra := mergedDone.ExtraHeapBytes()
		if newExtra != oldExtra {
			t.extraSlotBytes.Add(newExtra - oldExtra)
		}
		ext.overflow[overflowIdx].doneSlots = mergedDone
		_ = s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
		return
	}

	for i := range ext.entries {
		if ext.entries[i].seq == 0 {
			newExtra := mergedDone.ExtraHeapBytes()
			if newExtra > 0 {
				t.extraSlotBytes.Add(newExtra)
			}
			ext.entries[i] = slotSeqMask{
				seq:       seq,
				doneSlots: mergedDone,
			}
			_ = s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
			return
		}
	}

	// Inline 8 entries are full: append to overflow (never drop partial completion mask!).
	newExtra := mergedDone.ExtraHeapBytes() + partialSeqEntryOverhead
	t.extraSlotBytes.Add(newExtra)
	ext.overflow = append(ext.overflow, slotSeqMask{
		seq:       seq,
		doneSlots: mergedDone,
	})
	_ = s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
}

func (t *StageSeqTracker) InspectChannelStage(channelID string, channelType uint8, stage TrackStage) (contiguous uint64, oooSetBits int, intervalCount int, totalChannels int) {
	if t == nil {
		return 0, 0, 0, 0
	}
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		totalChannels += len(sh.channels)
		sh.mu.Unlock()
	}
	k, ok := MakeChannelWatermarkKey(channelID, channelType)
	if !ok {
		return 0, 0, 0, totalChannels
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels == nil {
		return 0, 0, 0, totalChannels
	}
	wm := s.channels[k]
	if wm == nil {
		return 0, 0, 0, totalChannels
	}
	if wm.frontierHi[stage] > 0 {
		intervalCount = 1
	}
	if w := wm.ooo[stage]; w != nil {
		oooSetBits = w.countSetBits()
		intervalCount += w.histCount()
		if oooSetBits > 0 {
			intervalCount++
		}
	}
	return wm.frontierHi[stage], oooSetBits, intervalCount, totalChannels
}
