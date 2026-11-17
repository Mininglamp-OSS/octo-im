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

	seqTrackerShards      = 64
	MaxChannelsPerShard   = 256 // 64 * 256 = 16,384 max tracked channels globally
	MaxGlobalOOOWindows   = 8192
	MaxGlobalSlotExts     = 2048
	MaxInFlightPerShard   = 1024
	oooWindowBits         = 2048
	oooWindowWords        = oooWindowBits / 64 // 32 uint64s = 256 B
	maxHistIntervals      = 4
	maxPartialSeqsPerExt  = 8
	MaxExtraSlotMaskBytes = int64(2 * 1024 * 1024)
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
	base uint64
	bits [oooWindowWords]uint64
	hist [maxHistIntervals]seqInterval
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
	for i := range w.hist {
		if w.hist[i].hi != 0 {
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
	n := 0
	for i := range w.hist {
		if w.hist[i].hi != 0 {
			n++
		}
	}
	return n
}

func (w *OOOWindow) hasInHist(seq uint64) bool {
	if w == nil || seq == 0 {
		return false
	}
	for i := range w.hist {
		iv := w.hist[i]
		if iv.hi != 0 && seq >= iv.lo && seq <= iv.hi {
			return true
		}
	}
	return false
}

// recordHistInterval records [lo, hi] strictly without bridging uncommitted gaps (closing BF-R7-1a).
// Adjacent intervals (hi+1 == lo) merge cleanly; if all 4 slots are occupied by disjoint intervals,
// the lowest (oldest) committed interval is retired rather than coalescing across an uncommitted gap.
func (w *OOOWindow) recordHistInterval(lo, hi uint64) {
	if w == nil || hi == 0 || lo > hi {
		return
	}
	for i := range w.hist {
		iv := &w.hist[i]
		if iv.hi == 0 {
			continue
		}
		if lo >= iv.lo && hi <= iv.hi {
			return
		}
		if hi+1 >= iv.lo && lo <= iv.hi+1 {
			if lo < iv.lo {
				iv.lo = lo
			}
			if hi > iv.hi {
				iv.hi = hi
			}
			// Merge with any other now-adjacent slot.
			for j := range w.hist {
				if j == i || w.hist[j].hi == 0 {
					continue
				}
				other := w.hist[j]
				if iv.hi+1 >= other.lo && iv.lo <= other.hi+1 {
					if other.lo < iv.lo {
						iv.lo = other.lo
					}
					if other.hi > iv.hi {
						iv.hi = other.hi
					}
					w.hist[j] = seqInterval{}
				}
			}
			return
		}
	}
	for i := range w.hist {
		if w.hist[i].hi == 0 {
			w.hist[i] = seqInterval{lo: lo, hi: hi}
			return
		}
	}
	// Retire lowest historical committed interval; never bridge uncommitted gaps.
	minIdx := 0
	for i := 1; i < len(w.hist); i++ {
		if w.hist[i].hi < w.hist[minIdx].hi {
			minIdx = i
		}
	}
	w.hist[minIdx] = seqInterval{lo: lo, hi: hi}
}

func (w *OOOWindow) absorbAdjacentToFrontier(frontierLo, frontierHi uint64) (uint64, uint64) {
	if w == nil || frontierHi == 0 {
		return frontierLo, frontierHi
	}
	changed := true
	for changed {
		changed = false
		for i := range w.hist {
			iv := w.hist[i]
			if iv.hi == 0 {
				continue
			}
			if iv.hi+1 >= frontierLo && iv.lo <= frontierHi+1 {
				if iv.lo < frontierLo {
					frontierLo = iv.lo
				}
				if iv.hi > frontierHi {
					frontierHi = iv.hi
				}
				w.hist[i] = seqInterval{}
				changed = true
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
	entries [maxPartialSeqsPerExt]slotSeqMask
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
	return true
}

type ChannelWatermark struct {
	key        ChannelWatermarkKey
	lastAccess uint64
	frontierLo [numTrackStages]uint64
	frontierHi [numTrackStages]uint64
	ooo        [numTrackStages]*OOOWindow
	slotExt    [2]*SlotHandoffExt // [0]: StageChannelDistribute, [1]: StageLocalDistribute
}

type shardInFlightSlot struct {
	inUse      bool
	key        ChannelWatermarkKey
	seq        uint64
	stage      TrackStage
	hasMask    bool
	mask       eventbus.SlotBitset
	extraBytes int64
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

func (s *seqTrackerShard) allocInFlightLocked(t *StageSeqTracker, k ChannelWatermarkKey, stage TrackStage, seq uint64, hasMask bool, mask eventbus.SlotBitset) bool {
	if s.inFlightUsed >= MaxInFlightPerShard {
		return false
	}
	clonedMask := mask.Clone()
	extra := clonedMask.ExtraHeapBytes()
	if extra > 0 && !t.tryReserveExtraBytes(extra) {
		return false
	}
	for i := range s.inFlight {
		if !s.inFlight[i].inUse {
			s.inFlight[i] = shardInFlightSlot{
				inUse:      true,
				key:        k,
				seq:        seq,
				stage:      stage,
				hasMask:    hasMask,
				mask:       clonedMask,
				extraBytes: extra,
			}
			s.inFlightUsed++
			return true
		}
	}
	if extra > 0 {
		t.extraSlotBytes.Add(-extra)
	}
	return false
}

func (s *seqTrackerShard) releaseInFlightLocked(t *StageSeqTracker, k ChannelWatermarkKey, stage TrackStage, seq uint64, matchMask *eventbus.SlotBitset) {
	if s.inFlightUsed == 0 {
		return
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
		*it = shardInFlightSlot{}
		s.inFlightUsed--
		return
	}
}

func (t *StageSeqTracker) freeWatermarkResourcesLocked(wm *ChannelWatermark) {
	if wm == nil {
		return
	}
	for st := 0; st < numTrackStages; st++ {
		if wm.ooo[st] != nil {
			wm.ooo[st] = nil
			t.activeOOOWindows.Add(-1)
		}
	}
	for idx := 0; idx < 2; idx++ {
		if ext := wm.slotExt[idx]; ext != nil {
			var extra int64
			for i := range ext.entries {
				extra += ext.entries[i].doneSlots.ExtraHeapBytes()
			}
			if extra > 0 {
				t.extraSlotBytes.Add(-extra)
			}
			wm.slotExt[idx] = nil
			t.activeSlotExts.Add(-1)
		}
	}
}

func (t *StageSeqTracker) getOrCreateWatermarkLocked(s *seqTrackerShard, k ChannelWatermarkKey) *ChannelWatermark {
	if s.channels == nil {
		s.channels = make(map[ChannelWatermarkKey]*ChannelWatermark, 64)
	}
	if wm := s.channels[k]; wm != nil {
		s.accessClock++
		wm.lastAccess = s.accessClock
		return wm
	}
	if len(s.channels) >= MaxChannelsPerShard {
		var (
			evictKey ChannelWatermarkKey
			oldest   uint64
			found    bool
		)
		for ck, cwm := range s.channels {
			if cwm.slotExt[0] != nil || cwm.slotExt[1] != nil || s.hasInFlightKeyLocked(ck) {
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

func (t *StageSeqTracker) drainOOOBitsIntoFrontierLocked(wm *ChannelWatermark, stage TrackStage) {
	w := wm.ooo[stage]
	if w == nil {
		return
	}
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
	wm.frontierLo[stage], wm.frontierHi[stage] = w.absorbAdjacentToFrontier(wm.frontierLo[stage], wm.frontierHi[stage])
	if w.isEmpty() {
		wm.ooo[stage] = nil
		t.activeOOOWindows.Add(-1)
	}
}

func (t *StageSeqTracker) commitFullSeqLocked(wm *ChannelWatermark, stage TrackStage, seq uint64) bool {
	if wm == nil || seq == 0 {
		return false
	}
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

	// Historical out-of-order sequence below frontierLo - 1: record in bounded hist intervals without gap-bridging.
	if seq < wm.frontierLo[stage] {
		w := t.allocOOOWindowLocked(wm, stage, wm.frontierHi[stage]+2)
		if w == nil {
			return false
		}
		w.recordHistInterval(seq, seq)
		wm.frontierLo[stage], wm.frontierHi[stage] = w.absorbAdjacentToFrontier(wm.frontierLo[stage], wm.frontierHi[stage])
		if w.isEmpty() {
			wm.ooo[stage] = nil
			t.activeOOOWindows.Add(-1)
		}
		return true
	}

	// Above frontierHi + 1: record in bounded 2048-bit bitmap (or slide frontier up if >2048 ahead while preserving old frontier in hist).
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
			w.recordHistInterval(seq, seq)
			return true
		}
	}

	if seq >= w.base && seq-w.base < oooWindowBits {
		off := seq - w.base
		w.bits[off/64] |= uint64(1) << (off % 64)
		return true
	}

	// seq exceeds w.base + 2048 while older bits exist: promote current frontier into hist and advance frontier to seq
	// without bridging any uncommitted gap.
	w.recordHistInterval(wm.frontierLo[stage], wm.frontierHi[stage])
	// Also flush any contiguous runs in w.bits into hist before resetting bitmap at seq.
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
		w.recordHistInterval(runLo, runHi)
	}
	for i := range w.bits {
		w.bits[i] = 0
	}
	wm.frontierLo[stage] = seq
	wm.frontierHi[stage] = seq
	w.base = seq + 2
	wm.frontierLo[stage], wm.frontierHi[stage] = w.absorbAdjacentToFrontier(wm.frontierLo[stage], wm.frontierHi[stage])
	if w.isEmpty() {
		wm.ooo[stage] = nil
		t.activeOOOWindows.Add(-1)
	}
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

func (t *StageSeqTracker) BeginStage(k ChannelWatermarkKey, stage TrackStage, seq uint64) (skip bool, leased bool) {
	if t == nil || seq == 0 {
		return false, false
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels != nil {
		if wm := s.channels[k]; isSeqCommittedInWindow(wm, stage, seq) {
			s.accessClock++
			wm.lastAccess = s.accessClock
			return true, false
		}
	}
	if s.hasExactInFlightLocked(k, stage, seq) {
		return true, false
	}
	_ = t.getOrCreateWatermarkLocked(s, k)
	if !s.allocInFlightLocked(t, k, stage, seq, false, eventbus.SlotBitset{}) {
		return true, false
	}
	return false, true
}

func (t *StageSeqTracker) CommitStage(k ChannelWatermarkKey, stage TrackStage, seq uint64) {
	if t == nil || seq == 0 {
		return
	}
	s := &t.shards[k.shardIndex()]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseInFlightLocked(t, k, stage, seq, nil)
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
	s.releaseInFlightLocked(t, k, stage, seq, nil)
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
	_ = t.commitFullSeqLocked(wm, stage, seq)
	return false
}

func (t *StageSeqTracker) BeginSlotStage(k ChannelWatermarkKey, stage TrackStage, seq uint64, requestedMask eventbus.SlotBitset) (deliverMask eventbus.SlotBitset, leased bool) {
	if t == nil || seq == 0 || requestedMask.IsZero() {
		return eventbus.SlotBitset{}, false
	}
	extIdx, ok := stageSlotExtIndex(stage)
	if !ok {
		return eventbus.SlotBitset{}, false
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
		return eventbus.SlotBitset{}, false
	}

	todo := requestedMask.Clone()
	if wm != nil && wm.slotExt[extIdx] != nil {
		ext := wm.slotExt[extIdx]
		for i := range ext.entries {
			if ext.entries[i].seq == seq {
				todo = todo.AndNot(ext.entries[i].doneSlots)
				break
			}
		}
	}
	if s.inFlightUsed > 0 {
		for i := range s.inFlight {
			it := &s.inFlight[i]
			if !it.inUse || it.key != k || it.stage != stage || it.seq != seq {
				continue
			}
			if !it.hasMask {
				return eventbus.SlotBitset{}, false
			}
			todo = todo.AndNot(it.mask)
		}
	}
	if todo.IsZero() {
		return eventbus.SlotBitset{}, false
	}
	_ = t.getOrCreateWatermarkLocked(s, k)
	if !s.allocInFlightLocked(t, k, stage, seq, true, todo) {
		return eventbus.SlotBitset{}, false
	}
	return todo, true
}

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

	s.releaseInFlightLocked(t, k, stage, seq, &leasedMask)
	if succeededMask.IsZero() {
		return
	}

	wm := t.getOrCreateWatermarkLocked(s, k)
	if wm == nil || isSeqCommittedInWindow(wm, stage, seq) {
		return
	}

	mergedDone := succeededMask
	ext := wm.slotExt[extIdx]
	entryIdx := -1
	if ext != nil {
		for i := range ext.entries {
			if ext.entries[i].seq == seq {
				entryIdx = i
				mergedDone = ext.entries[i].doneSlots.Or(succeededMask)
				break
			}
		}
	}

	fullyCovered := !allRecipientSlots.IsZero() && allRecipientSlots.AndNot(mergedDone).IsZero()
	if fullyCovered {
		if t.commitFullSeqLocked(wm, stage, seq) {
			if ext != nil && entryIdx >= 0 {
				oldExtra := ext.entries[entryIdx].doneSlots.ExtraHeapBytes()
				if oldExtra > 0 {
					t.extraSlotBytes.Add(-oldExtra)
				}
				ext.entries[entryIdx] = slotSeqMask{}
				if ext.isEmpty() {
					wm.slotExt[extIdx] = nil
					t.activeSlotExts.Add(-1)
				}
			}
			return
		}
	}

	if ext == nil {
		for {
			cur := t.activeSlotExts.Load()
			if cur >= MaxGlobalSlotExts {
				return
			}
			if t.activeSlotExts.CompareAndSwap(cur, cur+1) {
				break
			}
		}
		ext = &SlotHandoffExt{}
		wm.slotExt[extIdx] = ext
	}
	if entryIdx >= 0 {
		oldExtra := ext.entries[entryIdx].doneSlots.ExtraHeapBytes()
		newExtra := mergedDone.ExtraHeapBytes()
		if newExtra > oldExtra && !t.tryReserveExtraBytes(newExtra-oldExtra) {
			return
		}
		if newExtra < oldExtra {
			t.extraSlotBytes.Add(newExtra - oldExtra)
		}
		ext.entries[entryIdx].doneSlots = mergedDone
		return
	}
	for i := range ext.entries {
		if ext.entries[i].seq == 0 {
			newExtra := mergedDone.ExtraHeapBytes()
			if newExtra > 0 && !t.tryReserveExtraBytes(newExtra) {
				if ext.isEmpty() {
					wm.slotExt[extIdx] = nil
					t.activeSlotExts.Add(-1)
				}
				return
			}
			ext.entries[i] = slotSeqMask{
				seq:       seq,
				doneSlots: mergedDone,
			}
			return
		}
	}
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
