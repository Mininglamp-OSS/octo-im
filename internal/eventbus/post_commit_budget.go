package eventbus

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	MaxPostCommitBytes       int64 = 16 * 1024 * 1024 // 16.00 MiB primary persisted post-commit budget
	MaxPostCommitItems       int64 = 16384
	MaxLegacyInboundBytes    int64 = 8 * 1024 * 1024 // 8.00 MiB dedicated rolling-upgrade 21cd8f88 spillover budget
	MaxLegacyInboundItems    int64 = 8192
	MaxTransientEventBytes   int64 = 1 * 1024 * 1024 // 1.00 MiB transient (MessageSeq == 0) event budget
	MaxTransientEventItems   int64 = 1024
	FixedPostCommitOverheadB int64 = 2048 // Covers Event header + RecoveryItem + map slot

	// MaxWireStringBytes matches WuKongIMGoProto Encoder.WriteBinary / Decoder.Binary signed int16 framing limit.
	MaxWireStringBytes        int    = math.MaxInt16
	MaxWireSparseSlotWords    int    = 4096
	MaxRepresentableSlotCount uint32 = (4 + uint32(MaxWireSparseSlotWords)) * 64 // 262,400 contiguous slots
)

var (
	slotMaskMagic  = [4]byte{'W', 'K', 'S', 'M'}
	stageMetaMagic = [4]byte{'W', 'K', 'S', 'T'}

	ErrPostCommitBackpressure  = errors.New("post-commit budget exhausted")
	ErrTransientBudgetFull     = errors.New("transient event budget exhausted")
	ErrWireChannelIDTooLong    = errors.New("channel_id exceeds wire framing limit (32767 bytes)")
	ErrSlotCountExceedsWireCap = errors.New("slot count exceeds wire sparse slot mask capacity (262400)")
	ErrTicketClosed            = errors.New("post-commit ticket entry is already closed")
	ErrDuplicateTicketRelease  = errors.New("post-commit ticket handle already released")
)

// SlotWord represents one non-zero 64-slot block for slot IDs >= 256 across the full uint32 slot domain.
type SlotWord struct {
	Word uint32
	Bits uint64
}

// SlotBitset stores slots 0..255 inline with zero heap allocations (covering default SlotCount=64..256)
// and represents arbitrary uint32 slot IDs (256..math.MaxUint32) via sorted non-zero sparse 64-bit blocks
// without any artificial 65,536 cap or dense O(maxSlotID) allocation.
type SlotBitset struct {
	inline [4]uint64
	sparse []SlotWord
}

func ValidateSlotCountForWire(slotCount uint32) error {
	if slotCount > MaxRepresentableSlotCount {
		return fmt.Errorf("%w: slotCount=%d max=%d", ErrSlotCountExceedsWireCap, slotCount, MaxRepresentableSlotCount)
	}
	return nil
}

// AllSlotsUpTo constructs a dense SlotBitset for [0, slotCount) and returns an explicit error
// instead of silently truncating when slotCount exceeds MaxRepresentableSlotCount (262,400).
func AllSlotsUpTo(slotCount uint32) (SlotBitset, error) {
	return AllSlotsUpToChecked(slotCount)
}

func AllSlotsUpToChecked(slotCount uint32) (SlotBitset, error) {
	var b SlotBitset
	if err := ValidateSlotCountForWire(slotCount); err != nil {
		return b, err
	}
	if slotCount == 0 {
		return b, nil
	}
	inlineLimit := slotCount
	if inlineLimit > 256 {
		inlineLimit = 256
	}
	fullInlineWords := inlineLimit / 64
	for w := uint32(0); w < fullInlineWords; w++ {
		b.inline[w] = ^uint64(0)
	}
	if rem := inlineLimit % 64; rem > 0 && fullInlineWords < 4 {
		b.inline[fullInlineWords] = (uint64(1) << rem) - 1
	}
	if slotCount <= 256 {
		return b, nil
	}
	totalWords := (uint64(slotCount) + 63) / 64
	extraWords := totalWords - 4
	b.sparse = make([]SlotWord, 0, extraWords)
	for w := uint64(4); w < 4+extraWords; w++ {
		bitsVal := ^uint64(0)
		if (w+1)*64 > uint64(slotCount) {
			rem := uint64(slotCount) - w*64
			if rem < 64 {
				bitsVal = (uint64(1) << rem) - 1
			}
		}
		if bitsVal != 0 {
			b.sparse = append(b.sparse, SlotWord{Word: uint32(w), Bits: bitsVal})
		}
	}
	return b, nil
}

func SlotIDsToBitset(slots []uint32) SlotBitset {
	var b SlotBitset
	for _, s := range slots {
		b.Set(s)
	}
	return b
}

func (b SlotBitset) ExtraHeapBytes() int64 {
	return int64(cap(b.sparse)) * 16
}

func (b SlotBitset) SparseWordCount() int {
	return len(b.sparse)
}

func (b SlotBitset) Count() int {
	n := 0
	for i := 0; i < 4; i++ {
		n += bits.OnesCount64(b.inline[i])
	}
	for _, sw := range b.sparse {
		n += bits.OnesCount64(sw.Bits)
	}
	return n
}

func (b SlotBitset) Clone() SlotBitset {
	out := SlotBitset{inline: b.inline}
	if len(b.sparse) > 0 {
		out.sparse = make([]SlotWord, len(b.sparse))
		copy(out.sparse, b.sparse)
	}
	return out
}

func (b SlotBitset) IsZero() bool {
	if b.inline[0] != 0 || b.inline[1] != 0 || b.inline[2] != 0 || b.inline[3] != 0 {
		return false
	}
	for _, sw := range b.sparse {
		if sw.Bits != 0 {
			return false
		}
	}
	return true
}

func (b SlotBitset) Equal(o SlotBitset) bool {
	if b.inline != o.inline || len(b.sparse) != len(o.sparse) {
		return false
	}
	for i := range b.sparse {
		if b.sparse[i] != o.sparse[i] {
			return false
		}
	}
	return true
}

func (b *SlotBitset) Set(slotID uint32) {
	w := slotID / 64
	mask := uint64(1) << (slotID % 64)
	if w < 4 {
		b.inline[w] |= mask
		return
	}
	idx := sort.Search(len(b.sparse), func(i int) bool {
		return b.sparse[i].Word >= w
	})
	if idx < len(b.sparse) && b.sparse[idx].Word == w {
		b.sparse[idx].Bits |= mask
		return
	}
	b.sparse = append(b.sparse, SlotWord{})
	copy(b.sparse[idx+1:], b.sparse[idx:])
	b.sparse[idx] = SlotWord{Word: w, Bits: mask}
}

func (b SlotBitset) Has(slotID uint32) bool {
	w := slotID / 64
	mask := uint64(1) << (slotID % 64)
	if w < 4 {
		return (b.inline[w] & mask) != 0
	}
	idx := sort.Search(len(b.sparse), func(i int) bool {
		return b.sparse[i].Word >= w
	})
	if idx < len(b.sparse) && b.sparse[idx].Word == w {
		return (b.sparse[idx].Bits & mask) != 0
	}
	return false
}

func (b SlotBitset) Or(o SlotBitset) SlotBitset {
	var out SlotBitset
	for i := 0; i < 4; i++ {
		out.inline[i] = b.inline[i] | o.inline[i]
	}
	if len(b.sparse) == 0 && len(o.sparse) == 0 {
		return out
	}
	out.sparse = make([]SlotWord, 0, len(b.sparse)+len(o.sparse))
	i, j := 0, 0
	for i < len(b.sparse) && j < len(o.sparse) {
		bw, ow := b.sparse[i], o.sparse[j]
		if bw.Word < ow.Word {
			if bw.Bits != 0 {
				out.sparse = append(out.sparse, bw)
			}
			i++
		} else if ow.Word < bw.Word {
			if ow.Bits != 0 {
				out.sparse = append(out.sparse, ow)
			}
			j++
		} else {
			if merged := bw.Bits | ow.Bits; merged != 0 {
				out.sparse = append(out.sparse, SlotWord{Word: bw.Word, Bits: merged})
			}
			i++
			j++
		}
	}
	for ; i < len(b.sparse); i++ {
		if b.sparse[i].Bits != 0 {
			out.sparse = append(out.sparse, b.sparse[i])
		}
	}
	for ; j < len(o.sparse); j++ {
		if o.sparse[j].Bits != 0 {
			out.sparse = append(out.sparse, o.sparse[j])
		}
	}
	if len(out.sparse) == 0 {
		out.sparse = nil
	}
	return out
}

func (b SlotBitset) And(o SlotBitset) SlotBitset {
	var out SlotBitset
	for i := 0; i < 4; i++ {
		out.inline[i] = b.inline[i] & o.inline[i]
	}
	if len(b.sparse) == 0 || len(o.sparse) == 0 {
		return out
	}
	i, j := 0, 0
	for i < len(b.sparse) && j < len(o.sparse) {
		bw, ow := b.sparse[i], o.sparse[j]
		if bw.Word < ow.Word {
			i++
		} else if ow.Word < bw.Word {
			j++
		} else {
			if inter := bw.Bits & ow.Bits; inter != 0 {
				out.sparse = append(out.sparse, SlotWord{Word: bw.Word, Bits: inter})
			}
			i++
			j++
		}
	}
	return out
}

func (b SlotBitset) AndNot(o SlotBitset) SlotBitset {
	var out SlotBitset
	for i := 0; i < 4; i++ {
		out.inline[i] = b.inline[i] &^ o.inline[i]
	}
	if len(b.sparse) == 0 {
		return out
	}
	i, j := 0, 0
	for i < len(b.sparse) {
		bw := b.sparse[i]
		for j < len(o.sparse) && o.sparse[j].Word < bw.Word {
			j++
		}
		diff := bw.Bits
		if j < len(o.sparse) && o.sparse[j].Word == bw.Word {
			diff &^= o.sparse[j].Bits
		}
		if diff != 0 {
			out.sparse = append(out.sparse, SlotWord{Word: bw.Word, Bits: diff})
		}
		i++
	}
	return out
}

func (b SlotBitset) ForEachSetBit(fn func(slotID uint32)) {
	for w := uint32(0); w < 4; w++ {
		word := b.inline[w]
		for word != 0 {
			bit := uint32(bits.TrailingZeros64(word))
			word &= word - 1
			fn(w*64 + bit)
		}
	}
	for _, sw := range b.sparse {
		word := sw.Bits
		base := sw.Word * 64
		for word != 0 {
			bit := uint32(bits.TrailingZeros64(word))
			word &= word - 1
			fn(base + bit)
		}
	}
}

// EncodeSlotMaskTrailer writes:
// "WKSM" (4B) + ver=1 (1B) + inline[4]uint64 (32B BE) + sparseCount uint32 (4B BE) + sparseCount*(4B Word BE + 8B Bits BE).
func EncodeSlotMaskTrailer(dst []byte, hasMask bool, includeTrailer bool, mask SlotBitset) ([]byte, error) {
	if !includeTrailer || !hasMask {
		return dst, nil
	}
	if len(mask.sparse) > MaxWireSparseSlotWords {
		return nil, fmt.Errorf("%w: WKSM sparse word count %d exceeds %d", ErrSlotCountExceedsWireCap, len(mask.sparse), MaxWireSparseSlotWords)
	}
	dst = append(dst, slotMaskMagic[:]...)
	dst = append(dst, 1)
	for i := 0; i < 4; i++ {
		dst = binary.BigEndian.AppendUint64(dst, mask.inline[i])
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(mask.sparse)))
	for _, sw := range mask.sparse {
		dst = binary.BigEndian.AppendUint32(dst, sw.Word)
		dst = binary.BigEndian.AppendUint64(dst, sw.Bits)
	}
	return dst, nil
}

func DecodeSlotMaskTrailer(rem []byte) (hasMask bool, mask SlotBitset, rest []byte, err error) {
	if len(rem) == 0 {
		return false, SlotBitset{}, nil, nil
	}
	if len(rem) >= 4 &&
		rem[0] == stageMetaMagic[0] &&
		rem[1] == stageMetaMagic[1] &&
		rem[2] == stageMetaMagic[2] &&
		rem[3] == stageMetaMagic[3] {
		return false, SlotBitset{}, rem, nil
	}
	if len(rem) < 41 ||
		rem[0] != slotMaskMagic[0] ||
		rem[1] != slotMaskMagic[1] ||
		rem[2] != slotMaskMagic[2] ||
		rem[3] != slotMaskMagic[3] ||
		rem[4] != 1 {
		return false, SlotBitset{}, nil, fmt.Errorf("invalid WKSM trailer header: len=%d", len(rem))
	}
	for i := 0; i < 4; i++ {
		mask.inline[i] = binary.BigEndian.Uint64(rem[5+i*8 : 5+(i+1)*8])
	}
	sc := binary.BigEndian.Uint32(rem[37:41])
	wireLen := 41 + int(sc)*12
	if sc > uint32(MaxWireSparseSlotWords) || len(rem) < wireLen {
		return false, SlotBitset{}, nil, fmt.Errorf("invalid WKSM trailer sparseCount=%d len=%d", sc, len(rem))
	}
	if sc > 0 {
		mask.sparse = make([]SlotWord, sc)
		var prevWord uint32 = 3
		for i := uint32(0); i < sc; i++ {
			off := 41 + int(i)*12
			w := binary.BigEndian.Uint32(rem[off : off+4])
			bitsVal := binary.BigEndian.Uint64(rem[off+4 : off+12])
			if w <= prevWord || bitsVal == 0 {
				return false, SlotBitset{}, nil, fmt.Errorf("invalid WKSM sparse word[%d]=%d bits=%x", i, w, bitsVal)
			}
			prevWord = w
			mask.sparse[i] = SlotWord{Word: w, Bits: bitsVal}
		}
	}
	return true, mask, rem[wireLen:], nil
}

// EncodeStageMetaTrailer writes per-event PartAV2 post-commit stage ownership metadata:
// "WKST" (4B) + ver=1 (1B) + count uint32 (4B BE) + count*(hasMeta 1B + pluginStatus 1B + webhookStatus 1B + ownerNode uint64 BE + ownerGen uint32 BE).
func EncodeStageMetaTrailer(dst []byte, includeTrailer bool, events []*Event) []byte {
	if !includeTrailer || len(events) == 0 {
		return dst
	}
	hasAny := false
	for _, ev := range events {
		if ev != nil && (ev.HasStageMeta || ev.PluginStageStatus != StageStatusPending || ev.WebhookStageStatus != StageStatusPending || ev.StageOwnerNodeId != 0) {
			hasAny = true
			break
		}
	}
	if !hasAny {
		return dst
	}
	dst = append(dst, stageMetaMagic[:]...)
	dst = append(dst, 1)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(events)))
	for _, ev := range events {
		if ev == nil {
			dst = append(dst, 0, 0, 0)
			dst = binary.BigEndian.AppendUint64(dst, 0)
			dst = binary.BigEndian.AppendUint32(dst, 0)
			continue
		}
		has := byte(0)
		if ev.HasStageMeta || ev.PluginStageStatus != StageStatusPending || ev.WebhookStageStatus != StageStatusPending || ev.StageOwnerNodeId != 0 {
			has = 1
		}
		dst = append(dst, has, byte(ev.PluginStageStatus), byte(ev.WebhookStageStatus))
		dst = binary.BigEndian.AppendUint64(dst, ev.StageOwnerNodeId)
		dst = binary.BigEndian.AppendUint32(dst, ev.StageOwnerGen)
	}
	return dst
}

func DecodeStageMetaTrailer(rem []byte, events []*Event) error {
	if len(rem) == 0 {
		return nil
	}
	if len(rem) < 9 ||
		rem[0] != stageMetaMagic[0] ||
		rem[1] != stageMetaMagic[1] ||
		rem[2] != stageMetaMagic[2] ||
		rem[3] != stageMetaMagic[3] ||
		rem[4] != 1 {
		return fmt.Errorf("invalid WKST trailer header: len=%d", len(rem))
	}
	cnt := binary.BigEndian.Uint32(rem[5:9])
	expectedLen := 9 + int(cnt)*15
	if len(rem) != expectedLen || int(cnt) != len(events) {
		return fmt.Errorf("invalid WKST trailer count=%d events=%d len=%d", cnt, len(events), len(rem))
	}
	for i := 0; i < int(cnt); i++ {
		off := 9 + i*15
		ev := events[i]
		if ev == nil {
			continue
		}
		ev.HasStageMeta = rem[off] != 0
		ev.PluginStageStatus = PostCommitStageStatus(rem[off+1])
		ev.WebhookStageStatus = PostCommitStageStatus(rem[off+2])
		ev.StageOwnerNodeId = binary.BigEndian.Uint64(rem[off+3 : off+11])
		ev.StageOwnerGen = binary.BigEndian.Uint32(rem[off+11 : off+15])
	}
	return nil
}

type PostCommitBudget struct {
	usedBytes      atomic.Int64
	usedItems      atomic.Int64
	maxBytes       atomic.Int64
	maxItems       atomic.Int64
	rejected       atomic.Uint64
	doubleReleases atomic.Uint64
	notifyCh       chan struct{}
}

func NewPostCommitBudget(maxBytes, maxItems int64) *PostCommitBudget {
	b := &PostCommitBudget{
		notifyCh: make(chan struct{}, 1),
	}
	b.maxBytes.Store(maxBytes)
	b.maxItems.Store(maxItems)
	return b
}

func (b *PostCommitBudget) SetLimits(maxBytes, maxItems int64) {
	if b == nil {
		return
	}
	b.maxBytes.Store(maxBytes)
	b.maxItems.Store(maxItems)
}

func (b *PostCommitBudget) Limits() (maxBytes, maxItems int64) {
	if b == nil {
		return 0, 0
	}
	return b.maxBytes.Load(), b.maxItems.Load()
}

func (b *PostCommitBudget) Stats() (usedBytes, usedItems int64, rejected uint64) {
	if b == nil {
		return 0, 0, 0
	}
	return b.usedBytes.Load(), b.usedItems.Load(), b.rejected.Load()
}

func (b *PostCommitBudget) DoubleReleases() uint64 {
	if b == nil {
		return 0
	}
	return b.doubleReleases.Load()
}

func (b *PostCommitBudget) tryReserveBytesOnly(delta int64) bool {
	if b == nil || delta <= 0 {
		if b != nil && delta < 0 {
			b.usedBytes.Add(delta)
		}
		return true
	}
	maxB := b.maxBytes.Load()
	for {
		curB := b.usedBytes.Load()
		if curB+delta > maxB {
			b.rejected.Add(1)
			return false
		}
		if b.usedBytes.CompareAndSwap(curB, curB+delta) {
			return true
		}
	}
}

// PostCommitBudgetEntry is the shared underlying budget entry (A4 + B3).
// It tracks total reserved bytes and live stage handles under a short critical-section lock
// so AcquireHandle and the final Release are strictly mutually exclusive and never resurrect a closed entry.
type PostCommitBudgetEntry struct {
	mu           sync.Mutex
	budget       *PostCommitBudget
	bytes        int64
	extraBytes   int64
	liveHandles  int32
	closed       bool
	nextHandleID uint64
}

func (e *PostCommitBudgetEntry) IsClosed() bool {
	if e == nil {
		return true
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	return closed
}

func (e *PostCommitBudgetEntry) Refs() int32 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	refs := e.liveHandles
	e.mu.Unlock()
	return refs
}

func (e *PostCommitBudgetEntry) TotalBytes() int64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	total := e.bytes + e.extraBytes
	e.mu.Unlock()
	return total
}

// PostCommitTicket is an independent single-owner TicketHandle over a shared PostCommitBudgetEntry (A4 + B3).
// Each handle has a unique handleID and can be released at most once.
type PostCommitTicket struct {
	entry      *PostCommitBudgetEntry
	handleID   uint64
	stageOwner string
	released   atomic.Bool
}

// AcquireHandle atomically creates a new independent TicketHandle on the same live PostCommitBudgetEntry
// if and only if the entry is still live (liveHandles > 0 && !closed). It never resurrects a closed entry.
func (tk *PostCommitTicket) AcquireHandle() (*PostCommitTicket, error) {
	return tk.AcquireForStage("")
}

func (tk *PostCommitTicket) AcquireForStage(stageOwner string) (*PostCommitTicket, error) {
	if tk == nil || tk.released.Load() || tk.entry == nil {
		return nil, ErrTicketClosed
	}
	e := tk.entry
	e.mu.Lock()
	if e.closed || e.liveHandles <= 0 || tk.released.Load() {
		e.mu.Unlock()
		return nil, ErrTicketClosed
	}
	e.liveHandles++
	e.nextHandleID++
	hid := e.nextHandleID
	e.mu.Unlock()
	return &PostCommitTicket{
		entry:      e,
		handleID:   hid,
		stageOwner: stageOwner,
	}, nil
}

// Retain acquires a new distinct TicketHandle sharing the underlying PostCommitBudgetEntry.
// If the entry has already been closed by a concurrent final Release, Retain returns nil without panicking or resurrecting.
func (tk *PostCommitTicket) Retain() *PostCommitTicket {
	h, err := tk.AcquireHandle()
	if err != nil {
		return nil
	}
	return h
}

// Move explicitly transfers ownership of this TicketHandle.
func (tk *PostCommitTicket) Move() *PostCommitTicket {
	return tk
}

// ReleaseWithCheck releases this specific TicketHandle at most once.
// When the last live handle on the shared PostCommitBudgetEntry is released, the entry is marked closed
// and its bytes and item count are returned to PostCommitBudget before ReleaseWithCheck returns.
func (tk *PostCommitTicket) ReleaseWithCheck() error {
	if tk == nil || tk.entry == nil {
		return nil
	}
	if !tk.released.CompareAndSwap(false, true) {
		if tk.entry.budget != nil {
			tk.entry.budget.doubleReleases.Add(1)
		}
		return ErrDuplicateTicketRelease
	}
	e := tk.entry
	e.mu.Lock()
	if e.closed || e.liveHandles <= 0 {
		e.mu.Unlock()
		return ErrDuplicateTicketRelease
	}
	e.liveHandles--
	if e.liveHandles == 0 {
		e.closed = true
		totalBytes := e.bytes + e.extraBytes
		if e.budget != nil {
			e.budget.usedBytes.Add(-totalBytes)
			e.budget.usedItems.Add(-1)
			select {
			case e.budget.notifyCh <- struct{}{}:
			default:
			}
		}
	}
	e.mu.Unlock()
	return nil
}

func (tk *PostCommitTicket) Release() {
	_ = tk.ReleaseWithCheck()
}

func (tk *PostCommitTicket) IsReleased() bool {
	if tk == nil || tk.released.Load() || tk.entry == nil {
		return true
	}
	return tk.entry.IsClosed()
}

func (tk *PostCommitTicket) IsHandleReleased() bool {
	if tk == nil {
		return true
	}
	return tk.released.Load()
}

func (tk *PostCommitTicket) Refs() int32 {
	if tk == nil || tk.entry == nil {
		return 0
	}
	return tk.entry.Refs()
}

func (tk *PostCommitTicket) HandleID() uint64 {
	if tk == nil {
		return 0
	}
	return tk.handleID
}

func (tk *PostCommitTicket) Entry() *PostCommitBudgetEntry {
	if tk == nil {
		return nil
	}
	return tk.entry
}

func (tk *PostCommitTicket) SameHandle(other *PostCommitTicket) bool {
	if tk == nil || other == nil {
		return tk == other
	}
	return tk.entry != nil && tk.entry == other.entry && tk.handleID == other.handleID
}

func (tk *PostCommitTicket) SameEntry(other *PostCommitTicket) bool {
	if tk == nil || other == nil {
		return false
	}
	return tk.entry != nil && tk.entry == other.entry
}

func (tk *PostCommitTicket) Bytes() int64 {
	if tk == nil || tk.entry == nil {
		return 0
	}
	return tk.entry.TotalBytes()
}

// TryAddExtraBytes charges or credits incremental metadata/mask capacity bytes against the ticket's budget entry (B7).
func (tk *PostCommitTicket) TryAddExtraBytes(delta int64) bool {
	if tk == nil || tk.released.Load() || tk.entry == nil {
		return false
	}
	if delta == 0 {
		return true
	}
	e := tk.entry
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.liveHandles <= 0 || tk.released.Load() {
		return false
	}
	if delta > 0 {
		if e.budget != nil && !e.budget.tryReserveBytesOnly(delta) {
			return false
		}
		e.extraBytes += delta
		return true
	}
	credit := -delta
	if credit > e.extraBytes {
		credit = e.extraBytes
	}
	if credit > 0 {
		e.extraBytes -= credit
		if e.budget != nil {
			e.budget.usedBytes.Add(-credit)
		}
	}
	return true
}

func EstimatePostCommitEventBytes(fakeChannelID string, ev *Event) int64 {
	if ev == nil {
		return FixedPostCommitOverheadB
	}
	var payloadCap int64
	var clientMsgNoLen int
	var origChannelLen int
	switch f := ev.Frame.(type) {
	case *wkproto.SendPacket:
		if f != nil {
			payloadCap = int64(cap(f.Payload))
			clientMsgNoLen = len(f.ClientMsgNo)
			origChannelLen = len(f.ChannelID)
		}
	case *wkproto.EventPacket:
		if f != nil {
			payloadCap = int64(cap(f.Data))
			clientMsgNoLen = len(f.Id)
		}
	}
	var connBytes int
	if ev.Conn != nil {
		connBytes = len(ev.Conn.Uid) + len(ev.Conn.DeviceId)
	}
	return FixedPostCommitOverheadB +
		payloadCap +
		ev.TargetSlotMask.ExtraHeapBytes() +
		int64(2*len(fakeChannelID)+len(ev.ChannelId)+origChannelLen+len(ev.TagKey)+clientMsgNoLen+len(ev.ReqId)+connBytes)
}

func (b *PostCommitBudget) TryReserve(fakeChannelID string, events []*Event) ([]*PostCommitTicket, bool) {
	if b == nil || len(events) == 0 {
		return nil, true
	}
	maxI := b.maxItems.Load()
	maxB := b.maxBytes.Load()
	sizes := make([]int64, len(events))
	var total int64
	for i, ev := range events {
		sizes[i] = EstimatePostCommitEventBytes(fakeChannelID, ev)
		total += sizes[i]
	}
	n := int64(len(events))
	for {
		curI := b.usedItems.Load()
		if curI+n > maxI {
			b.rejected.Add(1)
			return nil, false
		}
		if b.usedItems.CompareAndSwap(curI, curI+n) {
			break
		}
	}
	for {
		curB := b.usedBytes.Load()
		if curB+total > maxB {
			b.usedItems.Add(-n)
			b.rejected.Add(1)
			return nil, false
		}
		if b.usedBytes.CompareAndSwap(curB, curB+total) {
			break
		}
	}
	tickets := make([]*PostCommitTicket, len(events))
	for i, sz := range sizes {
		entry := &PostCommitBudgetEntry{
			budget:       b,
			bytes:        sz,
			liveHandles:  1,
			nextHandleID: 1,
		}
		tickets[i] = &PostCommitTicket{
			entry:    entry,
			handleID: 1,
		}
	}
	return tickets, true
}

func (b *PostCommitBudget) ReserveWithGrace(ctx context.Context, grace time.Duration, fakeChannelID string, events []*Event) ([]*PostCommitTicket, error) {
	if tks, ok := b.TryReserve(fakeChannelID, events); ok {
		return tks, nil
	}
	if grace <= 0 {
		return nil, ErrPostCommitBackpressure
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, ErrPostCommitBackpressure
		case <-b.notifyCh:
			if tks, ok := b.TryReserve(fakeChannelID, events); ok {
				return tks, nil
			}
		}
	}
}

// CompactSendPayloadClone clones the Event header without duplicating immutable SendPacket.Payload
// unless the underlying slice has large excess capacity (e.g. >64B slack from a larger decode buffer).
func CompactSendPayloadClone(e *Event) *Event {
	if e == nil {
		return nil
	}
	cp := e.Clone()
	if sp, ok := cp.Frame.(*wkproto.SendPacket); ok && sp != nil {
		if cap(sp.Payload) > len(sp.Payload)+64 {
			spCopy := *sp
			spCopy.Payload = bytes.Clone(spCopy.Payload)
			cp.Frame = &spCopy
		}
	}
	return cp
}

var (
	DefaultPostCommitBudget    = NewPostCommitBudget(MaxPostCommitBytes, MaxPostCommitItems)
	DefaultLegacyInboundBudget = NewPostCommitBudget(MaxLegacyInboundBytes, MaxLegacyInboundItems)
	DefaultTransientBudget     = NewPostCommitBudget(MaxTransientEventBytes, MaxTransientEventItems)
	LegacyForwardDroppedTotal  atomic.Uint64
	PendingPersistFullTotal    atomic.Uint64
	LegacyBudgetExhaustedTotal atomic.Uint64
	RecoveryBackpressureTotal  atomic.Uint64
)

type PartATelemetrySnapshot struct {
	PostCommitInflightItems       int64
	PostCommitInflightBytes       int64
	LegacyInboundInflightItems    int64
	LegacyInboundInflightBytes    int64
	TransientInflightItems        int64
	TransientInflightBytes        int64
	PostCommitBackpressureRejects uint64
	LegacyForwardDroppedTotal     uint64
	PendingPersistActiveItems     int
	PendingPersistFullTotal       uint64
	LegacyBudgetExhaustedTotal    uint64
	RecoveryBackpressureTotal     uint64
	RecoveryChannelsActive        int
	RecoveryItemsActive           int
	RecoveryQueuedMetadataBytes   int64
	RecoveryInFlightMetadataBytes int64
	RecoveryEnqueueRejectedTotal  uint64
	RecoveryCooldownProbesTotal   uint64
	TicketDoubleReleasesTotal     uint64
	SeqTrackerActiveOOOWindows    int32
	SeqTrackerActiveSlotExts      int32
	SeqTrackerExtraSlotBytes      int64
	SeqTrackerShardSaturatedTotal uint64
	SeqTrackerEvictedChannels     uint64
}

var globalTelemetryProvider atomic.Value // func() PartATelemetrySnapshot

func RegisterPartATelemetryProvider(fn func() PartATelemetrySnapshot) {
	if fn != nil {
		globalTelemetryProvider.Store(fn)
	}
}

func SnapshotPartATelemetry() PartATelemetrySnapshot {
	if v := globalTelemetryProvider.Load(); v != nil {
		if fn, ok := v.(func() PartATelemetrySnapshot); ok && fn != nil {
			return fn()
		}
	}
	pcB, pcI, pcRej := DefaultPostCommitBudget.Stats()
	lgB, lgI, _ := DefaultLegacyInboundBudget.Stats()
	trB, trI, _ := DefaultTransientBudget.Stats()
	return PartATelemetrySnapshot{
		PostCommitInflightItems:       pcI,
		PostCommitInflightBytes:       pcB,
		LegacyInboundInflightItems:    lgI,
		LegacyInboundInflightBytes:    lgB,
		TransientInflightItems:        trI,
		TransientInflightBytes:        trB,
		PostCommitBackpressureRejects: pcRej,
		LegacyForwardDroppedTotal:     LegacyForwardDroppedTotal.Load(),
		PendingPersistFullTotal:       PendingPersistFullTotal.Load(),
		LegacyBudgetExhaustedTotal:    LegacyBudgetExhaustedTotal.Load(),
		RecoveryBackpressureTotal:     RecoveryBackpressureTotal.Load(),
		TicketDoubleReleasesTotal:     DefaultPostCommitBudget.DoubleReleases() + DefaultLegacyInboundBudget.DoubleReleases(),
	}
}

var globalPersistedRecoveryFallback atomic.Value // func(channelID string, channelType uint8, ev *Event) bool

func RegisterPersistedRecoveryFallback(fn func(channelID string, channelType uint8, ev *Event) bool) {
	if fn != nil {
		globalPersistedRecoveryFallback.Store(fn)
	}
}

func EnqueuePersistedRecoveryFallback(channelID string, channelType uint8, ev *Event) bool {
	if v := globalPersistedRecoveryFallback.Load(); v != nil {
		if fn, ok := v.(func(string, uint8, *Event) bool); ok && fn != nil {
			return fn(channelID, channelType, ev)
		}
	}
	return false
}

var globalPendingPersistFallback atomic.Value // func(channelID string, channelType uint8, ev *Event) bool

func RegisterPendingPersistFallback(fn func(channelID string, channelType uint8, ev *Event) bool) {
	if fn != nil {
		globalPendingPersistFallback.Store(fn)
	}
}

func EnqueuePendingPersistFallback(channelID string, channelType uint8, ev *Event) bool {
	if v := globalPendingPersistFallback.Load(); v != nil {
		if fn, ok := v.(func(string, uint8, *Event) bool); ok && fn != nil {
			return fn(channelID, channelType, ev)
		}
	}
	return false
}
