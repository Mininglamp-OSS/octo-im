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
	FixedPostCommitOverheadB int64 = 2048 // Covers up to 3 cloned Event headers + RecoveryItem + map slot

	// MaxWireStringBytes matches WuKongIMGoProto Encoder.WriteBinary / Decoder.Binary signed int16 framing limit.
	MaxWireStringBytes     int = math.MaxInt16
	MaxWireSparseSlotWords int = 4096
)

var (
	slotMaskMagic = [4]byte{'W', 'K', 'S', 'M'}

	ErrPostCommitBackpressure = errors.New("post-commit budget exhausted")
	ErrTransientBudgetFull    = errors.New("transient event budget exhausted")
	ErrWireChannelIDTooLong   = errors.New("channel_id exceeds wire framing limit (32767 bytes)")
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

func AllSlotsUpTo(slotCount uint32) SlotBitset {
	var b SlotBitset
	if slotCount == 0 {
		return b
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
		return b
	}
	totalWords := (uint64(slotCount) + 63) / 64
	extraWords := totalWords - 4
	if extraWords > uint64(MaxWireSparseSlotWords) {
		extraWords = uint64(MaxWireSparseSlotWords)
	}
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
	return b
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
		return nil, fmt.Errorf("WKSM sparse word count %d exceeds %d", len(mask.sparse), MaxWireSparseSlotWords)
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

func DecodeSlotMaskTrailer(rem []byte) (hasMask bool, mask SlotBitset, err error) {
	if len(rem) == 0 {
		return false, SlotBitset{}, nil
	}
	if len(rem) < 41 ||
		rem[0] != slotMaskMagic[0] ||
		rem[1] != slotMaskMagic[1] ||
		rem[2] != slotMaskMagic[2] ||
		rem[3] != slotMaskMagic[3] ||
		rem[4] != 1 {
		return false, SlotBitset{}, fmt.Errorf("invalid WKSM trailer header: len=%d", len(rem))
	}
	for i := 0; i < 4; i++ {
		mask.inline[i] = binary.BigEndian.Uint64(rem[5+i*8 : 5+(i+1)*8])
	}
	sc := binary.BigEndian.Uint32(rem[37:41])
	if sc > uint32(MaxWireSparseSlotWords) || len(rem) != 41+int(sc)*12 {
		return false, SlotBitset{}, fmt.Errorf("invalid WKSM trailer sparseCount=%d len=%d", sc, len(rem))
	}
	if sc > 0 {
		mask.sparse = make([]SlotWord, sc)
		var prevWord uint32 = 3
		for i := uint32(0); i < sc; i++ {
			off := 41 + int(i)*12
			w := binary.BigEndian.Uint32(rem[off : off+4])
			bitsVal := binary.BigEndian.Uint64(rem[off+4 : off+12])
			if w <= prevWord || bitsVal == 0 {
				return false, SlotBitset{}, fmt.Errorf("invalid WKSM sparse word[%d]=%d bits=%x", i, w, bitsVal)
			}
			prevWord = w
			mask.sparse[i] = SlotWord{Word: w, Bits: bitsVal}
		}
	}
	return true, mask, nil
}

type PostCommitBudget struct {
	usedBytes atomic.Int64
	usedItems atomic.Int64
	maxBytes  atomic.Int64
	maxItems  atomic.Int64
	rejected  atomic.Uint64
	notifyCh  chan struct{}
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

type PostCommitTicket struct {
	budget   *PostCommitBudget
	bytes    int64
	refs     atomic.Int32
	released atomic.Uint32
}

func (tk *PostCommitTicket) Retain() *PostCommitTicket {
	if tk == nil {
		return nil
	}
	if tk.released.Load() != 0 || tk.refs.Add(1) <= 1 {
		panic("PostCommitTicket.Retain called on released ticket")
	}
	return tk
}

func (tk *PostCommitTicket) Release() {
	if tk == nil {
		return
	}
	rem := tk.refs.Add(-1)
	if rem < 0 {
		panic("PostCommitTicket.Release negative refcount")
	}
	if rem == 0 && tk.released.CompareAndSwap(0, 1) {
		tk.budget.usedBytes.Add(-tk.bytes)
		tk.budget.usedItems.Add(-1)
		select {
		case tk.budget.notifyCh <- struct{}{}:
		default:
		}
	}
}

func (tk *PostCommitTicket) IsReleased() bool {
	if tk == nil {
		return true
	}
	return tk.released.Load() != 0
}

func (tk *PostCommitTicket) Refs() int32 {
	if tk == nil {
		return 0
	}
	return tk.refs.Load()
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
		tk := &PostCommitTicket{budget: b, bytes: sz}
		tk.refs.Store(1)
		tickets[i] = tk
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

func CompactSendPayloadClone(e *Event) *Event {
	if e == nil {
		return nil
	}
	cp := e.Clone()
	if sp, ok := cp.Frame.(*wkproto.SendPacket); ok && sp != nil {
		spCopy := *sp
		if cap(spCopy.Payload) > len(spCopy.Payload)+64 {
			spCopy.Payload = bytes.Clone(spCopy.Payload)
		}
		cp.Frame = &spCopy
	}
	return cp
}

var (
	DefaultPostCommitBudget    = NewPostCommitBudget(MaxPostCommitBytes, MaxPostCommitItems)
	DefaultLegacyInboundBudget = NewPostCommitBudget(MaxLegacyInboundBytes, MaxLegacyInboundItems)
	DefaultTransientBudget     = NewPostCommitBudget(MaxTransientEventBytes, MaxTransientEventItems)
	LegacyForwardDroppedTotal  atomic.Uint64
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
	RecoveryChannelsActive        int
	RecoveryItemsActive           int
	RecoveryCooldownProbesTotal   uint64
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
