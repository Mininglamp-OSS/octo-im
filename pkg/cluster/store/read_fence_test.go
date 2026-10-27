package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// applyingSlot applies every proposal to the store synchronously and records
// the slot it was proposed to. It is also the read fence.
type applyingSlot struct {
	mu       sync.Mutex
	store    *Store
	ids      map[string]uint32
	index    uint64
	proposed map[uint32]struct{}
	fenced   map[uint32]bool
}

func (a *applyingSlot) SlotLeaderId(uint32) uint64 { return 1 }

func (a *applyingSlot) GetSlotId(v string) uint32 {
	if id, ok := a.ids[v]; ok {
		return id
	}
	panic("unmapped slot key " + v)
}

func (a *applyingSlot) apply(slotId uint32, data []byte) (*types.ProposeResp, error) {
	a.mu.Lock()
	a.index++
	idx := a.index
	if a.proposed != nil {
		a.proposed[slotId] = struct{}{}
	}
	a.mu.Unlock()
	if err := a.store.ApplySlotLogs(slotId, []types.Log{{Index: idx, Term: 1, Data: data}}); err != nil {
		return nil, err
	}
	return &types.ProposeResp{Index: idx}, nil
}

func (a *applyingSlot) Propose(slotId uint32, data []byte) (*types.ProposeResp, error) {
	return a.apply(slotId, data)
}
func (a *applyingSlot) ProposeUntilApplied(slotId uint32, data []byte) (*types.ProposeResp, error) {
	return a.apply(slotId, data)
}
func (a *applyingSlot) ProposeUntilAppliedTimeout(_ context.Context, slotId uint32, data []byte) (*types.ProposeResp, error) {
	return a.apply(slotId, data)
}

func (a *applyingSlot) CheckSlotReadable(slotId uint32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fenced[slotId] {
		return fmt.Errorf("%w: slot %d", types.ErrSlotDiverged, slotId)
	}
	return nil
}

func (a *applyingSlot) record() func() []uint32 {
	a.mu.Lock()
	a.proposed = map[uint32]struct{}{}
	a.mu.Unlock()
	return func() []uint32 {
		a.mu.Lock()
		defer a.mu.Unlock()
		out := make([]uint32, 0, len(a.proposed))
		for id := range a.proposed {
			out = append(out, id)
		}
		a.proposed = nil
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
}

func (a *applyingSlot) fence(ids ...uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fenced = map[uint32]bool{}
	for _, id := range ids {
		a.fenced[id] = true
	}
}

func newFencedTestStore(t *testing.T) (*Store, *applyingSlot) {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1), wkdb.WithSlotCount(64), wkdb.WithNodeId(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { _ = db.Close() })
	slot := &applyingSlot{ids: map[string]uint32{
		"ch-a": 5, "u-a": 7, "g-b": 9, "u-c": 11, "u-other": 13, "ch-other": 15,
	}}
	s := New(NewOptions(WithDB(db), WithSlot(slot)))
	slot.store = s
	require.Same(t, slot, s.opts.ReadFence, "the slot is used as the read fence")
	return s, slot
}

type fenceCase struct {
	name  string
	write func(s *Store) error
	read  func(s *Store) error
	// other is a slot that owns none of the rows read.
	other uint32
}

func fenceCases() []fenceCase {
	members := []wkdb.Member{{Uid: "u-a"}}
	expectFound := func(found bool, err error) error {
		if err != nil {
			return err
		}
		if !found {
			return errors.New("row not found")
		}
		return nil
	}
	expectRows := func(n int, err error) error {
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("no rows")
		}
		return nil
	}
	return []fenceCase{
		{"subscribers", func(s *Store) error { return s.AddSubscribers("ch-a", 2, members) }, func(s *Store) error {
			if err := expectFound(s.ExistSubscriber("ch-a", 2, "u-a")); err != nil {
				return err
			}
			m, err := s.GetSubscribers("ch-a", 2)
			return expectRows(len(m), err)
		}, 15},
		{"channel info", func(s *Store) error {
			return s.AddChannelInfo(wkdb.ChannelInfo{ChannelId: "ch-a", ChannelType: 2})
		}, func(s *Store) error {
			if err := expectFound(s.ExistChannel("ch-a", 2)); err != nil {
				return err
			}
			_, err := s.GetChannel("ch-a", 2)
			return err
		}, 15},
		{"denylist", func(s *Store) error { return s.AddDenylist("ch-a", 2, members) }, func(s *Store) error {
			if err := expectFound(s.ExistDenylist("ch-a", 2, "u-a")); err != nil {
				return err
			}
			m, err := s.GetDenylist("ch-a", 2)
			return expectRows(len(m), err)
		}, 15},
		{"allowlist", func(s *Store) error { return s.AddAllowlist("ch-a", 2, members) }, func(s *Store) error {
			if err := expectFound(s.ExistAllowlist("ch-a", 2, "u-a")); err != nil {
				return err
			}
			if err := expectFound(s.HasAllowlist("ch-a", 2)); err != nil {
				return err
			}
			m, err := s.GetAllowlist("ch-a", 2)
			return expectRows(len(m), err)
		}, 15},
		{"message events", func(s *Store) error {
			_, _, err := s.AppendMessageEventWithState("ch-a", 2, &wkdb.MessageEvent{ClientMsgNo: "m1", EventID: "e1", EventType: "stream.delta", Payload: []byte("p")})
			return err
		}, func(s *Store) error {
			if _, err := s.GetMessageEventState("ch-a", 2, "m1", wkdb.EventKeyDefault); err != nil {
				return err
			}
			if _, err := s.GetMessageEventByEventID("ch-a", 2, "m1", "e1"); err != nil {
				return err
			}
			if st, err := s.GetMessageEventStates("ch-a", 2, "m1"); expectRows(len(st), err) != nil {
				return expectRows(len(st), err)
			}
			if _, err := s.GetMessageEventStatesBatch("ch-a", 2, []string{"m1"}); err != nil {
				return err
			}
			ev, err := s.ListMessageEvents("ch-a", 2, "m1", 0, wkdb.EventKeyDefault, 10)
			return expectRows(len(ev), err)
		}, 15},
		{"user", func(s *Store) error { return s.AddUser(wkdb.User{Uid: "u-a"}) }, func(s *Store) error {
			_, err := s.GetUser("u-a")
			return err
		}, 15},
		{"device", func(s *Store) error {
			return s.AddDevice(wkdb.Device{Id: 1, Uid: "u-a", DeviceFlag: uint64(wkproto.APP), Token: "t"})
		}, func(s *Store) error {
			_, err := s.GetDevice("u-a", wkproto.APP)
			return err
		}, 15},
		{"system uids (slot 0)", func(s *Store) error { return s.AddSystemUids([]string{"sys"}) }, func(s *Store) error {
			u, err := s.GetSystemUids()
			return expectRows(len(u), err)
		}, 15},
		{"testers (slot 0)", func(s *Store) error {
			now := time.Now()
			return s.AddOrUpdateTester(wkdb.Tester{No: "t1", Addr: "a", CreatedAt: &now, UpdatedAt: &now})
		}, func(s *Store) error {
			if _, err := s.GetTester("t1"); err != nil {
				return err
			}
			ts, err := s.GetTesters()
			return expectRows(len(ts), err)
		}, 15},
		{"plugin users (slot 0)", func(s *Store) error { return s.UpdateUserPluginNo("u-a", "p1") }, func(s *Store) error {
			if err := expectFound(s.ExistPluginUser("u-a")); err != nil {
				return err
			}
			if _, err := s.GetHighestPriorityPluginByUid("u-a"); err != nil && !errors.Is(err, wkdb.ErrNotFound) {
				return err
			}
			pu, err := s.SearchPluginUsers(wkdb.SearchPluginUserReq{Uid: "u-a"})
			return expectRows(len(pu), err)
		}, 15},
		{"person conversation (uid slot only)", func(s *Store) error {
			if err := s.AddOrUpdateUserConversations("u-c", []wkdb.Conversation{{Uid: "u-c", ChannelId: "u-c@u-x", ChannelType: wkproto.ChannelTypePerson, Type: wkdb.ConversationTypeChat}}); err != nil {
				return err
			}
			return s.UpdateConversationIfSeqGreaterAsync("u-c", "u-c@u-x", wkproto.ChannelTypePerson, 3)
		}, func(s *Store) error {
			c, err := s.GetConversations("u-c")
			if err := expectRows(len(c), err); err != nil {
				return err
			}
			if _, err := s.GetConversation("u-c", "u-c@u-x", wkproto.ChannelTypePerson); err != nil {
				return err
			}
			if c, err := s.GetConversationsByType("u-c", wkdb.ConversationTypeChat); expectRows(len(c), err) != nil {
				return expectRows(len(c), err)
			}
			c, err = s.GetLastConversations("u-c", wkdb.ConversationTypeChat, 0, nil, 10)
			return expectRows(len(c), err)
		}, 15},
		{"group conversation (uid and channel slots)", func(s *Store) error {
			if err := s.AddOrUpdateConversations([]wkdb.Conversation{{Id: 101, Uid: "u-a", ChannelId: "g-b", ChannelType: 2, Type: wkdb.ConversationTypeChat}}); err != nil {
				return err
			}
			return s.UpdateConversationIfSeqGreaterAsync("u-a", "g-b", 2, 5)
		}, func(s *Store) error {
			c, err := s.GetConversations("u-a")
			if err := expectRows(len(c), err); err != nil {
				return err
			}
			if _, err := s.GetConversation("u-a", "g-b", 2); err != nil {
				return err
			}
			if c, err := s.GetConversationsByType("u-a", wkdb.ConversationTypeChat); expectRows(len(c), err) != nil {
				return expectRows(len(c), err)
			}
			c, err = s.GetLastConversations("u-a", wkdb.ConversationTypeChat, 0, nil, 10)
			if err := expectRows(len(c), err); err != nil {
				return err
			}
			// The write-path existence check is fenced on the same slots.
			return s.AddConversationsIfNotExist([]wkdb.Conversation{{Uid: "u-a", ChannelId: "g-b", ChannelType: 2}})
		}, 15},
	}
}

// T25 and T25b: every slot-owned read is fenced on exactly the slots its
// write path proposes to. A fenced read returns ErrSlotDiverged (never rows,
// never not-found); fencing an unrelated slot changes nothing.
func TestStoreReadFenceSelectorParity(t *testing.T) {
	for _, tc := range fenceCases() {
		t.Run(tc.name, func(t *testing.T) {
			s, slot := newFencedTestStore(t)
			done := slot.record()
			require.NoError(t, tc.write(s))
			owners := done()
			require.NotEmpty(t, owners)
			require.NoError(t, tc.read(s), "healthy read")

			for _, owner := range owners {
				slot.fence(owner)
				err := tc.read(s)
				require.ErrorIs(t, err, types.ErrSlotDiverged, "read with owner slot %d fenced", owner)
				require.NotErrorIs(t, err, wkdb.ErrNotFound)
			}
			slot.fence(tc.other)
			require.NoError(t, tc.read(s), "an unrelated slot does not affect the read")
			slot.fence()
		})
	}
}

// The group conversation row is written through both slots, and a read of a
// user's conversation list must fail as a whole when any row's channel slot
// is fenced; another user's list on a healthy slot is unaffected.
func TestConversationListFailsWholeWhenRowSlotFenced(t *testing.T) {
	s, slot := newFencedTestStore(t)
	require.NoError(t, s.AddOrUpdateConversations([]wkdb.Conversation{
		{Id: 201, Uid: "u-a", ChannelId: "g-b", ChannelType: 2, Type: wkdb.ConversationTypeChat},
		{Id: 202, Uid: "u-a", ChannelId: "u-a@u-c", ChannelType: wkproto.ChannelTypePerson, Type: wkdb.ConversationTypeChat},
		{Id: 203, Uid: "u-c", ChannelId: "u-a@u-c", ChannelType: wkproto.ChannelTypePerson, Type: wkdb.ConversationTypeChat},
	}))
	slot.fence(9) // the channel slot of g-b only
	rows, err := s.GetConversations("u-a")
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	require.Nil(t, rows, "no partial list")
	rows, err = s.GetConversations("u-c")
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

// Without a fence (non-cluster use) reads are served as before.
func TestStoreWithoutFenceReadsUnfenced(t *testing.T) {
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1), wkdb.WithSlotCount(64), wkdb.WithNodeId(1)))
	require.NoError(t, db.Open())
	defer db.Close()
	s := New(NewOptions(WithDB(db), WithSlot(&recordingSlot{slotID: 3})))
	require.Nil(t, s.opts.ReadFence)
	_, err := s.GetUser("nobody")
	require.NotErrorIs(t, err, types.ErrSlotDiverged)
}
