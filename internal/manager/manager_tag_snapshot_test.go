package manager

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// Every uid lives on node 1, so a tag has a single node.
type snapshotTestCluster struct{ icluster.ICluster }

func (snapshotTestCluster) GetSlotId(string) uint32    { return 1 }
func (snapshotTestCluster) SlotLeaderId(uint32) uint64 { return 1 }

func newSnapshotTestTagManager(t *testing.T, uids []string) *TagManager {
	t.Helper()
	old := service.Cluster
	service.Cluster = snapshotTestCluster{}
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", uids)
	require.NoError(t, err)
	tm.SetChannelTag("g1", 2, "tag")
	return tm
}

// Fan-out holds the tag returned by Get and ranges over node.Uids while
// subscriber changes are applied. The list it started with must not change.
func TestTagSnapshotSurvivesRemoveUsers(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b", "c", "d", "e"})
	inFlight := tm.Get("tag").Nodes[0].Uids

	require.NoError(t, tm.RemoveUsers("tag", []string{"b"}))

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, inFlight, "in-flight fan-out saw the removal shift its list")
	require.Equal(t, []string{"a", "c", "d", "e"}, tm.Get("tag").Nodes[0].Uids)
}

func TestTagSnapshotSurvivesReAddingMember(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b", "c", "d", "e"})
	inFlight := tm.Get("tag").Nodes[0].Uids

	// Octo-server often re-adds members who are already subscribed.
	require.NoError(t, tm.AddUsers("tag", []string{"b"}))

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, inFlight, "in-flight fan-out saw the re-add rewrite its list")
	require.ElementsMatch(t, []string{"a", "b", "c", "d", "e"}, tm.Get("tag").Nodes[0].Uids)
}

func TestTagSnapshotKeepsKeyAcrossRename(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	inFlight := tm.Get("tag")

	require.NoError(t, tm.RenameTag("tag", "tag2"))

	// Fan-out stamps events with the key of the tag it delivered with.
	require.Equal(t, "tag", inFlight.Key)
	require.Equal(t, "tag2", tm.Get("tag2").Key)
	require.Nil(t, tm.Get("tag"))
}

// Readers fan out continuously while members leave and rejoin, as the channel
// handler and the subscriber API do. Every pass must see each member exactly
// once; the always-present members must never be skipped. Run with -race.
func TestTagFanoutDuringMembershipChanges(t *testing.T) {
	stable := []string{"a", "s", "m0", "m1", "m2", "m3"}
	churn := []string{"b", "c", "d"}
	tm := newSnapshotTestTagManager(t, append(append([]string{}, stable...), churn...))

	stop := make(chan struct{})
	var passes atomic.Int64
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tag := tm.Get(tm.GetChannelTag("g1", 2))
				if tag == nil {
					continue
				}
				seen := map[string]int{}
				for _, node := range tag.Nodes {
					for _, uid := range node.Uids {
						seen[uid]++
					}
				}
				for uid, n := range seen {
					if n > 1 {
						errs <- fmt.Errorf("uid %s delivered %d times in one pass", uid, n)
						return
					}
				}
				for _, uid := range stable {
					if seen[uid] == 0 {
						errs <- fmt.Errorf("always-subscribed uid %s skipped", uid)
						return
					}
				}
				passes.Add(1)
			}
		}()
	}

	renames := 0
	update := func(uid string, remove bool) {
		key := tm.GetChannelTag("g1", 2)
		var err error
		if remove {
			err = tm.RemoveUsers(key, []string{uid})
		} else {
			err = tm.AddUsers(key, []string{uid})
		}
		require.NoError(t, err)
		renames++
		next := fmt.Sprintf("tag-%d", renames)
		require.NoError(t, tm.RenameTag(key, next))
		tm.SetChannelTag("g1", 2, next)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		uid := churn[i%len(churn)]
		update(uid, true)
		update(uid, false)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	require.Positive(t, passes.Load())
}

func TestTagSnapshotSurvivesSetChannelTag(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	inFlight := tm.Get("tag")

	tm.SetChannelTag("g2", 3, "tag")

	require.Equal(t, "g1", inFlight.ChannelId)
	require.EqualValues(t, 2, inFlight.ChannelType)
	require.Equal(t, "g2", tm.Get("tag").ChannelId)
	require.EqualValues(t, 3, tm.Get("tag").ChannelType)
}

// A reader that resolves a channel's key must find a tag already labelled
// with that channel: the mapping is set only after the labelled tag is
// published. Run with -race.
func TestSetChannelTagPublishesTagBeforeMapping(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a"})
	stop := make(chan struct{})
	done := make(chan struct{})
	var checked atomic.Int64
	var readErr error
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := tm.GetChannelTag("g9", 5)
			if key == "" {
				continue
			}
			checked.Add(1)
			if tag := tm.Get(key); tag == nil || tag.ChannelId != "g9" {
				readErr = fmt.Errorf("key %s resolved before its tag was labelled: %v", key, tag)
				return
			}
		}
	}()
	var once sync.Once
	join := func() { once.Do(func() { close(stop); <-done }) }
	defer join()

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%d", i)
		_, err := tm.MakeTagWithTagKey(key, []string{"a"})
		require.NoError(t, err)
		tm.SetChannelTag("g9", 5, key)
	}
	// The last mapping stays, so the reader checks at least one tag.
	require.Eventually(t, func() bool { return checked.Load() > 0 }, 5*time.Second, time.Millisecond)
	join()
	require.NoError(t, readErr)
}

// uids prefixed "n2" live on node 2, "orphan" on a slot without a leader, the
// rest on node 1.
type multiNodeTestCluster struct{ icluster.ICluster }

func (multiNodeTestCluster) GetSlotId(uid string) uint32 {
	switch {
	case uid == "orphan":
		return 3
	case strings.HasPrefix(uid, "n2"):
		return 2
	}
	return 1
}
func (multiNodeTestCluster) SlotLeaderId(slot uint32) uint64 {
	if slot == 3 {
		return 0
	}
	return uint64(slot)
}

func newMultiNodeTestTagManager(t *testing.T, uids []string) *TagManager {
	t.Helper()
	old := service.Cluster
	service.Cluster = multiNodeTestCluster{}
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", uids)
	require.NoError(t, err)
	tm.SetChannelTag("g1", 2, "tag")
	return tm
}

func nodeUids(tag *types.Tag, leader uint64) []string {
	for _, node := range tag.Nodes {
		if node.LeaderId == leader {
			return node.Uids
		}
	}
	return nil
}

func TestTagSnapshotsAcrossNodes(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "a2", "n2a", "n2b"})
	inFlight := tm.Get("tag")
	node1, node2 := nodeUids(inFlight, 1), nodeUids(inFlight, 2)

	require.NoError(t, tm.RemoveUsers("tag", []string{"n2a"}))
	require.NoError(t, tm.AddUsers("tag", []string{"a1", "n2c"}))

	require.Equal(t, []string{"a1", "a2"}, node1)
	require.Equal(t, []string{"n2a", "n2b"}, node2)
	require.ElementsMatch(t, []string{"a1", "a2"}, nodeUids(tm.Get("tag"), 1))
	require.ElementsMatch(t, []string{"n2b", "n2c"}, nodeUids(tm.Get("tag"), 2))
}

// An add that fails part-way must leave the published tag untouched.
func TestFailedAddUsersKeepsPublishedTag(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "n2a"})
	before := tm.Get("tag")

	require.Error(t, tm.AddUsers("tag", []string{"n2b", "orphan"}))

	require.Same(t, before, tm.Get("tag"))
	require.Equal(t, []string{"a1"}, nodeUids(before, 1))
	require.Equal(t, []string{"n2a"}, nodeUids(before, 2))
}

// Removing does not need a slot leader: members are removed from whichever
// node they are filed under, even while a slot has no leader.
func TestRemoveUsersNeedsNoSlotLeader(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "a2", "n2a"})
	before := tm.Get("tag")

	require.NoError(t, tm.RemoveUsers("tag", []string{"orphan", "a1", "n2a"}))

	require.Equal(t, []string{"a2"}, nodeUids(tm.Get("tag"), 1))
	require.Empty(t, nodeUids(tm.Get("tag"), 2))
	require.Equal(t, []string{"a1", "a2"}, nodeUids(before, 1))
	require.Equal(t, []string{"n2a"}, nodeUids(before, 2))
}

// A member filed under a node that no longer leads their slot is still
// removed. Looking only under the current slot leader used to miss them, so
// a removed member kept receiving the channel's messages.
func TestRemoveUsersAfterSlotLeaderMoved(t *testing.T) {
	c := movingLeaderCluster{leaders: map[uint32]uint64{0: 1, 1: 1, 2: 1, 3: 1}}
	old := service.Cluster
	service.Cluster = c
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", []string{"u1", "u2"})
	require.NoError(t, err)

	c.leaders[c.GetSlotId("u1")] = 2
	require.NoError(t, tm.RemoveUsers("tag", []string{"u1"}))

	for _, node := range tm.Get("tag").Nodes {
		require.NotContains(t, node.Uids, "u1")
	}
}

// Membership changes are not accesses: they must not keep an unused tag from
// expiring. A rename has always counted as one.
func TestMembershipChangeKeepsAccessState(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	tm.Get("tag")
	snap := tm.getTag("tag") // getTag does not record an access
	accessed, count := snap.LastGet(), snap.GetCount.Load()
	time.Sleep(2 * time.Millisecond)

	require.NoError(t, tm.AddUsers("tag", []string{"c"}))
	require.NoError(t, tm.RemoveUsers("tag", []string{"a"}))
	tm.SetChannelTag("g2", 3, "tag")
	got := tm.getTag("tag")
	require.Equal(t, accessed, got.LastGet())
	require.Equal(t, count, got.GetCount.Load())

	require.NoError(t, tm.RenameTag("tag", "tag2"))
	require.True(t, tm.getTag("tag2").LastGet().After(accessed))
}

// /tags reports an emptied node as "uids": [], and later copies keep it so.
func TestEmptiedNodeKeepsEmptyUidList(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "n2a"})
	require.NoError(t, tm.RemoveUsers("tag", []string{"n2a"}))
	require.NoError(t, tm.AddUsers("tag", []string{"a2"}))

	var node2 *types.Node
	for _, node := range tm.Get("tag").Nodes {
		if node.LeaderId == 2 {
			node2 = node
		}
	}
	require.NotNil(t, node2)
	data, err := json.Marshal(node2)
	require.NoError(t, err)
	require.Contains(t, string(data), `"uids":[]`)
}

// A subscriber change renames the channel's tag. A fan-out that started before
// it still holds the old key when it updates conversations, and the update
// must not be dropped.
func TestConversationPushAfterTagRename(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	oldTagManager, oldNodeId := service.TagManager, options.G.Cluster.NodeId
	service.TagManager, options.G.Cluster.NodeId = tm, 1
	t.Cleanup(func() { service.TagManager, options.G.Cluster.NodeId = oldTagManager, oldNodeId })

	channelId := "g1" + options.G.Channel.CmdSuffix
	tm.SetChannelTag(channelId, 2, "tag")
	inFlight := tm.Get("tag")

	// What a subscriber add does on the channel leader.
	require.NoError(t, tm.AddUsers("tag", []string{"c"}))
	require.NoError(t, tm.RenameTag("tag", "tag2"))
	tm.SetChannelTag(channelId, 2, "tag2")

	u := newConversationUpdater(nil)
	u.push(channelId, 2, inFlight.Key, 5, nil)

	update := u.pendingUpdates[wkutil.ChannelToKey(channelId, 2)]
	require.NotNil(t, update, "conversation update dropped")
	require.Len(t, update.UserReadSeqs, 3)
}

// Re-adding present members and removing absent ones change nothing, so the
// published tag is kept rather than copied.
func TestNoOpMembershipChangeKeepsPublishedTag(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	before := tm.Get("tag")

	require.NoError(t, tm.AddUsers("tag", []string{"a", "b"}))
	require.NoError(t, tm.RemoveUsers("tag", []string{"x"}))

	require.Same(t, before, tm.Get("tag"))
}

// A change on one node replaces only that node.
func TestMembershipChangeSharesUnchangedNodes(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "n2a"})
	node1 := findNode(tm.Get("tag").Nodes, 1)

	require.NoError(t, tm.AddUsers("tag", []string{"n2b"}))
	require.NoError(t, tm.RemoveUsers("tag", []string{"n2a"}))

	require.Same(t, node1, findNode(tm.Get("tag").Nodes, 1))
	require.Equal(t, []string{"n2b"}, nodeUids(tm.Get("tag"), 2))
}

// Slot leaders can move between membership changes.
type movingLeaderCluster struct {
	icluster.ICluster
	leaders map[uint32]uint64
}

func (c movingLeaderCluster) GetSlotId(uid string) uint32 {
	return uint32(len(uid)+int(uid[len(uid)-1])) % 4
}
func (c movingLeaderCluster) SlotLeaderId(slot uint32) uint64 { return c.leaders[slot] }

type refNode struct {
	uids  []string
	slots []uint32
}

// refAdd is the in-place algorithm AddUsers used before tags were published
// as copies. refRemove is the previous RemoveUsers too, except that it removes
// a member from every node: the previous one looked only under the member's
// current slot leader and missed members still filed under an old leader.
func refAdd(ref map[uint64]*refNode, order *[]uint64, c movingLeaderCluster, uids []string) {
	for _, leader := range *order {
		node := ref[leader]
		for _, uid := range uids {
			if i := slices.Index(node.uids, uid); i >= 0 {
				node.uids = slices.Delete(node.uids, i, i+1)
			}
		}
	}
	for _, uid := range uids {
		slot := c.GetSlotId(uid)
		leader := c.SlotLeaderId(slot)
		node := ref[leader]
		if node == nil {
			node = &refNode{}
			ref[leader] = node
			*order = append(*order, leader)
		}
		if !slices.Contains(node.uids, uid) {
			node.uids = append(node.uids, uid)
		}
		if !slices.Contains(node.slots, slot) {
			node.slots = append(node.slots, slot)
		}
	}
}

func refRemove(ref map[uint64]*refNode, uids []string) {
	for _, uid := range uids {
		for _, node := range ref {
			if i := slices.Index(node.uids, uid); i >= 0 {
				node.uids = slices.Delete(node.uids, i, i+1)
			}
		}
	}
}

func cloneTagNodes(nodes []*types.Node) []types.Node {
	out := make([]types.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, types.Node{LeaderId: node.LeaderId, Uids: slices.Clone(node.Uids), SlotIds: slices.Clone(node.SlotIds)})
	}
	return out
}

// Random membership changes, with slot leaders moving in between, must give
// the same members per node as refAdd and refRemove, and must never change a
// tag that was already handed out.
func TestMembershipChangesMatchPreviousAlgorithm(t *testing.T) {
	c := movingLeaderCluster{leaders: map[uint32]uint64{0: 1, 1: 2, 2: 3, 3: 1}}
	old := service.Cluster
	service.Cluster = c
	t.Cleanup(func() { service.Cluster = old })

	pool := make([]string, 30)
	for i := range pool {
		pool[i] = fmt.Sprintf("u%d", i)
	}
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", pool[:10])
	require.NoError(t, err)
	ref := map[uint64]*refNode{}
	var order []uint64
	refAdd(ref, &order, c, pool[:10])

	rnd := rand.New(rand.NewSource(1))
	for op := 0; op < 3000; op++ {
		if rnd.Intn(10) == 0 {
			c.leaders[uint32(rnd.Intn(4))] = uint64(rnd.Intn(3) + 1)
			continue
		}
		uids := make([]string, rnd.Intn(4)+1)
		for i := range uids {
			uids[i] = pool[rnd.Intn(len(pool))]
		}
		inFlight := tm.Get("tag")
		want := cloneTagNodes(inFlight.Nodes)
		if rnd.Intn(2) == 0 {
			require.NoError(t, tm.AddUsers("tag", uids))
			refAdd(ref, &order, c, uids)
		} else {
			require.NoError(t, tm.RemoveUsers("tag", uids))
			refRemove(ref, uids)
		}
		require.Equal(t, want, cloneTagNodes(inFlight.Nodes), "op %d changed a handed-out tag", op)

		got := tm.Get("tag").Nodes
		require.Len(t, got, len(ref), "op %d", op)
		for _, node := range got {
			r := ref[node.LeaderId]
			require.NotNil(t, r, "op %d: unexpected node %d", op, node.LeaderId)
			require.ElementsMatch(t, r.uids, node.Uids, "op %d node %d uids", op, node.LeaderId)
			require.ElementsMatch(t, r.slots, node.SlotIds, "op %d node %d slots", op, node.LeaderId)
		}
	}
}
