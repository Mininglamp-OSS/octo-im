package ingress_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/stretchr/testify/require"
)

type tagOwnerCluster struct {
	icluster.ICluster
	owner uint64
	err   error
}

func (c *tagOwnerCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return c.owner, c.err }
func (*tagOwnerCluster) GetSlotId(string) uint32                               { return 1 }
func (*tagOwnerCluster) SlotLeaderId(uint32) uint64                            { return 1001 }

func setupTagOwner(t *testing.T) (*ingress.Ingress, *tagOwnerCluster, *manager.TagManager) {
	t.Helper()
	oldOpts, oldCluster, oldTags := options.G, service.Cluster, service.TagManager
	t.Cleanup(func() { options.G, service.Cluster, service.TagManager = oldOpts, oldCluster, oldTags })
	options.G = options.New()
	options.G.Cluster.NodeId = 1001
	c := &tagOwnerCluster{owner: 1001}
	service.Cluster = c
	tags := manager.NewTagManager(2, func() uint64 { return 1 })
	service.TagManager = tags
	return ingress.New(), c, tags
}

func TestChannelTagRPCUpdatesRecipientsOnSlotOwner(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		for _, cmd := range []bool{false, true} {
			t.Run(fmt.Sprintf("type%d/cmd%v", typ, cmd), func(t *testing.T) {
				i, _, tags := setupTagOwner(t)
				id := "room"
				if cmd {
					id = options.G.OrginalConvertCmdChannel(id)
				}
				tag, err := tags.MakeTag([]string{"sender", "old"})
				require.NoError(t, err)
				tags.SetChannelTag(id, typ, tag.Key)
				originalKey := tag.Key
				req := &ingress.TagUpdateReq{ChannelId: id, ChannelType: typ, ChannelTag: true, Uids: []string{"old"}, Remove: true}
				require.NoError(t, ingress.UpdateTagForTest(i, req))
				req.Uids, req.Remove = []string{"new"}, false
				require.NoError(t, ingress.UpdateTagForTest(i, req))
				key := tags.GetChannelTag(id, typ)
				require.NotEqual(t, originalKey, key)
				require.ElementsMatch(t, []string{"sender", "new"}, tags.GetUsers(key))
			})
		}
	}
}

func TestChannelTagRPCRejectsStaleOwnerBeforeTouchingCache(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, owner := range []uint64{0, 1002} {
			t.Run(fmt.Sprintf("warm%v/owner%d", warm, owner), func(t *testing.T) {
				i, cluster, tags := setupTagOwner(t)
				if warm {
					tag, err := tags.MakeTag([]string{"old"})
					require.NoError(t, err)
					tags.SetChannelTag("room", 2, tag.Key)
				}
				key := tags.GetChannelTag("room", 2)
				cluster.owner = owner
				require.Error(t, ingress.UpdateTagForTest(i, &ingress.TagUpdateReq{ChannelId: "room", ChannelType: 2, ChannelTag: true, Uids: []string{"old"}, Remove: true}))
				require.Equal(t, key, tags.GetChannelTag("room", 2))
				if warm {
					require.Equal(t, []string{"old"}, tags.GetUsers(key))
				}
			})
		}
	}
}

type failingTags struct {
	service.ITagManager
	operation string
	err       error
}

func (m *failingTags) AddUsers(key string, users []string) error {
	if m.operation == "add" {
		return m.err
	}
	return m.ITagManager.AddUsers(key, users)
}
func (m *failingTags) RemoveUsers(key string, users []string) error {
	if m.operation == "remove" {
		return m.err
	}
	return m.ITagManager.RemoveUsers(key, users)
}
func (m *failingTags) RenameTag(old, next string) error {
	if m.operation == "rename" {
		return m.err
	}
	return m.ITagManager.RenameTag(old, next)
}

func TestChannelTagRPCReportsLookupAndMutationFailures(t *testing.T) {
	for _, operation := range []string{"lookup", "add", "remove", "rename"} {
		t.Run(operation, func(t *testing.T) {
			i, cluster, tags := setupTagOwner(t)
			tag, err := tags.MakeTag([]string{"old"})
			require.NoError(t, err)
			tags.SetChannelTag("room", 2, tag.Key)
			want := errors.New("update failed")
			if operation == "lookup" {
				cluster.err = want
			} else {
				service.TagManager = &failingTags{tags, operation, want}
			}
			require.ErrorIs(t, ingress.UpdateTagForTest(i, &ingress.TagUpdateReq{ChannelId: "room", ChannelType: 2, ChannelTag: true, Uids: []string{"old"}, Remove: operation == "remove"}), want)
		})
	}
}

func TestChannelTagRPCAllowsColdOwnerWithoutCreatingTag(t *testing.T) {
	i, _, tags := setupTagOwner(t)
	require.NoError(t, ingress.UpdateTagForTest(i, &ingress.TagUpdateReq{ChannelId: "cold", ChannelType: 2, ChannelTag: true, Uids: []string{"new"}}))
	require.Empty(t, tags.GetAllTags())
}

func TestTagKeyRPCPreservesLocalNonChannelUpdate(t *testing.T) {
	i, cluster, tags := setupTagOwner(t)
	tag, err := tags.MakeTag([]string{"old"})
	require.NoError(t, err)
	cluster.err = errors.New("channel route must not be used for a direct Tag key")
	require.NoError(t, ingress.UpdateTagForTest(i, &ingress.TagUpdateReq{TagKey: tag.Key, Uids: []string{"new"}}))
	require.ElementsMatch(t, []string{"old", "new"}, tags.GetUsers(tag.Key))
}
