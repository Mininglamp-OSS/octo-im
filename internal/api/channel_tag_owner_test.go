package api

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	tagmanager "github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type tagOwnerRequest struct {
	node uint64
	req  ingress.TagUpdateReq
}

type tagOwnerCluster struct {
	icluster.ICluster
	owners        map[string]uint64
	resolveErr    error
	requestErr    error
	status        proto.Status
	requests      []tagOwnerRequest
	invalidations []struct {
		node uint64
		req  ingress.ChannelReq
	}
	messageReads int
	messageOwner uint64
}

func (c *tagOwnerCluster) SlotLeaderIdOfChannel(id string, _ uint8) (uint64, error) {
	return c.owners[id], c.resolveErr
}
func (c *tagOwnerCluster) LeaderIdOfChannel(string, uint8) (uint64, error) {
	c.messageReads++
	return c.messageOwner, nil
}
func (c *tagOwnerCluster) LoadOnlyChannelClusterConfig(string, uint8) (wkdb.ChannelClusterConfig, error) {
	c.messageReads++
	return wkdb.ChannelClusterConfig{LeaderId: c.messageOwner}, nil
}
func (c *tagOwnerCluster) GetSlotId(string) uint32    { return 1 }
func (c *tagOwnerCluster) SlotLeaderId(uint32) uint64 { return 1001 }
func (c *tagOwnerCluster) RequestWithContext(_ context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if path == "/wk/ingress/invalidateChannelTag" {
		var req ingress.ChannelReq
		if err := req.Decode(body); err != nil {
			return nil, err
		}
		c.invalidations = append(c.invalidations, struct {
			node uint64
			req  ingress.ChannelReq
		}{node, req})
		return &proto.Response{Status: c.status}, c.requestErr
	}
	if path != "/wk/ingress/updateTag" {
		return nil, fmt.Errorf("unexpected RPC %s", path)
	}
	var req ingress.TagUpdateReq
	if err := req.Decode(body); err != nil {
		return nil, err
	}
	c.requests = append(c.requests, tagOwnerRequest{node, req})
	return &proto.Response{Status: c.status}, c.requestErr
}

func setupTagOwner(t *testing.T) (*channel, *tagOwnerCluster, *tagmanager.TagManager) {
	t.Helper()
	oldOpts, oldCluster, oldTags := options.G, service.Cluster, service.TagManager
	t.Cleanup(func() { options.G, service.Cluster, service.TagManager = oldOpts, oldCluster, oldTags })
	options.G = options.New()
	options.G.Cluster.NodeId = 1001
	c := &tagOwnerCluster{owners: map[string]uint64{}, status: proto.StatusOK, messageOwner: 1002}
	service.Cluster = c
	tags := tagmanager.NewTagManager(2, func() uint64 { return 1 })
	service.TagManager = tags
	return newChannel(&Server{client: ingress.NewClient()}), c, tags
}

func warmOwnerTag(t *testing.T, tags *tagmanager.TagManager, id string, typ uint8) string {
	t.Helper()
	tag, err := tags.MakeTag([]string{"sender", "old-recipient"})
	require.NoError(t, err)
	tags.SetChannelTag(id, typ, tag.Key)
	return tag.Key
}

// The message leader deliberately differs from the slot owner. Check the real
// authoritative recipient list after each completed membership change.
func TestSubscriberTagUpdatesAuthoritativeRecipients(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			ch, cluster, tags := setupTagOwner(t)
			id := "room"
			cmd := options.G.OrginalConvertCmdChannel(id)
			cluster.owners[id], cluster.owners[cmd] = 1001, 1001
			keys := map[string]string{id: warmOwnerTag(t, tags, id, typ), cmd: warmOwnerTag(t, tags, cmd, typ)}
			require.NoError(t, ch.updateTagBySubscribers(id, typ, []string{"old-recipient"}, true))
			for _, channelID := range []string{id, cmd} {
				key := tags.GetChannelTag(channelID, typ)
				require.NotEqual(t, keys[channelID], key)
				require.ElementsMatch(t, []string{"sender"}, tags.GetUsers(key))
				keys[channelID] = key
			}
			require.NoError(t, ch.updateTagBySubscribers(id, typ, []string{"new-recipient"}, false))
			for _, channelID := range []string{id, cmd} {
				key := tags.GetChannelTag(channelID, typ)
				require.NotEqual(t, keys[channelID], key)
				require.ElementsMatch(t, []string{"sender", "new-recipient"}, tags.GetUsers(key))
			}
			require.Empty(t, cluster.requests, "both authoritative Tags are local")
			require.Zero(t, cluster.messageReads, "membership must not resolve/activate message Raft")
		})
	}
}

func TestSubscriberTagRoutesEachChannelToItsOwnSlot(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("type%d/remove%v", typ, remove), func(t *testing.T) {
				ch, cluster, _ := setupTagOwner(t)
				id := "room"
				cmd := options.G.OrginalConvertCmdChannel(id)
				cluster.owners[id], cluster.owners[cmd] = 1003, 1002
				require.NoError(t, ch.updateTagBySubscribers(id, typ, []string{"recipient"}, remove))
				require.Len(t, cluster.requests, 2)
				for i, want := range []struct {
					id   string
					node uint64
				}{{id, 1003}, {cmd, 1002}} {
					call := cluster.requests[i]
					require.Equal(t, want.node, call.node)
					require.Equal(t, want.id, call.req.ChannelId)
					require.Equal(t, typ, call.req.ChannelType)
					require.Equal(t, remove, call.req.Remove)
					require.True(t, call.req.ChannelTag)
					require.Equal(t, []string{"recipient"}, call.req.Uids)
				}
				require.Zero(t, cluster.messageReads)
			})
		}
	}
}

func TestSubscriberTagColdChannelsDoNotCreateMessageRaft(t *testing.T) {
	ch, cluster, tags := setupTagOwner(t)
	id := "cold-room"
	cmd := options.G.OrginalConvertCmdChannel(id)
	cluster.owners[id], cluster.owners[cmd] = 1001, 1001
	require.NoError(t, ch.updateTagBySubscribers(id, 2, []string{"recipient"}, false))
	require.Empty(t, tags.GetAllTags(), "first distribution builds a cold Tag from persisted membership")
	require.Empty(t, cluster.requests)
	require.Zero(t, cluster.messageReads)
}

func TestSubscriberTagRoutingErrorsAreReturned(t *testing.T) {
	for _, scenario := range []string{"lookup", "no-owner", "cmd-no-owner", "transport", "remote-status"} {
		t.Run(scenario, func(t *testing.T) {
			ch, cluster, _ := setupTagOwner(t)
			id := "room"
			cmd := options.G.OrginalConvertCmdChannel(id)
			cluster.owners[id], cluster.owners[cmd] = 1001, 1001
			switch scenario {
			case "lookup":
				cluster.resolveErr = errors.New("slot unavailable")
			case "no-owner":
				cluster.owners[id] = 0
			case "cmd-no-owner":
				cluster.owners[cmd] = 0
			case "transport":
				cluster.owners[id], cluster.requestErr = 1003, errors.New("RPC failed")
			case "remote-status":
				cluster.owners[id], cluster.status = 1003, proto.StatusError
			}
			require.Error(t, ch.updateTagBySubscribers(id, 2, []string{"recipient"}, false))
		})
	}
}

type failingOwnerTags struct {
	service.ITagManager
	operation string
	err       error
}

func (m *failingOwnerTags) AddUsers(key string, users []string) error {
	if m.operation == "add" {
		return m.err
	}
	return m.ITagManager.AddUsers(key, users)
}
func (m *failingOwnerTags) RemoveUsers(key string, users []string) error {
	if m.operation == "remove" {
		return m.err
	}
	return m.ITagManager.RemoveUsers(key, users)
}
func (m *failingOwnerTags) RenameTag(old, next string) error {
	if m.operation == "rename" {
		return m.err
	}
	return m.ITagManager.RenameTag(old, next)
}

func TestSubscriberTagLocalMutationErrorsAreReturned(t *testing.T) {
	for _, operation := range []string{"add", "remove", "rename"} {
		t.Run(operation, func(t *testing.T) {
			ch, cluster, tags := setupTagOwner(t)
			id := "room"
			cluster.owners[id], cluster.owners[options.G.OrginalConvertCmdChannel(id)] = 1001, 1001
			warmOwnerTag(t, tags, id, 2)
			cluster.messageOwner = 1001 // Reach the local path even on the unfixed version.
			want := errors.New("tag mutation failed")
			service.TagManager = &failingOwnerTags{tags, operation, want}
			require.ErrorIs(t, ch.updateTagBySubscribers(id, 2, []string{"old-recipient"}, operation == "remove"), want)
		})
	}
}

func TestSubscriberTagInvalidationDropsBothWarmRecipientSets(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			ch, cluster, tags := setupTagOwner(t)
			id, cmd := "room", options.G.OrginalConvertCmdChannel("room")
			cluster.owners[id], cluster.owners[cmd] = 1001, 1001
			keys := []string{warmOwnerTag(t, tags, id, typ), warmOwnerTag(t, tags, cmd, typ)}
			require.NoError(t, ch.invalidateSubscriberTags(id, typ))
			for _, key := range keys {
				require.False(t, tags.Exist(key))
			}
			require.Empty(t, tags.GetChannelTag(id, typ))
			require.Empty(t, tags.GetChannelTag(cmd, typ))
			// A retry with cold Tags succeeds without activating message Raft.
			require.NoError(t, ch.invalidateSubscriberTags(id, typ))
			require.Zero(t, cluster.messageReads)
		})
	}
}

func TestSubscriberTagInvalidationRoutesEachOwner(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			ch, cluster, _ := setupTagOwner(t)
			id, cmd := "room", options.G.OrginalConvertCmdChannel("room")
			cluster.owners[id], cluster.owners[cmd] = 1003, 1002
			require.NoError(t, ch.invalidateSubscriberTags(id, typ))
			require.Len(t, cluster.invalidations, 2)
			for index, channel := range []string{id, cmd} {
				got := cluster.invalidations[index]
				require.Equal(t, cluster.owners[channel], got.node)
				require.Equal(t, ingress.ChannelReq{ChannelId: channel, ChannelType: typ}, got.req)
			}
			require.Zero(t, cluster.messageReads)
		})
	}
}

func TestSubscriberTagInvalidationDoesNotHideFailures(t *testing.T) {
	for _, kind := range []string{"lookup", "no-owner", "cmd-no-owner", "transport", "status"} {
		t.Run(kind, func(t *testing.T) {
			ch, cluster, _ := setupTagOwner(t)
			id, cmd := "room", options.G.OrginalConvertCmdChannel("room")
			cluster.owners[id], cluster.owners[cmd] = 1001, 1002
			switch kind {
			case "lookup":
				cluster.resolveErr = errors.New("lookup failed")
			case "no-owner":
				cluster.owners[id] = 0
			case "cmd-no-owner":
				cluster.owners[cmd] = 0
			case "transport":
				cluster.requestErr = errors.New("rpc failed")
			case "status":
				cluster.status = proto.StatusError
			}
			require.Error(t, ch.invalidateSubscriberTags(id, 2))
		})
	}
}
