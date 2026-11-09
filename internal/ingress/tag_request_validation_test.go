package ingress_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	rpcclient "github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type tagRouteCluster struct {
	*tagOwnerCluster
	server *wkserver.Server
}

func (c *tagRouteCluster) Route(path string, h wkserver.Handler) { c.server.Route(path, h) }

// Exercise the registered route, including wire decode and error response.
func tagRouteClient(t *testing.T, i *ingress.Ingress, cluster *tagOwnerCluster) *rpcclient.Client {
	t.Helper()
	// Match server startup: initialize lazy global loggers before networking
	// goroutines can emit their first log concurrently.
	wklog.Info("starting isolated Tag RPC test")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + l.Addr().String()
	require.NoError(t, l.Close())
	s := wkserver.New(addr)
	service.Cluster = &tagRouteCluster{cluster, s}
	i.SetRoutes()
	s.Route("/test/ready", func(c *wkserver.Context) { c.WriteOk() })
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	c := rpcclient.New(addr, rpcclient.WithUid("tag-route-test"))
	require.NoError(t, c.Start())
	t.Cleanup(c.Stop)
	require.Eventually(t, c.IsAuthed, 5*time.Second, 10*time.Millisecond)
	ready, err := c.Request("/test/ready", nil)
	require.NoError(t, err)
	require.Equal(t, proto.StatusOK, ready.Status)
	return c
}

func TestChannelTagRouteRejectsInvalidIdentityWithoutMutation(t *testing.T) {
	i, cluster, tags := setupTagOwner(t)
	c := tagRouteClient(t, i, cluster)
	for _, kind := range []string{"empty-id", "zero-type", "other-channel-key", "unknown-key", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			tags.RemoveChannelTag("", 2)
			tags.RemoveChannelTag("room-a", 0)
			a, err := tags.MakeTag([]string{"alice"})
			require.NoError(t, err)
			b, err := tags.MakeTag([]string{"bob"})
			require.NoError(t, err)
			tags.SetChannelTag("room-a", 2, a.Key)
			tags.SetChannelTag("room-b", 2, b.Key)
			aKey, bKey := a.Key, b.Key
			req := ingress.TagUpdateReq{ChannelId: "room-a", ChannelType: 2, ChannelTag: true, Uids: []string{"intruder"}}
			switch kind {
			case "empty-id":
				req.ChannelId, req.TagKey = "", bKey
			case "zero-type":
				req.ChannelType, req.TagKey = 0, bKey
			case "other-channel-key":
				req.TagKey = bKey
			case "unknown-key":
				req.TagKey = "nonexistent"
			}
			body, err := req.Encode()
			require.NoError(t, err)
			if kind == "malformed" {
				body = []byte{0xff}
			}
			resp, err := c.Request("/wk/ingress/updateTag", body)
			require.NoError(t, err)
			t.Logf("kind=%s status=%d room-a=%v room-b=%v", kind, resp.Status, tags.GetUsers(tags.GetChannelTag("room-a", 2)), tags.GetUsers(tags.GetChannelTag("room-b", 2)))
			require.Equal(t, proto.StatusError, resp.Status, string(resp.Body))
			require.Equal(t, aKey, tags.GetChannelTag("room-a", 2))
			require.Equal(t, bKey, tags.GetChannelTag("room-b", 2))
			require.Equal(t, []string{"alice"}, tags.GetUsers(aKey))
			require.Equal(t, []string{"bob"}, tags.GetUsers(bKey))
			require.Empty(t, tags.GetChannelTag("", 2))
			require.Empty(t, tags.GetChannelTag("room-a", 0))
			// Reset only fixture state, never repair the request under test.
		})
	}
}

func TestInvalidateChannelTagRegisteredRoute(t *testing.T) {
	i, cluster, tags := setupTagOwner(t)
	c := tagRouteClient(t, i, cluster)
	for _, kind := range []string{"valid", "cold", "wrong-owner", "empty-id", "zero-type", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			cluster.owner = 1001
			id := fmt.Sprintf("invalidate-%s", kind)
			tag, err := tags.MakeTag([]string{"old"})
			require.NoError(t, err)
			tags.SetChannelTag(id, 2, tag.Key)
			req := ingress.ChannelReq{ChannelId: id, ChannelType: 2}
			switch kind {
			case "cold":
				tags.RemoveTag(tag.Key)
				tags.RemoveChannelTag(id, 2)
			case "wrong-owner":
				cluster.owner = 1002
			case "empty-id":
				req.ChannelId = ""
			case "zero-type":
				req.ChannelType = 0
			}
			body, err := req.Encode()
			require.NoError(t, err)
			if kind == "malformed" {
				body = []byte{0xff}
			}
			resp, err := c.Request("/wk/ingress/invalidateChannelTag", body)
			require.NoError(t, err)
			if kind == "valid" || kind == "cold" {
				require.Equal(t, proto.StatusOK, resp.Status)
				require.Empty(t, tags.GetChannelTag(id, 2))
				require.False(t, tags.Exist(tag.Key))
			} else {
				require.Equal(t, proto.StatusError, resp.Status)
				require.Equal(t, tag.Key, tags.GetChannelTag(id, 2))
				require.Equal(t, []string{"old"}, tags.GetUsers(tag.Key))
			}
		})
	}
}

type snapshotRouteDB struct {
	wkdb.DB
	err error
}

func (d *snapshotRouteDB) GetSubscribers(string, uint8) ([]wkdb.Member, error) {
	return []wkdb.Member{{Uid: "member"}}, d.err
}
func TestSubscriberSnapshotRegisteredRoute(t *testing.T) {
	i, cluster, _ := setupTagOwner(t)
	old := service.Store
	t.Cleanup(func() { service.Store = old })
	db := &snapshotRouteDB{}
	service.Store = store.New(store.NewOptions(store.WithDB(db)))
	c := tagRouteClient(t, i, cluster)
	req := ingress.SubscriberSnapshotReq{ChannelId: "room", ChannelType: 2}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	resp, err := c.Request("/wk/ingress/subscriberSnapshot", body)
	require.NoError(t, err)
	require.Equal(t, proto.StatusOK, resp.Status)
	var first ingress.SubscriberSnapshotResp
	require.NoError(t, json.Unmarshal(resp.Body, &first))
	require.True(t, first.Changed)
	require.NotEmpty(t, first.Revision)
	require.Equal(t, []string{"member"}, first.Subscribers)
	req.Revision = first.Revision
	body, err = json.Marshal(req)
	require.NoError(t, err)
	resp, err = c.Request("/wk/ingress/subscriberSnapshot", body)
	require.NoError(t, err)
	var unchanged ingress.SubscriberSnapshotResp
	require.NoError(t, json.Unmarshal(resp.Body, &unchanged))
	require.False(t, unchanged.Changed)
	require.Empty(t, unchanged.Subscribers)
	require.Equal(t, first.Revision, unchanged.Revision)
	for _, kind := range []string{"wrong-owner", "zero-owner", "empty-id", "zero-type", "malformed", "storage-error"} {
		t.Run(kind, func(t *testing.T) {
			cluster.owner = 1001
			db.err = nil
			req = ingress.SubscriberSnapshotReq{ChannelId: "room", ChannelType: 2}
			switch kind {
			case "wrong-owner":
				cluster.owner = 1002
			case "zero-owner":
				cluster.owner = 0
			case "empty-id":
				req.ChannelId = ""
			case "zero-type":
				req.ChannelType = 0
			case "storage-error":
				db.err = errors.New("snapshot read failed")
			}
			body, err = json.Marshal(req)
			require.NoError(t, err)
			if kind == "malformed" {
				body = []byte("{")
			}
			resp, err = c.Request("/wk/ingress/subscriberSnapshot", body)
			require.NoError(t, err)
			require.Equal(t, proto.StatusError, resp.Status)
		})
	}
}
