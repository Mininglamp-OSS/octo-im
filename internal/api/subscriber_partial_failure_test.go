package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

// Only consensus transport is replaced. Commands use the production codecs,
// ApplySlotLogs and a real isolated Pebble database.
type subscriberTestSlot struct {
	icluster.Slot
	sync.Mutex
	store *store.Store
	index uint64
}

func (*subscriberTestSlot) GetSlotId(string) uint32 { return 0 }
func (s *subscriberTestSlot) ProposeUntilApplied(id uint32, data []byte) (*rafttypes.ProposeResp, error) {
	s.Lock()
	defer s.Unlock()
	s.index++
	err := s.store.ApplySlotLogs(id, []rafttypes.Log{{Index: s.index, Data: data}})
	return &rafttypes.ProposeResp{Index: s.index}, err
}
func (s *subscriberTestSlot) Propose(id uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return s.ProposeUntilApplied(id, data)
}
func (s *subscriberTestSlot) ProposeUntilAppliedTimeout(_ context.Context, id uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return s.ProposeUntilApplied(id, data)
}

type subscriberFailureCluster struct {
	*tagOwnerCluster
	snapshotErr      error
	snapshotRequests []ingress.SubscriberSnapshotReq
}

func (c *subscriberFailureCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if path != "/wk/ingress/subscriberSnapshot" {
		return c.tagOwnerCluster.RequestWithContext(ctx, node, path, body)
	}
	if c.snapshotErr != nil {
		return nil, c.snapshotErr
	}
	var req ingress.SubscriberSnapshotReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if node != c.owners[req.ChannelId] {
		return nil, errors.New("snapshot routed to wrong member owner")
	}
	c.snapshotRequests = append(c.snapshotRequests, req)
	snap, err := service.Store.GetSubscriberSnapshot(req.ChannelId, req.ChannelType, req.Revision)
	if err != nil {
		return nil, err
	}
	resp := ingress.SubscriberSnapshotResp{Revision: snap.Revision, Changed: snap.Changed}
	for _, m := range snap.Members {
		resp.Subscribers = append(resp.Subscribers, m.Uid)
	}
	b, err := json.Marshal(resp)
	return &proto.Response{Status: proto.StatusOK, Body: b}, err
}

func (*subscriberFailureCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	return &clustertypes.Node{Id: 1001}, nil
}

type subscriberConversations struct{ service.IConversationManager }

func (*subscriberConversations) DeleteFromCache(string, string, uint8) error { return nil }

func TestSubscriberPartialRPCFailureKeepsDurableState(t *testing.T) {
	for _, typ := range []uint8{2, 5} {
		for _, operation := range []string{"add", "remove", "reset", "remove-all"} {
			t.Run(fmt.Sprintf("type%d/%s", typ, operation), func(t *testing.T) {
				ch, cluster, tags := setupTagOwner(t)
				oldStore, oldConversations, oldTrace := service.Store, service.ConversationManager, trace.GlobalTrace
				t.Cleanup(func() {
					service.Store, service.ConversationManager = oldStore, oldConversations
					trace.SetGlobalTrace(oldTrace)
				})
				trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
				db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
				require.NoError(t, db.Open())
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				slot := &subscriberTestSlot{}
				service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot)))
				slot.store = service.Store
				service.ConversationManager = &subscriberConversations{}
				network := &subscriberFailureCluster{tagOwnerCluster: cluster}
				service.Cluster = network
				id := "partial-room"
				cmd := options.G.OrginalConvertCmdChannel(id)
				cluster.owners[id], cluster.owners[cmd] = 1001, 1002
				require.NoError(t, service.Store.AddChannelInfo(wkdb.NewChannelInfo(id, typ)))
				now := time.Now()
				require.NoError(t, service.Store.AddSubscribers(id, typ, []wkdb.Member{{Uid: "sender", CreatedAt: &now, UpdatedAt: &now}, {Uid: "old-recipient", CreatedAt: &now, UpdatedAt: &now}}))
				warmOwnerTag(t, tags, id, typ)
				warmOwnerTag(t, tags, cmd, typ)
				cluster.requestErr = errors.New("injected: second Tag owner unavailable")
				body := map[string]any{"channel_id": id, "channel_type": typ, "subscribers": []string{"new-recipient"}}
				path := "/channel/subscriber_add"
				want := []string{"sender", "old-recipient", "new-recipient"}
				switch operation {
				case "remove":
					path = "/channel/subscriber_remove"
					body["subscribers"] = []string{"old-recipient"}
					want = []string{"sender"}
				case "reset":
					body["reset"] = 1
					want = nil // reset fails after durable clear, before adding replacements
				case "remove-all":
					path = "/channel/subscriber_remove_all"
					want = nil
				}
				router := wkhttp.New()
				ch.route(router)
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
				req.Header.Set("Content-Type", "application/json")
				resp := httptest.NewRecorder()
				router.GetGinRoute().ServeHTTP(resp, req)
				require.NotEqual(t, http.StatusOK, resp.Code, resp.Body.String())
				members, err := db.GetSubscribers(id, typ)
				require.NoError(t, err)
				var stored []string
				for _, m := range members {
					stored = append(stored, m.Uid)
				}
				require.ElementsMatch(t, want, stored)
				stale := tags.GetUsers(tags.GetChannelTag(cmd, typ))
				require.ElementsMatch(t, []string{"sender", "old-recipient"}, stale)
				t.Logf("HTTP=%d durable members=%v ordinary Tag=%v cmd Tag=%v", resp.Code, stored, tags.GetUsers(tags.GetChannelTag(id, typ)), stale)
				// The mutation RPC stays broken. A later distribution must repair
				// from the durable membership, without replaying the failed API.
				commonService := common.NewService()
				ordinary, err := commonService.GetOrMakeSubscriberTag(id, typ)
				require.NoError(t, err)
				require.ElementsMatch(t, want, ordinary.GetUsers())
				options.G.Cluster.NodeId = 1002 // cmd distribution owner, original owner is 1001
				network.snapshotErr = errors.New("membership authority unavailable")
				unavailable, err := commonService.GetOrMakeSubscriberTag(cmd, typ)
				require.Error(t, err)
				require.Nil(t, unavailable, "must not return warmed stale recipients")
				network.snapshotErr = nil
				repaired, err := commonService.GetOrMakeSubscriberTag(cmd, typ)
				require.NoError(t, err)
				require.ElementsMatch(t, want, repaired.GetUsers())
				require.Len(t, network.snapshotRequests, 1)
				require.Equal(t, id, network.snapshotRequests[0].ChannelId)
				require.NotEmpty(t, repaired.SubscriberRevision.Load())
				again, err := commonService.GetOrMakeSubscriberTag(cmd, typ)
				require.NoError(t, err)
				require.Same(t, repaired, again, "unchanged membership reuses the certified Tag")
				require.NotEmpty(t, network.snapshotRequests[1].Revision)
				options.G.Cluster.NodeId = 1001
			})
		}
	}
}
