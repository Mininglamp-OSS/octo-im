package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

type compatibilityCluster struct {
	icluster.ICluster
	icluster.Slot
	store        *store.Store
	mu           sync.Mutex
	index        uint64
	readFloorErr error
}

type migrationRecoveryDB struct{ wkdb.DB }

func (migrationRecoveryDB) SubscriberRecoveryActive() bool { return true }

func TestSubscriberRecoveryPausedMigrationRejectedBeforeStartup(t *testing.T) {
	oldStore, oldOptions := service.Store, options.G
	t.Cleanup(func() { service.Store, options.G = oldStore, oldOptions })
	options.G = options.New()
	options.G.SubscriberRecovery.Enabled = false
	options.G.OldV1Api = "http://old.example"
	service.Store = store.New(store.NewOptions(store.WithDB(migrationRecoveryDB{})))
	// An uninitialized API server is intentional: rejection must happen before
	// listeners, recovery workers, or migration tasks are touched.
	require.ErrorContains(t, (&Server{}).Start(), "previously activated subscriber recovery")
}

func (*compatibilityCluster) GetSlotId(string) uint32    { return 0 }
func (*compatibilityCluster) SlotLeaderId(uint32) uint64 { return 1 }
func (*compatibilityCluster) SlotLeaderOfChannel(string, uint8) (*nodetypes.Node, error) {
	return &nodetypes.Node{Id: 1}, nil
}
func (c *compatibilityCluster) LoadOnlyChannelClusterConfig(string, uint8) (wkdb.ChannelClusterConfig, error) {
	if c.readFloorErr != nil {
		return wkdb.ChannelClusterConfig{}, c.readFloorErr
	}
	return wkdb.ChannelClusterConfig{}, wkdb.ErrNotFound
}
func (c *compatibilityCluster) ProposeUntilAppliedTimeout(ctx context.Context, slot uint32, data []byte) (*types.ProposeResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	err := c.store.ApplySlotLogs(slot, []types.Log{{Index: c.index, Data: data}})
	return &types.ProposeResp{Index: c.index}, err
}

func TestSubscriberRecoveryMissingChannelHTTPCompatibility(t *testing.T) {
	oldStore, oldCluster, oldOptions := service.Store, service.Cluster, options.G
	t.Cleanup(func() { service.Store, service.Cluster, options.G = oldStore, oldCluster, oldOptions })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.SubscriberRecovery.Enabled = true
	options.G.SubscriberRecovery.Timeout = time.Second
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithNodeId(1), wkdb.WithShardNum(1), wkdb.WithMemTableSize(1<<20)))
	require.NoError(t, db.Open())
	cluster := &compatibilityCluster{}
	s := store.New(store.NewOptions(store.WithNodeId(1), store.WithDB(db), store.WithSlot(cluster)))
	cluster.store = s
	service.Store, service.Cluster = s, cluster
	require.NoError(t, s.StartSubscriberRecovery(store.SubscriberRecoveryConfig{Workers: 1, MaxPending: 1024, Interval: time.Hour, Timeout: time.Second}, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	t.Cleanup(func() { s.Stop(); require.NoError(t, db.Close()) })
	r := wkhttp.New()
	newChannel(&Server{}).route(r)
	for _, endpoint := range []string{"/channel/delete", "/channel/subscriber_remove", "/channel/subscriber_remove_all", "/channel/blacklist_add", "/channel/blacklist_set", "/channel/blacklist_remove", "/channel/blacklist_remove_all"} {
		t.Run(endpoint, func(t *testing.T) {
			id := endpoint
			body, err := json.Marshal(map[string]any{"channel_id": "missing" + endpoint, "channel_type": 2, "uids": []string{"u"}, "subscribers": []string{"u"}, "operation_id": id})
			require.NoError(t, err)
			for i := 0; i < 2; i++ {
				cluster.readFloorErr = nil
				if i == 1 {
					cluster.readFloorErr = errors.New("message leader unavailable after lost response")
				}
				request := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				r.GetGinRoute().ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Empty(t, response.Header().Get("Retry-After"))
			}
			receipt, found, err := db.GetSubscriberReceipt("missing"+endpoint, 2, id)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "complete", receipt.State)
			require.Zero(t, receipt.Total)
			info, err := db.GetChannel("missing"+endpoint, 2)
			require.NoError(t, err)
			require.True(t, wkdb.IsEmptyChannelInfo(info))
			cluster.readFloorErr = nil
		})
	}
	parts, err := s.SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, parts)
}
