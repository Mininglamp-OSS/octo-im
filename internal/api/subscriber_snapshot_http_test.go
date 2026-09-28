package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

func TestSubscriberSnapshotHTTPPageCompletionAndRejectedRetry(t *testing.T) {
	oldStore, oldCluster, oldOptions := service.Store, service.Cluster, options.G
	t.Cleanup(func() { service.Store, service.Cluster, options.G = oldStore, oldCluster, oldOptions })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.SubscriberRecovery.Enabled = true
	options.G.SubscriberRecovery.Timeout = 3 * time.Second
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithNodeId(1), wkdb.WithShardNum(1), wkdb.WithMemTableSize(1<<20)))
	require.NoError(t, db.Open())
	cluster := &compatibilityCluster{}
	s := store.New(store.NewOptions(store.WithNodeId(1), store.WithDB(db), store.WithSlot(cluster)))
	cluster.store = s
	service.Store, service.Cluster = s, cluster
	require.NoError(t, s.StartSubscriberRecovery(store.SubscriberRecoveryConfig{Workers: 1, MaxPending: 1, Interval: time.Hour, Timeout: time.Second}, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	t.Cleanup(func() { s.Stop(); require.NoError(t, db.Close()) })
	r := wkhttp.New()
	newChannel(&Server{}).route(r)
	post := func(path string, body any) *httptest.ResponseRecorder {
		wire, err := json.Marshal(body)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(wire))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		r.GetGinRoute().ServeHTTP(res, req)
		return res
	}
	first := map[string]any{"channel_id": "group", "channel_type": 2, "operation_id": "p0", "revision": 1, "snapshot_id": strings.Repeat("a", 64), "page_index": 0, "page_count": 2, "range_start": "", "range_end": "m", "subscribers": []string{"a"}}
	res := post("/channel/subscriber_reconcile", first)
	require.Equal(t, 200, res.Code, res.Body.String())
	status, _, err := db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.False(t, status.Complete)
	require.EqualValues(t, 1, status.CompletedPages)
	bad := map[string]any{"channel_id": "group", "channel_type": 2, "operation_id": "bad-p1", "revision": 1, "snapshot_id": strings.Repeat("a", 64), "page_index": 1, "page_count": 2, "range_start": "z", "range_end": "", "subscribers": []string{"z"}}
	res = post("/channel/subscriber_reconcile", bad)
	require.Equal(t, 409, res.Code, res.Body.String())
	last := map[string]any{"channel_id": "group", "channel_type": 2, "operation_id": "p1", "revision": 1, "snapshot_id": strings.Repeat("a", 64), "page_index": 1, "page_count": 2, "range_start": "m", "range_end": "", "subscribers": []string{"z"}}
	res = post("/channel/subscriber_reconcile", last)
	require.Equal(t, 200, res.Code, res.Body.String())
	status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.True(t, status.Complete)
	// A real source receipt, deliberately not completed, fills the queue.
	busy, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "busy", ChannelID: "busy", ChannelType: 2, Mode: "add"})
	require.NoError(t, err)
	change := map[string]any{"channel_id": "group", "channel_type": 2, "operation_id": "change", "revision": 2, "subscribers": []string{"b"}}
	res = post("/channel/subscriber_reconcile", change)
	require.Equal(t, 429, res.Code, res.Body.String())
	require.Equal(t, "1", res.Header().Get("Retry-After"))
	_, err = s.CompleteSubscriberOperation(context.Background(), busy)
	require.NoError(t, err)
	res = post("/channel/subscriber_reconcile", change)
	require.Equal(t, 200, res.Code, res.Body.String())
	for uid, deleted := range map[string]bool{"a": true, "z": true, "b": false} {
		life, found, err := db.ConversationLifecycle(uid, "group", 2)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, deleted, life.Deleted)
	}
	res = post("/channel/subscriber_operation", map[string]any{"channel_id": "group", "channel_type": 2, "operation_id": "change"})
	require.Equal(t, 200, res.Code, res.Body.String())
	var result struct {
		Data struct {
			Snapshot wkdb.SubscriberSnapshotStatus `json:"snapshot"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &result))
	require.True(t, result.Data.Snapshot.Complete)
}
