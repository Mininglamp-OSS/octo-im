package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

func TestRecentReadRejectsOversizedInputBeforeRouting(t *testing.T) {
	var calls atomic.Int32
	reader := setupRecentRead(t, &recentReadCluster{load: func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error) {
		calls.Add(1)
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}})
	options.G.Cluster.RecentReadMaxChannels = 2
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	for _, path := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2", "/conversation/syncByChannels"} {
		w := httptest.NewRecorder()
		// Raw duplicate entries count toward admission too, before normalization.
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"uid":"alice","budget_ms":1000,"channels":[{"channel_id":"g","channel_type":2},{"channel_id":"g","channel_type":2},{"channel_id":"g","channel_type":2}]}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "limit 2")
	}
	require.Zero(t, calls.Load())
	_, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, make([]*channelRecentMessageReq, 3), true)
	require.ErrorContains(t, err, "limit 2", "server-built batches must obey admission too")
	require.Zero(t, calls.Load())
}

func TestRecentReadOperatorTimeCeilingBoundsPeerBudget(t *testing.T) {
	reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, _ string, _ uint8) (wkdb.ChannelClusterConfig, error) {
		<-ctx.Done()
		return wkdb.EmptyChannelClusterConfig, ctx.Err()
	}})
	options.G.Cluster.ReqTimeout = time.Second
	options.G.Cluster.RecentReadMaxTimeout = 20 * time.Millisecond
	require.Equal(t, 20*time.Millisecond, recentReadBudget(64000))
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	for _, path := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"uid":"alice","budget_ms":1000000,"channels":[{"channel_id":"g","channel_type":2}]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.Less(t, time.Since(start), 200*time.Millisecond, "an inbound budget cannot exceed the operator ceiling")
	}
}

func recordRecentPeak(active, peak *atomic.Int32) func() {
	n := active.Add(1)
	for p := peak.Load(); n > p; p = peak.Load() {
		if peak.CompareAndSwap(p, n) {
			break
		}
	}
	return func() { active.Add(-1) }
}

func TestRecentReadMetadataLimitIsSharedAcrossRequests(t *testing.T) {
	var active, peak atomic.Int32
	pause := func(ctx context.Context) error {
		done := recordRecentPeak(&active, &peak)
		defer done()
		select {
		case <-time.After(10 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	reader := setupRecentRead(t, &recentReadCluster{
		load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			return readCfg(id, typ, 1), pause(ctx)
		},
		validate: func(ctx context.Context, _ wkdb.ChannelClusterConfig) error { return pause(ctx) },
	})
	service.Store = store.New(store.NewOptions(store.WithDB(&recentReadDB{})))
	var wg sync.WaitGroup
	for requestID := 0; requestID < 4; requestID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			chs := make([]*channelRecentMessageReq, 32)
			for i := range chs {
				chs[i] = &channelRecentMessageReq{ChannelId: fmt.Sprintf("r%d-c%d", id, i), ChannelType: 2}
			}
			got, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, chs, true)
			if err != nil {
				t.Errorf("request failed: %v", err)
				return
			}
			if len(got) != len(chs) {
				t.Errorf("incomplete response: %d", len(got))
			}
		}(requestID)
	}
	wg.Wait()
	require.Greater(t, peak.Load(), int32(1))
	require.LessOrEqual(t, peak.Load(), int32(16))
	require.Zero(t, active.Load())
}

func TestRecentReadPeerLimitIsSharedAcrossRequests(t *testing.T) {
	var active, peak atomic.Int32
	peer := recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
		done := recordRecentPeak(&active, &peak)
		defer done()
		time.Sleep(50 * time.Millisecond)
		var req recentReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		result := recentReadResponse{Version: 2}
		for _, ch := range req.Channels {
			result.Channels = append(result.Channels, &channelRecentMessage{ChannelId: ch.ChannelId, ChannelType: ch.ChannelType})
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(_ context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		return readCfg(id, typ, 2), nil
	}})
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: fmt.Sprint(i), ChannelType: 2}}, true)
			if err != nil {
				t.Error(err)
			}
			if len(got) != 1 {
				t.Errorf("incomplete response: %d", len(got))
			}
		}(i)
	}
	wg.Wait()
	require.Greater(t, peak.Load(), int32(1))
	require.LessOrEqual(t, peak.Load(), int32(16))
	require.Zero(t, active.Load())
}

func TestRecentReadDeletedChannelStopsCoordinatorRetries(t *testing.T) {
	var routes, fences atomic.Int32
	reader := setupRecentRead(t, &recentReadCluster{
		load: func(_ context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			if routes.Add(1) == 1 {
				return readCfg(id, typ, 1), nil
			}
			return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
		},
		validate: func(context.Context, wkdb.ChannelClusterConfig) error {
			fences.Add(1)
			return errors.New("channel deleted during read")
		},
	})
	started := time.Now()
	got, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: "deleted", ChannelType: 2}}, true)
	require.ErrorContains(t, err, "previously observed channel disappeared")
	require.Nil(t, got, "never silently omit the deleted channel from a successful response")
	require.Equal(t, int32(2), routes.Load(), "only the first retry may discover the deletion")
	require.Equal(t, int32(1), fences.Load())
	require.Less(t, time.Since(started), 500*time.Millisecond, "terminal deletion must not exhaust the one-second request budget")
}

func TestRecentReadDeletedChannelNotMaskedBySiblingFailure(t *testing.T) {
	var deletedCalls atomic.Int32
	var retry atomic.Bool
	var signal sync.Once
	deletedReading := make(chan struct{})
	reader := setupRecentRead(t, &recentReadCluster{
		load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			if id == "deleted" {
				if deletedCalls.Add(1) == 1 {
					return readCfg(id, typ, 1), nil
				}
				signal.Do(func() { close(deletedReading) })
				<-ctx.Done() // the sibling error is recorded first
				return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
			}
			if !retry.Load() {
				return readCfg(id, typ, 1), nil
			}
			<-deletedReading
			return wkdb.EmptyChannelClusterConfig, errors.New("transient sibling route failure")
		},
		validate: func(context.Context, wkdb.ChannelClusterConfig) error {
			retry.Store(true)
			return errors.New("route changed during read")
		},
	})
	got, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: "deleted", ChannelType: 2}, {ChannelId: "sibling", ChannelType: 2}}, true)
	require.ErrorContains(t, err, "previously observed channel disappeared")
	require.Nil(t, got)
	require.Equal(t, int32(2), deletedCalls.Load())
}
