package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

func TestRecentReadValidatedCompletionWinsDeadlineRace(t *testing.T) {
	for _, endpoint := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2"} {
		for _, completed := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/validated=%t", endpoint, completed), func(t *testing.T) {
				var fences atomic.Int32
				reader := setupRecentRead(t, &recentReadCluster{
					load: func(_ context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
						return readCfg(id, typ, 1), nil
					},
					validate: func(ctx context.Context, _ wkdb.ChannelClusterConfig) error {
						if fences.Add(1) == 2 {
							<-ctx.Done() // last fence completes just as the deadline expires
							if !completed {
								return ctx.Err()
							}
						}
						return nil
					},
				})
				options.G.Cluster.ReqTimeout = 30 * time.Millisecond
				service.Store = store.New(store.NewOptions(store.WithDB(&recentReadDB{})))
				router := wkhttp.New()
				newConversation(&Server{requset: reader}).route(router)
				req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{"uid":"alice","channels":[{"channel_id":"g","channel_type":2}],"budget_ms":30}`))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if completed {
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Contains(t, w.Body.String(), `"message_seq":18`)
				} else {
					require.Equal(t, http.StatusServiceUnavailable, w.Code)
				}
				require.Equal(t, int32(2), fences.Load())
			})
		}
	}
}

func TestRecentReadLargeBatchUsesBoundedParallelFencesAndScaledBudget(t *testing.T) {
	for _, endpoint := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2"} {
		t.Run(endpoint, func(t *testing.T) {
			var active, peak, routes, fences atomic.Int32
			pause := func(ctx context.Context) error {
				n := active.Add(1)
				defer active.Add(-1)
				for p := peak.Load(); n > p; p = peak.Load() {
					if peak.CompareAndSwap(p, n) {
						break
					}
				}
				select {
				case <-time.After(10 * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			reader := setupRecentRead(t, &recentReadCluster{
				load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
					routes.Add(1)
					return readCfg(id, typ, 1), pause(ctx)
				},
				validate: func(ctx context.Context, _ wkdb.ChannelClusterConfig) error { fences.Add(1); return pause(ctx) },
			})
			options.G.Cluster.ReqTimeout = 200 * time.Millisecond
			service.Store = store.New(store.NewOptions(store.WithDB(&recentReadDB{})))
			channels := make([]*channelRecentMessageReq, 256)
			for i := range channels {
				channels[i] = &channelRecentMessageReq{ChannelId: fmt.Sprintf("g-%d", i), ChannelType: 2}
			}
			body, err := json.Marshal(recentReadRequest{UID: "alice", Channels: channels, MsgCount: 15, BudgetMS: 800})
			require.NoError(t, err)
			router := wkhttp.New()
			newConversation(&Server{requset: reader}).route(router)
			run := func(parent context.Context) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body))).WithContext(parent)
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				return w
			}
			start := time.Now()
			w := run(context.Background())
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, int32(256), routes.Load())
			require.Equal(t, int32(512), fences.Load())
			require.Greater(t, peak.Load(), int32(1))
			require.LessOrEqual(t, peak.Load(), int32(16))
			require.Zero(t, active.Load())
			require.Greater(t, time.Since(start), 200*time.Millisecond, "must actually exceed the old fixed budget")
			var got []*channelRecentMessage
			if endpoint == "/conversation/syncMessages/v2" {
				var envelope recentReadResponse
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
				got = envelope.Channels
			} else {
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			}
			require.NoError(t, validateRecentBatch(channels, got))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			w = run(ctx)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, "scaling must not extend a caller deadline")
			require.Zero(t, active.Load(), "all workers must exit before returning")
		})
	}
}

func TestRecentReadRetainsExistingRouteAfterOtherWorkerFails(t *testing.T) {
	var round atomic.Int32
	seen := make(map[string]bool)
	resolved := make(chan struct{})
	reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		if round.Load() == 0 {
			if id == "existing" {
				close(resolved)
				return readCfg(id, typ, 1), nil
			}
			<-resolved
			return wkdb.EmptyChannelClusterConfig, errors.New("route temporarily unavailable")
		}
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}})
	channels := []*channelRecentMessageReq{{ChannelId: "existing", ChannelType: 2}, {ChannelId: "failed", ChannelType: 2}}
	_, err := reader.recentMessagesAttempt(context.Background(), "alice", 15, channels, true, seen)
	require.Error(t, err)
	round.Store(1)
	got, err := reader.recentMessagesAttempt(context.Background(), "alice", 15, channels, true, seen)
	require.Error(t, err, "a previously resolved channel must never decay into empty success")
	require.Nil(t, got)
}

func TestRecentReadLegacyPeerRequestRemainsLeafAndHonorsBudget(t *testing.T) {
	for _, slowRoute := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow_route=%t", slowRoute), func(t *testing.T) {
			reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
				if slowRoute {
					<-ctx.Done()
					return wkdb.EmptyChannelClusterConfig, ctx.Err()
				}
				return readCfg(id, typ, 2), nil
			}})
			router := wkhttp.New()
			newConversation(&Server{requset: reader}).route(router)
			req := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages", strings.NewReader(`{"uid":"alice","channels":[{"channel_id":"g","channel_type":2}],"peer_read":true,"budget_ms":20}`))
			req.Header.Set("Content-Type", "application/json")
			start := time.Now()
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, "a forwarded v1 request must not refanout to node 2")
			require.Less(t, time.Since(start), 200*time.Millisecond, "v1 must honor the peer's 20ms budget")
		})
	}
}
