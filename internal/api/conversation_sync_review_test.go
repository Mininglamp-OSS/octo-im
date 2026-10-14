package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

func TestRecentReadSlowHealthyBatchUsesWholeBudget(t *testing.T) {
	for _, where := range []string{"route", "peer"} {
		t.Run(where, func(t *testing.T) {
			var calls atomic.Int32
			pause := func(ctx context.Context) error {
				select {
				case <-time.After(650 * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			peer := recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if where == "peer" && pause(r.Context()) != nil {
					return
				}
				_, _ = w.Write([]byte(`{"version":2,"channels":[{"channel_id":"g","channel_type":2,"messages":[]}]}`))
			})
			reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
				if where == "route" {
					if err := pause(ctx); err != nil {
						return wkdb.EmptyChannelClusterConfig, err
					}
				}
				return readCfg(id, typ, 2), nil
			}})
			got, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2}}, true)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestRecentReadUnusedChannelWireArrays(t *testing.T) {
	reader := setupRecentRead(t, &recentReadCluster{load: func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error) {
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}})
	service.Store = store.New(store.NewOptions(store.WithDB(&recentReadDB{})))
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	for path, field := range map[string]string{"/conversation/syncMessages": "messages", "/conversation/syncByChannels": "recents"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"uid":"alice","channels":[{"channel_id":"unused","channel_type":2}]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var entries []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &entries))
		require.Len(t, entries, 1)
		require.JSONEq(t, `[]`, string(entries[0][field]))
	}
}

func TestRecentReadLegacyFallbackRequires404AndCompleteCoverage(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		ok     bool
	}{
		{"complete", 404, `[{"channel_id":"a","channel_type":2,"messages":[]},{"channel_id":"b","channel_type":2,"messages":[]}]`, true},
		{"missing", 404, `[]`, false},
		{"partial", 404, `[{"channel_id":"a","channel_type":2,"messages":[]}]`, false},
		{"duplicate", 404, `[{"channel_id":"a","channel_type":2},{"channel_id":"a","channel_type":2}]`, false},
		{"wrong", 404, `[{"channel_id":"a","channel_type":2},{"channel_id":"wrong","channel_type":2}]`, false},
		{"malformed", 404, `{`, false},
		{"timeout must not downgrade", 503, `[]`, false},
		{"v2 malformed must not downgrade", 200, `[]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var legacyCalls atomic.Int32
			peer := recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/conversation/syncMessages/v2" {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(`[]`))
					return
				}
				require.Equal(t, "/conversation/syncMessages", r.URL.Path)
				legacyCalls.Add(1)
				var req recentReadRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				require.True(t, req.PeerRead, "fallback must remain a leaf on upgraded peers")
				require.Equal(t, "alice", req.UID)
				require.Len(t, req.Channels, 2)
				require.Equal(t, 15, req.MsgCount)
				require.Equal(t, 1, req.OrderByLast)
				_, _ = fmt.Fprint(w, tc.body)
			})
			reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := reader.requestSyncMessage(ctx, 2, []*channelRecentMessageReq{{ChannelId: "a", ChannelType: 2}, {ChannelId: "b", ChannelType: 2}}, "alice", 15, true)
			if tc.ok {
				require.NoError(t, err)
				require.Len(t, got, 2)
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
			if tc.status == 404 {
				require.Equal(t, int32(1), legacyCalls.Load())
			} else {
				require.Zero(t, legacyCalls.Load())
			}
		})
	}
}
