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
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type recentReadCluster struct {
	icluster.ICluster
	load     func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error)
	validate func(context.Context, wkdb.ChannelClusterConfig) error
	nodes    map[uint64]*clustertypes.Node
}

func (c *recentReadCluster) LoadChannelReadConfig(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	return c.load(ctx, id, typ)
}
func (c *recentReadCluster) ValidateLocalChannelRead(ctx context.Context, cfg wkdb.ChannelClusterConfig) error {
	if c.validate != nil {
		return c.validate(ctx, cfg)
	}
	return ctx.Err()
}
func (c *recentReadCluster) NodeInfoById(id uint64) *clustertypes.Node { return c.nodes[id] }
func (c *recentReadCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	return &clustertypes.Node{Id: 1}, nil
}
func setupRecentRead(t *testing.T, c *recentReadCluster) *request {
	t.Helper()
	oldOptions, oldCluster, oldStore := options.G, service.Cluster, service.Store
	t.Cleanup(func() { options.G, service.Cluster, service.Store = oldOptions, oldCluster, oldStore })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.Cluster.ReqTimeout = time.Second
	service.Cluster = c
	return newRequset(nil)
}
func readCfg(id string, typ uint8, leader uint64) wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: typ, LeaderId: leader, Term: 1, ConfVersion: 1, Replicas: []uint64{leader}}
}
func recentPeer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s
}
func successfulRecentPeer(t *testing.T) *httptest.Server {
	return recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversation/syncMessages/v2" {
			http.NotFound(w, r)
			return
		}
		var req recentReadRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.BudgetMS <= 0 && req.Budget <= 0 {
			w.WriteHeader(400)
			return
		}
		result := recentReadResponse{Version: recentReadVersion}
		for _, ch := range req.Channels {
			result.Channels = append(result.Channels, &channelRecentMessage{ChannelId: ch.ChannelId, ChannelType: ch.ChannelType, Messages: types.MessageRespSlice{&types.MessageResp{MessageSeq: 18}}})
		}
		_ = json.NewEncoder(w).Encode(result)
	})
}

func TestRecentReadRefreshesFailedSlotRoute(t *testing.T) {
	peer := successfulRecentPeer(t)
	calls := 0
	c := &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		calls++
		if calls == 1 {
			return wkdb.EmptyChannelClusterConfig, errors.New("not slot leader")
		}
		return readCfg(id, typ, 2), nil
	}}
	reader := setupRecentRead(t, c)
	result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 300, []*channelRecentMessageReq{{ChannelId: "group", ChannelType: 2}}, true)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Len(t, result, 1)
	require.Equal(t, uint64(18), result[0].Messages[0].MessageSeq)
}

func TestRecentReadRefreshesUnavailableChannelLeader(t *testing.T) {
	slow := recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(220 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	healthy := successfulRecentPeer(t)
	calls := 0
	c := &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: slow.URL}, 3: {Id: 3, ApiServerAddr: healthy.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		calls++
		node := uint64(2)
		if calls > 1 {
			node = 3
		}
		return readCfg(id, typ, node), nil
	}}
	reader := setupRecentRead(t, c)
	options.G.Cluster.ReqTimeout = 300 * time.Millisecond
	started := time.Now()
	result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 300, []*channelRecentMessageReq{{ChannelId: "group", ChannelType: 2}}, true)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, 2, calls)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}

func TestRecentReadRejectsIncompleteOrLegacyPeer(t *testing.T) {
	for name, body := range map[string]string{
		"missing channel":   `{"version":2,"channels":[]}`,
		"partial batch":     `{"version":2,"channels":[{"channel_id":"a","channel_type":2}]}`,
		"wrong channel":     `{"version":2,"channels":[{"channel_id":"other","channel_type":2}]}`,
		"duplicate channel": `{"version":2,"channels":[{"channel_id":"a","channel_type":2},{"channel_id":"a","channel_type":2}]}`,
		"legacy":            `[]`, "malformed": `{`, "missing version": `{"channels":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			peer := recentPeer(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1); _, _ = w.Write([]byte(body)) })
			reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
				return readCfg(id, typ, 2), nil
			}})
			result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 10, []*channelRecentMessageReq{{ChannelId: "a", ChannelType: 2}, {ChannelId: "b", ChannelType: 2}}, true)
			require.Error(t, err)
			require.Nil(t, result)
			require.GreaterOrEqual(t, requests.Load(), int32(2))
			require.LessOrEqual(t, requests.Load(), int32(8))
		})
	}
}

func TestRecentReadNoPartialSuccessAndBoundedCancellation(t *testing.T) {
	t.Run("one missing route fails whole batch", func(t *testing.T) {
		peer := successfulRecentPeer(t)
		reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			if id == "bad" {
				return wkdb.EmptyChannelClusterConfig, errors.New("not slot leader")
			}
			return readCfg(id, typ, 2), nil
		}})
		result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 10, []*channelRecentMessageReq{{ChannelId: "good", ChannelType: 2}, {ChannelId: "bad", ChannelType: 2}}, true)
		require.Error(t, err)
		require.Nil(t, result)
	})
	t.Run("context reaches route RPC", func(t *testing.T) {
		calls := 0
		reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			calls++
			<-ctx.Done()
			return wkdb.EmptyChannelClusterConfig, ctx.Err()
		}})
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err := reader.getRecentMessagesForCluster(ctx, "alice", 10, []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2}}, true)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.LessOrEqual(t, calls, 2)
		require.Less(t, time.Since(started), 150*time.Millisecond)
		cancel()
		_, err = reader.getRecentMessagesForCluster(ctx, "alice", 10, []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2}}, true)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("unused channel remains empty", func(t *testing.T) {
		reader := setupRecentRead(t, &recentReadCluster{load: func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error) {
			return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
		}})
		result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 10, []*channelRecentMessageReq{{ChannelId: "unused", ChannelType: 2}}, true)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Empty(t, result[0].Messages)
	})
}

type recentReadDB struct {
	wkdb.DB
	afterLoad func()
	partial   bool
}

func (*recentReadDB) GetChannelShardIndex(string, uint8) uint32 { return 0 }
func (*recentReadDB) GetUserLastMsgSeqBatch(string, []wkdb.Channel) (map[string]uint64, error) {
	return map[string]uint64{}, nil
}
func (d *recentReadDB) LoadMsgsBatch(reqs []wkdb.BatchMsgRequest) ([]wkdb.BatchMsgResponse, error) {
	if d.partial {
		return nil, nil
	}
	if d.afterLoad != nil {
		d.afterLoad()
	}
	result := make([]wkdb.BatchMsgResponse, 0, len(reqs))
	for _, r := range reqs {
		result = append(result, wkdb.BatchMsgResponse{ChannelId: r.ChannelId, ChannelType: r.ChannelType, Messages: []wkdb.Message{{RecvPacket: wkproto.RecvPacket{ChannelID: r.ChannelId, ChannelType: r.ChannelType, MessageSeq: 18, Payload: []byte("hello")}}}})
	}
	return result, nil
}
func (*recentReadDB) GetConversation(uid, id string, typ uint8) (wkdb.Conversation, error) {
	return wkdb.Conversation{Uid: uid, ChannelId: id, ChannelType: typ}, nil
}

func TestRecentReadServingRoleAndHTTPFailure(t *testing.T) {
	for _, mode := range []string{"healthy", "wrong leader", "change during read", "route error"} {
		t.Run(mode, func(t *testing.T) {
			changed := false
			c := &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
				if mode == "route error" {
					return wkdb.EmptyChannelClusterConfig, errors.New("not slot leader: private storage details")
				}
				leader := uint64(1)
				if mode == "wrong leader" {
					leader = 2
				}
				return readCfg(id, typ, leader), nil
			}, validate: func(context.Context, wkdb.ChannelClusterConfig) error {
				if changed {
					return errors.New("role changed")
				}
				return nil
			}}
			reader := setupRecentRead(t, c)
			db := &recentReadDB{afterLoad: func() {
				if mode == "change during read" {
					changed = true
				}
			}}
			service.Store = store.New(store.NewOptions(store.WithDB(db)))
			server := &Server{requset: reader}
			router := wkhttp.New()
			newConversation(server).route(router)
			paths := []string{"/conversation/syncMessages", "/conversation/syncMessages/v2", "/conversation/syncByChannels"}
			// The direct leaf must reject a request routed to the wrong node. For the
			// orchestrator that node is intentionally absent, so retry must also fail.
			for _, path := range paths {
				changed = false
				body := fmt.Sprintf(`{"uid":"alice","channels":[{"channel_id":"g","channel_type":2}],"msg_count":10,"budget":%d}`, int64(time.Second))
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if mode == "healthy" {
					require.Equal(t, 200, w.Code, w.Body.String())
					require.Contains(t, w.Body.String(), "18")
				} else {
					require.Equal(t, 503, w.Code, w.Body.String())
					require.NotContains(t, w.Body.String(), "private storage")
				}
			}
		})
	}
}

func TestRecentReadPublicEndpointRoutesAndKeepsEmptyChannels(t *testing.T) {
	peer := successfulRecentPeer(t)
	reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		if id == "unused" {
			return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
		}
		return readCfg(id, typ, 2), nil
	}})
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	req := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages", strings.NewReader(`{"uid":"alice","channels":[{"channel_id":"remote","channel_type":2},{"channel_id":"unused","channel_type":2},{"channel_id":"remote","channel_type":2}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var result []*channelRecentMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result, 2)
	byID := map[string]*channelRecentMessage{}
	for _, ch := range result {
		byID[ch.ChannelId] = ch
	}
	require.Empty(t, byID["unused"].Messages)
	require.Equal(t, uint64(18), byID["remote"].Messages[0].MessageSeq)
}

func TestRecentReadInvalidChannelsAreBadRequests(t *testing.T) {
	reader := setupRecentRead(t, &recentReadCluster{load: func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error) {
		t.Fatal("invalid request reached routing")
		return wkdb.EmptyChannelClusterConfig, nil
	}})
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	for _, path := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2", "/conversation/syncByChannels"} {
		for _, ch := range []string{`{"channel_id":"g","channel_type":0}`, `{"channel_id":"","channel_type":2}`} {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"uid":"alice","channels":[`+ch+`],"budget_ms":500}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		}
	}
}

func TestRecentReadPacesRetriesUntilElectionCompletes(t *testing.T) {
	peer := successfulRecentPeer(t)
	start := time.Now()
	calls := 0
	reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{2: {Id: 2, ApiServerAddr: peer.URL}}, load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		calls++
		cfg := readCfg(id, typ, 2)
		if time.Since(start) < 160*time.Millisecond {
			cfg.Term = 0
		}
		return cfg, nil
	}})
	result, err := reader.getRecentMessagesForCluster(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2}}, true)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.GreaterOrEqual(t, calls, 4)
	require.LessOrEqual(t, calls, 6)
	require.GreaterOrEqual(t, time.Since(start), 160*time.Millisecond)
}

func TestRecentReadRejectsStorageOmission(t *testing.T) {
	reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		return readCfg(id, typ, 1), nil
	}})
	service.Store = store.New(store.NewOptions(store.WithDB(&recentReadDB{partial: true})))
	result, err := reader.localRecentMessages(context.Background(), "alice", 15, []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2}}, true)
	require.ErrorContains(t, err, "incomplete storage")
	require.Nil(t, result)
}

func TestRecentReadDeduplicatesCursorRanges(t *testing.T) {
	for _, last := range []bool{false, true} {
		reqs := []*channelRecentMessageReq{{ChannelId: "g", ChannelType: 2, LastMsgSeq: 10}, {ChannelId: "g", ChannelType: 2, LastMsgSeq: 20}}
		got, err := normalizeRecentChannels(reqs, last)
		require.NoError(t, err)
		require.Len(t, got, 1)
		if last {
			require.Equal(t, uint64(20), got[0].LastMsgSeq)
		} else {
			require.Equal(t, uint64(10), got[0].LastMsgSeq)
		}
		require.Equal(t, uint64(10), reqs[0].LastMsgSeq, "normalization must not mutate callers")
	}
}
