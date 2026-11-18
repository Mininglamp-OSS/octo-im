package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestXIN2857_YP108_LegacyPeer503PostForwardHydrationReachable(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.ManagerToken = "yp108-token"
	// Set ReqTimeout shorter than the 50ms inter-attempt sleep so round 1 MUST succeed via the post-503 fallback
	// rather than sleeping into round 2!
	opts.Cluster.ReqTimeout = 35 * time.Millisecond
	opts.Cluster.RecentReadMaxTimeout = 35 * time.Millisecond
	options.G = opts

	var syncCalls atomic.Int32
	var channelsCalls atomic.Int32
	peerTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("token") != opts.ManagerToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/conversation/sync":
			syncCalls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"msg":"retry required","status":503}`))
		case "/conversation/channels":
			channelsCalls.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"channel_id": "g_yp108", "channel_type": int(wkproto.ChannelTypeGroup)},
			})
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer peerTS.Close()

	var probeCalls atomic.Int32
	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	cl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:            1002,
			ClusterAddr:   "127.0.0.1:11112",
			ApiServerAddr: peerTS.URL,
			Online:        true,
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			// Pre-forward probe (#1) fails transiently before /conversation/sync is sent;
			// post-503 probe (#2) succeeds and identifies peer 1002 as BaseV1 (21cd8f88).
			if syncCalls.Load() == 0 {
				probeCalls.Add(1)
				return nil, errors.New("initial probe timeout")
			}
			if path == "/wk/ingress/allowSend" || path == forward.CapabilityPath {
				probeCalls.Add(1)
				return &proto.Response{Status: proto.StatusOK, Body: nil}, nil
			}
			return &proto.Response{Status: proto.StatusNotFound}, nil
		},
	}
	service.Cluster = cl

	srv := &Server{}
	apiSrv := &apiServer{s: srv}
	srv.apiServer = apiSrv
	convAPI := newConversation(srv)

	w := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(w)
	bodyBytes := []byte(`{"uid":"u_yp108","msg_count":5}`)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/conversation/sync", bytes.NewReader(bodyBytes))
	wkCtx := &wkhttp.Context{Context: ginCtx}

	convs, hydratedFromLegacyPeer, handleLocal := convAPI.forwardOrHydrateConversationSync(wkCtx, "u_yp108", 5, bodyBytes)
	require.True(t, hydratedFromLegacyPeer, "Y-P1-08: post-503 legacy peer hydration branch must be reachable on first attempt")
	require.True(t, handleLocal)
	require.Len(t, convs, 1)
	require.Equal(t, "g_yp108", convs[0].ChannelId)
	require.Equal(t, int32(1), syncCalls.Load())
	require.Equal(t, int32(1), channelsCalls.Load())
	_ = wkdb.Conversation{}
}

func TestXIN2857_YP109_PersonChannelParticipantBindingAndLegacyCanonicalSupport(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerUID = "____manager"
	options.G = opts

	var leaderLookups atomic.Int32
	var lastObservedChannelID string
	node2Router := wkhttp.New()
	node2Router.POST("/channel/messagesync", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, emptySyncMessageResp)
	})
	node2Router.GET("/channel/last_message", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, map[string]any{"message_seq": 1})
	})
	node2Router.GET("/channel/max_message_seq", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, map[string]any{"message_seq": 1})
	})
	node2TS := httptest.NewServer(node2Router.GetGinRoute())
	defer node2TS.Close()

	node2 := &clustertypes.Node{Id: 2, ApiServerAddr: node2TS.URL, Online: true}
	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{2: node2},
		leaderFn: func(channelID string, channelType uint8) (*clustertypes.Node, error) {
			leaderLookups.Add(1)
			lastObservedChannelID = channelID
			return node2, nil
		},
	}

	router := wkhttp.New()
	mAPI := newMessage(&Server{})
	mAPI.route(router)

	// 1. Valid canonical person channel ID where login_uid IS a participant ("u_alice@u_bob", login_uid="u_alice") MUST succeed without double-prefixing:
	w1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/channel/messagesync", bytes.NewReader([]byte(`{"login_uid":"u_alice","channel_id":"u_alice@u_bob","channel_type":1,"limit":1}`)))
	req1.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w1, req1)
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, "u_alice@u_bob", lastObservedChannelID)
	require.Equal(t, int32(1), leaderLookups.Load())

	// 2. Non-participant login_uid ("u_charlie") with canonical person channel_id ("u_alice@u_bob") MUST be rejected before any cluster lookup:
	leaderLookups.Store(0)
	for _, tc := range []struct {
		method string
		url    string
		body   []byte
	}{
		{
			method: http.MethodPost,
			url:    "/channel/messagesync",
			body:   []byte(`{"login_uid":"u_charlie","channel_id":"u_alice@u_bob","channel_type":1,"limit":1}`),
		},
		{
			method: http.MethodGet,
			url:    "/channel/last_message?login_uid=u_charlie&channel_id=u_alice@u_bob&channel_type=1",
		},
		{
			method: http.MethodGet,
			url:    "/channel/last_message?channel_id=u_alice@u_bob&channel_type=1",
		},
		{
			method: http.MethodGet,
			url:    "/channel/max_message_seq?login_uid=u_charlie&channel_id=u_alice@u_bob&channel_type=1",
		},
		{
			method: http.MethodPost,
			url:    "/channel/messagesync",
			body:   []byte(`{"login_uid":"u_alice","channel_id":"u_alice@u_bob@u_extra","channel_type":1,"limit":1}`),
		},
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(tc.method, tc.url, bytes.NewReader(tc.body))
		if len(tc.body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, "Y-P1-09: unbound or malformed person channel read (%s %s) must be rejected with 400", tc.method, tc.url)
		require.Zero(t, leaderLookups.Load(), "Y-P1-09: rejected person channel read must not invoke LeaderOfChannel")
	}
}

type yp101Cluster struct {
	*r3ClusterWithRPC
	extraNodes []*clustertypes.Node
}

func (c *yp101Cluster) Nodes() []*clustertypes.Node {
	return append(c.r3ClusterWithRPC.Nodes(), c.extraNodes...)
}

func TestXIN2857_YP101_LegacyPeerNoManagerPrivilegeAndAmbiguousIPFailClosed(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.ManagerToken = "yp101-secret-token"
	opts.ManagerUID = "____manager"
	options.G = opts

	node1002 := &clustertypes.Node{
		Id:            1002,
		ClusterAddr:   "10.240.0.12:11110",
		ApiServerAddr: "http://10.240.0.12:5001",
		Online:        true,
	}

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	rpcCluster := &yp101Cluster{
		r3ClusterWithRPC: &r3ClusterWithRPC{
			realChannelClusterAdapter: adapter,
			slotLeaderNode:            node1002,
			requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
				if toNodeId == 1002 && (path == "/wk/ingress/allowSend" || path == forward.CapabilityPath) {
					return &proto.Response{Status: proto.StatusOK, Body: nil}, nil
				}
				if toNodeId == 1003 && (path == "/wk/ingress/allowSend" || path == forward.CapabilityPath) {
					return &proto.Response{Status: proto.StatusOK, Body: forward.BuildCapabilityBody()}, nil
				}
				return &proto.Response{Status: proto.StatusError}, errors.New("unknown node")
			},
		},
	}
	service.Cluster = rpcCluster

	apiSrv := &apiServer{s: &Server{}}
	router := wkhttp.New()
	router.Use(apiSrv.jwtAndTokenAuthSetMiddleware())
	router.Use(apiSrv.managerTokenMiddleware())
	var observedUsername string
	var observedLegacyFlag bool
	router.POST("/conversation/syncMessages/v2", func(c *wkhttp.Context) {
		observedUsername = c.Username()
		observedLegacyFlag = c.GetBool("verified_legacy_peer_internal")
		c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	// 1. Unique verified BaseV1 peer IP (10.240.0.12) is admitted on allowlisted route WITHOUT ManagerUID elevation:
	req1 := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	req1.RemoteAddr = "10.240.0.12:54321"
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	require.Equal(t, http.StatusOK, w1.Code)
	require.Empty(t, observedUsername, "Y-P1-01: legacy headerless compatibility flow must NOT elevate username to ManagerUID")
	require.True(t, observedLegacyFlag, "Y-P1-01: legacy headerless compatibility flow must set narrowly scoped verified_legacy_peer_internal flag")

	// 2. Ambiguous/shared IP with local node (e.g., localNode ServerAddr is also 10.240.0.12) MUST fail closed (401):
	opts.Cluster.ServerAddr = "10.240.0.12:11110"
	reqCoLocated := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqCoLocated.RemoteAddr = "10.240.0.12:54321"
	wCoLocated := httptest.NewRecorder()
	router.ServeHTTP(wCoLocated, reqCoLocated)
	require.Equal(t, http.StatusUnauthorized, wCoLocated.Code, "Y-P1-01: shared IP with local node must fail closed (401)")
	opts.Cluster.ServerAddr = ""

	// 3. Ambiguous/shared IP across multiple peers (e.g., peer 1002 BaseV1 and peer 1003 PartAV2 share 10.240.0.12 via NAT/host) MUST fail closed (401):
	rpcCluster.extraNodes = []*clustertypes.Node{
		{Id: 1003, ClusterAddr: "10.240.0.12:11113", ApiServerAddr: "http://10.240.0.12:5003", Online: true},
	}
	reqSharedPeerIP := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqSharedPeerIP.RemoteAddr = "10.240.0.12:54321"
	wSharedPeerIP := httptest.NewRecorder()
	router.ServeHTTP(wSharedPeerIP, reqSharedPeerIP)
	require.Equal(t, http.StatusUnauthorized, wSharedPeerIP.Code, "Y-P1-01: shared IP across multiple peers must fail closed (401)")
	rpcCluster.extraNodes = nil

	// 4. Explicit configuration gate AllowLegacyHeaderlessPeerHTTP=false MUST fail closed (401):
	opts.Cluster.AllowLegacyHeaderlessPeerHTTP = false
	reqDisabled := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqDisabled.RemoteAddr = "10.240.0.12:54321"
	wDisabled := httptest.NewRecorder()
	router.ServeHTTP(wDisabled, reqDisabled)
	require.Equal(t, http.StatusUnauthorized, wDisabled.Code, "Y-P1-01: AllowLegacyHeaderlessPeerHTTP=false must fail closed (401)")
	opts.Cluster.AllowLegacyHeaderlessPeerHTTP = true
}

func TestXIN2857_YADD01_ConfigurationDocMatchesImplementedSlotDigestLimit(t *testing.T) {
	require.Equal(t, 10000, slot.MaxSlotLogsRangeCount)
	docBytes, err := os.ReadFile("../../docs/geely-im-p1-p3-configuration.md")
	require.NoError(t, err)
	doc := string(docBytes)
	require.Contains(t, doc, "slot.MaxSlotLogsRangeCount` (`10,000`)", "Y-ADD-01: configuration doc must state the implemented 10,000 limit")
	require.False(t, strings.Contains(doc, "slot.MaxSlotLogsRangeCount` (`100,000`)"), "Y-ADD-01: configuration doc must not claim 100,000 for MaxSlotLogsRangeCount")

	openAPIBytes, err := os.ReadFile("../../docs/api/openapi.json")
	require.NoError(t, err)
	var openAPIDoc map[string]any
	require.NoError(t, json.Unmarshal(openAPIBytes, &openAPIDoc), "openapi.json must be valid JSON")
	paths, _ := openAPIDoc["paths"].(map[string]any)
	require.Contains(t, paths, "/healthz")
	require.Contains(t, paths, "/readyz")
	require.Contains(t, paths, "/cluster/slots/{id}/digest")
}



