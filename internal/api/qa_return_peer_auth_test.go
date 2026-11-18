package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chEvent "github.com/WuKongIM/WuKongIM/internal/channel/event"
	"github.com/WuKongIM/WuKongIM/internal/channel/handler"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	wkmanager "github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/auth"
	"github.com/WuKongIM/WuKongIM/pkg/auth/resource"
	clusterchannel "github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

type qaPeerCluster struct {
	icluster.ICluster
	nodes        map[uint64]*clustertypes.Node
	slotLeaderFn func(channelID string, channelType uint8) (*clustertypes.Node, error)
	readConfigFn func(ctx context.Context, channelID string, channelType uint8) (wkdb.ChannelClusterConfig, error)
	leaderFn     func(channelID string, channelType uint8) (*clustertypes.Node, error)
}

func (c *qaPeerCluster) NodeInfoById(nodeID uint64) *clustertypes.Node {
	if c.nodes == nil {
		return nil
	}
	return c.nodes[nodeID]
}

func (c *qaPeerCluster) Nodes() []*clustertypes.Node {
	res := make([]*clustertypes.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		res = append(res, n)
	}
	return res
}

func (c *qaPeerCluster) SlotLeaderOfChannel(channelID string, channelType uint8) (*clustertypes.Node, error) {
	if c.slotLeaderFn != nil {
		return c.slotLeaderFn(channelID, channelType)
	}
	return c.nodes[1], nil
}

func (c *qaPeerCluster) SlotLeaderNodeInfo(slotID uint32) *clustertypes.Node {
	return c.nodes[1]
}

func (c *qaPeerCluster) LoadChannelReadConfig(ctx context.Context, channelID string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	if c.readConfigFn != nil {
		return c.readConfigFn(ctx, channelID, channelType)
	}
	return wkdb.ChannelClusterConfig{
		ChannelId:       channelID,
		ChannelType:     channelType,
		ReplicaMaxCount: 2,
		Replicas:        []uint64{1, 2},
		LeaderId:        2,
		Term:            1,
		ConfVersion:     1,
	}, nil
}

func (c *qaPeerCluster) LeaderOfChannel(channelID string, channelType uint8) (*clustertypes.Node, error) {
	if c.leaderFn != nil {
		return c.leaderFn(channelID, channelType)
	}
	return c.nodes[1], nil
}

func (c *qaPeerCluster) LeaderOfChannelForRead(channelID string, channelType uint8) (*clustertypes.Node, error) {
	if c.leaderFn != nil {
		return c.leaderFn(channelID, channelType)
	}
	return c.nodes[1], nil
}

func TestQAReturn_InternalPeerTokenForwarding(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-qa-secret-manager-token"
	opts.ManagerUID = "____manager"
	options.G = opts

	// Build a peer HTTP server protected by the exact apiServer managerTokenMiddleware
	apiSrv := &apiServer{s: &Server{}}
	peerRouter := wkhttp.New()
	peerRouter.Use(apiSrv.jwtAndTokenAuthSetMiddleware())
	peerRouter.Use(apiSrv.managerTokenMiddleware())

	var syncV2Hits, onlineHits, sysAddHits, sysRemHits, tmpSubHits atomic.Int32
	peerRouter.POST("/conversation/syncMessages/v2", func(c *wkhttp.Context) {
		syncV2Hits.Add(1)
		c.JSON(http.StatusOK, recentReadResponse{
			Version: recentReadVersion,
			Channels: []*channelRecentMessage{
				{
					ChannelId:   "grp_0000",
					ChannelType: wkproto.ChannelTypeGroup,
					Messages:    types.MessageRespSlice{{MessageId: 101, MessageSeq: 1, FromUID: "u_sender_00"}},
				},
			},
		})
	})
	peerRouter.POST("/user/onlinestatus", func(c *wkhttp.Context) {
		onlineHits.Add(1)
		c.JSON(http.StatusOK, []*OnlinestatusResp{{UID: "u_0002", Online: 1, DeviceFlag: 1}})
	})
	peerRouter.POST("/user/systemuids_add_to_cache", func(c *wkhttp.Context) {
		sysAddHits.Add(1)
		c.ResponseOK()
	})
	peerRouter.POST("/user/systemuids_remove_from_cache", func(c *wkhttp.Context) {
		sysRemHits.Add(1)
		c.ResponseOK()
	})
	peerRouter.POST("/tmpchannel/subscriber_set", func(c *wkhttp.Context) {
		tmpSubHits.Add(1)
		c.ResponseOK()
	})

	peerTS := httptest.NewServer(peerRouter.GetGinRoute())
	defer peerTS.Close()

	node1 := &clustertypes.Node{Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true}
	node2 := &clustertypes.Node{Id: 2, ApiServerAddr: peerTS.URL, Online: true}
	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{1: node1, 2: node2},
		slotLeaderFn: func(channelID string, channelType uint8) (*clustertypes.Node, error) {
			return node2, nil
		},
	}

	// 1. Direct external call without token or with wrong token to peerTS must be rejected with 401
	for _, badToken := range []string{"", "wrong-token"} {
		req, err := http.NewRequest(http.MethodPost, peerTS.URL+"/conversation/syncMessages/v2", bytes.NewReader([]byte(`{}`)))
		require.NoError(t, err)
		if badToken != "" {
			req.Header.Set("token", badToken)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	}

	// 2. Internal peer call via getRecentMessagesForCluster -> requestSyncMessage attaches ManagerToken to trusted peer node2
	reqReader := newRequset(&Server{})
	msgs, err := reqReader.getRecentMessagesForCluster(context.Background(), "u_0001", 5, []*channelRecentMessageReq{
		{ChannelId: "grp_0000", ChannelType: wkproto.ChannelTypeGroup, LastMsgSeq: 0},
	}, true)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Messages, 1)
	require.Equal(t, int32(1), syncV2Hits.Load())

	// 3. Internal peer calls in user.go and message.go attach ManagerToken to trusted peer node2
	uAPI := newUser(&Server{})
	onlineRes, err := uAPI.requestOnlineStatus(2, []string{"u_0002"})
	require.NoError(t, err)
	require.Len(t, onlineRes, 1)
	require.Equal(t, int32(1), onlineHits.Load())

	require.NoError(t, uAPI.requestSystemUidsAddToCache(node2, []string{"sys_1"}))
	require.Equal(t, int32(1), sysAddHits.Load())

	require.NoError(t, uAPI.requestSystemUidsRemoveFromCache(node2, []string{"sys_1"}))
	require.Equal(t, int32(1), sysRemHits.Load())

	mAPI := newMessage(&Server{})
	require.NoError(t, mAPI.requestSetSubscribersForTmpChannel("tmp_ch_1", []string{"u_0002"}))
	require.Equal(t, int32(1), tmpSubHits.Load())

	// 4. Untrusted or mismatched peer node address MUST NOT receive ManagerToken
	untrustedTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("token"), "must never leak ManagerToken to untrusted URL")
		w.WriteHeader(http.StatusOK)
	}))
	defer untrustedTS.Close()

	spoofedNode := &clustertypes.Node{Id: 2, ApiServerAddr: untrustedTS.URL, Online: true}
	require.Empty(t, service.TrustedPeerNodeHeaders(spoofedNode, nil))
	unknownNode := &clustertypes.Node{Id: 999, ApiServerAddr: untrustedTS.URL, Online: true}
	require.Empty(t, service.TrustedPeerNodeHeaders(unknownNode, nil))
}

func signTestJWT(t *testing.T, secret, username string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString([]byte(secret))
	require.NoError(t, err)
	return s
}

func TestQAReturn_ManagerAuthContext5001And5300AndForwarding(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-qa-secret-manager-token"
	opts.ManagerUID = "____manager"
	opts.Jwt.Secret = "geely-qa-jwt-secret-key"
	opts.Auth = auth.AuthConfig{
		On:   true,
		Kind: auth.KindJWT,
		Users: []auth.UserConfig{
			{Username: "____manager", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "admin", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "guest", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionRead}}}},
		},
	}
	options.G = opts

	// Peer node2 (:5001) serving /cluster/slots/:id/migrate with HasPermissionWithContext
	apiSrv2 := &apiServer{s: &Server{}}
	peer5001 := wkhttp.New()
	peer5001.Use(apiSrv2.jwtAndTokenAuthSetMiddleware())
	peer5001.Use(apiSrv2.managerTokenMiddleware())
	peer5001.POST("/cluster/slots/:id/migrate", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.Slot.Migrate, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ResponseOK()
	})
	peer5001TS := httptest.NewServer(peer5001.GetGinRoute())
	defer peer5001TS.Close()

	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{
			1: {Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true},
			2: {Id: 2, ApiServerAddr: peer5001TS.URL, Online: true},
		},
	}

	// Entry node1 (:5300 managerServer and :5001 apiServer) forwarding to peer5001TS via ForwardWithBody
	mgrSrv1 := &managerServer{s: &Server{}}
	entry5300 := wkhttp.New()
	entry5300.Use(mgrSrv1.jwtAndTokenAuthMiddleware())
	entry5300.POST("/cluster/slots/:id/migrate", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.Slot.Migrate, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ForwardWithBody(peer5001TS.URL+c.Request.URL.Path, []byte(`{"migrate_from":1001,"migrate_to":1003}`))
	})
	entry5300TS := httptest.NewServer(entry5300.GetGinRoute())
	defer entry5300TS.Close()

	// Case 1: Valid ManagerToken on :5001 directly -> 200 OK (sets username=ManagerUID)
	req, err := http.NewRequest(http.MethodPost, peer5001TS.URL+"/cluster/slots/0/migrate", bytes.NewReader([]byte(`{"migrate_from":1001,"migrate_to":1003}`)))
	require.NoError(t, err)
	req.Header.Set("token", opts.ManagerToken)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Case 2: Valid ManagerToken on :5300 forwarded to peer :5001 -> 200 OK
	req, err = http.NewRequest(http.MethodPost, entry5300TS.URL+"/cluster/slots/0/migrate", bytes.NewReader([]byte(`{"migrate_from":1001,"migrate_to":1003}`)))
	require.NoError(t, err)
	req.Header.Set("token", opts.ManagerToken)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Case 3: Valid admin JWT on :5300 (without token header) forwarded to peer :5001 -> 200 OK
	adminJWT := signTestJWT(t, opts.Jwt.Secret, "admin")
	req, err = http.NewRequest(http.MethodPost, entry5300TS.URL+"/cluster/slots/0/migrate", bytes.NewReader([]byte(`{"migrate_from":1001,"migrate_to":1003}`)))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+adminJWT)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Case 4: Low-privilege guest JWT ([*:r]) on :5001 or :5300 MUST NOT escalate to ManagerUID even when token header is present
	guestJWT := signTestJWT(t, opts.Jwt.Secret, "guest")
	for _, targetURL := range []string{peer5001TS.URL, entry5300TS.URL} {
		req, err = http.NewRequest(http.MethodPost, targetURL+"/cluster/slots/0/migrate", bytes.NewReader([]byte(`{"migrate_from":1001,"migrate_to":1003}`)))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+guestJWT)
		req.Header.Set("token", opts.ManagerToken)
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		var body map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Equal(t, float64(http.StatusForbidden), body["status"])
	}

	// Case 5: Missing or invalid token on :5001 and :5300 -> 401 Unauthorized
	for _, targetURL := range []string{peer5001TS.URL, entry5300TS.URL} {
		req, err = http.NewRequest(http.MethodPost, targetURL+"/cluster/slots/0/migrate", bytes.NewReader([]byte(`{}`)))
		require.NoError(t, err)
		req.Header.Set("token", "wrong-token")
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	}
}

func TestQAReturn_PersonChannelNoDoublePrefixAndRollingUpgradeForwardRetry(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-qa-secret-manager-token"
	options.G = opts

	// 1. Verify /channel/messagesync with already-prefixed person channel ("p_0000@u_sender_00") does NOT double-prefix
	var observedChannelID string
	node2Router := wkhttp.New()
	node2Router.POST("/channel/messagesync", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, emptySyncMessageResp)
	})
	node2Router.POST("/conversation/sync", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, []*syncUserConversationResp{})
	})
	node2TS := httptest.NewServer(node2Router.GetGinRoute())
	defer node2TS.Close()

	var lookupCalls atomic.Int32
	node1 := &clustertypes.Node{Id: 1, ApiServerAddr: "http://127.0.0.1:1", Online: true} // closed port simulating stopped node
	node2 := &clustertypes.Node{Id: 2, ApiServerAddr: node2TS.URL, Online: true}

	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{1: node1, 2: node2},
		leaderFn: func(channelID string, channelType uint8) (*clustertypes.Node, error) {
			observedChannelID = channelID
			return node2, nil
		},
		slotLeaderFn: func(channelID string, channelType uint8) (*clustertypes.Node, error) {
			if lookupCalls.Add(1) == 1 {
				// First lookup returns stopped peer with closed port (simulating rolling upgrade stop)
				return &clustertypes.Node{Id: 3, ApiServerAddr: "http://127.0.0.1:1", Online: true}, nil
			}
			// Second lookup after failover returns healthy node2
			return node2, nil
		},
	}

	router := wkhttp.New()
	mAPI := newMessage(&Server{})
	mAPI.route(router)
	convAPI := newConversation(&Server{})
	convAPI.route(router)

	// Send /channel/messagesync with "p_0000@u_sender_00"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/channel/messagesync", bytes.NewReader([]byte(`{"login_uid":"u_sender_00","channel_id":"p_0000@u_sender_00","channel_type":1,"limit":1}`)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "p_0000@u_sender_00", observedChannelID, "must not double-prefix person channel_id that already contains '@'")

	// 2. Verify /conversation/sync retries when initial slot leader refuses connection during rolling restart
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/conversation/sync", bytes.NewReader([]byte(`{"uid":"u_0001","msg_count":5}`)))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w2, req2)
	require.Equal(t, http.StatusOK, w2.Code)
	require.GreaterOrEqual(t, lookupCalls.Load(), int32(2))
}

type panickingPeerCluster struct {
	icluster.ICluster
}

func TestRound2_BFR2_1_FailClosedPeerTrustAndRedirectRefusal(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-r2-super-secret-token"
	options.G = opts

	// 1. Fail-closed check when Cluster panics on NodeInfoById and Nodes (nil embedded interface)
	service.Cluster = &panickingPeerCluster{}
	targetNode := &clustertypes.Node{Id: 2, ApiServerAddr: "http://127.0.0.1:25001", Online: true}
	require.False(t, service.IsTrustedClusterPeer(targetNode), "IsTrustedClusterPeer must fail closed when Cluster methods panic")
	require.False(t, service.IsTrustedClusterPeerURL("http://127.0.0.1:25001/cluster/nodes"), "IsTrustedClusterPeerURL must fail closed when Cluster methods panic")
	require.Empty(t, service.TrustedPeerNodeHeaders(targetNode, nil), "TrustedPeerNodeHeaders must not attach ManagerToken when Cluster panics")
	require.Empty(t, service.TrustedPeerURLHeaders("http://127.0.0.1:25001/cluster/nodes", nil), "TrustedPeerURLHeaders must not attach ManagerToken when Cluster panics")

	// 2. Redirect refusal check: if a trusted peer responds with 302/307 to an external server,
	// SendPeerRequest and ForwardWithBody must NOT follow the redirect or leak ManagerToken.
	var redirectTargetHit atomic.Int32
	var leakedToken atomic.Value
	attackerTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectTargetHit.Add(1)
		leakedToken.Store(r.Header.Get("token"))
		w.WriteHeader(http.StatusOK)
	}))
	defer attackerTS.Close()

	peerRedirectTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, attackerTS.URL+"/steal", http.StatusFound)
	}))
	defer peerRedirectTS.Close()

	peerNode := &clustertypes.Node{Id: 2, ApiServerAddr: peerRedirectTS.URL, Online: true}
	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{
			1: {Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true},
			2: peerNode,
		},
	}

	router := wkhttp.New()
	router.POST("/forward-test", func(c *wkhttp.Context) {
		c.ForwardWithBody(peerRedirectTS.URL+"/internal", []byte(`{}`))
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/forward-test", bytes.NewReader([]byte(`{}`)))
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusFound, w.Code, "peer client must return 302 directly via ErrUseLastResponse without following redirect")
	require.Equal(t, int32(0), redirectTargetHit.Load(), "redirect target must never be contacted")
	require.Nil(t, leakedToken.Load(), "ManagerToken must never leak to redirect target")
}

func TestRound2_BFR2_2_Strict5001BoundaryInvalidJWTEscalationAndMutatingRBAC(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-r2-secret-manager-token"
	opts.ManagerUID = "____manager"
	opts.Jwt.Secret = "geely-r2-jwt-secret"
	opts.Auth = auth.AuthConfig{
		On:   true,
		Kind: auth.KindJWT,
		Users: []auth.UserConfig{
			{Username: "____manager", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "admin", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "guest", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionRead}}}},
		},
	}
	options.G = opts

	apiSrv2 := &apiServer{s: &Server{}}
	peer5001 := wkhttp.New()
	peer5001.Use(apiSrv2.jwtAndTokenAuthSetMiddleware())
	peer5001.Use(apiSrv2.managerTokenMiddleware())
	conn2 := newConnApi(&Server{})
	conn2.route(peer5001)
	tag2 := newTag(&Server{})
	tag2.route(peer5001)
	peer5001.GET("/plugins", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, []string{"plugin-a"})
	})
	peer5001.POST("/cluster/channels/:channel_id/:channel_type/migrate", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.ClusterChannel.Migrate, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ResponseOK()
	})
	peer5001TS := httptest.NewServer(peer5001.GetGinRoute())
	defer peer5001TS.Close()

	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{
			1: {Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true},
			2: {Id: 2, ApiServerAddr: peer5001TS.URL, Online: true},
		},
	}

	mgrSrv1 := &managerServer{s: &Server{}}
	entry5300 := wkhttp.New()
	entry5300.Use(mgrSrv1.jwtAndTokenAuthMiddleware())
	conn1 := newConnApi(&Server{})
	conn1.route(entry5300)
	tag1 := newTag(&Server{})
	tag1.route(entry5300)
	entry5300.GET("/plugins", func(c *wkhttp.Context) {
		c.Forward(peer5001TS.URL + c.Request.URL.Path)
	})
	entry5300.POST("/cluster/channels/:channel_id/:channel_type/migrate", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.ClusterChannel.Migrate, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ForwardWithBody(peer5001TS.URL+c.Request.URL.Path, []byte(`{"migrate_from":1,"migrate_to":2}`))
	})
	entry5300TS := httptest.NewServer(entry5300.GetGinRoute())
	defer entry5300TS.Close()

	adminJWT := signTestJWT(t, opts.Jwt.Secret, "admin")
	guestJWT := signTestJWT(t, opts.Jwt.Secret, "guest")
	expiredClaim := jwt.MapClaims{
		"username": "guest",
		"exp":      time.Now().Add(-time.Hour).Unix(),
	}
	expiredTok := jwt.NewWithClaims(jwt.SigningMethodHS256, expiredClaim)
	expiredJWT, err := expiredTok.SignedString([]byte(opts.Jwt.Secret))
	require.NoError(t, err)

	// (a) Bare JWT without ManagerToken on :5001 MUST be rejected with 401 Unauthorized
	for _, path := range []string{"/plugins", "/cluster/channels/g1/2/migrate"} {
		method := http.MethodGet
		if path != "/plugins" {
			method = http.MethodPost
		}
		req, err := http.NewRequest(method, peer5001TS.URL+path, bytes.NewReader([]byte(`{}`)))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+adminJWT)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "bare JWT without ManagerToken on :5001 must return 401 for %s", path)
	}

	// (b) Expired/invalid JWT + valid gateway ManagerToken on :5001 MUST be rejected with 401 (never escalate to ManagerUID)
	for _, badAuth := range []string{"Bearer " + expiredJWT, "Bearer malformed.jwt.token", "InvalidPrefix " + adminJWT} {
		req, err := http.NewRequest(http.MethodPost, peer5001TS.URL+"/cluster/channels/g1/2/migrate", bytes.NewReader([]byte(`{"migrate_from":1,"migrate_to":2}`)))
		require.NoError(t, err)
		req.Header.Set("Authorization", badAuth)
		req.Header.Set("token", opts.ManagerToken)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "invalid/expired JWT + ManagerToken on :5001 must return 401")
	}

	// (c) Guest JWT ([*:r]) on mutating routes (:5300 or :5001 with token) MUST be denied with 403 Forbidden
	for _, mutPath := range []string{"/cluster/channels/g1/2/migrate", "/conn/remove", "/conn/kick", "/tag/remove"} {
		req, err := http.NewRequest(http.MethodPost, entry5300TS.URL+mutPath, bytes.NewReader([]byte(`{"op_node_id":2,"node_id":2,"conn_id":1,"tag_key":"t1"}`)))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+guestJWT)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		var body map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Equal(t, float64(http.StatusForbidden), body["status"], "guest JWT must be denied write access on %s", mutPath)
	}

	// (d) Valid admin JWT on :5300 forwarding to peer :5001 on /plugins, /conn/remove, /tag/remove, /cluster/channels/.../migrate -> 200 OK
	reqGet, err := http.NewRequest(http.MethodGet, entry5300TS.URL+"/plugins?node_id=2", nil)
	require.NoError(t, err)
	reqGet.Header.Set("Authorization", "Bearer "+adminJWT)
	respGet, err := http.DefaultClient.Do(reqGet)
	require.NoError(t, err)
	_ = respGet.Body.Close()
	require.Equal(t, http.StatusOK, respGet.StatusCode)

	reqMig, err := http.NewRequest(http.MethodPost, entry5300TS.URL+"/cluster/channels/g1/2/migrate", bytes.NewReader([]byte(`{"migrate_from":1,"migrate_to":2}`)))
	require.NoError(t, err)
	reqMig.Header.Set("Authorization", "Bearer "+adminJWT)
	respMig, err := http.DefaultClient.Do(reqMig)
	require.NoError(t, err)
	_ = respMig.Body.Close()
	require.Equal(t, http.StatusOK, respMig.StatusCode)
}

func TestRound2_BFR2_3_SyncByChannelsForwardingOnRollingRestart(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-r2-secret-manager-token"
	options.G = opts

	var peerReceivedToken string
	node2Router := wkhttp.New()
	node2Router.POST("/conversation/syncByChannels", func(c *wkhttp.Context) {
		peerReceivedToken = c.GetHeader("token")
		c.JSON(http.StatusOK, []*syncUserConversationResp{})
	})
	node2TS := httptest.NewServer(node2Router.GetGinRoute())
	defer node2TS.Close()

	var slotLookups atomic.Int32
	node2 := &clustertypes.Node{Id: 2, ApiServerAddr: node2TS.URL, Online: true}
	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{
			1: {Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true},
			2: node2,
		},
		slotLeaderFn: func(channelID string, channelType uint8) (*clustertypes.Node, error) {
			if slotLookups.Add(1) == 1 {
				return &clustertypes.Node{Id: 3, ApiServerAddr: "http://127.0.0.1:1", Online: true}, nil
			}
			return node2, nil
		},
	}

	router := wkhttp.New()
	convAPI := newConversation(&Server{})
	convAPI.route(router)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/conversation/syncByChannels", bytes.NewReader([]byte(`{"uid":"u_0001","channels":[{"channel_id":"g_0001","channel_type":2}],"msg_count":5}`)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.GreaterOrEqual(t, slotLookups.Load(), int32(2))
	require.Equal(t, opts.ManagerToken, peerReceivedToken)
}

func TestRound3_BFR3_1_UntrustedPeerHeaderStripAndEmptyManagerToken5001Compatibility(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.ManagerToken = "geely-r3-secret-manager-token"
	opts.ManagerUID = "____manager"
	opts.Jwt.Secret = "geely-r3-jwt-secret"
	opts.Auth = auth.AuthConfig{
		On:   true,
		Kind: auth.KindJWT,
		Users: []auth.UserConfig{
			{Username: "____manager", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "admin", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionAll}}}},
			{Username: "guest", Permissions: []auth.PermissionConfig{{Resource: resource.All, Actions: []auth.Action{auth.ActionRead}}}},
		},
	}
	options.G = opts

	trustedNode := &clustertypes.Node{Id: 2, ApiServerAddr: "http://127.0.0.1:25001", Online: true}
	untrustedNode := &clustertypes.Node{Id: 99, ApiServerAddr: "http://127.0.0.1:29999", Online: true}
	service.Cluster = &qaPeerCluster{
		nodes: map[uint64]*clustertypes.Node{
			1: {Id: 1, ApiServerAddr: "http://127.0.0.1:15001", Online: true},
			2: trustedNode,
		},
	}

	// 1. Untrusted node/URL MUST strip inbound Token, token, Authorization, authorization headers.
	inboundBase := map[string]string{
		"Token":         "inbound-caller-token",
		"Authorization": "Bearer inbound-caller-jwt",
		"Content-Type":  "application/json",
	}
	untrustedNodeHdrs := service.TrustedPeerNodeHeaders(untrustedNode, inboundBase)
	require.Empty(t, untrustedNodeHdrs["Token"])
	require.Empty(t, untrustedNodeHdrs["token"])
	require.Empty(t, untrustedNodeHdrs["Authorization"])
	require.Equal(t, "application/json", untrustedNodeHdrs["Content-Type"])

	untrustedURLHdrs := service.TrustedPeerURLHeaders("http://127.0.0.1:29999/conversation/sync", inboundBase)
	require.Empty(t, untrustedURLHdrs["Token"])
	require.Empty(t, untrustedURLHdrs["token"])
	require.Empty(t, untrustedURLHdrs["Authorization"])
	require.Equal(t, "application/json", untrustedURLHdrs["Content-Type"])

	// 2. Trusted node/URL normalizes canonical "Token" into "token" without duplicate keys.
	trustedURLHdrs := service.TrustedPeerURLHeaders("http://127.0.0.1:25001/conversation/sync", inboundBase)
	_, hasCanonicalToken := trustedURLHdrs["Token"]
	require.False(t, hasCanonicalToken, "canonical Token key must be normalized to lowercase token")
	require.Equal(t, "inbound-caller-token", trustedURLHdrs["token"])
	require.Equal(t, "Bearer inbound-caller-jwt", trustedURLHdrs["Authorization"])

	// 3. When ManagerToken == "", :5001 mutating endpoints must succeed for internal requests without
	// Authorization header, while still rejecting invalid JWT with 401 and guest JWT with 403.
	opts.ManagerToken = ""
	apiSrv := &apiServer{s: &Server{}}
	peer5001 := wkhttp.New()
	peer5001.Use(apiSrv.jwtAndTokenAuthSetMiddleware())
	peer5001.Use(apiSrv.managerTokenMiddleware())
	peer5001.POST("/cluster/channels/:channel_id/:channel_type/migrate", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.ClusterChannel.Migrate, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ResponseOK()
	})
	peer5001.POST("/plugins/:plugin/*path", func(c *wkhttp.Context) {
		if !opts.Auth.HasPermissionWithContext(c, resource.All, auth.ActionWrite) {
			c.ResponseErrorWithStatus(http.StatusForbidden, errInvalidRecentChannel)
			return
		}
		c.ResponseOK()
	})

	// (a) No Authorization header + ManagerToken == "" -> 200 OK (username defaults to ManagerUID)
	for _, p := range []string{"/cluster/channels/g1/2/migrate", "/plugins/p1/invoke"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, p, bytes.NewReader([]byte(`{}`)))
		peer5001.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, "expected 200 OK on %s when ManagerToken is empty", p)
	}

	// (b) Explicit guest JWT ([*:r]) + ManagerToken == "" -> 403 Forbidden
	guestJWT := signTestJWT(t, opts.Jwt.Secret, "guest")
	wGuest := httptest.NewRecorder()
	reqGuest := httptest.NewRequest(http.MethodPost, "/cluster/channels/g1/2/migrate", bytes.NewReader([]byte(`{}`)))
	reqGuest.Header.Set("Authorization", "Bearer "+guestJWT)
	peer5001.ServeHTTP(wGuest, reqGuest)
	require.Equal(t, http.StatusBadRequest, wGuest.Code)

	// (c) Explicit invalid JWT + ManagerToken == "" -> 401 Unauthorized
	wBad := httptest.NewRecorder()
	reqBad := httptest.NewRequest(http.MethodPost, "/cluster/channels/g1/2/migrate", bytes.NewReader([]byte(`{}`)))
	reqBad.Header.Set("Authorization", "Bearer malformed.jwt.token")
	peer5001.ServeHTTP(wBad, reqBad)
	require.Equal(t, http.StatusUnauthorized, wBad.Code)
}

type r3MockChannel struct {
	icluster.Channel
	proposeFn func(ctx context.Context, channelId string, channelType uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error)
}

func (m *r3MockChannel) ProposeBatchUntilAppliedTimeout(ctx context.Context, channelId string, channelType uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	if m.proposeFn != nil {
		return m.proposeFn(ctx, channelId, channelType, reqs)
	}
	return nil, nil
}

func TestRound3_BFR3_3_RemoteSlotPermissionWireDuplicateAndForwardGate(t *testing.T) {
	oldOpts, oldCluster, oldStore, oldPerm, oldEB := options.G, service.Cluster, service.Store, service.Permission, eventbus.Channel
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
		service.Store = oldStore
		service.Permission = oldPerm
		eventbus.Channel = oldEB
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Cluster.InitNodes = []*options.Node{
		{Id: 1001, ServerAddr: "127.0.0.1:11110"},
		{Id: 1002, ServerAddr: "127.0.0.1:11111"},
	}
	opts.Poller.ChannelCount = 2
	opts.Poller.ChannelGoroutine = 4
	options.G = opts

	// 1. BF-R3-3b: Event.PersistedDuplicate wire round-trip
	origEv := &eventbus.Event{
		Type:               eventbus.EventChannelOnSend,
		Conn:               &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
		Frame:              &wkproto.SendPacket{ClientMsgNo: "cno-dup-1", ChannelID: "g1", ChannelType: 2, Payload: []byte("hi")},
		MessageId:          99001,
		MessageSeq:         7,
		ReasonCode:         wkproto.ReasonSuccess,
		PersistedDuplicate: true,
	}
	batchBytes, err := eventbus.EventBatch{origEv}.Encode()
	require.NoError(t, err)
	var decodedBatch eventbus.EventBatch
	require.NoError(t, decodedBatch.Decode(batchBytes))
	require.Len(t, decodedBatch, 1)
	require.True(t, decodedBatch[0].PersistedDuplicate, "PersistedDuplicate must survive EventBatch wire encoding/decoding")

	// 2. BF-R3-3a: Ingress node 1001 queries authoritative slot leader 1002 for channel Ban/Disband
	// BEFORE appending messages to Raft.
	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dbDir), wkdb.WithNodeId(1001), wkdb.WithShardNum(1)))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	var appendCalls atomic.Int32
	mockCh := &r3MockChannel{
		proposeFn: func(ctx context.Context, channelId string, channelType uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
			appendCalls.Add(1)
			return nil, fmt.Errorf("leader request timed out: %w", context.DeadlineExceeded)
		},
	}
	service.Store = store.New(store.NewOptions(store.WithNodeId(1001), store.WithDB(wdb), store.WithChannel(mockCh)))
	service.Permission = service.NewPermissionService(nil)

	adapter := &realChannelClusterAdapter{
		nodeId:    1001,
		slotCount: 64,
	}
	// Route channel permission RPC to simulated slot leader 1002, which reports ReasonBan for "g_remote_banned".
	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode:            &clustertypes.Node{Id: 1002, Online: true},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if path == "/wk/ingress/hasPermissionForChannel" {
				dec := wkproto.NewDecoder(body)
				chID, _ := dec.String()
				if chID == "g_remote_banned" {
					return &proto.Response{Status: proto.Status(wkproto.ReasonBan)}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}

	_, evBanned, err := buildChannelSendEvent(messageSendReq{
		FromUID:     "u1",
		ChannelID:   "g_remote_banned",
		ChannelType: wkproto.ChannelTypeGroup,
		Payload:     []byte("blocked"),
	}, "g_remote_banned", wkproto.ChannelTypeGroup, "cno-ban-1")
	require.NoError(t, err)
	err = persistHTTPChannelMessages(context.Background(), "g_remote_banned", wkproto.ChannelTypeGroup, []*eventbus.Event{evBanned})
	require.ErrorContains(t, err, "ReasonBan")
	require.Equal(t, int32(0), appendCalls.Load(), "AppendMessages must NEVER be called when remote slot leader reports ReasonBan")

	// 3. BF-R3-3d: Narrow retry predicate in persistHTTPChannelMessages does NOT retry ambiguous timeout containing "leader".
	_, evTimeout, err := buildChannelSendEvent(messageSendReq{
		FromUID:     "u1",
		ChannelID:   "g_ok",
		ChannelType: wkproto.ChannelTypeGroup,
		Payload:     []byte("timeout-test"),
	}, "g_ok", wkproto.ChannelTypeGroup, "cno-timeout-1")
	require.NoError(t, err)
	err = persistHTTPChannelMessages(context.Background(), "g_ok", wkproto.ChannelTypeGroup, []*eventbus.Event{evTimeout})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), appendCalls.Load(), "ambiguous DeadlineExceeded error containing 'leader' must not be retried")

	// 4. BF-R3-3c & BF-R3-3d: Mixed-batch advanceEvents when slotLeaderId == 0 still executes Distribute/Webhook
	// immediately and retries OnSend once slotLeaderId resolves, while Handler.OnEvent never drops persisted batches.
	var (
		webhookCount    atomic.Int32
		distributeCount atomic.Int32
		onSendCount     atomic.Int32
		leaderResolved  atomic.Bool
	)
	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			if !leaderResolved.Load() {
				return 0, fmt.Errorf("slot leader election in progress")
			}
			return 1001, nil
		},
	}
	pool := chEvent.NewEventPool(&r3EventHandler{
		fn: func(ctx *eventbus.ChannelContext) {
			switch ctx.EventType {
			case eventbus.EventChannelWebhook:
				webhookCount.Add(int32(len(ctx.Events)))
			case eventbus.EventChannelDistribute:
				if ctx.SlotLeaderId != 0 {
					distributeCount.Add(int32(len(ctx.Events)))
				}
			case eventbus.EventChannelOnSend:
				if ctx.SlotLeaderId != 0 {
					onSendCount.Add(int32(len(ctx.Events)))
				}
			}
		},
	})
	require.NoError(t, pool.Start())
	t.Cleanup(func() { pool.Stop() })

	pool.AddEvent("g_mix", wkproto.ChannelTypeGroup, &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-mix-1", ChannelID: "g_mix", ChannelType: 2, Payload: []byte("m1")},
		MessageId:  101,
		MessageSeq: 1,
		ReasonCode: wkproto.ReasonSuccess,
	})
	pool.AddEvent("g_mix", wkproto.ChannelTypeGroup, &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-mix-2", ChannelID: "g_mix", ChannelType: 2, Payload: []byte("m2")},
		MessageId:  102,
		MessageSeq: 2,
		ReasonCode: wkproto.ReasonSuccess,
	})
	pool.AddEvent("g_mix", wkproto.ChannelTypeGroup, &eventbus.Event{
		Type:       eventbus.EventChannelWebhook,
		Conn:       &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-mix-3", ChannelID: "g_mix", ChannelType: 2, Payload: []byte("m3")},
		MessageId:  103,
		MessageSeq: 3,
		ReasonCode: wkproto.ReasonSuccess,
	})
	pool.Advance("g_mix", wkproto.ChannelTypeGroup)

	require.Eventually(t, func() bool {
		return webhookCount.Load() == 1
	}, 2*time.Second, 10*time.Millisecond, "EventChannelWebhook must be processed even while slotLeaderId == 0")
	require.Equal(t, int32(0), onSendCount.Load())
	require.Equal(t, int32(0), distributeCount.Load(), "EventChannelDistribute must be deferred while slotLeaderId == 0 so distributeByTag does not drop it")

	leaderResolved.Store(true)
	require.Eventually(t, func() bool {
		return onSendCount.Load() == 1 && distributeCount.Load() == 1
	}, 2*time.Second, 10*time.Millisecond, "deferred EventChannelOnSend and EventChannelDistribute must automatically retry once slotLeaderId resolves")

	// Also verify Handler.OnEvent with persisted events (MessageSeq > 0) under B5:
	// while remote slot leader 1002 lacks confirmed forward admission (StatusNotFound -> PeerCapUnsupported),
	// fire-and-forget Send is NOT treated as completed and the item stays retained in RecoveryManager until
	// 1002 upgrades to support confirmed /wk/forward/channel/v1 admission.
	var forwardedLegacy atomic.Int32
	var peerUpgraded atomic.Bool
	var confirmedAccepted atomic.Int32
	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if peerUpgraded.Load() {
				if path == forward.CapabilityPath {
					return &proto.Response{Status: proto.StatusOK, Body: []byte("WKF2\x01")}, nil
				}
				if path == forward.ChannelPath {
					confirmedAccepted.Add(1)
					return &proto.Response{Status: proto.StatusOK}, nil
				}
			}
			return &proto.Response{Status: proto.StatusNotFound}, nil
		},
		sendFn: func(toNodeId uint64, msg *proto.Message) error {
			forwardedLegacy.Add(1)
			return nil
		},
	}
	opts.Poller.ChannelGoroutine = 4 // forwardGate limit = 1
	chHandler := handler.NewHandler()
	t.Cleanup(func() { chHandler.Stop() })
	chHandler.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_persisted_gate",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events: []*eventbus.Event{
			{
				Type:       eventbus.EventChannelOnSend,
				Conn:       &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
				Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-gate-1", ChannelID: "g_persisted_gate", ChannelType: 2, Payload: []byte("p1")},
				MessageId:  201,
				MessageSeq: 1,
				ReasonCode: wkproto.ReasonSuccess,
			},
		},
	})
	require.Zero(t, forwardedLegacy.Load(), "B5: persisted event batch must not treat unacknowledged fire-and-forget Send as completed")
	recCh, recItems, _, _ := chHandler.InspectRecoveryQueue()
	require.Equal(t, 1, recCh)
	require.Equal(t, 1, recItems)

	peerUpgraded.Store(true)
	chHandler.TriggerPersistedRecovery()
	require.Eventually(t, func() bool {
		return confirmedAccepted.Load() == 1
	}, 2*time.Second, 10*time.Millisecond, "persisted event batch must be delivered once remote slot leader supports confirmed forward admission")
}

type r3EventHandler struct {
	fn func(ctx *eventbus.ChannelContext)
}

func (h *r3EventHandler) OnEvent(ctx *eventbus.ChannelContext) {
	if h.fn != nil {
		h.fn(ctx)
	}
}

type r3ClusterWithRPC struct {
	*realChannelClusterAdapter
	slotLeaderNode *clustertypes.Node
	slotLeaderIdFn func(channelId string, channelType uint8) (uint64, error)
	requestFn      func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error)
	sendFn         func(toNodeId uint64, msg *proto.Message) error
	customSlots    []*clustertypes.Slot
	panicOnSlots   bool
	untrustedNodes bool
}

func (r *r3ClusterWithRPC) Slots() []*clustertypes.Slot {
	if r.panicOnSlots {
		panic("simulated Slots() panic")
	}
	if r.customSlots != nil {
		return r.customSlots
	}
	if r.realChannelClusterAdapter != nil {
		return r.realChannelClusterAdapter.Slots()
	}
	return nil
}

func (r *r3ClusterWithRPC) NodeInfoById(nodeId uint64) *clustertypes.Node {
	if r.untrustedNodes {
		return nil
	}
	if r.slotLeaderNode != nil && r.slotLeaderNode.Id == nodeId {
		return r.slotLeaderNode
	}
	if r.realChannelClusterAdapter != nil {
		return r.realChannelClusterAdapter.NodeInfoById(nodeId)
	}
	return nil
}

func (r *r3ClusterWithRPC) Nodes() []*clustertypes.Node {
	if r.untrustedNodes {
		return nil
	}
	var nodes []*clustertypes.Node
	if r.realChannelClusterAdapter != nil {
		nodes = append(nodes, r.realChannelClusterAdapter.Nodes()...)
	}
	if r.slotLeaderNode != nil {
		nodes = append(nodes, r.slotLeaderNode)
	}
	return nodes
}

func (r *r3ClusterWithRPC) SlotLeaderOfChannel(channelId string, channelType uint8) (*clustertypes.Node, error) {
	if r.slotLeaderNode != nil {
		return r.slotLeaderNode, nil
	}
	return r.realChannelClusterAdapter.SlotLeaderOfChannel(channelId, channelType)
}

func (r *r3ClusterWithRPC) SlotLeaderIdOfChannel(channelId string, channelType uint8) (uint64, error) {
	if r.slotLeaderIdFn != nil {
		return r.slotLeaderIdFn(channelId, channelType)
	}
	if r.slotLeaderNode != nil {
		return r.slotLeaderNode.Id, nil
	}
	return r.realChannelClusterAdapter.SlotLeaderIdOfChannel(channelId, channelType)
}

func (r *r3ClusterWithRPC) RequestWithContext(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
	if r.requestFn != nil {
		return r.requestFn(ctx, toNodeId, path, body)
	}
	return &proto.Response{Status: proto.StatusOK}, nil
}

func (r *r3ClusterWithRPC) Send(toNodeId uint64, msg *proto.Message) error {
	if r.sendFn != nil {
		return r.sendFn(toNodeId, msg)
	}
	return nil
}

func TestRound4_BFR4_1_MixedVersion21cd8f88AndAmbiguousForwardDedupAndForwardGate(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldChannel, oldPerm, oldCluster, oldTag, oldSys := options.G, service.Store, eventbus.Channel, service.Permission, service.Cluster, service.TagManager, service.SystemAccountManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.Store = oldStore
		eventbus.Channel = oldChannel
		service.Permission = oldPerm
		service.Cluster = oldCluster
		service.TagManager = oldTag
		service.SystemAccountManager = oldSys
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Mode = options.TestMode
	opts.Channel.ProcessTimeout = 2 * time.Second
	opts.Poller.ChannelCount = 2
	opts.Poller.ChannelGoroutine = 4 // forwardGate limit = 1
	options.G = opts

	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dbDir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	// 1. BF-R4-1b: Simulate a remote slot leader (node 1002) running legacy baseline 21cd8f88:
	//    - /wk/ingress/allowSend decodes AllowSendReq (which returns EOF in <1ms on 0xff,0xff)
	//    - /wk/ingress/hasPermissionForChannel does NOT exist (would time out if called)
	//    - /wk/ingress/getSubscribers returns ["u_member"]
	var (
		legacyAllowSendCalls        atomic.Int32
		legacyHasPermChannelCalls   atomic.Int32
		legacyGetSubscribersCalls   atomic.Int32
		legacyForwardDistributeRecv atomic.Int32
		legacyForwardOnSendRecv     atomic.Int32
	)
	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	slotAd := &realChannelSlotAdapter{adapter: adapter}
	nodeAd := &realChannelNodeAdapter{adapter: adapter}
	chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
		clusterchannel.WithNodeId(1001),
		clusterchannel.WithGroupCount(2),
		clusterchannel.WithDB(wdb),
		clusterchannel.WithSlot(slotAd),
		clusterchannel.WithNode(nodeAd),
		clusterchannel.WithCluster(adapter),
		clusterchannel.WithRPC(adapter),
	))
	require.NoError(t, chSrv.Start())
	t.Cleanup(func() { chSrv.Stop() })

	service.Store = store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(slotAd),
		store.WithChannel(chSrv),
	))
	service.TagManager = wkmanager.NewTagManager(8, func() uint64 { return 1 })
	service.SystemAccountManager = wkmanager.NewSystemAccountManager()

	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode:            &clustertypes.Node{Id: 1002, Online: true},
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			switch path {
			case "/wk/ingress/allowSend":
				legacyAllowSendCalls.Add(1)
				// Exact 21cd8f88 AllowSendReq.decode behavior: reads uint16 length (0xffff) and returns EOF immediately.
				dec := wkproto.NewDecoder(body)
				if _, err := dec.String(); err != nil {
					return &proto.Response{Status: proto.StatusError, Body: []byte(err.Error())}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			case "/wk/ingress/hasPermissionForChannel":
				legacyHasPermChannelCalls.Add(1)
				<-ctx.Done()
				return nil, ctx.Err()
			case "/wk/ingress/getSubscribers":
				legacyGetSubscribersCalls.Add(1)
				enc := wkproto.NewEncoder()
				defer enc.End()
				enc.WriteUint32(1)
				enc.WriteString("u_member")
				return &proto.Response{Status: proto.StatusOK, Body: enc.Bytes()}, nil
			case forward.CapabilityPath:
				return &proto.Response{Status: proto.StatusOK}, nil
			case forward.ChannelPath:
				raw, _, _, err := forward.Decode(body)
				require.NoError(t, err)
				dec := wkproto.NewDecoder(raw)
				_, _ = dec.String()
				_, _ = dec.Uint8()
				_, _ = dec.Uint64()
				evBytes, _ := dec.BinaryAll()
				var batch eventbus.EventBatch
				require.NoError(t, batch.Decode(evBytes))
				for _, ev := range batch {
					if ev.Type == eventbus.EventChannelDistribute {
						legacyForwardDistributeRecv.Add(1)
					} else if ev.Type == eventbus.EventChannelOnSend {
						legacyForwardOnSendRecv.Add(1)
					}
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}

	service.Permission = service.NewPermissionService(ingress.NewClient())

	t0 := time.Now()
	rc, err := service.Permission.HasPermissionForChannel("g_legacy_21cd8f88", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc)
	require.Less(t, time.Since(t0), 100*time.Millisecond, "mixed-version 21cd8f88 permission check must complete in <100ms without 2s timeout")
	require.Equal(t, int32(0), legacyHasPermChannelCalls.Load(), "must not block on /wk/ingress/hasPermissionForChannel against a legacy 21cd8f88 peer")

	// Member on legacy 21cd8f88 slot leader is allowed via /wk/ingress/getSubscribers:
	rc, err = service.Permission.HasPermissionForSender("g_legacy_21cd8f88", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_member", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc)
	require.GreaterOrEqual(t, legacyGetSubscribersCalls.Load(), int32(1))

	// Non-member on legacy 21cd8f88 slot leader is rejected with ReasonSubscriberNotExist:
	rc, err = service.Permission.HasPermissionForSender("g_legacy_21cd8f88", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_outsider", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSubscriberNotExist, rc)

	// Forwarding an already-persisted EventChannelOnSend (MessageSeq > 0) to a legacy 21cd8f88 slot leader
	// must convert the wire event to EventChannelDistribute so the 21cd8f88 slot leader never re-runs AppendMessages.
	chHandler := handler.NewHandler()
	t.Cleanup(func() { chHandler.Stop() })
	chHandler.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_legacy_21cd8f88",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events: []*eventbus.Event{
			{
				Type:       eventbus.EventChannelOnSend,
				Conn:       &eventbus.Conn{Uid: "u_member", DeviceId: opts.SystemDeviceId},
				Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-leg-1", ChannelID: "g_legacy_21cd8f88", ChannelType: 2, Payload: []byte("hi")},
				MessageId:  9001,
				MessageSeq: 1,
				ReasonCode: wkproto.ReasonSuccess,
			},
		},
	})
	require.Equal(t, int32(1), legacyForwardDistributeRecv.Load(), "legacy 21cd8f88 slot leader must receive EventChannelDistribute instead of EventChannelOnSend")
	require.Equal(t, int32(0), legacyForwardOnSendRecv.Load(), "must not send pre-persisted EventChannelOnSend to legacy 21cd8f88 slot leader")

	// 2. BF-R4-1c: Ambiguous ErrOutcomeUnknown replay on an upgraded slot leader must deduplicate
	//    EventChannelDistribute and side effects via recordPersistedSideEffect.
	var distributedOnLeader atomic.Int32
	leaderBus := &r3CaptureChannelBus{
		onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
			if ev != nil && ev.Type == eventbus.EventChannelDistribute {
				distributedOnLeader.Add(1)
			}
		},
	}
	leaderHandler := handler.NewHandler()
	t.Cleanup(func() { leaderHandler.Stop() })
	leaderHandler.BindNode(1002, leaderBus)

	persistedEv1 := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "u_member", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-dedup-1", ChannelID: "g_dedup", ChannelType: 2, Payload: []byte("p")},
		MessageId:  9101,
		MessageSeq: 7,
		ReasonCode: wkproto.ReasonSuccess,
	}
	// First delivery on slot leader 1002:
	leaderHandler.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_dedup",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       []*eventbus.Event{persistedEv1.Clone()},
	})
	// Second delivery (replay after ErrOutcomeUnknown timeout) of the same (channelId, channelType, MessageSeq=7):
	leaderHandler.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_dedup",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       []*eventbus.Event{persistedEv1.Clone()},
	})
	require.Equal(t, int32(1), distributedOnLeader.Load(), "replay of already-persisted event after ErrOutcomeUnknown must not duplicate EventChannelDistribute")
}

type r3CaptureChannelBus struct {
	onAdd func(channelId string, channelType uint8, ev *eventbus.Event)
}

func (b *r3CaptureChannelBus) AddEvent(channelId string, channelType uint8, ev *eventbus.Event) {
	if b.onAdd != nil {
		b.onAdd(channelId, channelType, ev)
	}
}

func (b *r3CaptureChannelBus) Advance(channelId string, channelType uint8) {}

func TestRound5_BFR5_1_2_PostCommitRetention_PerChannelWatermarkDedup_LegacyAuthoritativePermissionAndSlotHandoff(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldChannel, oldPerm, oldCluster, oldTag, oldSys := options.G, service.Store, eventbus.Channel, service.Permission, service.Cluster, service.TagManager, service.SystemAccountManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.Store = oldStore
		eventbus.Channel = oldChannel
		service.Permission = oldPerm
		service.Cluster = oldCluster
		service.TagManager = oldTag
		service.SystemAccountManager = oldSys
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Mode = options.TestMode
	opts.Channel.ProcessTimeout = 2 * time.Second
	opts.Poller.ChannelCount = 2
	opts.Poller.ChannelGoroutine = 8
	options.G = opts

	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dbDir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	// Populate stale local subscriber u_removed on Node 1001 to prove non-replica ingress node
	// NEVER trusts stale local Store over the legacy 21cd8f88 slot leader.
	require.NoError(t, wdb.AddSubscribers("g_legacy_acl", wkproto.ChannelTypeGroup, []wkdb.Member{
		{Uid: "u_removed"},
	}))

	// Mock legacy 21cd8f88 :5001 HTTP server on Node 1002
	legacy5001 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/cluster/channels":
			chId := r.URL.Query().Get("channel_id")
			switch chId {
			case "g_banned":
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"g_banned","channel_type":2,"ban":1,"disband":0}],"more":0}`))
			case "g_disbanded":
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"g_disbanded","channel_type":2,"ban":0,"disband":1}],"more":0}`))
			default:
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"` + chId + `","channel_type":2,"ban":0,"disband":0}],"more":0}`))
			}
		case "/cluster/channels/g_legacy_acl/2/denylist":
			_, _ = w.Write([]byte(`["u_blacklisted"]`))
		case "/cluster/channels/g_legacy_acl/2/allowlist":
			_, _ = w.Write([]byte(`["u_whitelisted"]`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer legacy5001.Close()

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	slotAd := &realChannelSlotAdapter{adapter: adapter}
	nodeAd := &realChannelNodeAdapter{adapter: adapter}
	chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
		clusterchannel.WithNodeId(1001),
		clusterchannel.WithGroupCount(2),
		clusterchannel.WithDB(wdb),
		clusterchannel.WithSlot(slotAd),
		clusterchannel.WithNode(nodeAd),
		clusterchannel.WithCluster(adapter),
		clusterchannel.WithRPC(adapter),
	))
	require.NoError(t, chSrv.Start())
	t.Cleanup(func() { chSrv.Stop() })

	service.Store = store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(slotAd),
		store.WithChannel(chSrv),
	))
	service.TagManager = wkmanager.NewTagManager(8, func() uint64 { return 1 })
	service.SystemAccountManager = wkmanager.NewSystemAccountManager()

	var failForwardRPC atomic.Bool
	var failGetSubscribersRPC atomic.Bool
	var currentLeader atomic.Uint64
	currentLeader.Store(1002)

	legacyLeaderNode := &clustertypes.Node{
		Id:            1002,
		ApiServerAddr: legacy5001.URL,
		Online:        true,
	}

	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode:            legacyLeaderNode,
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return currentLeader.Load(), nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			switch path {
			case "/wk/ingress/allowSend":
				dec := wkproto.NewDecoder(body)
				if _, err := dec.String(); err != nil {
					return &proto.Response{Status: proto.StatusError, Body: []byte(err.Error())}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			case "/wk/ingress/getSubscribers":
				if failGetSubscribersRPC.Load() {
					return nil, fmt.Errorf("simulated getSubscribers RPC timeout")
				}
				enc := wkproto.NewEncoder()
				defer enc.End()
				enc.WriteUint32(3)
				enc.WriteString("u_member")
				enc.WriteString("u_blacklisted")
				enc.WriteString("u_whitelisted")
				return &proto.Response{Status: proto.StatusOK, Body: enc.Bytes()}, nil
			case forward.CapabilityPath:
				return &proto.Response{Status: proto.StatusOK}, nil
			case forward.ChannelPath:
				if failForwardRPC.Load() {
					return &proto.Response{Status: forward.StatusRetry}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}

	service.Permission = service.NewPermissionService(ingress.NewClient())

	// 1. BF-R5-2a: Authoritative Ban, Disband, Subscriber removal, Denylist, and Allowlist against legacy 21cd8f88 slot leader
	rc, err := service.Permission.HasPermissionForChannel("g_banned", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonBan, rc, "banned channel on legacy 21cd8f88 slot leader must be rejected with ReasonBan")

	rc, err = service.Permission.HasPermissionForChannel("g_disbanded", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonDisband, rc, "disbanded channel on legacy 21cd8f88 slot leader must be rejected with ReasonDisband")

	rc, err = service.Permission.HasPermissionForSender("g_legacy_acl", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_removed", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSubscriberNotExist, rc, "removed subscriber on legacy slot leader must be rejected even if present in local Store")

	rc, err = service.Permission.HasPermissionForSender("g_legacy_acl", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_blacklisted", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonInBlacklist, rc, "denylisted sender on legacy slot leader must be rejected with ReasonInBlacklist")

	rc, err = service.Permission.HasPermissionForSender("g_legacy_acl", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_member", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonNotInWhitelist, rc, "sender not in allowlist on legacy slot leader must be rejected with ReasonNotInWhitelist")

	rc, err = service.Permission.HasPermissionForSender("g_legacy_acl", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_whitelisted", DeviceID: "dev1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc, "whitelisted subscriber on legacy slot leader must succeed")

	// Fail-closed when /wk/ingress/getSubscribers RPC fails:
	failGetSubscribersRPC.Store(true)
	rc, err = service.Permission.HasPermissionForSender("g_legacy_acl", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_removed", DeviceID: "dev1"})
	require.Error(t, err)
	require.Equal(t, wkproto.ReasonSystemError, rc, "getSubscribers RPC failure must fail closed with ReasonSystemError")
	failGetSubscribersRPC.Store(false)

	// 2. BF-R5-1a: Failed legacy forward followed by leadership transition to local node must NOT suppress EventChannelDistribute
	var distributedSeq42 atomic.Int32
	var requeuedOnSend atomic.Int32
	captureBus := &r3CaptureChannelBus{
		onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
			if ev == nil {
				return
			}
			if ev.Type == eventbus.EventChannelDistribute && ev.MessageSeq == 42 {
				distributedSeq42.Add(1)
			}
			if ev.Type == eventbus.EventChannelOnSend && ev.MessageSeq == 42 {
				requeuedOnSend.Add(1)
			}
		},
	}
	h := handler.NewHandler()
	t.Cleanup(func() { h.Stop() })
	h.BindNode(1001, captureBus)

	failForwardRPC.Store(true)
	ev42 := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "u_whitelisted", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-seq-42", ChannelID: "g_legacy_acl", ChannelType: 2, Payload: []byte("p42")},
		MessageId:  42001,
		MessageSeq: 42,
		ReasonCode: wkproto.ReasonSuccess,
	}
	// First attempt: SlotLeaderId is legacy node 1002, forward fails and is retained in RecoveryManager (never requeued onto chBus per BF-R7-1b):
	h.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_legacy_acl",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       []*eventbus.Event{ev42.Clone()},
	})
	require.Equal(t, int32(0), distributedSeq42.Load(), "failed legacy forward must not count as distributed")
	require.Equal(t, int32(0), requeuedOnSend.Load(), "failed forward must not blindly requeue onto chBus (BF-R7-1b)")
	recCh, recInMem, _, _ := h.InspectRecoveryQueue()
	require.Equal(t, 1, recCh)
	require.Equal(t, 1, recInMem, "failed forward must be retained in RecoveryManager")
	h.Stop()

	// Slot leadership transitions to local node 1001; EventChannelDistribute must NOT be suppressed:
	currentLeader.Store(1001)
	h.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_legacy_acl",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1001,
		Events:       []*eventbus.Event{ev42.Clone()},
	})
	require.Equal(t, int32(1), distributedSeq42.Load(), "EventChannelDistribute must execute after failed legacy forward when leadership moves to upgraded node")

	// 3. BF-R5-1b: Per-channel monotonic watermark deduplication across 100,000 messages (> 65,536 ring capacity)
	var dist100kCount atomic.Int32
	dedupBus := &r3CaptureChannelBus{
		onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
			if ev != nil && ev.Type == eventbus.EventChannelDistribute {
				dist100kCount.Add(1)
			}
		},
	}
	dedupHandler := handler.NewHandler()
	t.Cleanup(func() { dedupHandler.Stop() })
	dedupHandler.BindNode(1001, dedupBus)

	batch := make([]*eventbus.Event, 0, 1000)
	for seq := uint64(1); seq <= 100000; seq++ {
		batch = append(batch, &eventbus.Event{
			Type:       eventbus.EventChannelOnSend,
			Conn:       &eventbus.Conn{Uid: "u_whitelisted", DeviceId: opts.SystemDeviceId},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", seq), ChannelID: "g_100k_dedup", ChannelType: 2, Payload: []byte("p")},
			MessageId:  int64(seq),
			MessageSeq: seq,
			ReasonCode: wkproto.ReasonSuccess,
		})
		if len(batch) == 1000 {
			dedupHandler.OnEvent(&eventbus.ChannelContext{
				ChannelId:    "g_100k_dedup",
				ChannelType:  wkproto.ChannelTypeGroup,
				EventType:    eventbus.EventChannelOnSend,
				SlotLeaderId: 1001,
				Events:       batch,
			})
			batch = batch[:0]
		}
	}
	require.Equal(t, int32(100000), dist100kCount.Load())

	// Replay seq=1 and seq=50000 after 100,000 messages have been processed:
	for _, replaySeq := range []uint64{1, 50000, 100000} {
		dedupHandler.OnEvent(&eventbus.ChannelContext{
			ChannelId:    "g_100k_dedup",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_whitelisted", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", replaySeq), ChannelID: "g_100k_dedup", ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(replaySeq),
					MessageSeq: replaySeq,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	require.Equal(t, int32(100000), dist100kCount.Load(), "per-channel monotonic watermark must deduplicate seq=1 and seq=50000 even after 100,000 messages")

	// 4. BF-R5-2b: Untagged EventChannelDistribute (TagKey == "") must be rejected with StatusRetry on non-leader acceptForward
	currentLeader.Store(1003) // Node 1001 is NOT the slot leader
	untaggedEv := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "u_whitelisted", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "cno-untagged", ChannelID: "g_handoff", ChannelType: 2, Payload: []byte("p")},
		MessageId:  77001,
		MessageSeq: 1,
		ReasonCode: wkproto.ReasonSuccess,
		TagKey:     "",
	}
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteString("g_handoff")
	enc.WriteUint8(wkproto.ChannelTypeGroup)
	enc.WriteUint64(1002)
	evBatch := eventbus.EventBatch{untaggedEv}
	evBytes, err := evBatch.Encode()
	require.NoError(t, err)
	enc.WriteBytes(evBytes)
	envBytes, _, err := forward.Envelope(enc.Bytes(), []*eventbus.Event{untaggedEv})
	require.NoError(t, err)

	status := dedupHandler.AcceptForward(envBytes)
	require.Equal(t, forward.StatusRetry, status, "acceptForward must return StatusRetry for untagged EventChannelDistribute when local node is no longer slot leader")
}

type r6PluginRecorder struct {
	calls atomic.Int64
	seqs  sync.Map
}

func (p *r6PluginRecorder) GetNo() string { return "r6_plugin" }
func (p *r6PluginRecorder) Send(ctx context.Context, frame *pluginproto.SendPacket) (*pluginproto.SendPacket, error) {
	return frame, nil
}
func (p *r6PluginRecorder) PersistAfter(ctx context.Context, msgBatch *pluginproto.MessageBatch) error {
	if msgBatch == nil {
		return nil
	}
	for _, m := range msgBatch.Messages {
		if m != nil {
			p.calls.Add(1)
			p.seqs.Store(m.MessageSeq, true)
		}
	}
	return nil
}
func (p *r6PluginRecorder) Receive(ctx context.Context, recv *pluginproto.RecvPacket) error {
	return nil
}
func (p *r6PluginRecorder) Route(ctx context.Context, req *pluginproto.HttpRequest) (*pluginproto.HttpResponse, error) {
	return nil, nil
}
func (p *r6PluginRecorder) Status() types.PluginStatus {
	return types.PluginStatusNormal
}

type r6PluginManager struct {
	plugin types.Plugin
}

func (m *r6PluginManager) Plugins(methods ...types.PluginMethod) []types.Plugin {
	for _, method := range methods {
		if method == types.PluginPersistAfter && m.plugin != nil {
			return []types.Plugin{m.plugin}
		}
	}
	return nil
}
func (m *r6PluginManager) Plugin(no string) types.Plugin              { return m.plugin }
func (m *r6PluginManager) UserIsAI(uid string) bool                   { return false }
func (m *r6PluginManager) GetUserPluginNo(uid string) (string, error) { return "", nil }

func TestRound6_BFR6_1_NonOneStartWatermarkAndBoundedForwardRecoveryAndCrossNodeHandoff(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldChannel, oldPerm, oldCluster, oldTag, oldSys, oldPlugin := options.G, service.Store, eventbus.Channel, service.Permission, service.Cluster, service.TagManager, service.SystemAccountManager, service.PluginManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.Store = oldStore
		eventbus.Channel = oldChannel
		service.Permission = oldPerm
		service.Cluster = oldCluster
		service.TagManager = oldTag
		service.SystemAccountManager = oldSys
		service.PluginManager = oldPlugin
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Mode = options.TestMode
	opts.Channel.ProcessTimeout = 2 * time.Second
	opts.Webhook.GRPCAddr = "127.0.0.1:19999"
	opts.Webhook.MsgNotifyEventPushInterval = time.Second
	options.G = opts

	// 1. BF-R6-1a: Non-1 starting sequence (1001..2000 and 1001..101000) after restart/handoff:
	//    - wm.contiguous advances to 2000 / 101000 with aboveLen == 0 and spanCount == 1
	//    - unseen sequences < 1001 (e.g., 500, 1000) are NOT falsely treated as seen
	//    - out-of-order and handoff gaps merge cleanly into bounded spans
	//    - total tracked channels bounded at <= 16,384
	var distCount atomic.Int64
	dedupBus := &r3CaptureChannelBus{
		onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
			if ev != nil && ev.Type == eventbus.EventChannelDistribute {
				distCount.Add(1)
			}
		},
	}
	h := handler.NewHandler()
	t.Cleanup(func() { h.Stop() })
	h.BindNode(1001, dedupBus)

	// Run 1001..2000 first:
	for seq := uint64(1001); seq <= 2000; seq++ {
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    "g_restart_1001",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", seq), ChannelID: "g_restart_1001", ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(seq),
					MessageSeq: seq,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	contig, aboveLen, spanCount, _ := h.InspectDistributeWatermark("g_restart_1001", wkproto.ChannelTypeGroup)
	require.Equal(t, uint64(2000), contig, "non-1 starting sequence 1001..2000 must advance contiguous to 2000")
	require.Equal(t, 0, aboveLen, "above map must remain empty (0 entries)")
	require.Equal(t, 1, spanCount, "1001..2000 must merge into a single contiguous span [1001, 2000]")
	require.Equal(t, int64(1000), distCount.Load())

	// Unseen lower sequences (seq = 500 and seq = 1000) MUST NOT be falsely treated as already persisted/distributed:
	for _, unseenLower := range []uint64{500, 1000} {
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    "g_restart_1001",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", unseenLower), ChannelID: "g_restart_1001", ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(unseenLower),
					MessageSeq: unseenLower,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	require.Equal(t, int64(1002), distCount.Load(), "unseen sequences 500 and 1000 below initial 1001 start must execute distribution once")
	_, aboveLen, spanCount, _ = h.InspectDistributeWatermark("g_restart_1001", wkproto.ChannelTypeGroup)
	require.Equal(t, 0, aboveLen)
	require.Equal(t, 2, spanCount, "seq=1000 merges into [1000, 2000] while seq=500 forms [500, 500] (2 spans total)")

	// Replaying 500, 1000, 1001, 1500, 2000 must be deduplicated:
	for _, replaySeq := range []uint64{500, 1000, 1001, 1500, 2000} {
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    "g_restart_1001",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", replaySeq), ChannelID: "g_restart_1001", ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(replaySeq),
					MessageSeq: replaySeq,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	require.Equal(t, int64(1002), distCount.Load(), "replayed sequences must be deduplicated")

	// Continue 2001..101000 (100,000 messages starting at 1001):
	batch := make([]*eventbus.Event, 0, 1000)
	for seq := uint64(2001); seq <= 101000; seq++ {
		batch = append(batch, &eventbus.Event{
			Type:       eventbus.EventChannelOnSend,
			Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("cno-%d", seq), ChannelID: "g_restart_1001", ChannelType: 2, Payload: []byte("p")},
			MessageId:  int64(seq),
			MessageSeq: seq,
			ReasonCode: wkproto.ReasonSuccess,
		})
		if len(batch) == 1000 {
			h.OnEvent(&eventbus.ChannelContext{
				ChannelId:    "g_restart_1001",
				ChannelType:  wkproto.ChannelTypeGroup,
				EventType:    eventbus.EventChannelOnSend,
				SlotLeaderId: 1001,
				Events:       batch,
			})
			batch = batch[:0]
		}
	}
	contig, aboveLen, spanCount, _ = h.InspectDistributeWatermark("g_restart_1001", wkproto.ChannelTypeGroup)
	require.Equal(t, uint64(101000), contig, "100,000 messages starting at 1001 must reach contiguous == 101000")
	require.Equal(t, 0, aboveLen, "len(wm.above) must be 0 after 100,000 non-1-start messages")
	require.Equal(t, 2, spanCount)

	// Out-of-order sequences [1001, 1003, 1002, 1005, 1004] must coalesce into 1 span [1001, 1005]:
	for _, seq := range []uint64{1001, 1003, 1002, 1005, 1004} {
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    "g_ooo",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("ooo-%d", seq), ChannelID: "g_ooo", ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(seq),
					MessageSeq: seq,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	contig, aboveLen, spanCount, _ = h.InspectDistributeWatermark("g_ooo", wkproto.ChannelTypeGroup)
	require.Equal(t, uint64(1005), contig)
	require.Equal(t, 0, aboveLen)
	require.Equal(t, 1, spanCount, "out-of-order 1001..1005 must merge into 1 span")

	// Bounded memory across 18,000 distinct channels (> 16,384 cap):
	for chIdx := 0; chIdx < 18000; chIdx++ {
		chID := fmt.Sprintf("g_cap_%d", chIdx)
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    chID,
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1001,
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelOnSend,
					Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: "c1", ChannelID: chID, ChannelType: 2, Payload: []byte("p")},
					MessageId:  int64(chIdx + 1),
					MessageSeq: 100,
					ReasonCode: wkproto.ReasonSuccess,
				},
			},
		})
	}
	_, _, _, totalCh := h.InspectDistributeWatermark("g_cap_17999", wkproto.ChannelTypeGroup)
	require.LessOrEqual(t, totalCh, 16384, "tracked watermark channels must be bounded at <= 16,384")

	// 2. BF-R6-1b: Bounded forward recovery queue with store-backed compaction & reload (no silent drop, no infinite spin)
	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dbDir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	slotAd := &realChannelSlotAdapter{adapter: adapter}
	nodeAd := &realChannelNodeAdapter{adapter: adapter}
	chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
		clusterchannel.WithNodeId(1001),
		clusterchannel.WithGroupCount(2),
		clusterchannel.WithDB(wdb),
		clusterchannel.WithSlot(slotAd),
		clusterchannel.WithNode(nodeAd),
		clusterchannel.WithCluster(adapter),
		clusterchannel.WithRPC(adapter),
	))
	require.NoError(t, chSrv.Start())
	t.Cleanup(func() { chSrv.Stop() })

	st := store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(slotAd),
		store.WithChannel(chSrv),
	))
	service.Store = st

	// Persist 120 messages into Store for channel "g_recover_store" (> 64 per-channel in-memory cap):
	const totalRecoverMsgs = 120
	persistedMsgs := make([]wkdb.Message, 0, totalRecoverMsgs)
	for i := 1; i <= totalRecoverMsgs; i++ {
		persistedMsgs = append(persistedMsgs, wkdb.Message{
			RecvPacket: wkproto.RecvPacket{
				MessageID:   int64(800000 + i),
				MessageSeq:  uint32(i),
				ClientMsgNo: fmt.Sprintf("rec-cno-%d", i),
				FromUID:     "u_sender",
				ChannelID:   "g_recover_store",
				ChannelType: wkproto.ChannelTypeGroup,
				Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			},
		})
	}
	err := wdb.AppendMessages("g_recover_store", wkproto.ChannelTypeGroup, persistedMsgs)
	require.NoError(t, err)

	var leaderReachable atomic.Bool
	var recoveredOnLeader sync.Map
	var recoveredCount atomic.Int32
	pluginRec := &r6PluginRecorder{}
	service.PluginManager = &r6PluginManager{plugin: pluginRec}

	service.Cluster = &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode:            &clustertypes.Node{Id: 1002, Online: true},
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			switch path {
			case forward.CapabilityPath:
				return &proto.Response{Status: proto.StatusOK}, nil
			case forward.ChannelPath:
				if !leaderReachable.Load() {
					return &proto.Response{Status: forward.StatusRetry}, nil
				}
				raw, _, _, err := forward.Decode(body)
				if err != nil {
					return &proto.Response{Status: forward.StatusInvalid}, nil
				}
				dec := wkproto.NewDecoder(raw)
				_, _ = dec.String()
				_, _ = dec.Uint8()
				_, _ = dec.Uint64()
				evBytes, _ := dec.BinaryAll()
				var eb eventbus.EventBatch
				if err := eb.Decode(evBytes); err != nil {
					return &proto.Response{Status: forward.StatusInvalid}, nil
				}
				for _, ev := range eb {
					if ev != nil && ev.MessageSeq > 0 {
						if _, loaded := recoveredOnLeader.LoadOrStore(ev.MessageSeq, true); !loaded {
							recoveredCount.Add(1)
						}
					}
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}

	var webhookCount atomic.Int32
	recHandler := handler.NewHandler()
	t.Cleanup(func() { recHandler.Stop() })
	var recBus *r3CaptureChannelBus
	recBus = &r3CaptureChannelBus{
		onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
			if ev == nil {
				return
			}
			if ev.Type == eventbus.EventChannelWebhook {
				webhookCount.Add(1)
				return
			}
			if ev.Type == eventbus.EventChannelOnSend && leaderReachable.Load() {
				go recHandler.OnEvent(&eventbus.ChannelContext{
					ChannelId:    channelId,
					ChannelType:  channelType,
					EventType:    ev.Type,
					SlotLeaderId: 1002,
					Events:       []*eventbus.Event{ev},
				})
			}
		},
	}
	recHandler.BindNodeDeps(handler.NodeDeps{
		NodeId:     1001,
		ChannelBus: recBus,
		Cluster:    service.Cluster,
		Store:      st,
	})

	// Build 120 persisted events whose per-window hop budget is already at MaxHops-1 so failed forward enters bounded recovery:
	failBatch := make([]*eventbus.Event, 0, totalRecoverMsgs)
	for i := 1; i <= totalRecoverMsgs; i++ {
		failBatch = append(failBatch, &eventbus.Event{
			Type:            eventbus.EventChannelOnSend,
			Conn:            &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
			Frame:           &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("rec-cno-%d", i), ChannelID: "g_recover_store", ChannelType: 2, Payload: []byte("p")},
			MessageId:       int64(800000 + i),
			MessageSeq:      uint64(i),
			ReasonCode:      wkproto.ReasonSuccess,
			ForwardHops:     forward.MaxHops - 1,
			ForwardDeadline: time.Now().Add(-time.Millisecond).UnixMilli(),
		})
	}
	recHandler.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_recover_store",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       failBatch,
	})

	chCount, inMem, compacted, dormant := recHandler.InspectRecoveryQueue()
	require.Equal(t, 1, chCount)
	require.Equal(t, totalRecoverMsgs, inMem, "ticket-bounded Part A RecoveryManager retains all 120 events in memory without lossy numeric range compaction")
	require.Equal(t, 0, compacted, "Part A removes [minCompactedSeq, maxCompactedSeq] / st.LoadMsg range scans (BF-R7-1c)")
	require.Equal(t, 0, dormant, "Part A removes dead-end dormant flag in favor of CooldownProbe (BF-R7-1d)")
	require.Equal(t, int64(totalRecoverMsgs), pluginRec.calls.Load(), "PluginPersistAfter must execute once per persisted message")
	require.Equal(t, int32(totalRecoverMsgs), webhookCount.Load(), "EventChannelWebhook must execute once per persisted message")

	// Restore slot leader 1002 reachability and trigger recovery: all 120 messages (64 in-memory + 56 reloaded from Store) must arrive with 0 loss and 0 duplicate plugin/webhook!
	leaderReachable.Store(true)
	recHandler.TriggerPersistedRecovery()
	require.Eventually(t, func() bool {
		return recoveredCount.Load() == int32(totalRecoverMsgs)
	}, 3*time.Second, 20*time.Millisecond, "all 120 persisted messages (including compacted store-reloaded range) must be recovered on slot leader")
	require.Equal(t, int64(totalRecoverMsgs), pluginRec.calls.Load(), "recovery replay must NOT duplicate PluginPersistAfter")
	require.Equal(t, int32(totalRecoverMsgs), webhookCount.Load(), "recovery replay must NOT duplicate EventChannelWebhook")
	recHandler.Stop()

	// 3. BF-R6-1c: Cross-node ErrOutcomeUnknown followed by slot-leader handoff (1002 -> 1003)
	//    must produce zero duplicate push, unread/conversation, plugin, or webhook side effects.
	tm1001 := wkmanager.NewTagManager(4, func() uint64 { return 1 })
	tm1002 := wkmanager.NewTagManager(4, func() uint64 { return 1 })
	tm1003 := wkmanager.NewTagManager(4, func() uint64 { return 1 })
	require.NoError(t, wdb.AddSubscribers("g_handoff_dedup", wkproto.ChannelTypeGroup, []wkdb.Member{
		{Uid: "u_sub_a"},
		{Uid: "u_sub_b"},
	}))

	userBus := &realSeedUserBus{nodeId: 1001}
	pusher1001 := &realSeedPusherBus{}
	pusher1002 := &realSeedPusherBus{}
	pusher1003 := &realSeedPusherBus{}
	conv1001 := &realSeedConversationManager{}
	conv1002 := &realSeedConversationManager{}
	conv1003 := &realSeedConversationManager{}

	h1001 := handler.NewHandler()
	h1002 := handler.NewHandler()
	h1003 := handler.NewHandler()
	t.Cleanup(func() {
		h1001.Stop()
		h1002.Stop()
		h1003.Stop()
	})

	handlers := map[uint64]*handler.Handler{
		1001: h1001,
		1002: h1002,
		1003: h1003,
	}
	tagMgrs := map[uint64]*wkmanager.TagManager{
		1001: tm1001,
		1002: tm1002,
		1003: tm1003,
	}

	var handoffLeader atomic.Uint64
	handoffLeader.Store(1002)

	makeNodeCluster := func(localId uint64) icluster.ICluster {
		ad := &realChannelClusterAdapter{
			nodeId:    localId,
			slotCount: 64,
			peers: map[uint64]*realSeedClusterNode{
				1001: nil,
				1002: nil,
				1003: nil,
			},
		}
		return &r3ClusterWithRPC{
			realChannelClusterAdapter: ad,
			slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
				if channelId == "g_handoff_dedup" {
					return handoffLeader.Load(), nil
				}
				return uint64(1001 + (ad.GetSlotId(channelId) % 3)), nil
			},
			requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
				targetH := handlers[toNodeId]
				switch path {
				case forward.CapabilityPath:
					return &proto.Response{Status: proto.StatusOK}, nil
				case forward.ChannelPath:
					status := targetH.AcceptForward(body)
					return &proto.Response{Status: status}, nil
				case "/wk/ingress/persistAfter":
					req := &ingress.PersistAfterReq{}
					_ = req.Decode(body)
					mb := &pluginproto.MessageBatch{}
					_ = mb.Unmarshal(req.Messages)
					_ = pluginRec.PersistAfter(ctx, mb)
					return &proto.Response{Status: proto.StatusOK}, nil
				case "/wk/ingress/getTag":
					req := &ingress.TagReq{}
					_ = req.Decode(body)
					tm := tagMgrs[toNodeId]
					tKey := req.TagKey
					if tKey == "" {
						tKey = tm.GetChannelTag(req.ChannelId, req.ChannelType)
					}
					tag := tm.Get(tKey)
					if tag == nil {
						return &proto.Response{Status: proto.StatusError, Body: []byte("tag not found")}, nil
					}
					uids := tag.GetUsers()
					if req.NodeId != 0 {
						uids = tag.GetNodeUsers(req.NodeId)
					}
					resp := &ingress.TagResp{TagKey: tag.Key, Uids: uids}
					b, _ := resp.Encode()
					return &proto.Response{Status: proto.StatusOK, Body: b}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			},
			sendFn: func(toNodeId uint64, msg *proto.Message) error {
				handlers[toNodeId].OnMessage(msg)
				return nil
			},
		}
	}

	var handoffWebhookCount atomic.Int64
	makeSyncBus := func(nodeId uint64, hRef *handler.Handler) eventbus.IChannel {
		return &r3CaptureChannelBus{
			onAdd: func(channelId string, channelType uint8, ev *eventbus.Event) {
				if ev == nil {
					return
				}
				if ev.Type == eventbus.EventChannelWebhook {
					handoffWebhookCount.Add(1)
				}
				ldr := handoffLeader.Load()
				hRef.OnEvent(&eventbus.ChannelContext{
					ChannelId:    channelId,
					ChannelType:  channelType,
					EventType:    ev.Type,
					SlotLeaderId: ldr,
					Events:       []*eventbus.Event{ev},
				})
			},
		}
	}

	cl1001 := makeNodeCluster(1001)
	cl1002 := makeNodeCluster(1002)
	cl1003 := makeNodeCluster(1003)
	service.Cluster = cl1001
	tm1001.BindCluster(cl1001)
	tm1002.BindCluster(cl1002)
	tm1003.BindCluster(cl1003)

	h1001.BindNodeDeps(handler.NodeDeps{NodeId: 1001, ChannelBus: makeSyncBus(1001, h1001), Cluster: cl1001, Store: st, TagManager: tm1001, Conversation: conv1001, UserBus: userBus, PusherBus: pusher1001})
	h1002.BindNodeDeps(handler.NodeDeps{NodeId: 1002, ChannelBus: makeSyncBus(1002, h1002), Cluster: cl1002, Store: st, TagManager: tm1002, Conversation: conv1002, UserBus: userBus, PusherBus: pusher1002})
	h1003.BindNodeDeps(handler.NodeDeps{NodeId: 1003, ChannelBus: makeSyncBus(1003, h1003), Cluster: cl1003, Store: st, TagManager: tm1003, Conversation: conv1003, UserBus: userBus, PusherBus: pusher1003})

	pluginBefore := pluginRec.calls.Load()
	handoffEv := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "u_sub_a", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "handoff-cno-1", ChannelID: "g_handoff_dedup", ChannelType: 2, Payload: []byte("hello")},
		MessageId:  990001,
		MessageSeq: 1,
		ReasonCode: wkproto.ReasonSuccess,
	}

	// Step A: Ingress 1001 forwards to Slot Leader 1002, which executes persist + distribute across the cluster:
	h1001.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_handoff_dedup",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       []*eventbus.Event{handoffEv.Clone()},
	})
	totalOnlineAfterA := pusher1001.onlineEvents.Load() + pusher1002.onlineEvents.Load() + pusher1003.onlineEvents.Load()
	totalConvAfterA := conv1001.pushedEvents.Load() + conv1002.pushedEvents.Load() + conv1003.pushedEvents.Load()
	require.Equal(t, int64(2), totalOnlineAfterA, "2 subscribers must each receive 1 online push on initial delivery")
	require.GreaterOrEqual(t, totalConvAfterA, int64(1))
	require.Equal(t, pluginBefore+1, pluginRec.calls.Load(), "PluginPersistAfter must run once on initial delivery")

	// Step B: Simulate ErrOutcomeUnknown + slot leadership handoff from 1002 to 1003, and Ingress 1001 retries forwarding to new Slot Leader 1003:
	handoffLeader.Store(1003)
	h1001.OnEvent(&eventbus.ChannelContext{
		ChannelId:    "g_handoff_dedup",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1003,
		Events:       []*eventbus.Event{handoffEv.Clone()},
	})
	totalOnlineAfterB := pusher1001.onlineEvents.Load() + pusher1002.onlineEvents.Load() + pusher1003.onlineEvents.Load()
	totalConvAfterB := conv1001.pushedEvents.Load() + conv1002.pushedEvents.Load() + conv1003.pushedEvents.Load()
	require.Equal(t, totalOnlineAfterA, totalOnlineAfterB, "cross-node retry after slot-leader handoff (1002->1003) must NOT duplicate online push events")
	require.Equal(t, totalConvAfterA, totalConvAfterB, "cross-node retry after slot-leader handoff (1002->1003) must NOT duplicate conversation/unread updates")
	require.Equal(t, pluginBefore+1, pluginRec.calls.Load(), "cross-node retry after slot-leader handoff (1002->1003) must NOT duplicate PluginPersistAfter")
	require.Equal(t, int64(1), handoffWebhookCount.Load(), "cross-node retry after slot-leader handoff (1002->1003) must NOT duplicate EventChannelWebhook")
}

func TestRound6_BFR6_2_LegacyPeerTrustAndCmdChannelBanDisbandAndReplicaFailClosed(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldPerm, oldCluster, oldSys := options.G, service.Store, service.Permission, service.Cluster, service.SystemAccountManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.Store = oldStore
		service.Permission = oldPerm
		service.Cluster = oldCluster
		service.SystemAccountManager = oldSys
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Mode = options.TestMode
	opts.ManagerToken = "super-secret-manager-token-r6"
	options.G = opts

	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dbDir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	// Create banned and disbanded channels in local Store to test HasPermissionForChannelLocal with ____cmd_ prefix:
	_, err := wdb.AddChannel(wkdb.ChannelInfo{
		ChannelId:   "g_local_banned",
		ChannelType: wkproto.ChannelTypeGroup,
		Ban:         true,
	})
	require.NoError(t, err)
	_, err = wdb.AddChannel(wkdb.ChannelInfo{
		ChannelId:   "g_local_disbanded",
		ChannelType: wkproto.ChannelTypeGroup,
		Disband:     true,
	})
	require.NoError(t, err)
	_, err = wdb.AddChannel(wkdb.ChannelInfo{
		ChannelId:   "g_local_active",
		ChannelType: wkproto.ChannelTypeGroup,
	})
	require.NoError(t, err)
	require.NoError(t, wdb.AddSubscribers("g_local_active", wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "u_ok"}}))

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	slotAd := &realChannelSlotAdapter{adapter: adapter}
	nodeAd := &realChannelNodeAdapter{adapter: adapter}
	chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
		clusterchannel.WithNodeId(1001),
		clusterchannel.WithGroupCount(2),
		clusterchannel.WithDB(wdb),
		clusterchannel.WithSlot(slotAd),
		clusterchannel.WithNode(nodeAd),
		clusterchannel.WithCluster(adapter),
		clusterchannel.WithRPC(adapter),
	))
	require.NoError(t, chSrv.Start())
	t.Cleanup(func() { chSrv.Stop() })

	st := store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(slotAd),
		store.WithChannel(chSrv),
	))
	service.Store = st
	service.SystemAccountManager = wkmanager.NewSystemAccountManager()

	// 1. BF-R6-2a: Untrusted peer URL in requestLegacyPeerJSON MUST be rejected without sending ManagerToken
	var untrustedHits atomic.Int32
	var leakedTokenCount atomic.Int32
	var trustedReceivedTokens atomic.Int32
	var lastQueriedChannelId atomic.Value

	peerHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := r.Header.Get("token")
		if tok != "" {
			trustedReceivedTokens.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/cluster/channels":
			chId := r.URL.Query().Get("channel_id")
			lastQueriedChannelId.Store(chId)
			switch chId {
			case "g_remote_banned":
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"g_remote_banned","channel_type":2,"ban":1,"disband":0}],"more":0}`))
			case "g_remote_disbanded":
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"g_remote_disbanded","channel_type":2,"ban":0,"disband":1}],"more":0}`))
			default:
				_, _ = w.Write([]byte(`{"data":[{"channel_id":"` + chId + `","channel_type":2,"ban":0,"disband":0}],"more":0}`))
			}
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer peerHTTP.Close()

	untrustedHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		untrustedHits.Add(1)
		if r.Header.Get("token") != "" {
			leakedTokenCount.Add(1)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer untrustedHTTP.Close()

	clRPC := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:            1002,
			ApiServerAddr: untrustedHTTP.URL,
			Online:        true,
		},
		untrustedNodes: true, // Node 1002 is NOT in trusted Cluster.Nodes()
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			switch path {
			case "/wk/ingress/allowSend":
				return &proto.Response{Status: proto.StatusError, Body: []byte("EOF")}, nil
			case "/wk/ingress/getSubscribers":
				enc := wkproto.NewEncoder()
				defer enc.End()
				enc.WriteUint32(1)
				enc.WriteString("u_ok")
				return &proto.Response{Status: proto.StatusOK, Body: enc.Bytes()}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}
	service.Cluster = clRPC
	service.Permission = service.NewPermissionService(ingress.NewClient())

	rc, err := service.Permission.HasPermissionForChannel("g_remote_banned", wkproto.ChannelTypeGroup)
	require.Error(t, err, "untrusted legacy peer URL must be rejected")
	require.Equal(t, wkproto.ReasonSystemError, rc)
	require.Equal(t, int32(0), untrustedHits.Load(), "must not even send HTTP request to untrusted peer URL")
	require.Equal(t, int32(0), leakedTokenCount.Load(), "must NEVER leak ManagerToken to untrusted peer URL")

	// Register node 1002 as a trusted cluster peer with peerHTTP.URL:
	clRPC.untrustedNodes = false
	clRPC.slotLeaderNode = &clustertypes.Node{
		Id:            1002,
		ApiServerAddr: peerHTTP.URL,
		Online:        true,
	}

	// Verify trusted peer receives ManagerToken when configured:
	rc, err = service.Permission.HasPermissionForChannel("g_remote_banned", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonBan, rc)
	require.GreaterOrEqual(t, trustedReceivedTokens.Load(), int32(1))

	// Verify empty-token compatibility on trusted legacy peer:
	opts.ManagerToken = ""
	rc, err = service.Permission.HasPermissionForChannel("g_remote_disbanded", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonDisband, rc)
	opts.ManagerToken = "super-secret-manager-token-r6"

	// 2. BF-R6-2b: CMD channel (____cmd_, sync_once: 1) normalization on ALL local and remote permission paths
	// Local CMD channel checks:
	rc, err = service.Permission.HasPermissionForChannelLocal(opts.OrginalConvertCmdChannel("g_local_banned"), wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonBan, rc, "CMD channel ____cmd_g_local_banned must enforce Ban on local path")

	rc, err = service.Permission.HasPermissionForChannelLocal(opts.OrginalConvertCmdChannel("g_local_disbanded"), wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonDisband, rc, "CMD channel ____cmd_g_local_disbanded must enforce Disband on local path")

	rc, err = service.Permission.HasPermissionForChannelLocal(opts.OrginalConvertCmdChannel("g_local_active"), wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc, "CMD channel ____cmd_g_local_active must be allowed when original channel is active")

	// Remote legacy CMD channel checks (must normalize ____cmd_ BEFORE SlotLeaderOfChannel and remote RPC):
	var slotLeaderLookupArg atomic.Value
	clRPC.slotLeaderIdFn = func(channelId string, channelType uint8) (uint64, error) {
		slotLeaderLookupArg.Store(channelId)
		return 1002, nil
	}
	rc, err = service.Permission.HasPermissionForChannel(opts.OrginalConvertCmdChannel("g_remote_banned"), wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonBan, rc, "CMD channel ____cmd_g_remote_banned must enforce Ban on remote legacy slot leader")
	require.Equal(t, "g_remote_banned", lastQueriedChannelId.Load(), "remote query must normalize ____cmd_ to g_remote_banned")

	// Remote upgraded peer CMD channel check:
	var upgradedCheckedCh atomic.Value
	upgradedCl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:     1003,
			Online: true,
		},
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1003, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if path == "/wk/ingress/allowSend" {
				if op, payload, ok := service.DecodeExtendedAllowSendHeader(body); ok && op == 1 {
					dec := wkproto.NewDecoder(payload)
					chId, _ := dec.String()
					upgradedCheckedCh.Store(chId)
					if chId == "g_upgraded_banned" {
						return &proto.Response{Status: proto.StatusOK, Body: []byte{1, byte(wkproto.ReasonBan)}}, nil
					}
					return &proto.Response{Status: proto.StatusOK, Body: []byte{1, byte(wkproto.ReasonSuccess)}}, nil
				}
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}
	service.Cluster = upgradedCl
	service.Permission = service.NewPermissionService(ingress.NewClient())

	rc, err = service.Permission.HasPermissionForChannel(opts.OrginalConvertCmdChannel("g_upgraded_banned"), wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonBan, rc, "CMD channel ____cmd_g_upgraded_banned must enforce Ban on remote upgraded slot leader")
	require.Equal(t, "g_upgraded_banned", upgradedCheckedCh.Load(), "upgraded allowSend probe must receive normalized channelId without ____cmd_ prefix")

	// 3. BF-R6-2c: isLocalNodeSlotReplica must fail closed on missing cluster, panic, or empty Slots()
	failClosedCl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:            1002,
			ApiServerAddr: "", // No HTTP ApiServerAddr -> forces isLocalNodeSlotReplica check
			Online:        true,
		},
		customSlots: []*clustertypes.Slot{}, // Empty slots!
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if path == "/wk/ingress/allowSend" {
				return &proto.Response{Status: proto.StatusError, Body: []byte("EOF")}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}
	service.Cluster = failClosedCl
	service.Permission = service.NewPermissionService(ingress.NewClient())

	rc, err = service.Permission.HasPermissionForChannel("g_local_active", wkproto.ChannelTypeGroup)
	require.Error(t, err, "empty Slots() must fail closed in isLocalNodeSlotReplica instead of falling back to local Store")
	require.Equal(t, wkproto.ReasonSystemError, rc)

	failClosedCl.panicOnSlots = true
	rc, err = service.Permission.HasPermissionForChannel("g_local_active", wkproto.ChannelTypeGroup)
	require.Error(t, err, "panicking Slots() must fail closed in isLocalNodeSlotReplica instead of falling back to local Store")
	require.Equal(t, wkproto.ReasonSystemError, rc)
}

func TestRound6_BFR6_3_TagSnapshotRaceAndNonLeaderPersonTagReuse(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster, oldTag := options.G, service.Cluster, service.TagManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
		service.TagManager = oldTag
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Mode = options.TestMode
	options.G = opts

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	service.Cluster = adapter
	tm := wkmanager.NewTagManager(4, func() uint64 { return 1 })
	tm.BindCluster(adapter)
	service.TagManager = tm

	userBus := &realSeedUserBus{nodeId: 1001}
	pusherBus := &realSeedPusherBus{}
	convMgr := &realSeedConversationManager{}

	h := handler.NewHandler()
	t.Cleanup(func() { h.Stop() })
	h.BindNodeDeps(handler.NodeDeps{
		NodeId:       1001,
		Cluster:      adapter,
		TagManager:   tm,
		Conversation: convMgr,
		UserBus:      userBus,
		PusherBus:    pusherBus,
	})

	// 1. BF-R6-3a: Exercise real distribution readers concurrently with Get, GetUsers, AddUsers, RemoveUsers, RenameTag, SetChannelTag under -race
	tag, err := tm.MakeTagWithTagKey("race-tag-main", []string{"u_1", "u_2"})
	require.NoError(t, err)
	require.NotNil(t, tag)
	tm.SetChannelTag("g_race_ch", wkproto.ChannelTypeGroup, "race-tag-main")

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Writer goroutines mutating users, channel bindings, and renaming temporary tags:
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				select {
				case <-stopCh:
					return
				default:
				}
				_ = tm.AddUsers("race-tag-main", []string{"u_3", "u_4"})
				_ = tm.RemoveUsers("race-tag-main", []string{"u_4"})
				tm.SetChannelTag("g_race_ch", wkproto.ChannelTypeGroup, "race-tag-main")
				tmpOld := fmt.Sprintf("tmp-tag-%d-%d-a", worker, i)
				tmpNew := fmt.Sprintf("tmp-tag-%d-%d-b", worker, i)
				_, _ = tm.MakeTagWithTagKey(tmpOld, []string{"u_1", "u_2"})
				_ = tm.RenameTag(tmpOld, tmpNew)
				tm.RemoveTag(tmpNew)
			}
		}(w)
	}

	// Reader goroutines running real Handler.distribute (distributeCommon -> distributeByTag) and TagManager reads:
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				snap := tm.Get("race-tag-main")
				if snap != nil {
					_ = snap.String()
					_ = snap.GetUsers()
					_ = snap.GetNodeUsers(1001)
				}
				_ = tm.GetUsers("race-tag-main")
				_ = tm.GetAllTags()

				seq := uint64(reader*10000 + i + 1)
				h.OnEvent(&eventbus.ChannelContext{
					ChannelId:    "g_race_ch",
					ChannelType:  wkproto.ChannelTypeGroup,
					EventType:    eventbus.EventChannelDistribute,
					SlotLeaderId: 1001,
					Events: []*eventbus.Event{
						{
							Type:       eventbus.EventChannelDistribute,
							Conn:       &eventbus.Conn{Uid: "u_1", DeviceId: opts.SystemDeviceId},
							Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("rc-%d-%d", reader, i), ChannelID: "g_race_ch", ChannelType: 2, Payload: []byte("p")},
							MessageId:  int64(seq),
							MessageSeq: seq,
							ReasonCode: wkproto.ReasonSuccess,
							TagKey:     "race-tag-main",
						},
					},
				})
			}
		}(r)
	}

	wg.Wait()
	close(stopCh)
	require.GreaterOrEqual(t, pusherBus.onlineEvents.Load(), int64(1200), "real online push events must be emitted during concurrent Tag distribution")
	require.Equal(t, int64(600), convMgr.pushedEvents.Load(), "real ConversationManager.Push must execute for all 600 distributed batches")

	// 2. BF-R6-3b: Non-leader personal-channel delivery must preserve supplied non-empty tagKey without per-message delete/recreate
	pusherBefore := pusherBus.onlineEvents.Load()
	convBefore := convMgr.pushedEvents.Load()
	fakePersonCh := options.GetFakeChannelIDWith("u_1", "u_2")
	leaderPersonTagKey := "leader-person-tag-stable-v1"

	for i := 1; i <= 40; i++ {
		h.OnEvent(&eventbus.ChannelContext{
			ChannelId:    fakePersonCh,
			ChannelType:  wkproto.ChannelTypePerson,
			EventType:    eventbus.EventChannelDistribute,
			SlotLeaderId: 1002, // Remote slot leader 1002; local node 1001 is non-leader recipient node
			Events: []*eventbus.Event{
				{
					Type:       eventbus.EventChannelDistribute,
					Conn:       &eventbus.Conn{Uid: "u_1", DeviceId: opts.SystemDeviceId},
					Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("p-cno-%d", i), ChannelID: "u_2", ChannelType: 1, Payload: []byte("dm")},
					MessageId:  int64(70000 + i),
					MessageSeq: uint64(i),
					ReasonCode: wkproto.ReasonSuccess,
					TagKey:     leaderPersonTagKey,
				},
			},
		})
		cachedKey := tm.GetChannelTag(fakePersonCh, wkproto.ChannelTypePerson)
		require.Equal(t, leaderPersonTagKey, cachedKey, "non-leader person channel must preserve slot leader's TagKey")
	}

	cachedTag := tm.Get(leaderPersonTagKey)
	require.NotNil(t, cachedTag)
	require.GreaterOrEqual(t, cachedTag.GetCount.Load(), uint64(40), "same cached person tag must be reused across all 40 deliveries without delete/recreate")
	require.Equal(t, pusherBefore+80, pusherBus.onlineEvents.Load(), "each of the 40 person messages must push online to both u_1 and u_2")
	require.Equal(t, convBefore+40, convMgr.pushedEvents.Load(), "each of the 40 person messages must update ConversationManager")

	// When the slot leader rotates TagKey to v2, the stale v1 tag is cleaned up and v2 is cached:
	leaderPersonTagKeyV2 := "leader-person-tag-stable-v2"
	h.OnEvent(&eventbus.ChannelContext{
		ChannelId:    fakePersonCh,
		ChannelType:  wkproto.ChannelTypePerson,
		EventType:    eventbus.EventChannelDistribute,
		SlotLeaderId: 1002,
		Events: []*eventbus.Event{
			{
				Type:       eventbus.EventChannelDistribute,
				Conn:       &eventbus.Conn{Uid: "u_1", DeviceId: opts.SystemDeviceId},
				Frame:      &wkproto.SendPacket{ClientMsgNo: "p-cno-41", ChannelID: "u_2", ChannelType: 1, Payload: []byte("dm")},
				MessageId:  70041,
				MessageSeq: 41,
				ReasonCode: wkproto.ReasonSuccess,
				TagKey:     leaderPersonTagKeyV2,
			},
		},
	})
	require.False(t, tm.Exist(leaderPersonTagKey), "stale person tag key v1 must be removed when leader rotates to v2")
	require.True(t, tm.Exist(leaderPersonTagKeyV2), "new person tag key v2 must be cached")
	require.Equal(t, leaderPersonTagKeyV2, tm.GetChannelTag(fakePersonCh, wkproto.ChannelTypePerson))
}
