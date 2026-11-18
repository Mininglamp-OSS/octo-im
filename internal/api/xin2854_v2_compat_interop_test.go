package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	wkmanager "github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// decodeExact21cd8f88AllowSendReq mirrors base-21cd8f88 internal/ingress/model.go AllowSendReq.decode verbatim.
func decodeExact21cd8f88AllowSendReq(data []byte) (from, to string, err error) {
	dec := wkproto.NewDecoder(data)
	if from, err = dec.String(); err != nil {
		return "", "", err
	}
	if to, err = dec.String(); err != nil {
		return "", "", err
	}
	return from, to, nil
}

// TestXIN2854_V2_Defect2_AllowSendZeroUnmarshalErrorOn21cd8f88 reproduces TC-COMPAT-004's
// 8x "【Ingress】handleAllowSend Unmarshal err: size is less than 0, size: -1" errors when an
// upgraded node probes /wk/ingress/allowSend on a frozen 21cd8f88 peer.
func TestXIN2854_V2_Defect2_AllowSendZeroUnmarshalErrorOn21cd8f88(t *testing.T) {
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
	opts.ManagerToken = "compat-secret-token"
	options.G = opts

	wdb := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithNodeId(1001), wkdb.WithShardNum(1)))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	slotAd := &realChannelSlotAdapter{adapter: adapter}
	service.Store = store.New(store.NewOptions(store.WithNodeId(1001), store.WithDB(wdb), store.WithSlot(slotAd)))
	service.SystemAccountManager = wkmanager.NewSystemAccountManager()

	// Stand up a legacy 21cd8f88 HTTP peer (:5001) serving /cluster/channels, denylist, allowlist
	legacyPeerHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("token") != opts.ManagerToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/cluster/channels":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		default:
			_ = json.NewEncoder(w).Encode([]string{})
		}
	}))
	t.Cleanup(legacyPeerHTTP.Close)

	var legacyUnmarshalErrors atomic.Int32
	cl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:            1002,
			ApiServerAddr: legacyPeerHTTP.URL,
			Online:        true,
		},
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			switch path {
			case forward.CapabilityPath:
				// Exact 21cd8f88 /wk/forward/capability/v1 response: empty StatusOK
				return &proto.Response{Status: proto.StatusOK}, nil
			case "/wk/ingress/allowSend":
				// Exact 21cd8f88 handleAllowSend: runs AllowSendReq.decode(body) and logs ERROR if err != nil!
				if _, _, err := decodeExact21cd8f88AllowSendReq(body); err != nil {
					legacyUnmarshalErrors.Add(1)
					return &proto.Response{Status: proto.StatusError, Body: []byte(err.Error())}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			case "/wk/ingress/getSubscribers":
				enc := wkproto.NewEncoder()
				defer enc.End()
				enc.WriteUint32(1)
				enc.WriteString("u_sender")
				return &proto.Response{Status: proto.StatusOK, Body: enc.Bytes()}, nil
			}
			return &proto.Response{Status: proto.StatusNotFound}, nil
		},
	}
	service.Cluster = cl
	perm := service.NewPermissionService(ingress.NewClient())
	service.Permission = perm

	rc, err := perm.HasPermissionForChannel("grp_compat", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc)

	rc, err = perm.HasPermissionForSender("grp_compat", wkproto.ChannelTypeGroup, service.SenderInfo{UID: "u_sender", DeviceID: "d1"})
	require.NoError(t, err)
	require.Equal(t, wkproto.ReasonSuccess, rc)

	_ = perm.CheckOrProbeLegacyPeer(1002, "grp_compat_2", wkproto.ChannelTypeGroup)
	require.Zero(t, legacyUnmarshalErrors.Load(), "21cd8f88 peer must observe ZERO handleAllowSend Unmarshal errors")
}

// TestXIN2854_V2_Defect2_Legacy21cd8f88HeaderlessPeerSyncMessagesAuthAndSpoofRejection reproduces
// TC-COMPAT-004's 26x 503 errors where frozen 21cd8f88 calls POST /conversation/syncMessages/v2
// on an upgraded peer with Headers=nil while WK_MANAGERTOKEN is enabled.
func TestXIN2854_V2_Defect2_Legacy21cd8f88HeaderlessPeerSyncMessagesAuthAndSpoofRejection(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.ManagerToken = "geely-compat-secret-token"
	opts.ManagerUID = "____manager"
	options.G = opts

	var peer1002CapBody []byte // nil = BaseV1 (21cd8f88); "WKF2\x01" = upgraded PartAV2
	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	cl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode: &clustertypes.Node{
			Id:            1002,
			ClusterAddr:   "127.0.0.1:11112",
			ApiServerAddr: "http://127.0.0.1:15002",
			Online:        true,
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if toNodeId == 1002 && path == forward.CapabilityPath {
				return &proto.Response{Status: proto.StatusOK, Body: peer1002CapBody}, nil
			}
			return &proto.Response{Status: proto.StatusError}, errors.New("unknown node")
		},
	}
	service.Cluster = cl

	apiSrv := &apiServer{s: &Server{}}
	router := wkhttp.New()
	router.Use(apiSrv.jwtAndTokenAuthSetMiddleware())
	router.Use(apiSrv.managerTokenMiddleware())
	router.POST("/conversation/syncMessages/v2", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, map[string]any{"version": 2, "username": c.Username(), "channels": []any{}})
	})
	router.POST("/message/send", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	// 1. Live legacy 21cd8f88 peer (127.0.0.1, registered in Cluster.Nodes(), verified BaseV1 over :11110)
	// calling internal POST /conversation/syncMessages/v2 with NO token header MUST succeed (200 OK):
	req1 := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	req1.RemoteAddr = "127.0.0.1:54321"
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	require.Equal(t, http.StatusOK, w1.Code, "verified live 21cd8f88 peer internal /conversation/syncMessages/v2 must be admitted")

	// 2. External/unregistered IP with spoofed X-Forwarded-For / X-Real-IP MUST be rejected (401):
	reqSpoof := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqSpoof.RemoteAddr = "203.0.113.99:54321"
	reqSpoof.Header.Set("X-Forwarded-For", "127.0.0.1")
	reqSpoof.Header.Set("X-Real-IP", "127.0.0.1")
	reqSpoof.Header.Set("X-Internal-Peer", "1002")
	wSpoof := httptest.NewRecorder()
	router.ServeHTTP(wSpoof, reqSpoof)
	require.Equal(t, http.StatusUnauthorized, wSpoof.Code, "unregistered RemoteAddr with spoofed proxy headers must be rejected")

	// 3. Non-allowlisted mutation route (/message/send) without token from peer IP MUST be rejected (401):
	reqSend := httptest.NewRequest(http.MethodPost, "/message/send", bytes.NewBufferString(`{}`))
	reqSend.RemoteAddr = "127.0.0.1:54321"
	wSend := httptest.NewRecorder()
	router.ServeHTTP(wSend, reqSend)
	require.Equal(t, http.StatusUnauthorized, wSend.Code, "client mutation route /message/send without token must remain 401")

	// 4. Forged/wrong token header from peer IP MUST be rejected (401):
	reqBadTok := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqBadTok.RemoteAddr = "127.0.0.1:54321"
	reqBadTok.Header.Set("token", "wrong-forged-token")
	wBadTok := httptest.NewRecorder()
	router.ServeHTTP(wBadTok, reqBadTok)
	require.Equal(t, http.StatusUnauthorized, wBadTok.Code, "wrong token header must fail closed")

	// 5. Once peer 1002 upgrades to PartAV2 ("WKF2\x01"), headerless /conversation/syncMessages/v2 MUST be rejected (401):
	peer1002CapBody = forward.BuildCapabilityBody()
	// Reset any cached capability in apiSrv by testing on a fresh apiServer instance:
	apiSrvAfterUpgrade := &apiServer{s: &Server{}}
	routerAfter := wkhttp.New()
	routerAfter.Use(apiSrvAfterUpgrade.jwtAndTokenAuthSetMiddleware())
	routerAfter.Use(apiSrvAfterUpgrade.managerTokenMiddleware())
	routerAfter.POST("/conversation/syncMessages/v2", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, map[string]any{"version": 2})
	})
	reqUpgradedNoTok := httptest.NewRequest(http.MethodPost, "/conversation/syncMessages/v2", bytes.NewBufferString(`{"uid":"u1","channels":[]}`))
	reqUpgradedNoTok.RemoteAddr = "127.0.0.1:54321"
	wUpgradedNoTok := httptest.NewRecorder()
	routerAfter.ServeHTTP(wUpgradedNoTok, reqUpgradedNoTok)
	require.Equal(t, http.StatusUnauthorized, wUpgradedNoTok.Code, "upgraded PartAV2 peer omitting token must be rejected")
}

// TestXIN2854_V2_Defect2_TransientConnectNotAuthedRetryOnMessageSend reproduces TC-COMPAT-004's
// 400 {"msg":"connect not authed","status":400} during peer reconnect handshake.
func TestXIN2854_V2_Defect2_TransientConnectNotAuthedRetryOnMessageSend(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldPerm, oldCluster := options.G, service.Permission, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Permission = oldPerm
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Cluster.InitNodes = []*options.Node{
		{Id: 1001, ServerAddr: "127.0.0.1:11111"},
		{Id: 1002, ServerAddr: "127.0.0.1:11112"},
	}
	options.G = opts

	var attempts atomic.Int32
	adapter := &realChannelClusterAdapter{nodeId: 1001, slotCount: 64}
	cl := &r3ClusterWithRPC{
		realChannelClusterAdapter: adapter,
		slotLeaderNode:            &clustertypes.Node{Id: 1002, Online: true},
		slotLeaderIdFn: func(channelId string, channelType uint8) (uint64, error) {
			return 1002, nil
		},
		requestFn: func(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
			if path == "/wk/ingress/allowSend" {
				if attempts.Add(1) <= 2 {
					return nil, errors.New("connect not authed")
				}
				return &proto.Response{Status: proto.StatusOK, Body: []byte{1, byte(wkproto.ReasonSuccess)}}, nil
			}
			return &proto.Response{Status: proto.StatusOK, Body: forward.BuildCapabilityBody()}, nil
		},
	}
	service.Cluster = cl
	perm := service.NewPermissionService(ingress.NewClient())
	service.Permission = perm

	rc, err := perm.HasPermissionForChannel("grp_reconnect", wkproto.ChannelTypeGroup)
	require.NoError(t, err, "transient 'connect not authed' during peer reconnect must retry and succeed")
	require.Equal(t, wkproto.ReasonSuccess, rc)
	require.GreaterOrEqual(t, attempts.Load(), int32(3))
}
