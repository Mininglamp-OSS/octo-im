package api

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/plugin"
	"github.com/WuKongIM/WuKongIM/internal/service"
	cluster "github.com/WuKongIM/WuKongIM/pkg/cluster/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

type cachedPeerIPs struct {
	ips       []net.IP
	expiresAt time.Time
}

type cachedLegacyPeerVerdict struct {
	isLegacy  bool
	expiresAt time.Time
}

type apiServer struct {
	r    *wkhttp.WKHttp
	addr string
	s    *Server
	wklog.Log

	legacyPeerCaps   forward.CapabilityCache
	peerIPMu         sync.Mutex
	peerIPCache      map[string]cachedPeerIPs
	peerVerdictCache map[uint64]cachedLegacyPeerVerdict
}

// NewAPIServer new一个api server
func newApiServer(s *Server) *apiServer {
	// r := wkhttp.New()
	log := wklog.NewWKLog("apiServer")
	r := wkhttp.NewWithLogger(wkhttp.LoggerWithWklog(log))

	if options.G.PprofOn {
		pprof.Register(r.GetGinRoute()) // 注册pprof
	}

	hs := &apiServer{
		r:    r,
		addr: options.G.HTTPAddr,
		s:    s,
		Log:  log,
	}
	return hs
}

// Start 开始
func (s *apiServer) start() {

	s.r.Use(s.jwtAndTokenAuthSetMiddleware()) // 如果存在jwt则解析jwt将信息放入上下文（并不做验证）
	s.r.Use(s.managerTokenMiddleware())       // 管理者权限判断

	// 跨域
	s.r.Use(wkhttp.CORSMiddleware())
	// 带宽流量计算中间件
	s.r.Use(bandwidthMiddleware())

	s.setRoutes()
	go func() {
		err := s.r.Run(s.addr) // listen and serve
		if err != nil {
			panic(err)
		}
	}()
	s.Info("ApiServer started", zap.String("addr", s.addr))
}

// Stop 停止服务
func (s *apiServer) stop() {
	s.Debug("stop...")
}

func (s *apiServer) setRoutes() {

	s.r.GET("/healthz", func(c *wkhttp.Context) {
		if checker, ok := service.Cluster.(icluster.ILocalHealthChecker); ok {
			if err := checker.CheckLocalProcessHealth(); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": err.Error()})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	s.r.GET("/readyz", func(c *wkhttp.Context) {
		if checker, ok := service.Cluster.(icluster.ILocalHealthChecker); ok {
			if err := checker.CheckLocalServiceReady(); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": err.Error()})
				return
			}
		} else if service.Cluster != nil {
			if err := service.Cluster.CheckClusterStatus(); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": err.Error()})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	s.r.GET("/health", func(c *wkhttp.Context) {

		// 检查分布式集群是否正常
		if checker, ok := service.Cluster.(icluster.ILocalHealthChecker); ok {
			if err := checker.CheckLocalServiceReady(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
				return
			}
		}
		clusterServer, ok := service.Cluster.(*cluster.Server)
		if ok {
			err := clusterServer.CheckClusterStatus()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	s.r.GET("/migrate/result", func(c *wkhttp.Context) {
		c.JSON(http.StatusOK, s.s.migrateTask.GetMigrateResult())
	})

	// route
	rt := newRoute(s.s)
	rt.route(s.r)
	// connz
	connz := newConnz(s.s)
	connz.route(s.r)
	// varz
	varz := newVarz(s.s)
	varz.route(s.r)
	// user
	user := newUser(s.s)
	user.route(s.r)
	// conn
	conn := newConnApi(s.s)
	conn.route(s.r)
	// channel
	ch := newChannel(s.s)
	ch.route(s.r)
	// message
	msg := newMessage(s.s)
	msg.route(s.r)
	// conversation
	conves := newConversation(s.s)
	conves.route(s.r)
	// manager
	mg := newManager(s.s)
	mg.route(s.r)
	// stress
	if options.G.Stress {
		st := newStress(s.s)
		st.route(s.r)
	}
	// tag
	tag := newTag(s.s)
	tag.route(s.r)

	// docs - API documentation with Swagger UI
	docs := newDocs(s.s)
	docs.route(s.r)

	// 分布式api
	clusterServer, ok := service.Cluster.(*cluster.Server)
	if ok {
		clusterServer.ServerAPI(s.r, "/cluster")
	}

	// plugin
	pluginServer := service.PluginManager.(*plugin.Server)
	pluginServer.SetRoute(s.r)

	// // 系统api
	// system := NewSystemAPI(s.s)
	// system.Route(s.r)

}

func (s *apiServer) jwtAndTokenAuthSetMiddleware() wkhttp.HandlerFunc {
	return func(c *wkhttp.Context) {
		authorization := c.GetHeader("Authorization")
		if authorization == "" {
			authorization = c.Query("Authorization")
		}

		if strings.TrimSpace(authorization) == "" {
			c.Next()
			return
		}
		if !strings.HasPrefix(authorization, "Bearer ") {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		rawJWT := strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
		if rawJWT == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		jwtToken, err := jwt.ParseWithClaims(rawJWT, jwt.MapClaims{}, func(token *jwt.Token) (interface{}, error) {
			return []byte(options.G.Jwt.Secret), nil
		})
		if err != nil || jwtToken == nil || !jwtToken.Valid {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		mapClaims, ok := jwtToken.Claims.(jwt.MapClaims)
		if !ok {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		username, _ := mapClaims["username"].(string)
		if strings.TrimSpace(username) == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set("username", username)
		c.Next()
	}
}

func (s *apiServer) managerTokenMiddleware() wkhttp.HandlerFunc {
	return func(c *wkhttp.Context) {
		if c.Request != nil && c.Request.URL != nil {
			switch c.Request.URL.Path {
			case "/health", "/healthz", "/readyz":
				c.Next()
				return
			}
		}
		configuredToken := strings.TrimSpace(options.G.ManagerToken)
		if configuredToken == "" {
			if c.GetString("username") == "" && strings.TrimSpace(c.GetHeader("Authorization")) == "" && strings.TrimSpace(c.Query("Authorization")) == "" {
				c.Set("username", options.G.ManagerUID)
			}
			c.Next()
			return
		}
		managerToken := c.GetHeader("token")
		if managerToken == options.G.ManagerToken {
			if c.GetString("username") == "" && strings.TrimSpace(c.GetHeader("Authorization")) == "" && strings.TrimSpace(c.Query("Authorization")) == "" {
				c.Set("username", options.G.ManagerUID)
			}
			c.Next()
			return
		}
		// Rolling-upgrade compatibility for frozen 21cd8f88 peers whose internal HTTP helpers
		// (e.g. requestSyncMessage -> POST /conversation/syncMessages/v2) construct rest.Request
		// with Headers=nil: admit ONLY when (1) no wrong token or Authorization was supplied,
		// (2) method+path is on the bounded internal peer route allowlist, (3) kernel TCP socket
		// RemoteAddr IP (never X-Forwarded-For / X-Real-IP) matches a registered non-local peer,
		// and (4) authenticated :11110 /wk/forward/capability/v1 proves that peer is a live BaseV1
		// (21cd8f88) node. Once all peers upgrade to PartAV2, this branch admits zero requests.
		if managerToken == "" &&
			strings.TrimSpace(c.GetHeader("Authorization")) == "" &&
			strings.TrimSpace(c.Query("Authorization")) == "" &&
			s.isVerifiedLegacyPeerInternalRequest(c) {
			if c.GetString("username") == "" {
				c.Set("username", options.G.ManagerUID)
			}
			c.Next()
			return
		}
		c.AbortWithStatus(http.StatusUnauthorized)
	}
}

func isLegacyHeaderlessPeerRoute(method, path string) bool {
	switch strings.ToUpper(method) {
	case http.MethodPost:
		switch path {
		case "/conversation/syncMessages/v2",
			"/conversation/syncMessages",
			"/conversation/channels",
			"/user/onlinestatus",
			"/user/systemuids_add_to_cache",
			"/user/systemuids_remove_from_cache",
			"/tmpchannel/subscriber_set",
			"/cluster/channel/status":
			return true
		}
	case http.MethodGet:
		switch path {
		case "/user/systemuids",
			"/cluster/node",
			"/cluster/slots":
			return true
		}
	}
	return false
}

func extractHostFromPeerEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "://") {
		parsed, err := url.Parse(trimmed)
		if err != nil || parsed.Host == "" {
			return ""
		}
		if h, _, splitErr := net.SplitHostPort(parsed.Host); splitErr == nil {
			return strings.Trim(h, "[]")
		}
		return strings.Trim(parsed.Host, "[]")
	}
	if h, _, splitErr := net.SplitHostPort(trimmed); splitErr == nil {
		return strings.Trim(h, "[]")
	}
	return strings.Trim(trimmed, "[]")
}

func (s *apiServer) resolvePeerHostIPs(host string) []net.IP {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}
	}
	if strings.EqualFold(host, "localhost") {
		return []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}
	now := time.Now()
	s.peerIPMu.Lock()
	if s.peerIPCache == nil {
		s.peerIPCache = make(map[string]cachedPeerIPs)
	}
	if entry, ok := s.peerIPCache[host]; ok && now.Before(entry.expiresAt) {
		s.peerIPMu.Unlock()
		return entry.ips
	}
	s.peerIPMu.Unlock()

	lookupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	cancel()
	if err != nil || len(addrs) == 0 {
		return nil
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		if a.IP != nil {
			ips = append(ips, a.IP)
		}
	}
	s.peerIPMu.Lock()
	s.peerIPCache[host] = cachedPeerIPs{ips: ips, expiresAt: now.Add(5 * time.Second)}
	s.peerIPMu.Unlock()
	return ips
}

func (s *apiServer) isVerifiedLegacyPeerInternalRequest(c *wkhttp.Context) bool {
	if s == nil || c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	if !isLegacyHeaderlessPeerRoute(c.Request.Method, c.Request.URL.Path) {
		return false
	}
	remoteHost, _, err := net.SplitHostPort(strings.TrimSpace(c.Request.RemoteAddr))
	if err != nil {
		return false
	}
	remoteIP := net.ParseIP(strings.Trim(remoteHost, "[]"))
	if remoteIP == nil {
		return false
	}

	cl := service.Cluster
	if cl == nil {
		return false
	}
	var (
		nodes    []*clustertypes.Node
		panicked bool
	)
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		nodes = cl.Nodes()
	}()
	if panicked || len(nodes) == 0 {
		return false
	}

	localNodeID := uint64(0)
	if options.G != nil {
		localNodeID = options.G.Cluster.NodeId
	}

	for _, n := range nodes {
		if n == nil || n.Id == 0 || n.Id == localNodeID {
			continue
		}
		matchedIP := false
		for _, rawEndpoint := range []string{n.ClusterAddr, n.ApiServerAddr} {
			peerHost := extractHostFromPeerEndpoint(rawEndpoint)
			for _, peerIP := range s.resolvePeerHostIPs(peerHost) {
				if peerIP != nil && peerIP.Equal(remoteIP) {
					matchedIP = true
					break
				}
			}
			if matchedIP {
				break
			}
		}
		if !matchedIP {
			continue
		}
		if s.probeVerifiedLegacy21cd8f88Peer(c.Request.Context(), cl, n.Id) {
			return true
		}
	}
	return false
}

func (s *apiServer) cacheLegacyPeerVerdict(nodeID uint64, isLegacy bool, ttl time.Duration) {
	s.peerIPMu.Lock()
	if s.peerVerdictCache == nil {
		s.peerVerdictCache = make(map[uint64]cachedLegacyPeerVerdict)
	}
	s.peerVerdictCache[nodeID] = cachedLegacyPeerVerdict{
		isLegacy:  isLegacy,
		expiresAt: time.Now().Add(ttl),
	}
	s.peerIPMu.Unlock()
}

func (s *apiServer) probeVerifiedLegacy21cd8f88Peer(parentCtx context.Context, cl icluster.ICluster, nodeID uint64) bool {
	if s == nil || cl == nil || nodeID == 0 {
		return false
	}
	now := time.Now()
	s.peerIPMu.Lock()
	if entry, ok := s.peerVerdictCache[nodeID]; ok && now.Before(entry.expiresAt) {
		s.peerIPMu.Unlock()
		return entry.isLegacy
	}
	s.peerIPMu.Unlock()

	enc := wkproto.NewEncoder()
	service.EncodeExtendedAllowSendHeader(enc, 1)
	enc.WriteString("__peer_auth__")
	enc.WriteUint8(wkproto.ChannelTypeGroup)
	probePayload := append([]byte(nil), enc.Bytes()...)
	enc.End()

	deadline := time.Now().Add(350 * time.Millisecond)
	for {
		var (
			resp     *proto.Response
			rpcErr   error
			panicked bool
		)
		reqCtx, cancel := context.WithTimeout(parentCtx, 150*time.Millisecond)
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			resp, rpcErr = cl.RequestWithContext(reqCtx, nodeID, "/wk/ingress/allowSend", probePayload)
		}()
		cancel()
		if panicked {
			return false
		}
		if rpcErr == nil && resp != nil {
			if resp.Status == proto.StatusOK && len(resp.Body) == 2 && resp.Body[0] == 1 {
				// Upgraded v2 peer: must send ManagerToken header; never admit headerless HTTP.
				s.cacheLegacyPeerVerdict(nodeID, false, 3*time.Second)
				return false
			}
			if resp.Status == proto.StatusOK && len(resp.Body) == 0 {
				// Confirmed live authenticated 21cd8f88 peer over :11110 (zero Unmarshal errors).
				s.cacheLegacyPeerVerdict(nodeID, true, 2*time.Second)
				return true
			}
			break
		}
		if rpcErr != nil {
			msg := strings.ToLower(rpcErr.Error())
			transient := strings.Contains(msg, "connect not authed") ||
				strings.Contains(msg, "conn is nil") ||
				strings.Contains(msg, "connect is not connected")
			if transient && parentCtx.Err() == nil && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
		}
		break
	}

	// Fallback for in-process mocks that only implement CapabilityPath:
	var (
		mode        forward.PeerCapMode
		probePanics bool
	)
	capCtx, cancel := context.WithTimeout(parentCtx, 150*time.Millisecond)
	func() {
		defer func() {
			if recover() != nil {
				probePanics = true
			}
		}()
		mode = s.legacyPeerCaps.ProbeOn(capCtx, cl, nodeID)
	}()
	cancel()
	if !probePanics && mode == forward.PeerCapBaseV1 {
		s.cacheLegacyPeerVerdict(nodeID, true, 2*time.Second)
		return true
	}
	if !probePanics && mode == forward.PeerCapPartAV2 {
		s.cacheLegacyPeerVerdict(nodeID, false, 3*time.Second)
	}
	return false
}

func bandwidthMiddleware() wkhttp.HandlerFunc {

	return func(c *wkhttp.Context) {

		// fpath := c.FullPath()
		// if strings.HasPrefix(fpath, "/metrics") { // 监控不计算外网带宽
		// 	c.Next()
		// 	return
		// }
		// 获取请求大小
		requestSize := computeRequestSize(c.Request)
		trace.GlobalTrace.Metrics.System().ExtranetIncomingAdd(int64(requestSize))

		// 获取响应大小
		blw := &bodyLogWriter{ResponseWriter: c.Writer}
		c.Writer = blw
		c.Next()
		trace.GlobalTrace.Metrics.System().ExtranetOutgoingAdd(int64(blw.size))

	}
}

type bodyLogWriter struct {
	gin.ResponseWriter
	size int
}

func (blw *bodyLogWriter) Write(b []byte) (int, error) {
	blw.size += len(b)
	return blw.ResponseWriter.Write(b)
}

func computeRequestSize(r *http.Request) int {
	// 计算请求头部大小
	requestSize := 0
	if r.URL != nil {
		requestSize += len(r.URL.String())
	}
	requestSize += len(r.Method)
	requestSize += len(r.Proto)
	for name, values := range r.Header {
		requestSize += len(name)
		for _, value := range values {
			requestSize += len(value)
		}
	}
	// 计算请求体大小
	requestSize += int(r.ContentLength)
	return requestSize
}
