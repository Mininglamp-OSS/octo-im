package service

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/network"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/sendgrid/rest"
)

var Cluster icluster.ICluster // 分布式接口

var peerNoRedirectRestClient = &rest.Client{
	HTTPClient: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	},
}

func init() {
	if rest.DefaultClient != nil && rest.DefaultClient.HTTPClient != nil {
		rest.DefaultClient.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	network.PeerHeadersHook = TrustedPeerURLHeaders
	wkhttp.PeerHeadersHook = TrustedPeerURLHeaders
}

// SendPeerRequest executes an internal peer HTTP request with redirects disabled
// so custom headers such as ManagerToken are never forwarded to redirect targets.
func SendPeerRequest(request rest.Request) (*rest.Response, error) {
	return peerNoRedirectRestClient.Send(request)
}

// SendPeerRequestWithContext executes a context-bounded internal peer HTTP request
// with redirects disabled.
func SendPeerRequestWithContext(ctx context.Context, request rest.Request) (*rest.Response, error) {
	return peerNoRedirectRestClient.SendWithContext(ctx, request)
}

func normalizePeerAPIAddr(addr string) string {
	return strings.TrimRight(strings.TrimSpace(addr), "/")
}

func clonePeerHeaders(base map[string]string) map[string]string {
	if len(base) == 0 {
		return nil
	}
	cp := make(map[string]string, len(base))
	for k, v := range base {
		cp[k] = v
	}
	return cp
}

// IsTrustedClusterPeer verifies that node is a registered cluster peer with a
// matching ApiServerAddr before any internal ManagerToken header is attached.
// It fails closed (returns false) whenever Cluster is nil, node lookup fails,
// or any underlying Cluster method panics.
func IsTrustedClusterPeer(node *types.Node) bool {
	return IsTrustedClusterPeerOnCluster(Cluster, node)
}

// IsTrustedClusterPeerOnCluster verifies that node is a registered cluster peer on cl.
func IsTrustedClusterPeerOnCluster(cl icluster.ICluster, node *types.Node) bool {
	if node == nil || node.Id == 0 {
		return false
	}
	targetAddr := normalizePeerAPIAddr(node.ApiServerAddr)
	if targetAddr == "" || cl == nil {
		return false
	}
	var (
		reg       *types.Node
		panicked  bool
		nodesList []*types.Node
	)
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		reg = cl.NodeInfoById(node.Id)
	}()
	if !panicked && reg != nil {
		return reg.Id == node.Id && normalizePeerAPIAddr(reg.ApiServerAddr) == targetAddr
	}
	panicked = false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		nodesList = cl.Nodes()
	}()
	if !panicked {
		for _, n := range nodesList {
			if n != nil && n.Id == node.Id && normalizePeerAPIAddr(n.ApiServerAddr) == targetAddr {
				return true
			}
		}
		return false
	}
	return false
}

// IsTrustedClusterPeerURL verifies fail-closed that rawURL points to the
// ApiServerAddr of a registered internal cluster peer.
func IsTrustedClusterPeerURL(rawURL string) bool {
	return IsTrustedClusterPeerURLOnCluster(Cluster, rawURL)
}

// IsTrustedClusterPeerURLOnCluster verifies fail-closed that rawURL points to the
// ApiServerAddr of a registered internal cluster peer on cl.
func IsTrustedClusterPeerURLOnCluster(cl icluster.ICluster, rawURL string) bool {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" || cl == nil {
		return false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	origin := normalizePeerAPIAddr(parsed.Scheme + "://" + parsed.Host)
	var (
		nodesList []*types.Node
		panicked  bool
	)
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		nodesList = cl.Nodes()
	}()
	if !panicked {
		for _, n := range nodesList {
			if n == nil || n.Id == 0 {
				continue
			}
			peerAddr := normalizePeerAPIAddr(n.ApiServerAddr)
			if peerAddr != "" && (peerAddr == origin || strings.HasPrefix(trimmed, peerAddr+"/")) && IsTrustedClusterPeerOnCluster(cl, n) {
				return true
			}
		}
	}
	return false
}

func stripSensitivePeerHeaders(headers map[string]string) {
	for k := range headers {
		if strings.EqualFold(k, "token") || strings.EqualFold(k, "authorization") {
			delete(headers, k)
		}
	}
}

func setTrustedPeerTokenHeader(headers map[string]string, managerToken string) map[string]string {
	if headers == nil {
		headers = make(map[string]string, 1)
	}
	existingToken := ""
	for k, v := range headers {
		if strings.EqualFold(k, "token") {
			if existingToken == "" && strings.TrimSpace(v) != "" {
				existingToken = v
			}
			if k != "token" {
				delete(headers, k)
			}
		}
	}
	if strings.TrimSpace(existingToken) != "" {
		headers["token"] = existingToken
	} else if strings.TrimSpace(managerToken) != "" {
		headers["token"] = managerToken
	}
	return headers
}

// TrustedPeerNodeHeaders returns a header map including the internal ManagerToken
// only when options.G.ManagerToken is non-empty and node is a verified internal
// cluster peer. It strips sensitive auth headers when the target is untrusted.
func TrustedPeerNodeHeaders(node *types.Node, base map[string]string) map[string]string {
	return TrustedPeerNodeHeadersOnCluster(Cluster, node, base)
}

// TrustedPeerNodeHeadersOnCluster returns a header map including the internal ManagerToken
// only when node is a verified internal cluster peer on cl.
func TrustedPeerNodeHeadersOnCluster(cl icluster.ICluster, node *types.Node, base map[string]string) map[string]string {
	headers := clonePeerHeaders(base)
	if !IsTrustedClusterPeerOnCluster(cl, node) {
		stripSensitivePeerHeaders(headers)
		return headers
	}
	managerToken := ""
	if options.G != nil {
		managerToken = options.G.ManagerToken
	}
	if strings.TrimSpace(managerToken) == "" && len(headers) == 0 {
		return headers
	}
	return setTrustedPeerTokenHeader(headers, managerToken)
}

// TrustedPeerURLHeaders attaches options.G.ManagerToken (preserving existing headers
// such as Authorization) if and only if rawURL resolves to a verified cluster peer.
// When rawURL is untrusted, any inbound Token/Authorization headers are stripped.
func TrustedPeerURLHeaders(rawURL string, base map[string]string) map[string]string {
	return TrustedPeerURLHeadersOnCluster(Cluster, rawURL, base)
}

// TrustedPeerURLHeadersOnCluster attaches options.G.ManagerToken if and only if rawURL
// resolves to a verified cluster peer on cl.
func TrustedPeerURLHeadersOnCluster(cl icluster.ICluster, rawURL string, base map[string]string) map[string]string {
	headers := clonePeerHeaders(base)
	if !IsTrustedClusterPeerURLOnCluster(cl, rawURL) {
		stripSensitivePeerHeaders(headers)
		return headers
	}
	managerToken := ""
	if options.G != nil {
		managerToken = options.G.ManagerToken
	}
	if strings.TrimSpace(managerToken) == "" && len(headers) == 0 {
		return headers
	}
	return setTrustedPeerTokenHeader(headers, managerToken)
}

