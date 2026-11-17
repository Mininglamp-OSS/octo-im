package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/plugin"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/stretchr/testify/require"
)

type mockHealthCluster struct {
	icluster.ICluster
	healthErr  error
	readyErr   error
	clusterErr error
}

func (m *mockHealthCluster) CheckLocalProcessHealth() error { return m.healthErr }
func (m *mockHealthCluster) CheckLocalServiceReady() error  { return m.readyErr }
func (m *mockHealthCluster) CheckClusterStatus() error      { return m.clusterErr }

func TestP1_HealthzReadyzAndHealthRoutes(t *testing.T) {
	oldOpts, oldCluster, oldPlugin := options.G, service.Cluster, service.PluginManager
	t.Cleanup(func() {
		options.G, service.Cluster, service.PluginManager = oldOpts, oldCluster, oldPlugin
	})

	opts := options.New(options.WithRootDir(t.TempDir()), options.WithLoggerDir(t.TempDir()))
	opts.ManagerToken = "secret-manager-token"
	opts.ManagerTokenOn = true
	options.G = opts

	service.PluginManager = plugin.NewServer(plugin.NewOptions(plugin.WithDir(t.TempDir())))
	mockCluster := &mockHealthCluster{
		healthErr:  nil,
		readyErr:   errors.New("local slot[0] initial apply lagging: applied=0 committed=42"),
		clusterErr: errors.New("local slot[0] initial apply lagging: applied=0 committed=42"),
	}
	service.Cluster = mockCluster

	srv := New()
	hs := newApiServer(srv)
	hs.setRoutes()

	// 1. While node is still applying initial logs:
	// - /healthz returns 200 OK (process is alive, must not be restarted by kubelet livenessProbe)
	// - /readyz returns 503 Service Unavailable (not ready to receive traffic)
	// - /health returns 500 Internal Server Error (legacy readiness probe compatibility)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	hs.r.GetGinRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/readyz", nil)
	hs.r.GetGinRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "initial apply lagging")

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/health", nil)
	hs.r.GetGinRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// 2. Once local slots catch up:
	mockCluster.readyErr = nil
	mockCluster.clusterErr = nil

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/readyz", nil)
	hs.r.GetGinRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/health", nil)
	hs.r.GetGinRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}
