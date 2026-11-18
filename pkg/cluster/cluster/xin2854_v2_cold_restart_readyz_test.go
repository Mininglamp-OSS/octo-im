package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func reserveThreeFreeTCPAddrs(t *testing.T) (string, string, string) {
	t.Helper()
	var addrs [3]string
	var listeners [3]net.Listener
	for i := 0; i < 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}
	for i := 0; i < 3; i++ {
		_ = listeners[i].Close()
	}
	return addrs[0], addrs[1], addrs[2]
}

// TestXIN2854_V2_Defect3_ColdRestartReadyzGatedOnCfgServerLeaderAndLeaderLoss reproduces
// XIN-2851 v2.0 Cold Restart failure: /readyz returned 200 OK for 1,543.1 ms while
// s.cfgServer.LeaderId() == 0 caused /cluster/info to fail with 400 {"msg":"leader not found"}.
func TestXIN2854_V2_Defect3_ColdRestartReadyzGatedOnCfgServerLeaderAndLeaderLoss(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	a1, a2, a3 := reserveThreeFreeTCPAddrs(t)
	initNodes := map[uint64]string{1: a1, 2: a2, 3: a3}
	opts1 := newTestOptions(t, 1, initNodes, clusterconfig.WithPongMaxTick(5), clusterconfig.WithApiServerAddr("http://test1.com"))
	opts2 := newTestOptions(t, 2, initNodes, clusterconfig.WithPongMaxTick(5), clusterconfig.WithApiServerAddr("http://test2.com"))
	opts3 := newTestOptions(t, 3, initNodes, clusterconfig.WithPongMaxTick(5), clusterconfig.WithApiServerAddr("http://test3.com"))

	s1 := New(NewOptions(WithAddr("tcp://"+a1), WithConfigOptions(opts1), WithDataDir(fmt.Sprintf("%s/%d", t.TempDir(), 1))))
	s2 := New(NewOptions(WithAddr("tcp://"+a2), WithConfigOptions(opts2), WithDataDir(fmt.Sprintf("%s/%d", t.TempDir(), 2))))
	s3 := New(NewOptions(WithAddr("tcp://"+a3), WithConfigOptions(opts3), WithDataDir(fmt.Sprintf("%s/%d", t.TempDir(), 3))))

	require.NoError(t, s1.Start())
	require.NoError(t, s2.Start())
	require.NoError(t, s3.Start())
	defer s1.Stop()
	defer s2.Stop()
	defer s3.Stop()

	waitAllSlotReady(s1, s2, s3)
	waitApiServerAddr(s1, s2, s3)

	require.Eventually(t, func() bool {
		return s1.CheckLocalServiceReady() == nil && s1.bootstrapReady.Load() && s1.cfgServer.LeaderId() != 0
	}, 8*time.Second, 20*time.Millisecond)

	// 1. Simulate cold-restart pre-election / post-bootstrap leader-loss window on s1's cfgServer:
	// Pause s2 & s3 cfgServer elections briefly and step s1's cfgServer down via a higher-term VoteReq
	// so s1.cfgServer.LeaderId() == 0 while all 64 slots still have slot.Leader != 0 and healthy slotRafts!
	s1.cfgServer.StepRaftEvent(rafttypes.Event{
		Type: rafttypes.VoteReq,
		From: 2,
		To:   1,
		Term: 100,
	})

	// Immediately during LeaderId() == 0: /cluster/info returns 400 "leader not found",
	// so CheckLocalServiceReady() MUST ALSO fail (and reset bootstrapReady to false)!
	require.Eventually(t, func() bool {
		if s1.cfgServer.LeaderId() != 0 {
			return false
		}
		err := s1.CheckLocalServiceReady()
		return err != nil && !s1.bootstrapReady.Load()
	}, 1500*time.Millisecond, 5*time.Millisecond, "CheckLocalServiceReady must return non-nil error and clear bootstrapReady whenever cfgServer.LeaderId() == 0")

	// 2. Once cfgServer Raft elects a leader again, BOTH CheckLocalServiceReady() AND /cluster/info recover:
	require.Eventually(t, func() bool {
		return s1.cfgServer.LeaderId() != 0 && s1.CheckLocalServiceReady() == nil && s1.bootstrapReady.Load()
	}, 8*time.Second, 20*time.Millisecond, "CheckLocalServiceReady must recover once cfgServer elects a usable leader")

	leaderID := s1.cfgServer.LeaderId()
	var leaderSrv *Server
	switch leaderID {
	case 1:
		leaderSrv = s1
	case 2:
		leaderSrv = s2
	default:
		leaderSrv = s3
	}
	leaderRouter := wkhttp.New()
	leaderSrv.ServerAPI(leaderRouter, "/cluster")
	req := httptest.NewRequest(http.MethodGet, "/cluster/info", nil)
	w := httptest.NewRecorder()
	leaderRouter.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "/cluster/info on ready cfgServer leader must return 200 OK")

	// 3. Storage health check is NEVER bypassed when cfgServer leadership is healthy:
	wkdbDir := filepath.Join(s1.opts.DataDir, "db", "wukongimdb")
	require.NoError(t, os.Chmod(wkdbDir, 0500))
	t.Cleanup(func() { _ = os.Chmod(wkdbDir, 0700) })
	require.Error(t, s1.CheckLocalServiceReady(), "read-only storage must still fail CheckLocalServiceReady even when cfgServer leader is healthy")
	require.False(t, s1.bootstrapReady.Load())
	require.NoError(t, os.Chmod(wkdbDir, 0700))
	require.NoError(t, s1.CheckLocalServiceReady())
}

// TestXIN2854_V2_Defect1_ExpansionAPI_ChannelClusterConfigMonotonicConvergence verifies that
// after learner-join -> promotion proposals, GET /cluster/channels/:id/:type/clusterConfig
// serves the converged R=3 config and never regresses if an older ConfVersion write lands late.
func TestXIN2854_V2_Defect1_ExpansionAPI_ChannelClusterConfigMonotonicConvergence(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	s, _ := newConversationConsistencyServer(t, nil)
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil
	}, 5*time.Second, 20*time.Millisecond)

	chID := "grp_expansion_api_01"
	learnerCfg := wkdb.ChannelClusterConfig{
		ChannelId:       chID,
		ChannelType:     2,
		ReplicaMaxCount: 3,
		Replicas:        []uint64{1},
		Learners:        []uint64{2},
		LeaderId:        1,
		Term:            1,
		MigrateFrom:     2,
		MigrateTo:       2,
	}
	vLearner, err := s.store.SaveChannelClusterConfig(learnerCfg)
	require.NoError(t, err)

	promotedCfg := wkdb.ChannelClusterConfig{
		ChannelId:       chID,
		ChannelType:     2,
		ReplicaMaxCount: 3,
		Replicas:        []uint64{1, 2},
		Learners:        nil,
		LeaderId:        1,
		Term:            1,
		MigrateFrom:     0,
		MigrateTo:       0,
	}
	vPromoted, err := s.store.SaveChannelClusterConfig(promotedCfg)
	require.NoError(t, err)
	require.Greater(t, vPromoted, vLearner)

	// Late out-of-order save of vLearner must not downgrade durable or HTTP-returned config:
	learnerCfg.ConfVersion = vLearner
	require.NoError(t, s.db.SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig{learnerCfg}))

	router := wkhttp.New()
	s.ServerAPI(router, "/cluster")
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/cluster/channels/%s/2/config", chID), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var respCfg wkdb.ChannelClusterConfig
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &respCfg))
	require.Equal(t, vPromoted, respCfg.ConfVersion)
	require.Equal(t, []uint64{1, 2}, respCfg.Replicas)
	require.Empty(t, respCfg.Learners)
	require.Zero(t, respCfg.MigrateTo)
}
