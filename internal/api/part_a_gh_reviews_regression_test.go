package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type countingChannelServer struct {
	icluster.Channel
	appendCalls atomic.Int32
}

func (c *countingChannelServer) ProposeBatchUntilAppliedTimeout(_ context.Context, _ string, _ uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	c.appendCalls.Add(1)
	resps := make(rafttypes.ProposeRespSet, len(reqs))
	for i, r := range reqs {
		resps[i] = &rafttypes.ProposeResp{
			Id:          r.Id,
			CanonicalID: r.Id,
			Index:       uint64(i + 1),
		}
	}
	return resps, nil
}

type noopSlot struct {
	icluster.Slot
}

func (noopSlot) GetSlotId(string) uint32 { return 0 }

func TestPartA_HTTPSendAndSendBatch_Backpressure503ZeroRaftWrites_AndLongChannelIDs(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldCluster, oldPerm, oldCh := options.G, service.Store, service.Cluster, service.Permission, eventbus.Channel
	oldMaxB, oldMaxI := eventbus.DefaultPostCommitBudget.Limits()
	t.Cleanup(func() {
		eventbus.DefaultPostCommitBudget.SetLimits(oldMaxB, oldMaxI)
		options.G = oldOpts
		service.Store = oldStore
		service.Cluster = oldCluster
		service.Permission = oldPerm
		eventbus.Channel = oldCh
	})

	opts := options.New()
	opts.Mode = options.TestMode
	opts.Cluster.NodeId = 1001
	options.G = opts

	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(t.TempDir()),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	chSrv := &countingChannelServer{}
	service.Store = store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(noopSlot{}),
		store.WithChannel(chSrv),
	))
	service.Permission = nil

	var captured []*eventbus.Event
	eventbus.RegisterChannel(&captureIChannel{onAdd: func(_ string, _ uint8, ev *eventbus.Event) {
		captured = append(captured, ev)
		if ev != nil && ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
	}})

	router := wkhttp.New()
	msgAPI := newMessage(&Server{})
	msgAPI.route(router)

	// 1. Exhaust DefaultPostCommitBudget -> both /message/send and /message/sendbatch must return HTTP 503 with ZERO Raft writes!
	eventbus.DefaultPostCommitBudget.SetLimits(1, 0)
	beforeRejects := eventbus.SnapshotPartATelemetry().PostCommitBackpressureRejects

	sendBody := `{"header":{"red_dot":1},"from_uid":"alice","channel_id":"group_bp","channel_type":2,"payload":"aGVsbG8="}`
	w1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(sendBody))
	req1.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(w1, req1)
	require.Equal(t, http.StatusServiceUnavailable, w1.Code)
	require.Zero(t, chSrv.appendCalls.Load(), "HTTP 503 backpressure on /message/send must occur BEFORE AppendMessages")

	batchBody := `{"header":{"red_dot":1},"from_uid":"alice","subscribers":["u1","u2","u3"],"payload":"aGVsbG8="}`
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/message/sendbatch", strings.NewReader(batchBody))
	req2.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(w2, req2)
	require.Equal(t, http.StatusServiceUnavailable, w2.Code)
	require.Zero(t, chSrv.appendCalls.Load(), "HTTP 503 backpressure on /message/sendbatch must occur BEFORE any subscriber AppendMessages")
	require.Greater(t, eventbus.SnapshotPartATelemetry().PostCommitBackpressureRejects, beforeRejects)

	// 2. Restore DefaultPostCommitBudget and verify >256B group, Person+SyncOnce cmd (409B), and Agent channel IDs succeed cleanly,
	//    while >32,767B wire-overflow channel ID returns clean HTTP 400 error without panicking!
	eventbus.DefaultPostCommitBudget.SetLimits(oldMaxB, oldMaxI)

	longGroup := "grp_" + strings.Repeat("k", 400)
	wLong := httptest.NewRecorder()
	reqLong := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(
		`{"header":{"red_dot":1},"from_uid":"alice","channel_id":"`+longGroup+`","channel_type":2,"payload":"aGVsbG8="}`,
	))
	reqLong.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(wLong, reqLong)
	require.Equal(t, http.StatusOK, wLong.Code, ">256B group channel_id must succeed without arbitrary 256-byte rejection")

	longPeer := "u_" + strings.Repeat("p", 200)
	longSender := "u_" + strings.Repeat("s", 200)
	wCmd := httptest.NewRecorder()
	reqCmd := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(
		`{"header":{"sync_once":1},"from_uid":"`+longSender+`","channel_id":"`+longPeer+`","channel_type":1,"payload":"aGVsbG8="}`,
	))
	reqCmd.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(wCmd, reqCmd)
	require.Equal(t, http.StatusOK, wCmd.Code, "409B derived Person+SyncOnce u1@u2____cmd_ channel must succeed")

	overWire := strings.Repeat("z", eventbus.MaxWireStringBytes+10)
	wOver := httptest.NewRecorder()
	reqOver := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(
		`{"header":{"red_dot":1},"from_uid":"alice","channel_id":"`+overWire+`","channel_type":2,"payload":"aGVsbG8="}`,
	))
	reqOver.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(wOver, reqOver)
	require.Equal(t, http.StatusBadRequest, wOver.Code, ">32,767B channel_id must fail cleanly with HTTP 400 and zero panic")

	usedB, usedI, _ := eventbus.DefaultPostCommitBudget.Stats()
	require.Zero(t, usedB)
	require.Zero(t, usedI)
}

type captureIChannel struct {
	onAdd func(channelId string, channelType uint8, ev *eventbus.Event)
}

func (c *captureIChannel) AddEvent(ch string, ct uint8, ev *eventbus.Event) {
	if c.onAdd != nil {
		c.onAdd(ch, ct, ev)
	}
}
func (c *captureIChannel) Advance(string, uint8) {}

type electionTransitionCluster struct {
	icluster.ICluster
	readyAfter time.Time
}

func (c *electionTransitionCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	if time.Now().Before(c.readyAfter) {
		return nil, nil // Leader == 0 during Raft election
	}
	return &clustertypes.Node{Id: 1001}, nil
}

func (c *electionTransitionCluster) GetChannelLastMessageSeq(ctx context.Context, _ string, _ uint8) (uint64, error) {
	return 9, nil
}

func TestPartA_V11_5_ConversationElectionRetry503_AndGHR26_1_MsgCountClamp(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldCluster, oldStore := options.G, service.Cluster, service.Store
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
		service.Store = oldStore
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Cluster.ReqTimeout = 180 * time.Millisecond
	options.G = opts

	db := &conversationBoundaryDB{localSeq: 9, readTo: 1}
	slot := &conversationBoundarySlot{}
	service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot)))

	router := wkhttp.New()
	newConversation(nil).route(router)

	// 1. Election completes within ReqTimeout (80ms < 180ms) -> clearUnread rides through leader==0 and returns HTTP 200!
	service.Cluster = &electionTransitionCluster{readyAfter: time.Now().Add(80 * time.Millisecond)}
	wOK := httptest.NewRecorder()
	reqOK := httptest.NewRequest(http.MethodPost, "/conversations/clearUnread", bytes.NewReader([]byte(`{"uid":"alice","channel_id":"bob","channel_type":2}`)))
	reqOK.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(wOK, reqOK)
	require.Equal(t, http.StatusOK, wOK.Code, "V11-5: user-slot election completing within ReqTimeout must succeed without HTTP 400")

	// 2. Election exceeds ReqTimeout (500ms > 180ms) -> must return HTTP 503 {"msg":"retry required","status":503}, NEVER HTTP 400!
	service.Cluster = &electionTransitionCluster{readyAfter: time.Now().Add(500 * time.Millisecond)}
	w503 := httptest.NewRecorder()
	req503 := httptest.NewRequest(http.MethodPost, "/conversations/clearUnread", bytes.NewReader([]byte(`{"uid":"alice","channel_id":"bob","channel_type":2}`)))
	req503.Header.Set("Content-Type", "application/json")
	router.GetGinRoute().ServeHTTP(w503, req503)
	require.Equal(t, http.StatusServiceUnavailable, w503.Code, "V11-5: user-slot election exceeding ReqTimeout must return HTTP 503, never false 400")
	var body503 map[string]interface{}
	require.NoError(t, json.Unmarshal(w503.Body.Bytes(), &body503))
	require.Equal(t, "retry required", body503["msg"])

	// 3. GH-R26-1: clampRecentMsgCount caps oversized positive msg_count at maxRecentMsgCount (500) and defaults <=0 to 15.
	require.Equal(t, 15, clampRecentMsgCount(0))
	require.Equal(t, 15, clampRecentMsgCount(-99))
	require.Equal(t, 50, clampRecentMsgCount(50))
	require.Equal(t, maxRecentMsgCount, clampRecentMsgCount(1<<30), "GH-R26-1: oversized msg_count must clamp at 500")
}
