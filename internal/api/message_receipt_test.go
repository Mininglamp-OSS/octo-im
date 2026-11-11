package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/service"
	cluster "github.com/WuKongIM/WuKongIM/pkg/cluster/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestCommittedMessageReceiptRequiresAppliedSenderScopedOriginal(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "group", ChannelType: 2, FromUID: "bot", ClientMsgNo: "retry", MessageSeq: 1, MessageID: 123, Payload: []byte("answer")}}
	require.NoError(t, db.AppendMessages("group", 2, []wkdb.Message{msg}))
	_, err := committedMessageReceipt(context.Background(), db, "group", 2, "bot", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound, "stored but uncommitted is not delivered")
	require.NoError(t, db.UpdateChannelAppliedIndex("group", 2, 1))
	got, err := committedMessageReceipt(context.Background(), db, "group", 2, "bot", "retry")
	require.NoError(t, err)
	require.Equal(t, msg.MessageID, got.MessageID)
	_, err = committedMessageReceipt(context.Background(), db, "group", 2, "other", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	_, err = committedMessageReceipt(context.Background(), db, "elsewhere", 2, "bot", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	msg.MessageSeq, msg.MessageID, msg.ClientMsgNo = 2, 124, "next"
	require.NoError(t, db.AppendMessages("group", 2, []wkdb.Message{msg}))
	_, err = committedMessageReceipt(context.Background(), db, "group", 2, "bot", "next")
	require.ErrorIs(t, err, wkdb.ErrNotFound, "the unapplied suffix must not be acknowledged")
}

type receiptCluster struct {
	icluster.ICluster
	readNode  *clustertypes.Node
	readErr   error
	slotCalls int
}

func (c *receiptCluster) LeaderOfChannelForRead(string, uint8) (*clustertypes.Node, error) {
	return c.readNode, c.readErr
}

func (c *receiptCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	c.slotCalls++
	return &clustertypes.Node{Id: 1}, nil
}

func TestCommittedMessageReceiptHTTPCompatibilityAndForwarding(t *testing.T) {
	setupRecentRead(t, &recentReadCluster{})
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	service.Store = store.New(store.NewOptions(store.WithDB(db)))
	require.NoError(t, db.AppendMessages("group", 2, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{ChannelID: "group", ChannelType: 2, FromUID: "bot", ClientMsgNo: "retry", MessageSeq: 1, MessageID: 123}}}))
	c := &receiptCluster{readNode: &clustertypes.Node{Id: 1}}
	service.Cluster = c
	router := wkhttp.New()
	newMessage(nil).route(router)
	query := "/message/byclientmsgno?channel_id=group&channel_type=2&client_msg_no=retry"
	request := func(url string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
		return w
	}
	legacy := request(query)
	require.Contains(t, legacy.Body.String(), `"message_id":123`)
	require.Empty(t, legacy.Header().Get("X-WK-Receipt-Version"))
	require.Equal(t, 1, c.slotCalls)
	require.Contains(t, request(query+"&committed=1").Body.String(), "from_uid")
	pending := request(query + "&committed=1&from_uid=bot")
	require.JSONEq(t, `{"status":404}`, pending.Body.String())
	require.Equal(t, "1", pending.Header().Get("X-WK-Receipt-Version"))
	require.NoError(t, db.UpdateChannelAppliedIndex("group", 2, 1))
	require.Contains(t, request(query+"&committed=1&from_uid=bot").Body.String(), `"message_id":123`)
	require.JSONEq(t, `{"status":404}`, request(query+"&committed=1&from_uid=other").Body.String())
	c.readErr = cluster.ErrChannelClusterConfigNotFound
	require.JSONEq(t, `{"status":404}`, request(query+"&committed=1&from_uid=bot").Body.String())
	c.readErr = nil
	for _, version := range []string{"1", ""} {
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "1", r.URL.Query().Get("committed"))
			require.Equal(t, "bot", r.URL.Query().Get("from_uid"))
			if version != "" {
				w.Header().Set("X-WK-Receipt-Version", version)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"retry"}`))
		}))
		c.readNode = &clustertypes.Node{Id: 2, ApiServerAddr: peer.URL}
		resp := request(query + "&committed=1&from_uid=bot")
		peer.Close()
		require.Equal(t, http.StatusServiceUnavailable, resp.Code)
		require.Equal(t, version, resp.Header().Get("X-WK-Receipt-Version"))
		require.JSONEq(t, `{"error":"retry"}`, resp.Body.String())
	}
	require.Equal(t, 1, c.slotCalls, "receipt mode must never use the slot leader")
}
