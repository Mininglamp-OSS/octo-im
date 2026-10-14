package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Use the production Pebble range reader and HTTP handler, rather than a mock
// that merely echoes the normalized cursor. LastMsgSeq is a lower bound even
// when the final response is ordered newest-first.
func TestRecentReadDuplicateCursorsPreserveDatabaseWindow(t *testing.T) {
	reader := setupRecentRead(t, &recentReadCluster{load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
		return readCfg(id, typ, 1), nil
	}})
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	service.Store = store.New(store.NewOptions(store.WithDB(db)))
	messages := make([]wkdb.Message, 25)
	for i := range messages {
		messages[i] = wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "g", ChannelType: 2, MessageID: int64(i + 1), MessageSeq: uint32(i + 1), FromUID: "bob", Payload: []byte("data")}}
	}
	require.NoError(t, db.AppendMessages("g", 2, messages))
	router := wkhttp.New()
	newConversation(&Server{requset: reader}).route(router)
	for _, path := range []string{"/conversation/syncMessages", "/conversation/syncMessages/v2"} {
		for _, last := range []int{0, 1} {
			for _, cursors := range [][2]uint64{{10, 20}, {20, 10}, {0, 20}, {20, 0}, {21, 23}} {
				t.Run(fmt.Sprintf("%s/last=%d/%d-%d", path, last, cursors[0], cursors[1]), func(t *testing.T) {
					body := fmt.Sprintf(`{"uid":"alice","msg_count":15,"order_by_last":%d,"budget_ms":1000,"channels":[{"channel_id":"g","channel_type":2,"last_msg_seq":%d},{"channel_id":"g","channel_type":2,"last_msg_seq":%d}]}`, last, cursors[0], cursors[1])
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					w := httptest.NewRecorder()
					router.ServeHTTP(w, req)
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					var result []*channelRecentMessage
					if strings.HasSuffix(path, "/v2") {
						var v recentReadResponse
						require.NoError(t, json.Unmarshal(w.Body.Bytes(), &v))
						result = v.Channels
					} else {
						require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
					}
					require.Len(t, result, 1)
					first := max(uint64(1), min(cursors[0], cursors[1]))
					end := min(uint64(25), first+14)
					if last == 1 {
						first = max(first, uint64(11))
						end = 25
					}
					var want []uint64
					for i := first; i <= end; i++ {
						want = append(want, i)
					}
					if last == 1 {
						for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
							want[i], want[j] = want[j], want[i]
						}
					}
					var got []uint64
					for _, m := range result[0].Messages {
						got = append(got, m.MessageSeq)
					}
					require.Equal(t, want, got, "dedupe must retain the wider lower-bound window and respect the message limit")
				})
			}
		}
	}
}
