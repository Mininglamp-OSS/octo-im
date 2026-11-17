package cluster

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/auth"
	"github.com/WuKongIM/WuKongIM/pkg/auth/resource"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestGHR24_6_SeedNodeStrictValidation(t *testing.T) {
	id, addr, err := seedNode("1001@127.0.0.1:11110")
	require.NoError(t, err)
	require.Equal(t, uint64(1001), id)
	require.Equal(t, "127.0.0.1:11110", addr)

	for _, bad := range []string{
		"",
		"1001",
		"1001@",
		"@127.0.0.1:11110",
		"0@127.0.0.1:11110",
		"1001@127.0.0.1:11110@extra",
		"abc@127.0.0.1:11110",
	} {
		_, _, badErr := seedNode(bad)
		require.Error(t, badErr, "malformed seed %q must fail validation", bad)
	}
}

func TestGHR24_8_PebbleShardLogStorage_ReopenLifecycleAndLimitSizeZeroCap(t *testing.T) {
	dir := t.TempDir()
	st := slot.NewPebbleShardLogStorage(nil, dir, 2)

	// Open -> append -> Close -> Open -> append -> GetLogs(limitSize=0) -> Close must succeed with zero panic or lock leak.
	require.NoError(t, st.Open())
	logs1 := make([]rafttypes.Log, 0, 10)
	for i := uint64(1); i <= 10; i++ {
		logs1 = append(logs1, rafttypes.Log{Id: i, Term: 1, Index: i, Data: []byte(fmt.Sprintf("d-%d", i))})
	}
	require.NoError(t, st.AppendLogs("slot-0", logs1, nil))
	require.NoError(t, st.Close())

	// Second Open() after Close() previously panicked syncutil.Stopper or leaked open DBs (GH-R24-8).
	require.NoError(t, st.Open())
	defer func() { _ = st.Close() }()

	gotAll, err := st.GetLogs("slot-0", 1, 11, 0)
	require.NoError(t, err)
	require.Len(t, gotAll, 10)
}

func TestWukongDB_AppendMessagesIdempotentSeqAndOpenFailureCleanup(t *testing.T) {
	// 1. Failed Open() when directory path points to an existing regular file must return error cleanly and stop dblock.
	badFile := filepath.Join(t.TempDir(), "not_a_dir")
	require.NoError(t, os.WriteFile(badFile, []byte("file"), 0644))
	badDB := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(badFile),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(2),
	))
	require.Error(t, badDB.Open())

	// 2. Concurrent AppendMessages with overlapping / replayed MessageSeqs must deduplicate <= persistedLastSeq
	//    and maintain monotonic channelLastMessageSeq without regressing or duplicating entries.
	goodDB := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(t.TempDir()),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
		wkdb.WithMaxMessageLogCountPerBatch(5),
	))
	require.NoError(t, goodDB.Open())
	defer func() { _ = goodDB.Close() }()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs := make([]wkdb.Message, 0, 12)
			for seq := uint32(1); seq <= 12; seq++ {
				msgs = append(msgs, wkdb.Message{
					RecvPacket: wkproto.RecvPacket{
						MessageID:   int64(1000 + seq),
						MessageSeq:  seq,
						ClientMsgNo: fmt.Sprintf("cno-%d", seq),
						FromUID:     "u1",
						ChannelID:   "grp_idem",
						ChannelType: wkproto.ChannelTypeGroup,
						Payload:     []byte("hello"),
					},
				})
			}
			require.NoError(t, goodDB.AppendMessages("grp_idem", wkproto.ChannelTypeGroup, msgs))
		}()
	}
	wg.Wait()

	lastSeq, _, err := goodDB.GetChannelLastMessageSeq("grp_idem", wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	require.Equal(t, uint64(12), lastSeq)

	// LoadNextRangeMsgsForSize with limitSize == 0 must still enforce MaxMessageLogCountPerBatch (5)!
	cappedMsgs, err := goodDB.LoadNextRangeMsgsForSize("grp_idem", wkproto.ChannelTypeGroup, 1, 13, 0)
	require.NoError(t, err)
	require.Len(t, cappedMsgs, 5, "MaxMessageLogCountPerBatch must apply even when limitSize == 0")
}

func TestSlotDigestGet_RBACAndRangeBounds(t *testing.T) {
	srv := &Server{
		Log: wklog.NewWKLog("test-slot-digest"),
		opts: &Options{
			ConfigOptions: &clusterconfig.Options{NodeId: 1001},
			Auth: auth.AuthConfig{
				On: true,
				Users: []auth.UserConfig{
					{
						Username: "reader",
						Permissions: auth.PermissionConfigs{
							{Resource: resource.All, Actions: []auth.Action{auth.ActionRead}},
						},
					},
				},
			},
		},
	}

	r := wkhttp.New()
	r.GET("/cluster/slots/:id/digest", srv.slotDigestGet)

	// 1. Unauthenticated / missing read permission must return 403 status in wkhttp error envelope (GH-R12-1 / GH-R17-1 / GH-R19-1):
	wUnauth := httptest.NewRecorder()
	reqUnauth := httptest.NewRequest(http.MethodGet, "/cluster/slots/0/digest", nil)
	r.GetGinRoute().ServeHTTP(wUnauth, reqUnauth)
	require.Equal(t, http.StatusBadRequest, wUnauth.Code)
	require.Contains(t, wUnauth.Body.String(), `"status":403`)

	// 2. WalkLogsRange uint64 overflow protection on slot storage:
	dir := t.TempDir()
	st := slot.NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, st.Open())
	defer func() { _ = st.Close() }()
	require.NoError(t, st.AppendLogs("slot-0", []rafttypes.Log{
		{Id: 1, Term: 1, Index: 1, Data: []byte("a")},
		{Id: 2, Term: 1, Index: 2, Data: []byte("b")},
	}, nil))
	logs, err := st.GetLogs("slot-0", 1, ^uint64(0), 0)
	require.NoError(t, err)
	require.Len(t, logs, 2)
}
