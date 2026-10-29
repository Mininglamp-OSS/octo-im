package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestRecentReadPeerManagerToken(t *testing.T) {
	const managerToken = "recent-read-test-manager-token"
	for _, legacy := range []bool{false, true} {
		name := "v2"
		if legacy {
			name = "legacy fallback"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name        string
				peerToken   string
				clientToken string
				ok          bool
			}{
				{"matching token", managerToken, managerToken, true},
				{"missing token rejected", managerToken, "", false},
				{"wrong token rejected", managerToken, "wrong-test-token", false},
				{"manager authentication unset", "", "", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var v2Calls, legacyCalls, emptyTokenHeaders atomic.Int32
					peer := recentPeer(t, func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/conversation/syncMessages/v2" {
							v2Calls.Add(1)
						} else if r.URL.Path == "/conversation/syncMessages" {
							legacyCalls.Add(1)
						} else {
							http.NotFound(w, r)
							return
						}
						if len(r.Header.Values("token")) > 0 && r.Header.Get("token") == "" {
							emptyTokenHeaders.Add(1)
						}
						// Match the production peer's authentication gate before
						// checking route availability or returning any message data.
						if tc.peerToken != "" && r.Header.Get("token") != tc.peerToken {
							w.WriteHeader(http.StatusUnauthorized)
							return
						}
						if legacy && r.URL.Path == "/conversation/syncMessages/v2" {
							http.NotFound(w, r)
							return
						}
						channels := []*channelRecentMessage{{
							ChannelId: "group", ChannelType: 2,
							Messages: types.MessageRespSlice{&types.MessageResp{MessageSeq: 18}},
						}}
						if legacy {
							_ = json.NewEncoder(w).Encode(channels)
						} else {
							_ = json.NewEncoder(w).Encode(recentReadResponse{Version: recentReadVersion, Channels: channels})
						}
					})
					reader := setupRecentRead(t, &recentReadCluster{nodes: map[uint64]*clustertypes.Node{
						2: {Id: 2, ApiServerAddr: peer.URL},
					}})
					options.G.ManagerToken = tc.clientToken
					got, err := reader.requestSyncMessage(context.Background(), 2,
						[]*channelRecentMessageReq{{ChannelId: "group", ChannelType: 2}}, "alice", 10, true)
					if tc.ok {
						require.NoError(t, err)
						require.Len(t, got, 1)
						require.Equal(t, "group", got[0].ChannelId)
						require.Len(t, got[0].Messages, 1)
						require.Equal(t, uint64(18), got[0].Messages[0].MessageSeq)
					} else {
						require.Error(t, err)
						require.Nil(t, got)
					}
					require.Equal(t, int32(1), v2Calls.Load())
					if legacy && tc.ok {
						require.Equal(t, int32(1), legacyCalls.Load(), "fallback must also authenticate")
					} else {
						require.Zero(t, legacyCalls.Load(), "authentication failures must not downgrade")
					}
					require.Zero(t, emptyTokenHeaders.Load(), "unset authentication must not add an empty header")
				})
			}
		})
	}
}
