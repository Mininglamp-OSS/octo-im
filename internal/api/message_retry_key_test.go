package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type localTmpCluster struct{ icluster.ICluster }

func (localTmpCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	return &clustertypes.Node{Id: 1}, nil
}

type recordingTagManager struct{ service.ITagManager }

func (recordingTagManager) MakeTag(uids []string) (*types.Tag, error) {
	return &types.Tag{Key: "tag-" + strings.Join(uids, ",")}, nil
}
func (recordingTagManager) SetChannelTag(string, uint8, string) {}

// setupSubscriberSend serves subscriber-form sends locally and returns the
// temporary channel a persisted send to subscribers is submitted to.
func setupSubscriberSend(t *testing.T, subscribers ...string) string {
	t.Helper()
	oldCluster, oldTags := service.Cluster, service.TagManager
	t.Cleanup(func() { service.Cluster, service.TagManager = oldCluster, oldTags })
	service.Cluster = localTmpCluster{}
	service.TagManager = recordingTagManager{}
	return options.G.OrginalConvertCmdChannel(fmt.Sprintf("%d", key.HashWithString(strings.Join(subscribers, ","))))
}

func submittedKeys(l *fakeLeader, channel string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keys []string
	for _, e := range l.submitted {
		if p := e.Frame.(*wkproto.SendPacket); p.ChannelID == channel {
			keys = append(keys, p.ClientMsgNo)
		}
	}
	return keys
}

// A subscriber-form send whose first attempt commits but is never reported
// returns 504 with its key. Replaying that key finds the first message.
func TestMessageSendSubscribersReplaysSuppliedKey(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	tmp := setupSubscriberSend(t, "u1", "u2")
	leader.behaviors[tmp] = []attemptBehavior{silentCommitAttempt, persistAttempt}
	body := `{"header":{"sync_once":1},"from_uid":"alice","subscribers":["u1","u2"],"client_msg_no":"sk1","payload":"aGk="}`

	code, out := serveMessage(t, a, "/message/send", body)
	require.Equal(t, http.StatusGatewayTimeout, code)
	require.Equal(t, "unknown", out["data"].(map[string]any)["outcome"])
	require.Equal(t, "sk1", out["data"].(map[string]any)["client_msg_no"])

	code, _ = serveMessage(t, a, "/message/send", body)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, []string{"sk1", "sk1"}, submittedKeys(leader, tmp))
	require.Equal(t, 1, leader.durableCount(), "the replay must not create a second durable message")
}

// Without a supplied key the server generates one, returns it on 504, and a
// replay with it is deduplicated.
func TestMessageSendSubscribersReplaysGeneratedKey(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	tmp := setupSubscriberSend(t, "u3", "u4")
	leader.behaviors[tmp] = []attemptBehavior{silentCommitAttempt, persistAttempt}

	for _, supplied := range []string{``, `"client_msg_no":"  ",`} {
		leader.mu.Lock()
		leader.submitted, leader.durable = nil, map[durableKey]durableMessage{}
		leader.mu.Unlock()
		code, out := serveMessage(t, a, "/message/send", `{"header":{"sync_once":1},"from_uid":"alice","subscribers":["u3","u4"],`+supplied+`"payload":"aGk="}`)
		require.Equal(t, http.StatusGatewayTimeout, code)
		generated := out["data"].(map[string]any)["client_msg_no"].(string)
		require.NotEmpty(t, strings.TrimSpace(generated), "a blank key counts as absent")

		leader.behaviors[tmp] = []attemptBehavior{persistAttempt}
		code, _ = serveMessage(t, a, "/message/send", `{"header":{"sync_once":1},"from_uid":"alice","subscribers":["u3","u4"],"client_msg_no":"`+generated+`","payload":"aGk="}`)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, []string{generated, generated}, submittedKeys(leader, tmp))
		require.Equal(t, 1, leader.durableCount())
		leader.behaviors[tmp] = []attemptBehavior{silentCommitAttempt, persistAttempt}
	}
}

// A batch key is the retry identity of every recipient's message: the
// identity is (sender, recipient, key), so it does not depend on the order or
// the subset of subscribers in a replay.
func TestMessageSendBatchReplaysWithBatchKey(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	leader.behaviors["u2"] = []attemptBehavior{silentCommitAttempt, persistAttempt}

	code, out := serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u1","u2","u3"],"client_msg_no":"bk1","payload":"aGk="}`)
	require.Equal(t, http.StatusGatewayTimeout, code)
	require.Equal(t, []any{"u2"}, out["unknown_uids"])
	require.Equal(t, "bk1", out["client_msg_no"])
	require.Equal(t, 3, leader.durableCount())

	// Replay only the unknown subset.
	code, out = serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u2"],"client_msg_no":"bk1","payload":"aGk="}`)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, out["fail_uids"])
	require.Equal(t, "bk1", out["client_msg_no"])
	require.Equal(t, 3, leader.durableCount(), "u2's committed message is found, not duplicated")

	// Replay the whole batch in another order.
	code, out = serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u3","u1","u2"],"client_msg_no":"bk1","payload":"aGk="}`)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, out["fail_uids"])
	require.Equal(t, 3, leader.durableCount())
	for _, uid := range []string{"u1", "u2", "u3"} {
		for _, k := range submittedKeys(leader, uid) {
			require.Equal(t, "bk1", k)
		}
	}

	// The same key with another payload is a conflicting retry: rejected per
	// recipient, nothing new is stored.
	code, out = serveMessage(t, a, "/message/sendbatch", `{"header":{"sync_once":1},"from_uid":"alice","subscribers":["u1","u2"],"client_msg_no":"bk1","payload":"b3RoZXI="}`)
	require.Equal(t, http.StatusOK, code, "definite failures keep the partial-failure contract")
	require.ElementsMatch(t, []any{"u1", "u2"}, out["fail_uids"])
	require.Equal(t, 3, leader.durableCount())
}

// Without a supplied key one key is generated for the batch and returned, so
// the unknown subset can be replayed.
func TestMessageSendBatchReplaysGeneratedKey(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	leader.behaviors["u5"] = []attemptBehavior{silentCommitAttempt, persistAttempt}

	code, out := serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u4","u5"],"client_msg_no":" ","payload":"aGk="}`)
	require.Equal(t, http.StatusGatewayTimeout, code)
	generated, _ := out["client_msg_no"].(string)
	require.NotEmpty(t, strings.TrimSpace(generated))
	require.Equal(t, []string{generated}, submittedKeys(leader, "u4"))

	code, _ = serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u5"],"client_msg_no":"`+generated+`","payload":"aGk="}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, 2, leader.durableCount())
}

// Duplicate subscribers: with a key they name one message and share its
// outcome; without one they keep the previous behaviour (one message per
// entry) and no reusable key is claimed.
func TestMessageSendBatchDuplicateSubscribers(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)

	code, out := serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u7","u7"],"client_msg_no":"dk","payload":"aGk="}`)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, out["fail_uids"])
	require.Equal(t, "dk", out["client_msg_no"])
	require.Equal(t, 1, leader.durableCount())

	code, out = serveMessage(t, a, "/message/sendbatch", `{"from_uid":"alice","subscribers":["u8","u8"],"payload":"aGk="}`)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, out["fail_uids"])
	require.NotContains(t, out, "client_msg_no")
	keys := submittedKeys(leader, "u8")
	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1])
	require.Equal(t, 3, leader.durableCount())
}
