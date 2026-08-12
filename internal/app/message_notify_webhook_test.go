package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestMessageNotifyWebhookSignsRawPayload(t *testing.T) {
	const secret = "test-message-notify-secret"

	var gotBody []byte
	var gotContentType string
	var gotSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		gotContentType = r.Header.Get("Content-Type")
		gotSignature = r.Header.Get("X-Signature-256")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	notifier, err := newMessageNotifyWebhook(messageNotifyWebhookConfig{
		URL:     server.URL,
		Secret:  secret,
		Timeout: time.Second,
	})
	require.NoError(t, err)

	msg := channel.Message{
		MessageID:   99,
		MessageSeq:  7,
		Framer:      frame.Framer{NoPersist: false, RedDot: true, SyncOnce: true},
		Setting:     frame.SettingReceiptEnabled,
		ClientMsgNo: "fedg0-message-1",
		Expire:      60,
		Timestamp:   1_700_000_000,
		ChannelID:   "group-1",
		ChannelType: frame.ChannelTypeGroup,
		FromUID:     "shadow-u1",
		Payload:     []byte(`{"type":1,"content":"hello"}`),
	}

	err = notifier.SubmitCommitted(context.Background(), deliveryruntime.CommittedEnvelope{Message: msg})
	require.NoError(t, err)
	require.Equal(t, "application/json", gotContentType)

	mac := hmac.New(sha256.New, []byte(secret))
	_, err = mac.Write(gotBody)
	require.NoError(t, err)
	require.Equal(t, "sha256="+hex.EncodeToString(mac.Sum(nil)), gotSignature)

	var payload []struct {
		Header struct {
			NoPersist int `json:"no_persist"`
			RedDot    int `json:"red_dot"`
			SyncOnce  int `json:"sync_once"`
		} `json:"header"`
		Setting     uint8  `json:"setting"`
		ClientMsgNo string `json:"client_msg_no"`
		MessageID   uint64 `json:"message_id"`
		MessageSeq  uint64 `json:"message_seq"`
		FromUID     string `json:"from_uid"`
		ChannelID   string `json:"channel_id"`
		ChannelType uint8  `json:"channel_type"`
		Expire      uint32 `json:"expire"`
		Timestamp   int32  `json:"timestamp"`
		Payload     string `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(gotBody, &payload))
	require.Len(t, payload, 1)
	require.Equal(t, 0, payload[0].Header.NoPersist)
	require.Equal(t, 1, payload[0].Header.RedDot)
	require.Equal(t, 1, payload[0].Header.SyncOnce)
	require.Equal(t, uint8(frame.SettingReceiptEnabled), payload[0].Setting)
	require.Equal(t, msg.ClientMsgNo, payload[0].ClientMsgNo)
	require.Equal(t, msg.MessageID, payload[0].MessageID)
	require.Equal(t, msg.MessageSeq, payload[0].MessageSeq)
	require.Equal(t, msg.FromUID, payload[0].FromUID)
	require.Equal(t, msg.ChannelID, payload[0].ChannelID)
	require.Equal(t, msg.ChannelType, payload[0].ChannelType)
	require.Equal(t, msg.Expire, payload[0].Expire)
	require.Equal(t, msg.Timestamp, payload[0].Timestamp)
	require.Equal(t, base64.StdEncoding.EncodeToString(msg.Payload), payload[0].Payload)
}

func TestMessageNotifyWebhookRejectsIncompleteConfig(t *testing.T) {
	_, err := newMessageNotifyWebhook(messageNotifyWebhookConfig{URL: "https://octo.example.test/v1/webhook/message/notify"})
	require.Error(t, err)

	_, err = newMessageNotifyWebhook(messageNotifyWebhookConfig{Secret: "test-secret"})
	require.Error(t, err)
}

func TestMessageNotifyWebhookReplaysAfterNonOKResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	notifier, err := newMessageNotifyWebhook(messageNotifyWebhookConfig{
		URL:     server.URL,
		Secret:  "test-message-notify-secret",
		Timeout: time.Second,
	})
	require.NoError(t, err)

	key := channel.ChannelKey("channel/2/group-1")
	source := &fakeCommittedReplayLog{
		channels:  []committedReplayChannel{{Key: key, ID: channel.ChannelID{ID: "group-1", Type: frame.ChannelTypeGroup}}},
		committed: map[channel.ChannelKey]uint64{key: 1},
		messages: map[channel.ChannelKey][]channel.Message{key: {{
			MessageID: 1, MessageSeq: 1, ChannelID: "group-1", ChannelType: frame.ChannelTypeGroup,
			FromUID: "shadow-u1", ClientMsgNo: "fedg0-message-1", Payload: []byte("hello"),
		}}},
	}

	first := newCommittedReplayer(committedReplayerConfig{
		Log:        source,
		Delivery:   notifier,
		CursorName: messageNotifyWebhookCursorName,
	})
	require.Error(t, first.RunOnce(context.Background()))
	require.Empty(t, source.cursors, "a non-200 response must not advance the durable webhook cursor")

	second := newCommittedReplayer(committedReplayerConfig{
		Log:        source,
		Delivery:   notifier,
		CursorName: messageNotifyWebhookCursorName,
	})
	require.NoError(t, second.RunOnce(context.Background()))
	require.Equal(t, uint64(1), source.cursors[key], "a restarted replayer must advance only after HTTP 200")
	require.Equal(t, int32(2), calls.Load())
}
