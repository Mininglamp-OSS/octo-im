package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

const (
	// messageNotifyWebhookCursorName keeps msg.notify progress independent from
	// realtime delivery and conversation projection replay cursors.
	messageNotifyWebhookCursorName     = "message_notify_v1"
	defaultMessageNotifyWebhookTimeout = 15 * time.Second
)

// messageNotifyWebhookConfig is the construction-time configuration for the
// signed msg.notify callback. It intentionally contains no logging fields so a
// secret cannot be emitted by a formatter accidentally.
type messageNotifyWebhookConfig struct {
	URL     string
	Secret  string
	Timeout time.Duration
}

// messageNotifyWebhook sends one committed message per request. Its caller is
// responsible for advancing the durable replay cursor only after this method
// returns nil.
type messageNotifyWebhook struct {
	endpoint string
	secret   []byte
	client   *http.Client
}

// messageNotifyWebhookPayload matches octo-server modules/webhook.MsgResp.
// Payload is a byte slice so encoding/json keeps the established base64 wire
// representation used by the existing message webhook endpoint.
type messageNotifyWebhookPayload struct {
	Header      messageNotifyWebhookHeader `json:"header"`
	Setting     uint8                      `json:"setting"`
	ClientMsgNo string                     `json:"client_msg_no"`
	MessageID   int64                      `json:"message_id"`
	MessageSeq  uint32                     `json:"message_seq"`
	FromUID     string                     `json:"from_uid"`
	ChannelID   string                     `json:"channel_id"`
	ChannelType uint8                      `json:"channel_type"`
	Expire      uint32                     `json:"expire"`
	Timestamp   int32                      `json:"timestamp"`
	Payload     []byte                     `json:"payload"`
}

type messageNotifyWebhookHeader struct {
	NoPersist int `json:"no_persist"`
	RedDot    int `json:"red_dot"`
	SyncOnce  int `json:"sync_once"`
}

func newMessageNotifyWebhook(cfg messageNotifyWebhookConfig) (*messageNotifyWebhook, error) {
	if err := validateMessageNotifyWebhookConfig(WebhookConfig{
		HTTPAddr:        cfg.URL,
		MsgNotifySecret: cfg.Secret,
		Timeout:         cfg.Timeout,
	}); err != nil {
		return nil, err
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultMessageNotifyWebhookTimeout
	}
	return &messageNotifyWebhook{
		endpoint: strings.TrimSpace(cfg.URL),
		secret:   append([]byte(nil), []byte(cfg.Secret)...),
		client:   &http.Client{Timeout: cfg.Timeout},
	}, nil
}

func validateMessageNotifyWebhookConfig(cfg WebhookConfig) error {
	endpoint := strings.TrimSpace(cfg.HTTPAddr)
	if endpoint == "" {
		return errors.New("message notify webhook HTTP address must be set when enabled")
	}
	if strings.TrimSpace(cfg.MsgNotifySecret) == "" {
		return errors.New("message notify webhook secret must be set when enabled")
	}
	if cfg.Timeout < 0 {
		return errors.New("message notify webhook timeout must be > 0")
	}
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return errors.New("message notify webhook HTTP address must be an absolute HTTP(S) URL without user credentials")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("message notify webhook HTTP address must use HTTP or HTTPS")
	}
	return nil
}

// SubmitCommitted serializes and signs the exact JSON bytes passed to the
// HTTP client. Any transport error or response other than HTTP 200 leaves the
// caller's durable cursor unchanged so it can replay this message later.
func (w *messageNotifyWebhook) SubmitCommitted(ctx context.Context, env deliveryruntime.CommittedEnvelope) error {
	if w == nil || w.client == nil {
		return errors.New("message notify webhook is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := messageNotifyWebhookPayloadFromMessage(env.Message)
	if err != nil {
		return err
	}
	body, err := json.Marshal([]messageNotifyWebhookPayload{payload})
	if err != nil {
		return fmt.Errorf("marshal message notify webhook payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create message notify webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature-256", messageNotifyWebhookSignature(body, w.secret))

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("send message notify webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("message notify webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func messageNotifyWebhookPayloadFromMessage(msg channel.Message) (messageNotifyWebhookPayload, error) {
	if msg.MessageID > uint64(^uint64(0)>>1) {
		return messageNotifyWebhookPayload{}, errors.New("message notify webhook message ID exceeds int64")
	}
	if msg.MessageSeq > uint64(^uint32(0)) {
		return messageNotifyWebhookPayload{}, errors.New("message notify webhook message sequence exceeds uint32")
	}
	return messageNotifyWebhookPayload{
		Header: messageNotifyWebhookHeader{
			NoPersist: boolToInt(msg.Framer.NoPersist),
			RedDot:    boolToInt(msg.Framer.RedDot),
			SyncOnce:  boolToInt(msg.Framer.SyncOnce),
		},
		Setting:     uint8(msg.Setting),
		ClientMsgNo: msg.ClientMsgNo,
		MessageID:   int64(msg.MessageID),
		MessageSeq:  uint32(msg.MessageSeq),
		FromUID:     msg.FromUID,
		ChannelID:   msg.ChannelID,
		ChannelType: msg.ChannelType,
		Expire:      msg.Expire,
		Timestamp:   msg.Timestamp,
		Payload:     append([]byte(nil), msg.Payload...),
	}, nil
}

func messageNotifyWebhookSignature(body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
