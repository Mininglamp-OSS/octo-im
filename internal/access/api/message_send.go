package api

import (
	"encoding/base64"
	"net/http"

	"github.com/WuKongIM/WuKongIM/internal/observability/diagnostics/tracectx"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/gin-gonic/gin"
)

type sendMessageRequest struct {
	FromUID       string                   `json:"from_uid"`
	LegacyFromUID string                   `json:"sender_uid"`
	ChannelID     string                   `json:"channel_id"`
	ChannelType   uint8                    `json:"channel_type"`
	ClientMsgNo   string                   `json:"client_msg_no"`
	Payload       string                   `json:"payload"`
	Subscribers   []string                 `json:"subscribers"`
	Header        sendMessageHeaderRequest `json:"header"`
	Setting       uint8                    `json:"setting"`
	Topic         string                   `json:"topic"`
	Expire        uint32                   `json:"expire"`
	NoPersist     int                      `json:"no_persist"`
	SyncOnce      int                      `json:"sync_once"`
}

type sendMessageHeaderRequest struct {
	// NoPersist marks the send as non-durable when non-zero.
	NoPersist int `json:"no_persist"`
	// RedDot controls whether the persisted message increments unread indicators.
	RedDot int `json:"red_dot"`
	// SyncOnce marks the send as a one-shot command-channel message when non-zero.
	SyncOnce int `json:"sync_once"`
}

type legacySendMessageResponse struct {
	MessageID   int64                   `json:"message_id"`
	MessageSeq  uint64                  `json:"message_seq"`
	ClientMsgNo string                  `json:"client_msg_no"`
	Reason      uint8                   `json:"reason"`
	Status      int                     `json:"status"`
	Data        sendMessageResponseData `json:"data"`
}

type durableSendMessageResponse struct {
	Status int                     `json:"status"`
	Data   sendMessageResponseData `json:"data"`
}

type sendMessageResponseData struct {
	MessageID   int64  `json:"message_id"`
	MessageSeq  uint64 `json:"message_seq"`
	ClientMsgNo string `json:"client_msg_no"`
}

func (s *Server) handleSendMessage(c *gin.Context) {
	s.handleMessageSend(c, false)
}

func (s *Server) handleSendDurableMessage(c *gin.Context) {
	s.handleMessageSend(c, true)
}

func (s *Server) handleMessageSend(c *gin.Context, requireDurableResult bool) {
	var req sendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid request")
		return
	}
	if req.FromUID == "" {
		req.FromUID = req.LegacyFromUID
	}
	requestScoped := len(req.Subscribers) > 0
	if req.FromUID == "" || req.Payload == "" {
		writeJSONError(c, http.StatusBadRequest, "invalid request")
		return
	}
	if requestScoped {
		if req.ChannelID != "" {
			writeJSONError(c, http.StatusBadRequest, "invalid request")
			return
		}
	} else if req.ChannelID == "" || req.ChannelType == 0 {
		writeJSONError(c, http.StatusBadRequest, "invalid request")
		return
	}

	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid payload")
		return
	}

	if s == nil || s.messages == nil {
		writeJSONError(c, http.StatusInternalServerError, "message usecase not configured")
		return
	}

	reqCtx := c.Request.Context()
	if traceID, ok := tracectx.ValidateHeaderTraceID(c.GetHeader("X-WK-Trace-ID")); ok {
		reqCtx = tracectx.WithContext(reqCtx, tracectx.Context{TraceID: traceID, Sampled: true})
	}
	reqCtx, traceCtx := tracectx.Ensure(reqCtx, nil)
	noPersist := req.Header.NoPersist != 0 || req.NoPersist != 0
	syncOnce := req.Header.SyncOnce != 0 || req.SyncOnce != 0
	if requireDurableResult && (req.ClientMsgNo == "" || noPersist) {
		writeJSONError(c, http.StatusBadRequest, "durable message requires client_msg_no and persistence")
		return
	}
	channelID := req.ChannelID
	channelType := req.ChannelType
	if requestScoped {
		channelID = ""
		channelType = 0
	}

	result, err := s.messages.Send(reqCtx, message.SendCommand{
		TraceID:            traceCtx.TraceID,
		Framer:             frame.Framer{NoPersist: noPersist, RedDot: req.Header.RedDot != 0, SyncOnce: syncOnce},
		Setting:            frame.Setting(req.Setting),
		Topic:              req.Topic,
		Expire:             req.Expire,
		FromUID:            req.FromUID,
		DeviceID:           s.trustedMessageDeviceID,
		ChannelID:          channelID,
		ChannelType:        channelType,
		RequestSubscribers: req.Subscribers,
		ClientMsgNo:        req.ClientMsgNo,
		Payload:            payload,
		ProtocolVersion:    frame.LatestVersion,
	})
	if err != nil {
		if status, msg, ok := mapSendError(err); ok {
			writeJSONError(c, status, msg)
			return
		}
		writeJSONError(c, http.StatusInternalServerError, err.Error())
		return
	}
	data := sendMessageResponseData{
		MessageID:   result.MessageID,
		MessageSeq:  result.MessageSeq,
		ClientMsgNo: req.ClientMsgNo,
	}
	if !requireDurableResult {
		c.JSON(http.StatusOK, legacySendMessageResponse{
			MessageID:   result.MessageID,
			MessageSeq:  result.MessageSeq,
			ClientMsgNo: req.ClientMsgNo,
			Reason:      uint8(result.Reason),
			Status:      http.StatusOK,
			Data:        data,
		})
		return
	}
	if result.Reason != frame.ReasonSuccess {
		writeJSONError(c, http.StatusForbidden, "message rejected")
		return
	}
	if result.MessageID <= 0 || result.MessageSeq == 0 {
		writeJSONError(c, http.StatusInternalServerError, "message did not commit")
		return
	}
	c.JSON(http.StatusOK, durableSendMessageResponse{Status: http.StatusOK, Data: data})
}

func writeJSONError(c *gin.Context, status int, message string) {
	if c == nil {
		return
	}
	if message == "" {
		message = http.StatusText(status)
	}
	c.JSON(status, gin.H{"error": message})
}
