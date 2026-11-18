package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	clusterTypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/sendgrid/rest"
	"go.uber.org/zap"
)

const maxRecentMsgCount = 500

func clampRecentMsgCount(n int) int {
	if n <= 0 {
		return 15
	}
	if n > maxRecentMsgCount {
		return maxRecentMsgCount
	}
	return n
}

func userSlotRetryBudget() time.Duration {
	if options.G != nil && options.G.Cluster.ReqTimeout > 0 {
		return options.G.Cluster.ReqTimeout
	}
	return 10 * time.Second
}

func (s *conversation) getChannelLastMsgSeq(ctx context.Context, channelID string, channelType uint8) (uint64, error) {
	timeout := min(userSlotRetryBudget(), 3500*time.Millisecond)
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	delay := 40 * time.Millisecond
	for {
		rem := time.Until(func() time.Time { d, _ := deadlineCtx.Deadline(); return d }())
		if rem <= 0 {
			return 0, deadlineCtx.Err()
		}
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, min(1200*time.Millisecond, rem))
		seq, err := service.Cluster.GetChannelLastMessageSeq(attemptCtx, channelID, channelType)
		attemptCancel()
		if err == nil {
			return seq, nil
		}
		if ctx.Err() != nil || deadlineCtx.Err() != nil || !isTransientConversationSlotHandoffError(err) {
			return 0, err
		}
		select {
		case <-deadlineCtx.Done():
			return 0, err
		case <-time.After(delay):
		}
		delay = min(delay*2, 160*time.Millisecond)
	}
}

func respondConversationReadRetry(c *wkhttp.Context) {
	c.JSON(http.StatusServiceUnavailable, map[string]interface{}{"msg": "retry required", "status": http.StatusServiceUnavailable})
}

func isTransientPeerForwardResponse(resp *rest.Response) bool {
	if resp == nil {
		return true
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return true
	}
	if resp.StatusCode == http.StatusBadRequest {
		msg := strings.ToLower(resp.Body)
		return strings.Contains(msg, ": eof") ||
			strings.Contains(msg, "\"eof\"") ||
			strings.Contains(msg, "connection refused") ||
			strings.Contains(msg, "no such host") ||
			strings.Contains(msg, "connect not authed") ||
			strings.Contains(msg, "conn is nil") ||
			strings.Contains(msg, "leader not found") ||
			strings.Contains(msg, "not leader") ||
			strings.Contains(msg, "proposal dropped") ||
			strings.Contains(msg, "receive propose resp error") ||
			strings.Contains(msg, "foward propose failed") ||
			strings.Contains(msg, "raft not found") ||
			strings.Contains(msg, "raft not exist") ||
			strings.Contains(msg, "stopped")
	}
	return false
}

func isTransientConversationSlotHandoffError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "retry required") ||
		strings.Contains(msg, "proposal dropped") ||
		strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "receive propose resp error") ||
		strings.Contains(msg, "foward propose failed") ||
		strings.Contains(msg, "raft not found") ||
		strings.Contains(msg, "raft not exist") ||
		strings.Contains(msg, "stopped") ||
		strings.Contains(msg, "leadership changed") ||
		strings.Contains(msg, "leader is 0") ||
		strings.Contains(msg, "leader not found") ||
		strings.Contains(msg, "slot leader") ||
		strings.Contains(msg, "propose timeout") ||
		strings.Contains(msg, "apply wait timeout") ||
		strings.Contains(msg, "connect not authed") ||
		strings.Contains(msg, "conn is nil") ||
		strings.Contains(msg, "connect is not connected") ||
		strings.Contains(msg, "node not found") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout")
}

// forwardToUserSlotLeader routes a user-scoped conversation request to the
// current user-slot leader within options.G.Cluster.ReqTimeout (default 10s),
// returning HTTP 503 {"msg":"retry required","status":503} on election timeout
// instead of false HTTP 400 errors (closing V11-5).
func (s *conversation) forwardToUserSlotLeader(c *wkhttp.Context, uid string, channelType uint8, bodyBytes []byte) (handleLocal bool) {
	var log wklog.Log
	if s != nil {
		log = s.Log
	}
	return forwardRequestToUserSlotLeader(log, c, uid, channelType, bodyBytes)
}

func forwardRequestToUserSlotLeader(log wklog.Log, c *wkhttp.Context, uid string, channelType uint8, bodyBytes []byte) (handleLocal bool) {
	if service.Cluster == nil {
		return true
	}
	reqCtx := context.Background()
	if c.Request != nil && c.Request.Context() != nil {
		reqCtx = c.Request.Context()
	}
	budget := userSlotRetryBudget()
	deadlineCtx, cancel := context.WithTimeout(reqCtx, budget)
	defer cancel()

	delay := 50 * time.Millisecond
	for {
		leaderInfo, err := service.Cluster.SlotLeaderOfChannel(uid, channelType)
		if err == nil && leaderInfo != nil && leaderInfo.Id != 0 {
			if leaderInfo.Id == options.G.Cluster.NodeId {
				return true
			}
			if strings.TrimSpace(leaderInfo.ApiServerAddr) != "" {
				queryMap := map[string]string{}
				if c.Request != nil && c.Request.URL != nil {
					for key, value := range c.Request.URL.Query() {
						if len(value) > 0 {
							queryMap[key] = value[0]
						}
					}
				}
				method := "POST"
				path := ""
				var baseHeaders map[string]string
				if c.Request != nil {
					if c.Request.Method != "" {
						method = strings.ToUpper(c.Request.Method)
					}
					if c.Request.URL != nil {
						path = c.Request.URL.Path
					}
					baseHeaders = c.CopyRequestHeader(c.Request)
				}
				targetURL := fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, path)
				if log != nil {
					log.Debug("转发请求：", zap.String("url", targetURL))
				}
				attemptTimeout := min(1500*time.Millisecond, budget)
				attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, attemptTimeout)
				resp, sendErr := service.SendPeerRequestWithContext(attemptCtx, rest.Request{
					Method:      rest.Method(method),
					BaseURL:     targetURL,
					Headers:     service.TrustedPeerNodeHeaders(leaderInfo, baseHeaders),
					Body:        bodyBytes,
					QueryParams: queryMap,
				})
				attemptCancel()
				if sendErr == nil && resp != nil && !isTransientPeerForwardResponse(resp) {
					c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
					c.Writer.WriteHeader(resp.StatusCode)
					_, _ = c.Writer.Write([]byte(resp.Body))
					return false
				}
			}
		} else if err != nil && !isTransientConversationSlotHandoffError(err) && !options.G.ClusterOn() {
			c.ResponseError(err)
			return false
		}

		select {
		case <-deadlineCtx.Done():
			respondConversationReadRetry(c)
			return false
		case <-time.After(delay):
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

// forwardOrHydrateConversationSync routes /conversation/sync to the user-slot leader,
// and if an unpatched 21cd8f88 user-slot leader returns HTTP 503 when msg_count > 0
// (due to 21cd8f88->21cd8f88 missing ManagerToken on /conversation/syncMessages/v2),
// fetches authoritative conversation metadata from that 21cd8f88 slot leader using
// TrustedPeerNodeHeaders and returns it for authenticated v2 message hydration.
func (s *conversation) forwardOrHydrateConversationSync(c *wkhttp.Context, uid string, msgCount int64, bodyBytes []byte) ([]wkdb.Conversation, bool, bool) {
	if service.Cluster == nil {
		return nil, false, true
	}
	reqCtx := context.Background()
	if c.Request != nil && c.Request.Context() != nil {
		reqCtx = c.Request.Context()
	}
	budget := userSlotRetryBudget()
	deadlineCtx, cancel := context.WithTimeout(reqCtx, budget)
	defer cancel()

	delay := 50 * time.Millisecond
	for {
		leaderInfo, err := service.Cluster.SlotLeaderOfChannel(uid, wkproto.ChannelTypePerson)
		if err == nil && leaderInfo != nil && leaderInfo.Id != 0 {
			if leaderInfo.Id == options.G.Cluster.NodeId {
				return nil, false, true
			}
			if strings.TrimSpace(leaderInfo.ApiServerAddr) != "" {
				if msgCount > 0 && s != nil && s.s != nil && s.s.apiServer != nil &&
					s.s.apiServer.probeVerifiedLegacy21cd8f88Peer(deadlineCtx, service.Cluster, leaderInfo.Id) {
					if convs, ok := s.fetchPeerUserConversations(deadlineCtx, leaderInfo, uid); ok {
						return convs, true, true
					}
				}
				var baseHeaders map[string]string
				if c.Request != nil {
					baseHeaders = c.CopyRequestHeader(c.Request)
				}
				targetURL := fmt.Sprintf("%s/conversation/sync", strings.TrimRight(leaderInfo.ApiServerAddr, "/"))
				attemptTimeout := min(1500*time.Millisecond, budget)
				attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, attemptTimeout)
				resp, sendErr := service.SendPeerRequestWithContext(attemptCtx, rest.Request{
					Method:  rest.Post,
					BaseURL: targetURL,
					Headers: service.TrustedPeerNodeHeaders(leaderInfo, baseHeaders),
					Body:    bodyBytes,
				})
				attemptCancel()
				if sendErr == nil && resp != nil && !isTransientPeerForwardResponse(resp) {
					if resp.StatusCode == http.StatusServiceUnavailable && msgCount > 0 &&
						s != nil && s.s != nil && s.s.apiServer != nil &&
						s.s.apiServer.probeVerifiedLegacy21cd8f88Peer(deadlineCtx, service.Cluster, leaderInfo.Id) {
						if convs, ok := s.fetchPeerUserConversations(deadlineCtx, leaderInfo, uid); ok {
							return convs, true, true
						}
					}
					c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
					c.Writer.WriteHeader(resp.StatusCode)
					_, _ = c.Writer.Write([]byte(resp.Body))
					return nil, false, false
				}
			}
		} else if err != nil && !isTransientConversationSlotHandoffError(err) && !options.G.ClusterOn() {
			c.ResponseError(err)
			return nil, false, false
		}

		select {
		case <-deadlineCtx.Done():
			respondConversationReadRetry(c)
			return nil, false, false
		case <-time.After(delay):
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

func (s *conversation) fetchPeerUserConversations(ctx context.Context, leaderInfo *clusterTypes.Node, uid string) ([]wkdb.Conversation, bool) {
	if leaderInfo == nil || strings.TrimSpace(leaderInfo.ApiServerAddr) == "" {
		return nil, false
	}
	baseAddr := strings.TrimRight(strings.TrimSpace(leaderInfo.ApiServerAddr), "/")
	headers := service.TrustedPeerNodeHeaders(leaderInfo, map[string]string{"Content-Type": "application/json"})

	// 1. Fetch persisted conversations (including ReadToMsgSeq) directly from leaderInfo's local DB
	limit := 1000
	if options.G != nil && options.G.Conversation.UserMaxCount > 0 {
		limit = options.G.Conversation.UserMaxCount
	}
	searchCtx, searchCancel := context.WithTimeout(ctx, 800*time.Millisecond)
	searchResp, searchErr := service.SendPeerRequestWithContext(searchCtx, rest.Request{
		Method:  rest.Get,
		BaseURL: fmt.Sprintf("%s/cluster/conversations", baseAddr),
		Headers: headers,
		QueryParams: map[string]string{
			"uid":          uid,
			"node_id":      fmt.Sprintf("%d", leaderInfo.Id),
			"limit":        fmt.Sprintf("%d", limit),
			"current_page": "1",
		},
	})
	searchCancel()

	byKey := make(map[string]wkdb.Conversation)
	convs := make([]wkdb.Conversation, 0)
	if searchErr == nil && searchResp != nil && searchResp.StatusCode == http.StatusOK {
		var payload struct {
			Data []struct {
				Id             uint64                `json:"id"`
				Uid            string                `json:"uid"`
				Type           wkdb.ConversationType `json:"type"`
				ChannelId      string                `json:"channel_id"`
				ChannelType    uint8                 `json:"channel_type"`
				ReadedToMsgSeq uint64                `json:"readed_to_msg_seq"`
				CreatedAt      int64                 `json:"created_at"`
				UpdatedAt      int64                 `json:"updated_at"`
			} `json:"data"`
		}
		if json.Unmarshal([]byte(searchResp.Body), &payload) == nil {
			for _, item := range payload.Data {
				if item.ChannelId == "" || item.ChannelType == 0 {
					continue
				}
				var createdPtr, updatedPtr *time.Time
				if item.CreatedAt > 0 {
					t := time.Unix(item.CreatedAt, 0)
					createdPtr = &t
				}
				if item.UpdatedAt > 0 {
					t := time.Unix(item.UpdatedAt, 0)
					updatedPtr = &t
				}
				cv := wkdb.Conversation{
					Id:           item.Id,
					Uid:          uid,
					Type:         wkdb.ConversationTypeChat,
					ChannelId:    item.ChannelId,
					ChannelType:  item.ChannelType,
					ReadToMsgSeq: item.ReadedToMsgSeq,
					CreatedAt:    createdPtr,
					UpdatedAt:    updatedPtr,
				}
				if service.Store != nil {
					if localCv, lErr := service.Store.GetConversation(uid, item.ChannelId, item.ChannelType); lErr == nil && localCv.DeletedAtMsgSeq > 0 {
						cv.DeletedAtMsgSeq = localCv.DeletedAtMsgSeq
					}
				}
				k := wkutil.ChannelToKey(cv.ChannelId, cv.ChannelType)
				byKey[k] = cv
				convs = append(convs, cv)
			}
		}
	}

	// 2. Merge cached active channels from leaderInfo's ConversationManager via /conversation/channels
	chBody, _ := json.Marshal(map[string]string{"uid": uid})
	chCtx, chCancel := context.WithTimeout(ctx, 800*time.Millisecond)
	chResp, chErr := service.SendPeerRequestWithContext(chCtx, rest.Request{
		Method:  rest.Post,
		BaseURL: fmt.Sprintf("%s/conversation/channels", baseAddr),
		Headers: headers,
		Body:    chBody,
	})
	chCancel()
	if chErr != nil || chResp == nil || chResp.StatusCode != http.StatusOK {
		return nil, false
	}
	var chItems []struct {
		ChannelId   string `json:"channel_id"`
		ChannelType uint8  `json:"channel_type"`
	}
	if err := json.Unmarshal([]byte(chResp.Body), &chItems); err != nil {
		return nil, false
	}
	for _, ch := range chItems {
		if ch.ChannelId == "" || ch.ChannelType == 0 {
			continue
		}
		k := wkutil.ChannelToKey(ch.ChannelId, ch.ChannelType)
		if _, exists := byKey[k]; !exists {
			cv := wkdb.Conversation{
				Uid:         uid,
				Type:        wkdb.ConversationTypeChat,
				ChannelId:   ch.ChannelId,
				ChannelType: ch.ChannelType,
			}
			byKey[k] = cv
			convs = append(convs, cv)
		}
	}
	return convs, true
}
