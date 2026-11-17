package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
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
	timeout := userSlotRetryBudget()
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	delay := 40 * time.Millisecond
	for {
		seq, err := service.Cluster.GetChannelLastMessageSeq(deadlineCtx, channelID, channelType)
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
		delay = min(delay*2, 200*time.Millisecond)
	}
}

func respondConversationReadRetry(c *wkhttp.Context) {
	c.JSON(http.StatusServiceUnavailable, map[string]interface{}{"msg": "retry required", "status": http.StatusServiceUnavailable})
}

func isTransientConversationSlotHandoffError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "proposal dropped") ||
		strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "leadership changed") ||
		strings.Contains(msg, "leader is 0") ||
		strings.Contains(msg, "leader not found") ||
		strings.Contains(msg, "slot leader") ||
		strings.Contains(msg, "propose timeout") ||
		strings.Contains(msg, "apply wait timeout")
}

// forwardToUserSlotLeader routes a user-scoped conversation request to the
// current user-slot leader within options.G.Cluster.ReqTimeout (default 10s),
// returning HTTP 503 {"msg":"retry required","status":503} on election timeout
// instead of false HTTP 400 errors (closing V11-5).
func (s *conversation) forwardToUserSlotLeader(c *wkhttp.Context, uid string, channelType uint8, bodyBytes []byte) (handleLocal bool) {
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
				if s != nil && s.Log != nil {
					s.Debug("转发请求：", zap.String("url", targetURL))
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
				if sendErr == nil && resp != nil && resp.StatusCode != http.StatusServiceUnavailable {
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
