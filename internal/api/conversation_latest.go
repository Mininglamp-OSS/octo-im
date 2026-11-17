package api

import (
	"context"
	"errors"
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

func (s *conversation) getChannelLastMsgSeq(ctx context.Context, channelID string, channelType uint8) (uint64, error) {
	timeout := options.G.Cluster.ReqTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return service.Cluster.GetChannelLastMessageSeq(ctx, channelID, channelType)
}

func respondConversationReadRetry(c *wkhttp.Context) {
	c.JSON(http.StatusServiceUnavailable, map[string]interface{}{"msg": "retry required", "status": http.StatusServiceUnavailable})
}

func isTransientConversationSlotHandoffError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "proposal dropped") || strings.Contains(msg, "not leader") || strings.Contains(msg, "leadership changed")
}

// forwardToUserSlotLeader routes a user-scoped conversation request to the
// current slot leader. If the target node is mid-restart (transport dial/EOF
// error), it re-resolves SlotLeaderOfChannel briefly so rolling upgrades can
// ride through slot failover or local takeover without returning connection refused.
func (s *conversation) forwardToUserSlotLeader(c *wkhttp.Context, uid string, channelType uint8, bodyBytes []byte) (handleLocal bool) {
	reqCtx := context.Background()
	if c.Request != nil && c.Request.Context() != nil {
		reqCtx = c.Request.Context()
	}
	budget := 2500 * time.Millisecond
	if options.G != nil && options.G.Cluster.ReqTimeout > 0 && options.G.Cluster.ReqTimeout < budget {
		budget = options.G.Cluster.ReqTimeout
	}
	deadlineCtx, cancel := context.WithTimeout(reqCtx, budget)
	defer cancel()

	delay := 50 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < 16; attempt++ {
		leaderInfo, err := service.Cluster.SlotLeaderOfChannel(uid, channelType)
		if err == nil && leaderInfo != nil {
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
				s.Debug("转发请求：", zap.String("url", targetURL))
				attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, 1200*time.Millisecond)
				resp, sendErr := service.SendPeerRequestWithContext(attemptCtx, rest.Request{
					Method:      rest.Method(method),
					BaseURL:     targetURL,
					Headers:     service.TrustedPeerNodeHeaders(leaderInfo, baseHeaders),
					Body:        bodyBytes,
					QueryParams: queryMap,
				})
				attemptCancel()
				if sendErr == nil && resp != nil {
					c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
					c.Writer.WriteHeader(resp.StatusCode)
					_, _ = c.Writer.Write([]byte(resp.Body))
					return false
				}
				lastErr = sendErr
			} else {
				lastErr = errors.New("slot leader API unavailable")
			}
		} else {
			lastErr = err
		}

		select {
		case <-deadlineCtx.Done():
			if lastErr == nil {
				lastErr = errors.New("获取频道所在节点失败！")
			}
			c.ResponseError(lastErr)
			return false
		case <-time.After(delay):
		}
		delay = min(delay*2, 200*time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("获取频道所在节点失败！")
	}
	c.ResponseError(lastErr)
	return false
}

