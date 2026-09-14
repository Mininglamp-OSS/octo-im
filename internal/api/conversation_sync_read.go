package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/sendgrid/rest"
	"golang.org/x/sync/errgroup"
)

const recentReadVersion = 2

type recentReadRequest struct {
	UID         string                     `json:"uid"`
	Channels    []*channelRecentMessageReq `json:"channels"`
	MsgCount    int                        `json:"msg_count"`
	OrderByLast int                        `json:"order_by_last"`
	Budget      time.Duration              `json:"budget"`
}
type recentReadResponse struct {
	Version  int                     `json:"version"`
	Channels []*channelRecentMessage `json:"channels"`
}

func recentReadTimeout() time.Duration {
	if options.G.Cluster.ReqTimeout > 0 {
		return options.G.Cluster.ReqTimeout
	}
	return 5 * time.Second
}

// One end-to-end budget, at most two attempts. A failed channel must never
// disappear from a successful batch; each retry resolves all routes afresh.
func (s *request) getRecentMessagesForCluster(parent context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessage, error) {
	if len(channels) == 0 {
		return []*channelRecentMessage{}, nil
	}
	ctx, cancel := context.WithTimeout(parent, recentReadTimeout())
	defer cancel()
	seen := make(map[string]bool)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		deadline, _ := ctx.Deadline()
		attemptCtx, done := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(2-attempt))
		result, err := s.recentMessagesAttempt(attemptCtx, uid, count, channels, last, seen)
		done()
		if err == nil {
			return result, ctx.Err()
		}
		lastErr = err
		if attempt == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return nil, fmt.Errorf("recent message read requires retry: %w", lastErr)
}

func (s *request) recentMessagesAttempt(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool, seen map[string]bool) ([]*channelRecentMessage, error) {
	groups := make(map[uint64][]*channelRecentMessageReq)
	result := make([]*channelRecentMessage, 0, len(channels))
	for _, ch := range channels {
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return nil, errors.New("invalid channel")
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if errors.Is(err, wkdb.ErrNotFound) && !seen[key] {
			result = append(result, &channelRecentMessage{ChannelId: ch.ChannelId, ChannelType: ch.ChannelType})
			continue
		}
		if err != nil {
			return nil, err
		}
		seen[key] = true
		if cfg.ChannelId != ch.ChannelId || cfg.ChannelType != ch.ChannelType || cfg.LeaderId == 0 {
			return nil, errors.New("invalid channel route")
		}
		groups[cfg.LeaderId] = append(groups[cfg.LeaderId], ch)
	}
	// Each worker owns its result slot. No shared reqErr or append races.
	peers := make([]uint64, 0, len(groups))
	for node := range groups {
		peers = append(peers, node)
	}
	batches := make([][]*channelRecentMessage, len(peers))
	group, workerCtx := errgroup.WithContext(ctx)
	for i, node := range peers {
		i, node := i, node
		group.Go(func() error {
			var err error
			if node == options.G.Cluster.NodeId {
				batches[i], err = s.localRecentMessages(workerCtx, uid, count, groups[node], last)
			} else {
				batches[i], err = s.requestSyncMessage(workerCtx, node, groups[node], uid, count, last)
			}
			if err != nil {
				return err
			}
			return validateRecentBatch(groups[node], batches[i])
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for _, batch := range batches {
		result = append(result, batch...)
	}
	if err := validateRecentBatch(channels, result); err != nil {
		return nil, err
	}
	return result, ctx.Err()
}

// Validate coverage, including intentionally empty channels. Empty/partial or
// legacy peer responses must not turn a routing failure into a successful omission.
func validateRecentBatch(want []*channelRecentMessageReq, got []*channelRecentMessage) error {
	expected := make(map[string]bool, len(want))
	for _, ch := range want {
		if ch == nil {
			return errors.New("nil channel request")
		}
		expected[makeChannelKey(ch.ChannelId, ch.ChannelType)] = true
	}
	for _, ch := range got {
		if ch == nil {
			return errors.New("nil channel response")
		}
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if !expected[key] {
			return errors.New("unexpected or duplicate channel response")
		}
		delete(expected, key)
	}
	if len(expected) > 0 {
		return errors.New("incomplete channel response")
	}
	return nil
}

func (s *request) localRecentMessages(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessage, error) {
	configs := make([]wkdb.ChannelClusterConfig, 0, len(channels))
	for _, ch := range channels {
		if ch == nil {
			return nil, errors.New("nil channel request")
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		if err != nil {
			return nil, err
		}
		if cfg.LeaderId != options.G.Cluster.NodeId {
			return nil, errors.New("recent message route changed")
		}
		if err := service.Cluster.ValidateLocalChannelRead(ctx, cfg); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	result, err := s.getRecentMessages(uid, count, channels, last)
	if err != nil {
		return nil, err
	}
	for _, cfg := range configs {
		if err := service.Cluster.ValidateLocalChannelRead(ctx, cfg); err != nil {
			return nil, err
		}
	}
	return result, ctx.Err()
}

func (s *request) requestSyncMessage(ctx context.Context, nodeID uint64, channels []*channelRecentMessageReq, uid string, count int, last bool) ([]*channelRecentMessage, error) {
	node := service.Cluster.NodeInfoById(nodeID)
	if node == nil || node.ApiServerAddr == "" {
		return nil, errors.New("channel leader API unavailable")
	}
	budget := recentReadTimeout()
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	if budget <= 0 {
		return nil, context.DeadlineExceeded
	}
	data, err := json.Marshal(recentReadRequest{UID: uid, Channels: channels, MsgCount: count, OrderByLast: wkutil.BoolToInt(last), Budget: budget})
	if err != nil {
		return nil, err
	}
	resp, err := rest.SendWithContext(ctx, rest.Request{Method: rest.Method("POST"), BaseURL: node.ApiServerAddr + "/conversation/syncMessages/v2", Body: data})
	if err != nil {
		return nil, err
	}
	if err := handlerIMError(resp); err != nil {
		return nil, err
	}
	var result recentReadResponse
	if err := json.Unmarshal([]byte(resp.Body), &result); err != nil {
		return nil, err
	}
	if result.Version != recentReadVersion {
		return nil, errors.New("unsupported recent message response")
	}
	if err := validateRecentBatch(channels, result.Channels); err != nil {
		return nil, err
	}
	return result.Channels, ctx.Err()
}

func (s *conversation) syncRecentMessages(c *wkhttp.Context)   { s.serveRecentMessages(c, false) }
func (s *conversation) syncRecentMessagesV2(c *wkhttp.Context) { s.serveRecentMessages(c, true) }
func (s *conversation) serveRecentMessages(c *wkhttp.Context, versioned bool) {
	var req recentReadRequest
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("invalid recent message request"))
		return
	}
	budget := recentReadTimeout()
	if versioned {
		if req.Budget <= 0 {
			respondConversationReadRetry(c)
			return
		}
		if req.Budget < budget {
			budget = req.Budget
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), budget)
	defer cancel()
	if req.MsgCount <= 0 {
		req.MsgCount = 15
	}
	result, err := s.s.requset.localRecentMessages(ctx, req.UID, req.MsgCount, req.Channels, wkutil.IntToBool(req.OrderByLast))
	if err == nil {
		err = validateRecentBatch(req.Channels, result)
	}
	if err != nil {
		respondConversationReadRetry(c)
		return
	}
	if versioned {
		c.JSON(http.StatusOK, recentReadResponse{Version: recentReadVersion, Channels: result})
	} else {
		c.JSON(http.StatusOK, result)
	}
}
