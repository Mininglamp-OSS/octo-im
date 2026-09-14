package api

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/sendgrid/rest"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const recentReadVersion = 2

type recentReadRequest struct {
	UID         string                     `json:"uid"`
	Channels    []*channelRecentMessageReq `json:"channels"`
	MsgCount    int                        `json:"msg_count"`
	OrderByLast int                        `json:"order_by_last"`
	Budget      time.Duration              `json:"budget,omitempty"` // legacy nanoseconds, accepted during transition
	BudgetMS    int64                      `json:"budget_ms,omitempty"`
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

// One end-to-end budget with paced retries. A failed channel must never
// disappear from a successful batch; each retry resolves all routes afresh.
func (s *request) getRecentMessagesForCluster(parent context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessage, error) {
	var err error
	channels, err = normalizeRecentChannels(channels, last)
	if err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return []*channelRecentMessage{}, nil
	}
	ctx, cancel := context.WithTimeout(parent, recentReadTimeout())
	defer cancel()
	seen := make(map[string]bool)
	var lastErr error
	delay := 25 * time.Millisecond
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// A healthy slow batch can use the entire remaining deadline. Retry
		// failures that return early; slicing the budget makes large reads
		// impossible even when they would finish within the request deadline.
		result, err := s.recentMessagesAttempt(ctx, uid, count, channels, last, seen)
		if err == nil {
			return result, ctx.Err()
		}
		lastErr = err
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), lastErr)
		case <-time.After(delay + time.Duration(rand.Int64N(int64(delay/4)+1))):
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
	return nil, lastErr
}

func (s *request) recentMessagesAttempt(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool, seen map[string]bool) ([]*channelRecentMessage, error) {
	groups := make(map[uint64][]*channelRecentMessageReq)
	configsByNode := make(map[uint64][]wkdb.ChannelClusterConfig)
	result := make([]*channelRecentMessage, 0, len(channels))
	for _, ch := range channels {
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return nil, errors.New("invalid channel")
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if errors.Is(err, wkdb.ErrNotFound) && !seen[key] {
			result = append(result, &channelRecentMessage{ChannelId: ch.ChannelId, ChannelType: ch.ChannelType, Messages: types.MessageRespSlice{}})
			continue
		}
		if err != nil {
			return nil, err
		}
		seen[key] = true
		if !icluster.ValidChannelReadConfig(cfg, ch.ChannelId, ch.ChannelType) {
			return nil, errors.New("invalid channel route")
		}
		groups[cfg.LeaderId] = append(groups[cfg.LeaderId], ch)
		configsByNode[cfg.LeaderId] = append(configsByNode[cfg.LeaderId], cfg)
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
				batches[i], err = s.localRecentMessagesWithConfigs(workerCtx, uid, count, groups[node], last, configsByNode[node])
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return nil, errInvalidRecentChannel
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		if err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return s.localRecentMessagesWithConfigs(ctx, uid, count, channels, last, configs)
}

func (s *request) localRecentMessagesWithConfigs(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool, configs []wkdb.ChannelClusterConfig) ([]*channelRecentMessage, error) {
	for i, cfg := range configs {
		if !icluster.ValidChannelReadConfig(cfg, channels[i].ChannelId, channels[i].ChannelType) || cfg.LeaderId != options.G.Cluster.NodeId {
			return nil, errors.New("recent message route changed")
		}
		if err := service.Cluster.ValidateLocalChannelRead(ctx, cfg); err != nil {
			return nil, err
		}
	}
	result, err := s.getRecentMessages(ctx, uid, count, channels, last)
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
	data, err := json.Marshal(recentReadRequest{UID: uid, Channels: channels, MsgCount: count, OrderByLast: wkutil.BoolToInt(last), BudgetMS: max(1, budget.Milliseconds())})
	if err != nil {
		return nil, err
	}
	resp, err := rest.SendWithContext(ctx, rest.Request{Method: rest.Method("POST"), BaseURL: node.ApiServerAddr + "/conversation/syncMessages/v2", Body: data})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		// Old peers have no v2 route. Only route absence permits downgrade;
		// a v2 failure or malformed response must not bypass its read fences.
		resp, err = rest.SendWithContext(ctx, rest.Request{Method: rest.Method("POST"), BaseURL: node.ApiServerAddr + "/conversation/syncMessages", Body: data})
		if err != nil {
			return nil, err
		}
		if err := handlerIMError(resp); err != nil {
			return nil, err
		}
		var legacy []*channelRecentMessage
		if err := json.Unmarshal([]byte(resp.Body), &legacy); err != nil {
			return nil, err
		}
		if err := validateRecentBatch(channels, legacy); err != nil {
			return nil, err
		}
		return legacy, ctx.Err()
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
	var err error
	req.Channels, err = normalizeRecentChannels(req.Channels, wkutil.IntToBool(req.OrderByLast))
	if err != nil {
		c.ResponseError(errInvalidRecentChannel)
		return
	}
	budget := recentReadTimeout()
	if versioned {
		if req.BudgetMS > 0 {
			req.Budget = time.Duration(min(req.BudgetMS, budget.Milliseconds())) * time.Millisecond
		}
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
	var result []*channelRecentMessage
	if versioned {
		result, err = s.s.requset.localRecentMessages(ctx, req.UID, req.MsgCount, req.Channels, wkutil.IntToBool(req.OrderByLast))
	} else {
		result, err = s.s.requset.getRecentMessagesForCluster(ctx, req.UID, req.MsgCount, req.Channels, wkutil.IntToBool(req.OrderByLast))
	}
	if err == nil {
		err = validateRecentBatch(req.Channels, result)
	}
	if err != nil {
		s.Warn("recent message read failed", zap.Error(err), zap.Bool("internal", versioned), zap.Int("channels", len(req.Channels)))
		respondConversationReadRetry(c)
		return
	}
	if versioned {
		c.JSON(http.StatusOK, recentReadResponse{Version: recentReadVersion, Channels: result})
	} else {
		c.JSON(http.StatusOK, result)
	}
}

var errInvalidRecentChannel = errors.New("invalid channel_id or channel_type")

// Collapse repeated store/cache/input entries without losing the wider cursor
// range. Forward reads start at the oldest cursor; reverse reads at the newest
// (zero is the unbounded reverse cursor). Keep the first occurrence's order.
func normalizeRecentChannels(channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessageReq, error) {
	result := make([]*channelRecentMessageReq, 0, len(channels))
	seen := make(map[string]*channelRecentMessageReq, len(channels))
	for _, ch := range channels {
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return nil, errInvalidRecentChannel
		}
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if old, ok := seen[key]; ok {
			if !last {
				old.LastMsgSeq = min(old.LastMsgSeq, ch.LastMsgSeq)
			} else if old.LastMsgSeq == 0 || ch.LastMsgSeq == 0 {
				old.LastMsgSeq = 0
			} else {
				old.LastMsgSeq = max(old.LastMsgSeq, ch.LastMsgSeq)
			}
			continue
		}
		copy := *ch
		seen[key] = &copy
		result = append(result, &copy)
	}
	return result, nil
}
