package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

const (
	recentReadVersion        = 2
	recentReadWorkers        = 16
	recentReadBudgetChannels = 64
)

type recentReadRequest struct {
	UID         string                     `json:"uid"`
	Channels    []*channelRecentMessageReq `json:"channels"`
	MsgCount    int                        `json:"msg_count"`
	OrderByLast int                        `json:"order_by_last"`
	Budget      time.Duration              `json:"budget,omitempty"` // legacy nanoseconds, accepted during transition
	BudgetMS    int64                      `json:"budget_ms,omitempty"`
	PeerRead    bool                       `json:"peer_read,omitempty"` // fallback stays a leaf on upgraded peers
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

// Shared process-wide limits use separate pools for metadata and peer HTTP.
// A peer HTTP call must not hold a metadata permit while its receiver fences.
var recentMetadataSlots = make(chan struct{}, recentReadWorkers)
var recentPeerSlots = make(chan struct{}, recentReadWorkers)

func withRecentReadSlot(ctx context.Context, slots chan struct{}, task func() error) error {
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return task()
}

func recentReadMaxChannels() int {
	if options.G != nil && options.G.Cluster.RecentReadMaxChannels > 0 {
		return options.G.Cluster.RecentReadMaxChannels
	}
	return 10000
}

func checkRecentReadChannelCount(count int) error {
	if count > recentReadMaxChannels() {
		return fmt.Errorf("%w: limit %d; use smaller channel batches or conversation paging", errRecentReadTooLarge, recentReadMaxChannels())
	}
	return nil
}

// Scale within an operator-controlled ceiling. Oversized inputs are rejected
// before routing, never silently truncated. A shorter parent deadline wins.
func recentReadBudget(channels int) time.Duration {
	windows := max(1, (max(channels, 1)-1)/recentReadBudgetChannels+1)
	limit := options.G.Cluster.RecentReadMaxTimeout
	if limit <= 0 {
		limit = time.Minute
	}
	base := recentReadTimeout()
	if base > limit/time.Duration(windows) {
		return limit
	}
	return base * time.Duration(windows)
}

// Bound active metadata/fence RPCs independently of the channel count. Each
// task owns its indexed result; shared maps are assembled only after Wait.
func runRecentReadTasks(ctx context.Context, count int, task func(context.Context, int) error) error {
	group, workerCtx := errgroup.WithContext(ctx)
	group.SetLimit(recentReadWorkers)
	for i := 0; i < count; i++ {
		if workerCtx.Err() != nil {
			if err := group.Wait(); err != nil {
				return err
			}
			return ctx.Err()
		}
		i := i
		group.Go(func() error {
			if err := workerCtx.Err(); err != nil {
				return err
			}
			return withRecentReadSlot(workerCtx, recentMetadataSlots, func() error { return task(workerCtx, i) })
		})
	}
	return group.Wait()
}

// One end-to-end budget with paced retries. A failed channel must never
// disappear from a successful batch; each retry resolves all routes afresh.
func (s *request) getRecentMessagesForCluster(parent context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessage, error) {
	var err error
	channels, err = normalizeRecentChannels(channels)
	if err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return []*channelRecentMessage{}, nil
	}
	ctx, cancel := context.WithTimeout(parent, recentReadBudget(len(channels)))
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
			return result, nil
		}
		lastErr = err
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, errRecentReadProtocol) || errors.Is(err, errRecentReadDisappeared) {
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
	configs := make([]wkdb.ChannelClusterConfig, len(channels))
	missing, found := make([]bool, len(channels)), make([]bool, len(channels))
	err := runRecentReadTasks(ctx, len(channels), func(ctx context.Context, i int) error {
		ch := channels[i]
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return errInvalidRecentChannel
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if errors.Is(err, wkdb.ErrNotFound) {
			missing[i] = true
			if seen[key] {
				return errRecentReadDisappeared
			}
			return nil
		}
		if err != nil {
			return err
		}
		found[i] = true
		if !icluster.ValidChannelReadConfig(cfg, ch.ChannelId, ch.ChannelType) {
			return errors.New("invalid channel route")
		}
		configs[i] = cfg
		return nil
	})
	// Retain observations even when another task fails, so a retry cannot
	// turn a previously existing channel into an authoritative empty entry.
	disappeared := false
	for i, ch := range channels {
		if found[i] {
			seen[makeChannelKey(ch.ChannelId, ch.ChannelType)] = true
		}
		if missing[i] && seen[makeChannelKey(ch.ChannelId, ch.ChannelType)] {
			disappeared = true
		}
	}
	// A sibling's transient error must not mask an observed terminal deletion.
	if disappeared {
		return nil, errRecentReadDisappeared
	}
	if err != nil {
		return nil, err
	}
	for i, ch := range channels {
		if missing[i] {
			result = append(result, &channelRecentMessage{ChannelId: ch.ChannelId, ChannelType: ch.ChannelType, Messages: types.MessageRespSlice{}})
			continue
		}
		cfg := configs[i]
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
	group.SetLimit(recentReadWorkers)
	for i, node := range peers {
		i, node := i, node
		group.Go(func() error {
			var err error
			if node == options.G.Cluster.NodeId {
				batches[i], err = s.localRecentMessagesWithConfigs(workerCtx, uid, count, groups[node], last, configsByNode[node])
			} else {
				err = withRecentReadSlot(workerCtx, recentPeerSlots, func() error {
					var peerErr error
					batches[i], peerErr = s.requestSyncMessage(workerCtx, node, groups[node], uid, count, last)
					return peerErr
				})
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
	return result, nil
}

// Validate coverage, including intentionally empty channels. Empty/partial or
// legacy peer responses must not turn a routing failure into a successful omission.
func validateRecentBatch(want []*channelRecentMessageReq, got []*channelRecentMessage) error {
	expected := make(map[string]bool, len(want))
	for _, ch := range want {
		if ch == nil {
			return fmt.Errorf("%w: nil channel request", errRecentReadProtocol)
		}
		expected[makeChannelKey(ch.ChannelId, ch.ChannelType)] = true
	}
	for _, ch := range got {
		if ch == nil {
			return fmt.Errorf("%w: nil channel response", errRecentReadProtocol)
		}
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if !expected[key] {
			return fmt.Errorf("%w: unexpected or duplicate channel response", errRecentReadProtocol)
		}
		delete(expected, key)
	}
	if len(expected) > 0 {
		return fmt.Errorf("%w: incomplete channel response", errRecentReadProtocol)
	}
	return nil
}

func (s *request) localRecentMessages(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool) ([]*channelRecentMessage, error) {
	configs := make([]wkdb.ChannelClusterConfig, len(channels))
	err := runRecentReadTasks(ctx, len(channels), func(ctx context.Context, i int) error {
		ch := channels[i]
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return errInvalidRecentChannel
		}
		cfg, err := service.Cluster.LoadChannelReadConfig(ctx, ch.ChannelId, ch.ChannelType)
		if err != nil {
			return err
		}
		configs[i] = cfg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.localRecentMessagesWithConfigs(ctx, uid, count, channels, last, configs)
}

func (s *request) localRecentMessagesWithConfigs(ctx context.Context, uid string, count int, channels []*channelRecentMessageReq, last bool, configs []wkdb.ChannelClusterConfig) ([]*channelRecentMessage, error) {
	if len(channels) != len(configs) {
		return nil, errors.New("recent message config count mismatch")
	}
	for i, cfg := range configs {
		if channels[i] == nil || !icluster.ValidChannelReadConfig(cfg, channels[i].ChannelId, channels[i].ChannelType) || cfg.LeaderId != options.G.Cluster.NodeId {
			return nil, errors.New("recent message route changed")
		}
	}
	fence := func(ctx context.Context, i int) error {
		return service.Cluster.ValidateLocalChannelRead(ctx, configs[i])
	}
	if err := runRecentReadTasks(ctx, len(configs), fence); err != nil {
		return nil, err
	}
	result, err := s.getRecentMessages(ctx, uid, count, channels, last)
	if err != nil {
		return nil, err
	}
	if err := runRecentReadTasks(ctx, len(configs), fence); err != nil {
		return nil, err
	}
	// All I/O and both fences completed successfully. A deadline racing with
	// return/coverage assembly must not discard a fully validated result.
	return result, nil
}

func (s *request) requestSyncMessage(ctx context.Context, nodeID uint64, channels []*channelRecentMessageReq, uid string, count int, last bool) ([]*channelRecentMessage, error) {
	node := service.Cluster.NodeInfoById(nodeID)
	if node == nil || node.ApiServerAddr == "" {
		return nil, errors.New("channel leader API unavailable")
	}
	budget := recentReadBudget(len(channels))
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	if budget <= 0 {
		return nil, context.DeadlineExceeded
	}
	data, err := json.Marshal(recentReadRequest{UID: uid, Channels: channels, MsgCount: count, OrderByLast: wkutil.BoolToInt(last), PeerRead: true, BudgetMS: max(1, budget.Milliseconds())})
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
		if err := recentPeerError(resp); err != nil {
			return nil, err
		}
		var legacy []*channelRecentMessage
		if err := json.Unmarshal([]byte(resp.Body), &legacy); err != nil {
			return nil, fmt.Errorf("%w: %v", errRecentReadProtocol, err)
		}
		if err := validateRecentBatch(channels, legacy); err != nil {
			return nil, err
		}
		return legacy, nil
	}
	if err := recentPeerError(resp); err != nil {
		return nil, err
	}
	var result recentReadResponse
	if err := json.Unmarshal([]byte(resp.Body), &result); err != nil {
		return nil, fmt.Errorf("%w: %v", errRecentReadProtocol, err)
	}
	if result.Version != recentReadVersion {
		return nil, fmt.Errorf("%w: unsupported version", errRecentReadProtocol)
	}
	if err := validateRecentBatch(channels, result.Channels); err != nil {
		return nil, err
	}
	return result.Channels, nil
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
	req.Channels, err = normalizeRecentChannels(req.Channels)
	if err != nil {
		c.ResponseError(err)
		return
	}
	budget := recentReadBudget(len(req.Channels))
	if req.BudgetMS > 0 {
		req.Budget = time.Duration(min(req.BudgetMS, budget.Milliseconds())) * time.Millisecond
	}
	if (versioned || req.PeerRead) && req.Budget <= 0 {
		respondConversationReadRetry(c)
		return
	}
	if req.Budget > 0 && req.Budget < budget {
		budget = req.Budget
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), budget)
	defer cancel()
	if req.MsgCount <= 0 {
		req.MsgCount = 15
	}
	var result []*channelRecentMessage
	if versioned || req.PeerRead {
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
// range. LastMsgSeq is a lower bound in both query directions; use the
// minimum (including the unbounded zero). Keep the first occurrence's order.
func normalizeRecentChannels(channels []*channelRecentMessageReq) ([]*channelRecentMessageReq, error) {
	if err := checkRecentReadChannelCount(len(channels)); err != nil {
		return nil, err
	}
	result := make([]*channelRecentMessageReq, 0, len(channels))
	seen := make(map[string]*channelRecentMessageReq, len(channels))
	for _, ch := range channels {
		if ch == nil || ch.ChannelId == "" || ch.ChannelType == 0 {
			return nil, errInvalidRecentChannel
		}
		key := makeChannelKey(ch.ChannelId, ch.ChannelType)
		if old, ok := seen[key]; ok {
			old.LastMsgSeq = min(old.LastMsgSeq, ch.LastMsgSeq)
			continue
		}
		copy := *ch
		seen[key] = &copy
		result = append(result, &copy)
	}
	return result, nil
}

var errRecentReadProtocol = errors.New("invalid recent message response")

func recentPeerError(resp *rest.Response) error {
	err := handlerIMError(resp)
	if err != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
		return fmt.Errorf("%w: %v", errRecentReadProtocol, err)
	}
	return err
}

var errRecentReadTooLarge = errors.New("too many recent-message channels")
var errRecentReadDisappeared = errors.New("previously observed channel disappeared; refresh conversations")

func respondRecentReadError(c *wkhttp.Context, err error) {
	if errors.Is(err, errRecentReadTooLarge) {
		c.ResponseError(err)
		return
	}
	respondConversationReadRetry(c)
}
