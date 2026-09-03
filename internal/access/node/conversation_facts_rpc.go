package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
)

const (
	conversationFactsOpLatest              = "latest"
	conversationFactsOpLatestAuthoritative = "latest_authoritative"
	conversationFactsOpRecent              = "recent"
	conversationFactsStatusNotReady        = "not_ready"
	conversationFactsStatusStaleMeta       = "stale_meta"
)

var (
	// ErrConversationFactsRouteUnavailable marks peer RPC failures that should
	// invalidate channel routing and retry once before surfacing as unavailable.
	ErrConversationFactsRouteUnavailable  = errors.New("access/node: conversation facts route unavailable")
	errConversationFactsLocalRuntimeStale = errors.New("access/node: local conversation facts runtime stale")
)

type conversationFactsChannelKey struct {
	ID   string
	Type uint8
}

type conversationFactsRequest struct {
	Op                   string                        `json:"op"`
	Key                  conversationFactsChannelKey   `json:"key"`
	Keys                 []conversationFactsChannelKey `json:"keys,omitempty"`
	Limit                int                           `json:"limit,omitempty"`
	MaxBytes             int                           `json:"max_bytes,omitempty"`
	ExpectedChannelEpoch uint64                        `json:"expected_channel_epoch,omitempty"`
	ExpectedLeaderEpoch  uint64                        `json:"expected_leader_epoch,omitempty"`
}

type conversationFactsEntry struct {
	Key      conversationFactsChannelKey `json:"key"`
	Messages []channel.Message           `json:"messages,omitempty"`
}

type conversationFactsResponse struct {
	Status   string                   `json:"status"`
	Messages []channel.Message        `json:"messages,omitempty"`
	Entries  []conversationFactsEntry `json:"entries,omitempty"`
}

func newConversationFactsChannelKey(id channel.ChannelID) conversationFactsChannelKey {
	return conversationFactsChannelKey{ID: id.ID, Type: id.Type}
}

func (k conversationFactsChannelKey) channelID() channel.ChannelID {
	return channel.ChannelID{ID: k.ID, Type: k.Type}
}

func (a *Adapter) handleConversationFactsRPC(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeConversationFactsRequest(body)
	if err != nil {
		return nil, err
	}

	var (
		messages []channel.Message
		entries  []conversationFactsEntry
	)
	if len(req.Keys) > 0 {
		if req.Op == conversationFactsOpLatestAuthoritative {
			return nil, fmt.Errorf("access/node: authoritative conversation facts require one channel")
		}
		entries = make([]conversationFactsEntry, 0, len(req.Keys))
		for _, rawKey := range req.Keys {
			key := rawKey.channelID()
			entry := conversationFactsEntry{Key: newConversationFactsChannelKey(key)}
			switch req.Op {
			case conversationFactsOpLatest:
				var msg channel.Message
				var ok bool
				msg, ok, err = a.loadLatestConversationFact(ctx, key, req.MaxBytes)
				if ok {
					entry.Messages = []channel.Message{msg}
				}
			case conversationFactsOpRecent:
				entry.Messages, err = a.loadRecentConversationFacts(ctx, key, req.Limit, req.MaxBytes)
			default:
				return nil, fmt.Errorf("access/node: unknown conversation facts op %q", req.Op)
			}
			if errors.Is(err, channel.ErrChannelNotFound) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		return encodeConversationFactsResponse(conversationFactsResponse{
			Status:  rpcStatusOK,
			Entries: entries,
		})
	}

	key := req.Key.channelID()
	switch req.Op {
	case conversationFactsOpLatest:
		var msg channel.Message
		var ok bool
		msg, ok, err = a.loadLatestConversationFact(ctx, key, req.MaxBytes)
		if ok {
			messages = []channel.Message{msg}
		}
	case conversationFactsOpLatestAuthoritative:
		var msg channel.Message
		var ok bool
		msg, ok, err = a.loadLatestConversationFactAuthoritative(
			ctx,
			key,
			req.MaxBytes,
			req.ExpectedChannelEpoch,
			req.ExpectedLeaderEpoch,
		)
		if ok {
			messages = []channel.Message{msg}
		}
	case conversationFactsOpRecent:
		messages, err = a.loadRecentConversationFacts(ctx, key, req.Limit, req.MaxBytes)
	default:
		return nil, fmt.Errorf("access/node: unknown conversation facts op %q", req.Op)
	}
	if errors.Is(err, channel.ErrChannelNotFound) {
		err = nil
	}
	if err != nil {
		if status, ok := conversationFactsErrorStatus(err); ok {
			return encodeConversationFactsResponse(conversationFactsResponse{Status: status})
		}
		return nil, err
	}
	return encodeConversationFactsResponse(conversationFactsResponse{
		Status:   rpcStatusOK,
		Messages: messages,
	})
}

func (a *Adapter) loadLatestConversationFact(ctx context.Context, id channel.ChannelID, maxBytes int) (channel.Message, bool, error) {
	msg, ok, err := loadLatestConversationMessageStrict(ctx, a.channelLog, id, maxBytes)
	if errors.Is(err, channel.ErrNotReady) {
		return channel.Message{}, false, nil
	}
	if !errors.Is(err, channel.ErrStaleMeta) {
		return msg, ok, err
	}
	if _, refreshErr := a.refreshConversationFactsMeta(ctx, id); refreshErr != nil {
		return channel.Message{}, false, refreshErr
	}
	msg, ok, err = loadLatestConversationMessageStrict(ctx, a.channelLog, id, maxBytes)
	if errors.Is(err, channel.ErrNotReady) {
		return channel.Message{}, false, nil
	}
	return msg, ok, err
}

func (a *Adapter) loadLatestConversationFactAuthoritative(ctx context.Context, id channel.ChannelID, maxBytes int, expectedChannelEpoch, expectedLeaderEpoch uint64) (channel.Message, bool, error) {
	msg, ok, err := LoadLatestConversationMessageAuthoritative(
		ctx,
		a.channelLog,
		id,
		maxBytes,
		a.localNodeID,
		expectedChannelEpoch,
		expectedLeaderEpoch,
	)
	if !errors.Is(err, errConversationFactsLocalRuntimeStale) {
		return msg, ok, err
	}
	if _, refreshErr := a.refreshConversationFactsMeta(ctx, id); refreshErr != nil {
		return channel.Message{}, false, refreshErr
	}
	return LoadLatestConversationMessageAuthoritative(
		ctx,
		a.channelLog,
		id,
		maxBytes,
		a.localNodeID,
		expectedChannelEpoch,
		expectedLeaderEpoch,
	)
}

func (a *Adapter) loadRecentConversationFacts(ctx context.Context, id channel.ChannelID, limit, maxBytes int) ([]channel.Message, error) {
	messages, err := loadRecentConversationMessagesStrict(ctx, a.channelLog, id, limit, maxBytes)
	if errors.Is(err, channel.ErrNotReady) {
		return nil, nil
	}
	if !errors.Is(err, channel.ErrStaleMeta) {
		return messages, err
	}
	if _, refreshErr := a.refreshConversationFactsMeta(ctx, id); refreshErr != nil {
		return nil, refreshErr
	}
	messages, err = loadRecentConversationMessagesStrict(ctx, a.channelLog, id, limit, maxBytes)
	if errors.Is(err, channel.ErrNotReady) {
		return nil, nil
	}
	return messages, err
}

func (a *Adapter) refreshConversationFactsMeta(ctx context.Context, id channel.ChannelID) (channel.Meta, error) {
	if a == nil || a.channelMeta == nil {
		return channel.Meta{}, channel.ErrStaleMeta
	}
	a.channelMeta.InvalidateChannelMeta(id)
	return a.channelMeta.ActivateByID(ctx, id, channelruntime.ActivationSourceFetch)
}

func loadLatestConversationMessageStrict(ctx context.Context, cluster ChannelLog, id channel.ChannelID, maxBytes int) (channel.Message, bool, error) {
	if cluster == nil {
		return channel.Message{}, false, channel.ErrStaleMeta
	}
	status, err := cluster.Status(id)
	if err != nil {
		return channel.Message{}, false, err
	}
	if status.CommittedSeq == 0 {
		return channel.Message{}, false, nil
	}

	fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
		ChannelID: id,
		FromSeq:   status.CommittedSeq,
		Limit:     1,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		return channel.Message{}, false, err
	}
	if len(fetch.Messages) == 0 {
		return channel.Message{}, false, nil
	}
	return fetch.Messages[0], true, nil
}

// ConversationFactsLog is the narrow channel-log view required by conversation reads.
type ConversationFactsLog interface {
	Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error)
	Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error)
}

// LoadLatestConversationMessageAuthoritative returns the latest committed
// message only when the local replica proves it is still authoritative.
func LoadLatestConversationMessageAuthoritative(ctx context.Context, cluster ConversationFactsLog, id channel.ChannelID, maxBytes int, localNodeID, expectedChannelEpoch, expectedLeaderEpoch uint64) (channel.Message, bool, error) {
	if cluster == nil || localNodeID == 0 {
		return channel.Message{}, false, channel.ErrStaleMeta
	}
	status, err := cluster.Status(id)
	if err != nil {
		if errors.Is(err, channel.ErrStaleMeta) {
			return channel.Message{}, false, fmt.Errorf("%w: %w", errConversationFactsLocalRuntimeStale, err)
		}
		return channel.Message{}, false, err
	}
	if status.Leader == 0 {
		return channel.Message{}, false, raftcluster.ErrNoLeader
	}
	if uint64(status.Leader) != localNodeID {
		return channel.Message{}, false, channel.ErrNotLeader
	}
	if expectedLeaderEpoch != 0 && status.LeaderEpoch != expectedLeaderEpoch {
		return channel.Message{}, false, channel.ErrStaleMeta
	}

	fromSeq := status.CommittedSeq
	if fromSeq == 0 {
		fromSeq = 1
	}
	fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
		ChannelID:            id,
		FromSeq:              fromSeq,
		Limit:                1,
		MaxBytes:             maxBytes,
		ExpectedChannelEpoch: expectedChannelEpoch,
		ExpectedLeaderEpoch:  expectedLeaderEpoch,
		RequireLeader:        true,
	})
	if errors.Is(err, channel.ErrChannelNotFound) {
		return channel.Message{}, false, nil
	}
	if err != nil {
		return channel.Message{}, false, err
	}
	if len(fetch.Messages) == 0 {
		return channel.Message{}, false, nil
	}
	return fetch.Messages[0], true, nil
}

func conversationFactsErrorStatus(err error) (string, bool) {
	switch {
	case errors.Is(err, channel.ErrNotLeader):
		return rpcStatusNotLeader, true
	case errors.Is(err, channel.ErrNotReady):
		return conversationFactsStatusNotReady, true
	case errors.Is(err, channel.ErrStaleMeta):
		return conversationFactsStatusStaleMeta, true
	case errors.Is(err, raftcluster.ErrNoLeader):
		return rpcStatusNoLeader, true
	default:
		return "", false
	}
}

func loadRecentConversationMessagesStrict(ctx context.Context, cluster ChannelLog, id channel.ChannelID, limit, maxBytes int) ([]channel.Message, error) {
	if cluster == nil || limit <= 0 {
		return nil, nil
	}
	status, err := cluster.Status(id)
	if err != nil {
		return nil, err
	}
	if status.CommittedSeq == 0 {
		return nil, nil
	}

	fromSeq := uint64(1)
	if status.CommittedSeq >= uint64(limit) {
		fromSeq = status.CommittedSeq - uint64(limit) + 1
	}
	fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
		ChannelID: id,
		FromSeq:   fromSeq,
		Limit:     limit,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		return nil, err
	}
	return append([]channel.Message(nil), fetch.Messages...), nil
}

func encodeConversationFactsResponse(resp conversationFactsResponse) ([]byte, error) {
	return encodeConversationFactsResponseBinary(resp)
}

func decodeConversationFactsResponse(body []byte) (conversationFactsResponse, error) {
	return decodeConversationFactsResponseBinary(body)
}
