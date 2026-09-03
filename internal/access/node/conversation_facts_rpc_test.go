package node

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestConversationFactsBinaryCodecRoundTrip(t *testing.T) {
	req := conversationFactsRequest{
		Op: conversationFactsOpLatestAuthoritative,
		Key: newConversationFactsChannelKey(channel.ChannelID{
			ID:   "g1",
			Type: 2,
		}),
		Keys: []conversationFactsChannelKey{
			newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
			newConversationFactsChannelKey(channel.ChannelID{ID: "g2", Type: 3}),
		},
		Limit:                5,
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	}
	reqBody, err := encodeConversationFactsRequestBinary(req)
	require.NoError(t, err)
	require.True(t, isConversationFactsRequestBinary(reqBody))

	gotReq, err := decodeConversationFactsRequest(reqBody)
	require.NoError(t, err)
	require.Equal(t, req, gotReq)

	resp := conversationFactsResponse{
		Status: rpcStatusOK,
		Messages: []channel.Message{{
			ChannelID:   "g0",
			ChannelType: 1,
			MessageSeq:  8,
			Payload:     []byte("latest"),
		}},
		Entries: []conversationFactsEntry{{
			Key: newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
			Messages: []channel.Message{{
				ChannelID:   "g1",
				ChannelType: 2,
				MessageSeq:  9,
				Payload:     []byte("recent"),
			}},
		}},
	}
	respBody, err := encodeConversationFactsResponse(resp)
	require.NoError(t, err)
	require.True(t, isConversationFactsResponseBinary(respBody))

	gotResp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, resp, gotResp)
}

func TestConversationFactsRPCRejectsJSONPayload(t *testing.T) {
	adapter := New(Options{})

	_, err := adapter.handleConversationFactsRPC(context.Background(), []byte(`{"op":"latest","key":{"ChannelID":"g1","ChannelType":2}}`))
	require.Error(t, err)
}

func TestLoadLatestConversationMessageStrictReturnsNotReady(t *testing.T) {
	msg, ok, err := loadLatestConversationMessageStrict(context.Background(), notReadyConversationFactsLog{}, channel.ChannelID{ID: "g1", Type: 2}, 1024)
	require.ErrorIs(t, err, channel.ErrNotReady)
	require.False(t, ok)
	require.Equal(t, channel.Message{}, msg)
}

func TestLoadRecentConversationMessagesStrictReturnsNotReady(t *testing.T) {
	msgs, err := loadRecentConversationMessagesStrict(context.Background(), notReadyConversationFactsLog{}, channel.ChannelID{ID: "g1", Type: 2}, 10, 1024)
	require.ErrorIs(t, err, channel.ErrNotReady)
	require.Nil(t, msgs)
}

func TestConversationFactsRPCOrdinaryLatestKeepsNotReadyAsEmpty(t *testing.T) {
	adapter := New(Options{ChannelLog: notReadyConversationFactsLog{}})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:       conversationFactsOpLatest,
		Key:      newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes: 1024,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusOK, resp.Status)
	require.Empty(t, resp.Messages)
}

func TestConversationFactsRPCOrdinaryBatchDoesNotFailOnNotReadyChannel(t *testing.T) {
	adapter := New(Options{ChannelLog: notReadyConversationFactsLog{}})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op: conversationFactsOpLatest,
		Keys: []conversationFactsChannelKey{
			newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
			newConversationFactsChannelKey(channel.ChannelID{ID: "g2", Type: 2}),
		},
		MaxBytes: 1024,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusOK, resp.Status)
	require.Len(t, resp.Entries, 2)
	require.Empty(t, resp.Entries[0].Messages)
	require.Empty(t, resp.Entries[1].Messages)
}

func TestConversationFactsRPCAuthoritativeLatestReturnsNotReadyStatus(t *testing.T) {
	adapter := New(Options{ChannelLog: notReadyConversationFactsLog{}, LocalNodeID: 1})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:                   conversationFactsOpLatestAuthoritative,
		Key:                  newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, conversationFactsStatusNotReady, resp.Status)
}

func TestConversationFactsRPCAuthoritativeLatestRejectsFollower(t *testing.T) {
	log := &recordingAuthoritativeConversationFactsLog{
		status: channel.ChannelRuntimeStatus{Leader: 2, LeaderEpoch: 12, CommittedSeq: 7},
	}
	adapter := New(Options{ChannelLog: log, LocalNodeID: 1})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:                   conversationFactsOpLatestAuthoritative,
		Key:                  newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusNotLeader, resp.Status)
	require.Empty(t, log.fetches)
}

func TestConversationFactsRPCAuthoritativeLatestFencesEpochsAndRequiresLeader(t *testing.T) {
	log := &recordingAuthoritativeConversationFactsLog{
		status: channel.ChannelRuntimeStatus{Leader: 1, LeaderEpoch: 12, CommittedSeq: 7},
		fetch: channel.FetchResult{
			Messages: []channel.Message{{
				ChannelID:   "g1",
				ChannelType: 2,
				MessageSeq:  7,
			}},
		},
	}
	adapter := New(Options{ChannelLog: log, LocalNodeID: 1})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:                   conversationFactsOpLatestAuthoritative,
		Key:                  newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusOK, resp.Status)
	require.Len(t, resp.Messages, 1)
	require.Equal(t, []channel.FetchRequest{{
		ChannelID:            channel.ChannelID{ID: "g1", Type: 2},
		FromSeq:              7,
		Limit:                1,
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
		RequireLeader:        true,
	}}, log.fetches)
}

func TestLoadLatestConversationMessageAuthoritativeReturnsFirstConcurrentCommit(t *testing.T) {
	log := &recordingAuthoritativeConversationFactsLog{
		status: channel.ChannelRuntimeStatus{Leader: 1, LeaderEpoch: 12},
		fetch: channel.FetchResult{
			Messages: []channel.Message{{
				ChannelID:   "g1",
				ChannelType: 2,
				MessageSeq:  1,
			}},
		},
	}

	msg, ok, err := LoadLatestConversationMessageAuthoritative(
		context.Background(),
		log,
		channel.ChannelID{ID: "g1", Type: 2},
		1024,
		1,
		11,
		12,
	)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), msg.MessageSeq)
	require.Equal(t, uint64(1), log.fetches[0].FromSeq)
}

func TestConversationFactsRPCAuthoritativeStaleCallerEpochDoesNotRefreshLocalMeta(t *testing.T) {
	log := &recordingAuthoritativeConversationFactsLog{
		status: channel.ChannelRuntimeStatus{Leader: 1, LeaderEpoch: 13, CommittedSeq: 7},
	}
	refresher := &refreshingConversationFactsMetaRefresher{}
	adapter := New(Options{ChannelLog: log, ChannelMeta: refresher, LocalNodeID: 1})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:                   conversationFactsOpLatestAuthoritative,
		Key:                  newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, conversationFactsStatusStaleMeta, resp.Status)
	require.Empty(t, refresher.calls)
	require.Empty(t, refresher.invalidations)
	require.Empty(t, log.fetches)
}

func TestConversationFactsRPCAuthoritativeRefreshesMissingLocalRuntime(t *testing.T) {
	log := &refreshableConversationFactsLog{
		status: channel.ChannelRuntimeStatus{Leader: 1, LeaderEpoch: 12, CommittedSeq: 7},
		fetch: channel.FetchResult{
			Messages: []channel.Message{{
				ChannelID:   "g1",
				ChannelType: 2,
				MessageSeq:  7,
			}},
		},
	}
	refresher := &refreshingConversationFactsMetaRefresher{
		meta: channel.Meta{ID: channel.ChannelID{ID: "g1", Type: 2}, Epoch: 11, LeaderEpoch: 12, Leader: 1},
		onRefresh: func() {
			log.markRefreshed()
		},
	}
	adapter := New(Options{ChannelLog: log, ChannelMeta: refresher, LocalNodeID: 1})
	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op:                   conversationFactsOpLatestAuthoritative,
		Key:                  newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		MaxBytes:             1024,
		ExpectedChannelEpoch: 11,
		ExpectedLeaderEpoch:  12,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)
	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusOK, resp.Status)
	require.Len(t, resp.Messages, 1)
	require.Equal(t, []channel.ChannelID{{ID: "g1", Type: 2}}, refresher.invalidations)
	require.Equal(t, []channel.ChannelID{{ID: "g1", Type: 2}}, refresher.calls)
}

func TestConversationFactsAuthoritativeClientPreservesNotLeaderStatus(t *testing.T) {
	network := newFakeClusterNetwork(nil, nil)
	New(Options{
		Cluster:     network.cluster(2),
		ChannelLog:  &recordingAuthoritativeConversationFactsLog{status: channel.ChannelRuntimeStatus{Leader: 3, LeaderEpoch: 12, CommittedSeq: 7}},
		LocalNodeID: 2,
	})
	client := NewClient(network.cluster(1))

	_, ok, err := client.LoadLatestConversationMessageAuthoritative(
		context.Background(),
		2,
		channel.ChannelID{ID: "g1", Type: 2},
		1024,
		11,
		12,
	)

	require.ErrorIs(t, err, channel.ErrNotLeader)
	require.False(t, ok)
}

func TestConversationFactsAuthoritativeClientTreatsLegacyPeerCodecAsNotReady(t *testing.T) {
	client := NewClient(remoteErrorCluster{err: errors.New("nodetransport: remote error: access/node: invalid conversation facts request codec")})

	_, ok, err := client.LoadLatestConversationMessageAuthoritative(
		context.Background(),
		2,
		channel.ChannelID{ID: "g1", Type: 2},
		1024,
		11,
		12,
	)

	require.ErrorIs(t, err, channel.ErrNotReady)
	require.False(t, ok)
}

func TestConversationFactsAuthoritativeClientTreatsTransportFailureAsRouteUnavailable(t *testing.T) {
	client := NewClient(remoteErrorCluster{err: errors.New("dial tcp 10.0.0.2:11110: connect: connection refused")})

	_, ok, err := client.LoadLatestConversationMessageAuthoritative(
		context.Background(),
		2,
		channel.ChannelID{ID: "g1", Type: 2},
		1024,
		11,
		12,
	)

	require.ErrorIs(t, err, ErrConversationFactsRouteUnavailable)
	require.ErrorIs(t, err, channel.ErrNotReady)
	require.False(t, ok)
}

func TestConversationFactsOrdinaryClientPreservesStaleMetaStatus(t *testing.T) {
	network := newFakeClusterNetwork(nil, nil)
	New(Options{
		Cluster:    network.cluster(2),
		ChannelLog: &recordingAuthoritativeConversationFactsLog{statusErr: channel.ErrStaleMeta},
	})
	client := NewClient(network.cluster(1))

	_, ok, err := client.LoadLatestConversationMessage(
		context.Background(),
		2,
		channel.ChannelID{ID: "g1", Type: 2},
		1024,
	)

	require.ErrorIs(t, err, channel.ErrStaleMeta)
	require.False(t, ok)
}

func TestConversationFactsRPCRefreshesStaleMetaForBatchRecentLoads(t *testing.T) {
	log := &refreshableConversationFactsLog{
		status: channel.ChannelRuntimeStatus{CommittedSeq: 7},
		fetch: channel.FetchResult{Messages: []channel.Message{{
			ChannelID:   "g1",
			ChannelType: 2,
			MessageSeq:  7,
		}}},
	}
	refresher := &refreshingConversationFactsMetaRefresher{
		meta: channel.Meta{ID: channel.ChannelID{ID: "g1", Type: 2}},
		onRefresh: func() {
			log.markRefreshed()
		},
	}
	adapter := New(Options{
		ChannelLog:  log,
		ChannelMeta: refresher,
	})

	body := mustEncodeConversationFactsRequest(t, conversationFactsRequest{
		Op: conversationFactsOpRecent,
		Keys: []conversationFactsChannelKey{
			newConversationFactsChannelKey(channel.ChannelID{ID: "g1", Type: 2}),
		},
		Limit:    1,
		MaxBytes: 1024,
	})

	respBody, err := adapter.handleConversationFactsRPC(context.Background(), body)
	require.NoError(t, err)

	resp, err := decodeConversationFactsResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, []channel.ChannelID{{ID: "g1", Type: 2}}, refresher.calls)
	require.Equal(t, []channel.ChannelID{{ID: "g1", Type: 2}}, refresher.invalidations)
	require.Len(t, resp.Entries, 1)
	require.Len(t, resp.Entries[0].Messages, 1)
	require.Equal(t, uint64(7), resp.Entries[0].Messages[0].MessageSeq)
}

type notReadyConversationFactsLog struct{}

func (notReadyConversationFactsLog) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	return channel.ChannelRuntimeStatus{}, channel.ErrNotReady
}

func (notReadyConversationFactsLog) Fetch(context.Context, channel.FetchRequest) (channel.FetchResult, error) {
	return channel.FetchResult{}, channel.ErrNotReady
}

func (notReadyConversationFactsLog) Append(context.Context, channel.AppendRequest) (channel.AppendResult, error) {
	return channel.AppendResult{}, channel.ErrNotReady
}

func (notReadyConversationFactsLog) AppendBatch(context.Context, channel.AppendBatchRequest) (channel.AppendBatchResult, error) {
	return channel.AppendBatchResult{}, channel.ErrNotReady
}

type refreshableConversationFactsLog struct {
	status    channel.ChannelRuntimeStatus
	fetch     channel.FetchResult
	refreshed bool
}

type recordingAuthoritativeConversationFactsLog struct {
	status    channel.ChannelRuntimeStatus
	statusErr error
	fetch     channel.FetchResult
	fetchErr  error
	fetches   []channel.FetchRequest
}

func (l *recordingAuthoritativeConversationFactsLog) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	return l.status, l.statusErr
}

func (l *recordingAuthoritativeConversationFactsLog) Fetch(_ context.Context, req channel.FetchRequest) (channel.FetchResult, error) {
	l.fetches = append(l.fetches, req)
	return l.fetch, l.fetchErr
}

func (l *recordingAuthoritativeConversationFactsLog) Append(context.Context, channel.AppendRequest) (channel.AppendResult, error) {
	return channel.AppendResult{}, nil
}

func (l *recordingAuthoritativeConversationFactsLog) AppendBatch(context.Context, channel.AppendBatchRequest) (channel.AppendBatchResult, error) {
	return channel.AppendBatchResult{}, nil
}

func (l *refreshableConversationFactsLog) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	if !l.refreshed {
		return channel.ChannelRuntimeStatus{}, channel.ErrStaleMeta
	}
	return l.status, nil
}

func (l *refreshableConversationFactsLog) Fetch(context.Context, channel.FetchRequest) (channel.FetchResult, error) {
	if !l.refreshed {
		return channel.FetchResult{}, channel.ErrStaleMeta
	}
	return l.fetch, nil
}

func (l *refreshableConversationFactsLog) Append(context.Context, channel.AppendRequest) (channel.AppendResult, error) {
	return channel.AppendResult{}, nil
}

func (l *refreshableConversationFactsLog) AppendBatch(context.Context, channel.AppendBatchRequest) (channel.AppendBatchResult, error) {
	return channel.AppendBatchResult{}, nil
}

func (l *refreshableConversationFactsLog) markRefreshed() {
	l.refreshed = true
}

type refreshingConversationFactsMetaRefresher struct {
	meta          channel.Meta
	err           error
	calls         []channel.ChannelID
	invalidations []channel.ChannelID
	onRefresh     func()
}

func (r *refreshingConversationFactsMetaRefresher) RefreshChannelMeta(_ context.Context, id channel.ChannelID) (channel.Meta, error) {
	r.calls = append(r.calls, id)
	if r.onRefresh != nil {
		r.onRefresh()
	}
	return r.meta, r.err
}

func (r *refreshingConversationFactsMetaRefresher) InvalidateChannelMeta(id channel.ChannelID) {
	r.invalidations = append(r.invalidations, id)
}

func mustEncodeConversationFactsRequest(t *testing.T, req conversationFactsRequest) []byte {
	t.Helper()
	body, err := encodeConversationFactsRequestBinary(req)
	require.NoError(t, err)
	return body
}
