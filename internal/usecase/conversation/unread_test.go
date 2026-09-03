package conversation

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/stretchr/testify/require"
)

func TestClearUnreadAdvancesReadSeqToLatestMessage(t *testing.T) {
	now := time.Unix(123, 0)
	repo := newConversationSyncRepoStub()
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 10, "u2", 100, "c1")

	app := New(Options{
		States:             repo,
		Facts:              repo,
		AuthoritativeFacts: repo,
		Now:                func() time.Time { return now },
	})

	err := app.ClearUnread(context.Background(), ClearUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
	})

	require.NoError(t, err)
	require.Equal(t, []metadb.UserConversationState{
		{
			UID:         "u1",
			ChannelID:   "g1",
			ChannelType: 2,
			ReadSeq:     10,
			UpdatedAt:   now.UnixNano(),
		},
	}, repo.upsertedStates)
}

func TestClearUnreadUsesClientMessageSeqWhenLatestFactMissing(t *testing.T) {
	now := time.Unix(123, 0)
	repo := newConversationSyncRepoStub()

	app := New(Options{
		States:             repo,
		Facts:              repo,
		AuthoritativeFacts: repo,
		Now:                func() time.Time { return now },
	})

	err := app.ClearUnread(context.Background(), ClearUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		MessageSeq:  8,
	})

	require.NoError(t, err)
	require.Equal(t, []metadb.UserConversationState{
		{
			UID:         "u1",
			ChannelID:   "g1",
			ChannelType: 2,
			ReadSeq:     8,
			UpdatedAt:   now.UnixNano(),
		},
	}, repo.upsertedStates)
}

func TestSetUnreadAdvancesReadSeqFromLatestMessage(t *testing.T) {
	now := time.Unix(123, 0)
	repo := newConversationSyncRepoStub()
	repo.states[metadbKey("g1", 2)] = metadb.UserConversationState{
		UID:          "u1",
		ChannelID:    "g1",
		ChannelType:  2,
		ReadSeq:      5,
		DeletedToSeq: 2,
		ActiveAt:     99,
		UpdatedAt:    100,
	}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 10, "u2", 100, "c1")

	app := New(Options{
		States:             repo,
		Facts:              repo,
		AuthoritativeFacts: repo,
		Now:                func() time.Time { return now },
	})

	err := app.SetUnread(context.Background(), SetUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		Unread:      3,
	})

	require.NoError(t, err)
	require.Equal(t, []metadb.UserConversationState{
		{
			UID:          "u1",
			ChannelID:    "g1",
			ChannelType:  2,
			ReadSeq:      7,
			DeletedToSeq: 2,
			ActiveAt:     99,
			UpdatedAt:    now.UnixNano(),
		},
	}, repo.upsertedStates)
}

func TestSetUnreadDoesNotMoveReadSeqBackward(t *testing.T) {
	repo := newConversationSyncRepoStub()
	repo.states[metadbKey("g1", 2)] = metadb.UserConversationState{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		ReadSeq:     8,
		UpdatedAt:   100,
	}
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 10, "u2", 100, "c1")

	app := New(Options{
		States:             repo,
		Facts:              repo,
		AuthoritativeFacts: repo,
	})

	err := app.SetUnread(context.Background(), SetUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		Unread:      5,
	})

	require.NoError(t, err)
	require.Empty(t, repo.upsertedStates)
}

func TestSetUnreadUsesAuthoritativeLatestMessage(t *testing.T) {
	base := newConversationSyncRepoStub()
	base.latest[key("g1", 2)] = testMessage("g1", 2, 5, "u2", 100, "stale")
	facts := &authoritativeConversationFactsStub{
		conversationSyncRepoStub: base,
		latest: map[ConversationKey]channel.Message{
			key("g1", 2): testMessage("g1", 2, 10, "u2", 101, "leader"),
		},
	}
	app := New(Options{
		States:             base,
		Facts:              base,
		AuthoritativeFacts: facts,
	})

	err := app.SetUnread(context.Background(), SetUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		Unread:      3,
	})

	require.NoError(t, err)
	require.Equal(t, []ConversationKey{key("g1", 2)}, facts.loads)
	require.Empty(t, base.latestLoads)
	require.Len(t, base.upsertedStates, 1)
	require.Equal(t, uint64(7), base.upsertedStates[0].ReadSeq)
}

func TestSetUnreadFailsClosedWithoutAuthoritativeFacts(t *testing.T) {
	repo := newConversationSyncRepoStub()
	repo.latest[key("g1", 2)] = testMessage("g1", 2, 10, "u2", 100, "stale")
	app := New(Options{States: repo, Facts: repo})

	err := app.SetUnread(context.Background(), SetUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		Unread:      0,
	})

	require.ErrorContains(t, err, "authoritative message facts store not configured")
	require.Empty(t, repo.latestLoads)
	require.Empty(t, repo.upsertedStates)
}

func TestSetUnreadDoesNotWriteWhenAuthoritativeReadFails(t *testing.T) {
	base := newConversationSyncRepoStub()
	facts := &authoritativeConversationFactsStub{
		conversationSyncRepoStub: base,
		err:                      channel.ErrNotReady,
	}
	app := New(Options{States: base, Facts: base, AuthoritativeFacts: facts})

	err := app.SetUnread(context.Background(), SetUnreadCommand{
		UID:         "u1",
		ChannelID:   "g1",
		ChannelType: 2,
		Unread:      0,
	})

	require.ErrorIs(t, err, channel.ErrNotReady)
	require.Empty(t, base.upsertedStates)
}

type authoritativeConversationFactsStub struct {
	*conversationSyncRepoStub
	latest map[ConversationKey]channel.Message
	loads  []ConversationKey
	err    error
}

func (s *authoritativeConversationFactsStub) LoadLatestMessagesAuthoritative(_ context.Context, keys []ConversationKey) (map[ConversationKey]channel.Message, error) {
	s.loads = append(s.loads, keys...)
	if s.err != nil {
		return nil, s.err
	}
	out := make(map[ConversationKey]channel.Message, len(keys))
	for _, key := range keys {
		if msg, ok := s.latest[key]; ok {
			out[key] = msg
		}
	}
	return out, nil
}
