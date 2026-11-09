package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type subscriberSnapshotDB struct {
	wkdb.DB
	members []wkdb.Member
	reads   int
	read    func()
}

func (d *subscriberSnapshotDB) GetSubscribers(string, uint8) ([]wkdb.Member, error) {
	d.reads++
	if d.read != nil {
		d.read()
	}
	return append([]wkdb.Member(nil), d.members...), nil
}

func TestSubscriberSnapshotRevisionSurvivesLostInvalidationAndOwnerRestart(t *testing.T) {
	db := &subscriberSnapshotDB{members: []wkdb.Member{{Uid: "old"}}}
	s := New(NewOptions(WithDB(db)))
	first, err := s.GetSubscriberSnapshot("room", 2, "")
	require.NoError(t, err)
	require.True(t, first.Changed)
	require.NotEmpty(t, first.Revision)
	cached, err := s.GetSubscriberSnapshot("room", 2, first.Revision)
	require.NoError(t, err)
	require.False(t, cached.Changed)
	require.Empty(t, cached.Members)
	require.Equal(t, 1, db.reads)
	require.NoError(t, s.changeSubscribers("room", 2, func() error { db.members = []wkdb.Member{{Uid: "new"}}; return nil }))
	after, err := s.GetSubscriberSnapshot("room", 2, first.Revision)
	require.NoError(t, err)
	require.True(t, after.Changed)
	require.NotEqual(t, first.Revision, after.Revision)
	require.Equal(t, []wkdb.Member{{Uid: "new"}}, after.Members)
	restarted := New(NewOptions(WithDB(db)))
	refreshed, err := restarted.GetSubscriberSnapshot("room", 2, after.Revision)
	require.NoError(t, err)
	require.True(t, refreshed.Changed)
	require.NotEqual(t, after.Revision, refreshed.Revision)
	require.Equal(t, after.Members, refreshed.Members)
	failed := errors.New("write failed after changing data")
	require.ErrorIs(t, s.changeSubscribers("room", 2, func() error { db.members = nil; return failed }), failed)
	empty, err := s.GetSubscriberSnapshot("room", 2, after.Revision)
	require.NoError(t, err)
	require.True(t, empty.Changed)
	require.Empty(t, empty.Members)
}

func TestSubscriberSnapshotDoesNotMixRevisionWithConcurrentWrite(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	db := &subscriberSnapshotDB{members: []wkdb.Member{{Uid: "old"}}, read: func() { close(entered); <-release }}
	s := New(NewOptions(WithDB(db)))
	var before SubscriberSnapshot
	var readErr, errorWrite error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); before, readErr = s.GetSubscriberSnapshot("room", 2, "") }()
	<-entered
	started := make(chan struct{})
	go func() {
		defer wg.Done()
		close(started)
		errorWrite = s.changeSubscribers("room", 2, func() error { db.members = []wkdb.Member{{Uid: "new"}}; return nil })
	}()
	<-started
	close(release)
	wg.Wait()
	require.NoError(t, readErr)
	require.NoError(t, errorWrite)
	require.Equal(t, []wkdb.Member{{Uid: "old"}}, before.Members)
	db.read = nil
	after, err := s.GetSubscriberSnapshot("room", 2, before.Revision)
	require.NoError(t, err)
	require.True(t, after.Changed)
	require.NotEqual(t, before.Revision, after.Revision)
	require.Equal(t, []wkdb.Member{{Uid: "new"}}, after.Members)
}

func BenchmarkSubscriberSnapshotUnchanged(b *testing.B) {
	db := &subscriberSnapshotDB{members: []wkdb.Member{{Uid: "member"}}}
	s := New(NewOptions(WithDB(db)))
	first, _ := s.GetSubscriberSnapshot("room", 2, "")
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := s.GetSubscriberSnapshot("room", 2, first.Revision)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if db.reads != 1 {
		b.Fatalf("unchanged snapshot read database %d times", db.reads)
	}
}
