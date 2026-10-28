package store

import (
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type heldConfigReplayDB struct {
	wkdb.DB
	olderEntered chan struct{}
	releaseOlder chan struct{}
	newerWritten chan struct{}
}

func (d *heldConfigReplayDB) SaveChannelClusterConfigs(configs []wkdb.ChannelClusterConfig) error {
	for _, cfg := range configs {
		if cfg.ConfVersion == 1 {
			close(d.olderEntered)
			<-d.releaseOlder
		}
	}
	if err := d.DB.SaveChannelClusterConfigs(configs); err != nil {
		return err
	}
	for _, cfg := range configs {
		if cfg.ConfVersion == 2 {
			close(d.newerWritten)
		}
	}
	return nil
}

func TestChannelConfigReplayPreservesQueuedOrder(t *testing.T) {
	opts := wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1), wkdb.WithNodeId(1))
	db := wkdb.NewWukongDB(opts)
	require.NoError(t, db.Open())
	gate := &heldConfigReplayDB{DB: db, olderEntered: make(chan struct{}), releaseOlder: make(chan struct{}), newerWritten: make(chan struct{})}
	s := New(NewOptions(WithDB(gate)))
	// An unbuffered queue makes the write order explicit: version 1 has been
	// received and is held before version 2 is offered to the next consumer.
	s.channelCfgCh = make(chan *channelCfgReq)
	require.NoError(t, s.Start())
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate.releaseOlder) }) }
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(s.Stop) }
	t.Cleanup(func() { unblock(); stop(); _ = db.Close() })
	older := &channelCfgReq{cfg: wkdb.ChannelClusterConfig{ChannelId: "replayed-channel", ChannelType: 2,
		LeaderId: 2, Term: 1, Replicas: []uint64{1, 2, 3}, ConfVersion: 1}, errCh: make(chan error, 1)}
	newer := &channelCfgReq{cfg: older.cfg.Clone(), errCh: make(chan error, 1)}
	newer.cfg.LeaderId, newer.cfg.Term, newer.cfg.ConfVersion = 1, 2, 2
	s.channelCfgCh <- older
	select {
	case <-gate.olderEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("older replay never reached storage")
	}
	go func() { s.channelCfgCh <- newer }()
	select {
	case <-gate.newerWritten:
		t.Error("a later replay overtook an unfinished write for the same channel")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	for _, req := range []*channelCfgReq{older, newer} {
		select {
		case err := <-req.errCh:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("replay did not complete")
		}
	}
	actual, err := db.GetChannelClusterConfig(newer.cfg.ChannelId, newer.cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, newer.cfg.ConfVersion, actual.ConfVersion)
	require.Equal(t, newer.cfg.LeaderId, actual.LeaderId)
	stop()
	require.NoError(t, db.Close())
	// Reopening checks the physical value independently of the live cache.
	db = wkdb.NewWukongDB(opts)
	require.NoError(t, db.Open())
	actual, err = db.GetChannelClusterConfig(newer.cfg.ChannelId, newer.cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, newer.cfg.ConfVersion, actual.ConfVersion)
	require.Equal(t, newer.cfg.LeaderId, actual.LeaderId)
}
