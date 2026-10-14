package store

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type configSaveDB struct {
	wkdb.DB
	err   error
	saved bool
}

func (d *configSaveDB) SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig) error {
	d.saved = d.err == nil
	return d.err
}

func TestConfigNotificationAfterSuccessfulWrite(t *testing.T) {
	db := &configSaveDB{}
	notifications := 0
	s := New(NewOptions(WithDB(db), WithOnChannelConfigSaved(func(id string, typ uint8) {
		require.True(t, db.saved)
		require.Equal(t, "channel", id)
		require.Equal(t, uint8(2), typ)
		notifications++
	})))
	request := &channelCfgReq{cfg: wkdb.ChannelClusterConfig{ChannelId: "channel", ChannelType: 2}, errCh: make(chan error, 1)}
	require.NoError(t, s.handleChannelClusterConfigSaves([]*channelCfgReq{request}))
	require.NoError(t, <-request.errCh)
	require.Equal(t, 1, notifications)
	db.err = errors.New("disk write failed")
	require.NoError(t, s.handleChannelClusterConfigSaves([]*channelCfgReq{request}))
	require.ErrorIs(t, <-request.errCh, db.err)
	require.Equal(t, 1, notifications, "failed metadata must never be advertised")
}
