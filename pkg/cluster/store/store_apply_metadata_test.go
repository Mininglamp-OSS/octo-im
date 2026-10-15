package store

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type metadataBatchOrderDB struct {
	wkdb.DB
	events     []string
	batchSizes []int
}

func (d *metadataBatchOrderDB) UpdateChannelInfoBatch(infos []wkdb.ChannelInfo) error {
	d.batchSizes = append(d.batchSizes, len(infos))
	d.events = append(d.events, fmt.Sprintf("batch:%d", len(infos)))
	return nil
}
func (d *metadataBatchOrderDB) RemoveSubscribers(string, uint8, []string) error {
	d.events = append(d.events, "remove")
	return nil
}
func (d *metadataBatchOrderDB) DeleteChannel(string, uint8) error {
	d.events = append(d.events, "delete")
	return nil
}
func (d *metadataBatchOrderDB) UpdateChannel(wkdb.ChannelInfo) error {
	d.events = append(d.events, "single")
	return nil
}

func TestApplyMetadataBatchesStayWithinCommandBoundaries(t *testing.T) {
	db := &metadataBatchOrderDB{}
	s := &Store{wdb: db}
	metadata := func() *CMD {
		data, err := EncodeChannelInfo(wkdb.ChannelInfo{ChannelId: "batch-channel", ChannelType: 2}, CmdVersionChannelInfo)
		require.NoError(t, err)
		return NewCMDWithVersion(CMDUpdateChannelInfo, data, CmdVersionChannelInfo)
	}
	var cmds []*CMD
	for i := 0; i < 258; i++ {
		cmds = append(cmds, metadata())
	}
	cmds = append(cmds, NewCMD(CMDRemoveSubscribers, EncodeChannelUids("batch-channel", 2, []string{"member"})))
	cmds = append(cmds, metadata(), metadata(), NewCMD(CMDDeleteChannel, EncodeChannel("batch-channel", 2)), metadata())
	indexes := make([]uint64, len(cmds))
	for i := range indexes {
		indexes[i] = uint64(i + 1)
	}
	require.NoError(t, s.applyCMDs(cmds, indexes))
	require.Equal(t, []string{"batch:256", "batch:2", "remove", "batch:2", "delete", "single"}, db.events)
	require.Equal(t, []int{256, 2, 2}, db.batchSizes)
}
