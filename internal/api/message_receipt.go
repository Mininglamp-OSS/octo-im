package api

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func committedMessageReceipt(ctx context.Context, db wkdb.DB, channelID string, channelType uint8, fromUID, clientMsgNo string) (wkdb.Message, error) {
	// Read the durable boundary first: an indexed but unapplied suffix can still be replaced.
	applied, err := db.GetChannelAppliedIndex(channelID, channelType)
	if err != nil {
		return wkdb.Message{}, err
	}
	if applied == 0 {
		return wkdb.Message{}, wkdb.ErrNotFound
	}
	msg, err := db.LoadMsgBySenderClientMsgNo(ctx, channelID, channelType, fromUID, clientMsgNo)
	if err != nil {
		return wkdb.Message{}, err
	}
	if msg.MessageSeq == 0 || uint64(msg.MessageSeq) > applied {
		return wkdb.Message{}, wkdb.ErrNotFound
	}
	return msg, nil
}
