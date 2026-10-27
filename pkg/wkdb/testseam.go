package wkdb

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

// MessageColumnOrder is the order of the 14 stored message columns returned by
// RawMessageColumns.
var MessageColumnOrder = [14][2]byte{
	key.TableMessage.Column.Header,
	key.TableMessage.Column.Setting,
	key.TableMessage.Column.Expire,
	key.TableMessage.Column.MessageId,
	key.TableMessage.Column.MessageSeq,
	key.TableMessage.Column.ClientMsgNo,
	key.TableMessage.Column.StreamNo,
	key.TableMessage.Column.Timestamp,
	key.TableMessage.Column.ChannelId,
	key.TableMessage.Column.ChannelType,
	key.TableMessage.Column.Topic,
	key.TableMessage.Column.FromUid,
	key.TableMessage.Column.Payload,
	key.TableMessage.Column.Term,
}

// RawMessageColumns reads the raw stored bytes of the 14 message columns at seq,
// in MessageColumnOrder. It is read-only and is not part of the DB interface;
// tests reach it through wkdbtest.RawMessageColumns.
func (wk *wukongDB) RawMessageColumns(channelId string, channelType uint8, seq uint64) ([14][]byte, error) {
	var cols [14][]byte
	db := wk.channelDb(channelId, channelType)
	for i, col := range MessageColumnOrder {
		v, closer, err := db.Get(key.NewMessageColumnKey(channelId, channelType, seq, col))
		if err != nil {
			if err == pebble.ErrNotFound {
				return cols, fmt.Errorf("message %d column %x: %w", seq, col, ErrNotFound)
			}
			return cols, err
		}
		cols[i] = append([]byte{}, v...)
		_ = closer.Close()
	}
	return cols, nil
}
