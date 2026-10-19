// Package wkdbtest exposes read-only and construction helpers for tests that
// need the stored message columns or a crash-simulating filesystem. It holds no
// logic of its own and changes no production default.
package wkdbtest

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/cockroachdb/pebble/vfs"
)

type rawColumnReader interface {
	RawMessageColumns(channelId string, channelType uint8, seq uint64) ([14][]byte, error)
}

// RawMessageColumns returns the raw bytes of the 14 stored columns of message
// seq, in wkdb.MessageColumnOrder.
func RawMessageColumns(db wkdb.DB, channelId string, channelType uint8, seq uint64) ([14][]byte, error) {
	r, ok := db.(rawColumnReader)
	if !ok {
		return [14][]byte{}, errors.New("wkdbtest: db does not expose raw message columns")
	}
	return r.RawMessageColumns(channelId, channelType, seq)
}

// OpenWithFS opens a wukongDB in dir on fs, for example a vfs.NewStrictMem()
// used to drop unsynced writes.
func OpenWithFS(dir string, fs vfs.FS, opts ...wkdb.Option) (wkdb.DB, error) {
	opts = append([]wkdb.Option{wkdb.WithDir(dir), wkdb.WithFS(fs)}, opts...)
	db := wkdb.NewWukongDB(wkdb.NewOptions(opts...))
	if err := db.Open(); err != nil {
		return nil, err
	}
	return db, nil
}
