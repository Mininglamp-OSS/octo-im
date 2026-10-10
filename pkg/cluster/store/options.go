package store

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type Options struct {
	NodeId uint64 // 节点ID

	Slot icluster.Slot

	DB wkdb.DB

	Channel icluster.Channel

	IsCmdChannel func(channel string) bool

	// Called only after a successful metadata write. Must not wait or do I/O;
	// slot AppliedIndex may not yet include this write when the callback runs.
	OnChannelConfigSaved func(string, uint8)
}

func NewOptions(opt ...Option) *Options {
	opts := &Options{}
	for _, o := range opt {
		o(opts)
	}
	return opts
}

type Option func(*Options)

func WithOnChannelConfigSaved(fn func(string, uint8)) Option {
	return func(o *Options) { o.OnChannelConfigSaved = fn }
}

func WithNodeId(nodeId uint64) Option {
	return func(o *Options) {
		o.NodeId = nodeId
	}
}

func WithSlot(slot icluster.Slot) Option {
	return func(o *Options) {
		o.Slot = slot
	}
}

func WithChannel(channel icluster.Channel) Option {
	return func(o *Options) {
		o.Channel = channel
	}
}

func WithDB(db wkdb.DB) Option {
	return func(o *Options) {
		o.DB = db
	}
}

func WithIsCmdChannel(isCmdChannel func(channel string) bool) Option {
	return func(o *Options) {
		o.IsCmdChannel = isCmdChannel
	}
}
