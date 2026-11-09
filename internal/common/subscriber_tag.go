package common

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// GetOrMakeSubscriberTag is used by the distribution owner before assigning a
// Tag to new messages. A failed membership API may already have committed its
// database write, so an earlier warm Tag cannot certify the recipient set.
// Original and cmd channels validate against the ORIGINAL channel's slot owner.
// If that authority is unreachable, return an error instead of stale recipients.
func (s *Service) GetOrMakeSubscriberTag(channelId string, channelType uint8) (*types.Tag, error) {
	original := channelId
	isCmd := options.G.IsCmdChannel(channelId)
	if isCmd {
		original = options.G.CmdChannelConvertOrginalChannel(channelId)
	}
	owner, err := service.Cluster.SlotLeaderIdOfChannel(original, channelType)
	if err != nil {
		return nil, err
	}
	if owner == 0 {
		return nil, fmt.Errorf("subscriber slot owner is zero: %s/%d", original, channelType)
	}
	key := service.TagManager.GetChannelTag(channelId, channelType)
	tag := service.TagManager.Get(key)
	known := ""
	if tag != nil {
		known = tag.SubscriberRevision.Load()
	}
	var snapshot *ingress.SubscriberSnapshotResp
	if options.G.IsLocalNode(owner) {
		local, err := service.Store.GetSubscriberSnapshot(original, channelType, known)
		if err != nil {
			return nil, err
		}
		snapshot = &ingress.SubscriberSnapshotResp{Revision: local.Revision, Changed: local.Changed}
		for _, m := range local.Members {
			snapshot.Subscribers = append(snapshot.Subscribers, m.Uid)
		}
	} else {
		snapshot, err = s.client.RequestSubscriberSnapshot(owner, &ingress.SubscriberSnapshotReq{ChannelId: original, ChannelType: channelType, Revision: known})
		if err != nil {
			return nil, err
		}
	}
	if !snapshot.Changed && tag != nil {
		return tag, nil
	}
	uids := snapshot.Subscribers
	// Preserve the existing ordinary-channel implicit recipient rules. Cmd
	// channels historically use only the original channel's stored members.
	if !isCmd {
		if channelType == wkproto.ChannelTypeCustomerService {
			visitor, _ := options.G.GetCustomerServiceVisitorUID(channelId)
			if visitor != "" {
				uids = append(uids, visitor)
			}
		} else if channelType == wkproto.ChannelTypeVisitors {
			uids = append(uids, channelId)
		}
	}
	tag, err = service.TagManager.MakeSubscriberTag(uids, snapshot.Revision)
	if err != nil {
		return nil, err
	}
	service.TagManager.SetChannelTag(channelId, channelType, tag.Key)
	return tag, nil
}
