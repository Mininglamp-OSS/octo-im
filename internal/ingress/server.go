package ingress

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type Ingress struct {
	wklog.Log
	boundCluster    icluster.ICluster
	boundTagManager service.ITagManager
	boundPermission *service.PermissionService
	boundStore      *store.Store
}

func New() *Ingress {

	return &Ingress{
		Log: wklog.NewWKLog("Ingress"),
	}
}

func (i *Ingress) BindDeps(cl icluster.ICluster, tm service.ITagManager, perm *service.PermissionService, st *store.Store) {
	if i == nil {
		return
	}
	i.boundCluster = cl
	i.boundTagManager = tm
	i.boundPermission = perm
	i.boundStore = st
}

func (i *Ingress) cluster() icluster.ICluster {
	if i != nil && i.boundCluster != nil {
		return i.boundCluster
	}
	return service.Cluster
}

func (i *Ingress) tagManager() service.ITagManager {
	if i != nil && i.boundTagManager != nil {
		return i.boundTagManager
	}
	return service.TagManager
}

func (i *Ingress) permissionService() *service.PermissionService {
	if i != nil && i.boundPermission != nil {
		return i.boundPermission
	}
	return service.Permission
}

func (i *Ingress) store() *store.Store {
	if i != nil && i.boundStore != nil {
		return i.boundStore
	}
	return service.Store
}

func (i *Ingress) SetRoutes() {
	cl := i.cluster()
	if cl == nil {
		return
	}
	// 获取tag
	cl.Route("/wk/ingress/getTag", i.handleGetTag)
	// 判断接受者是否允许发送消息
	cl.Route("/wk/ingress/allowSend", i.handleAllowSend)
	// 判断频道是否允许发送消息（封禁/解散检查）
	cl.Route("/wk/ingress/hasPermissionForChannel", i.handleHasPermissionForChannel)
	// 更新tag
	cl.Route("/wk/ingress/updateTag", i.handleUpdateTag)
	// 添加tag
	cl.Route("/wk/ingress/addTag", i.handleAddTag)
	// 获取订阅者
	cl.Route("/wk/ingress/getSubscribers", i.handleGetSubscribers)
	// 处理转发的PersistAfter插件调用请求
	cl.Route("/wk/ingress/persistAfter", i.handlePersistAfter)
}

// handlePersistAfter 处理转发的PersistAfter插件调用请求
func (i *Ingress) handlePersistAfter(c *wkserver.Context) {
	req := &PersistAfterReq{}
	if err := req.Decode(c.Body()); err != nil {
		i.Error("handlePersistAfter: decode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	// 反序列化消息批次
	msgBatch := &pluginproto.MessageBatch{}
	if len(req.Messages) > 0 {
		if err := msgBatch.Unmarshal(req.Messages); err != nil {
			i.Error("handlePersistAfter: unmarshal message batch failed", zap.Error(err))
			c.WriteErr(err)
			return
		}
	}

	// 在领导节点上执行插件调用
	if err := i.executePluginPersistAfter(req.ChannelId, req.ChannelType, msgBatch); err != nil {
		c.WriteErr(err)
		return
	}
	c.WriteOk()
}

// executePluginPersistAfter 在本地执行插件PersistAfter调用
func (i *Ingress) executePluginPersistAfter(channelId string, channelType uint8, msgBatch *pluginproto.MessageBatch) error {
	if service.PluginManager == nil {
		return nil
	}
	plugins := service.PluginManager.Plugins(types.PluginPersistAfter)
	if len(plugins) == 0 {
		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), options.G.Channel.ProcessTimeout)
	defer cancel()

	var errs []error
	for _, pg := range plugins {
		err := pg.PersistAfter(timeoutCtx, msgBatch)
		if err != nil {
			i.Error("plugin persist after error", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (i *Ingress) handleGetTag(c *wkserver.Context) {
	req := &TagReq{}
	err := req.decode(c.Body())
	if err != nil {
		i.Error("getTag decode err", zap.Error(err))
		c.WriteErr(err)
		return
	}

	if req.TagKey == "" && req.ChannelId == "" {
		i.Error("tagKey and channelId is nil", zap.Any("req", req))
		c.WriteErr(errors.New("tagKey is nil"))
		return
	}

	if req.TagKey == "" {
		req.TagKey = i.tagManager().GetChannelTag(req.ChannelId, req.ChannelType)
	}

	tag := i.tagManager().Get(req.TagKey)
	if tag == nil {
		i.Error("handleGetTag: tag not exist", zap.Error(err), zap.String("tagKey", req.TagKey))
		c.WriteErr(errors.New("handleGetTag: tag not exist"))
		return
	}
	var uids []string
	var resp *TagResp
	if req.NodeId != 0 {
		resp = &TagResp{
			TagKey: tag.Key,
			Uids:   tag.GetNodeUsers(req.NodeId),
		}
	} else {
		uids = tag.GetUsers()
		resp = &TagResp{
			TagKey: tag.Key,
			Uids:   uids,
		}
	}
	data, err := resp.encode()
	if err != nil {
		i.Error("tagResp encode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

func (i *Ingress) handleAllowSend(ctx *wkserver.Context) {
	body := ctx.Body()
	if op, payload, ok := service.DecodeExtendedAllowSendHeader(body); ok {
		dec := wkproto.NewDecoder(payload)
		switch op {
		case 1:
			channelId, err := dec.String()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			channelType, err := dec.Uint8()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			perm := i.permissionService()
			if perm == nil {
				ctx.Write([]byte{1, byte(wkproto.ReasonSuccess)})
				return
			}
			rc, err := perm.HasPermissionForChannelLocal(channelId, channelType)
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			ctx.Write([]byte{1, byte(rc)})
			return
		case 2:
			channelId, err := dec.String()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			channelType, err := dec.Uint8()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			fromUid, err := dec.String()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			deviceId, err := dec.String()
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			perm := i.permissionService()
			if perm == nil {
				ctx.Write([]byte{1, byte(wkproto.ReasonSuccess)})
				return
			}
			rc, err := perm.HasPermissionForCommChannelLocal(channelId, channelType, service.SenderInfo{
				UID:      fromUid,
				DeviceID: deviceId,
			})
			if err != nil {
				ctx.WriteErr(err)
				return
			}
			ctx.Write([]byte{1, byte(rc)})
			return
		}
	}
	req := &AllowSendReq{}
	err := req.decode(body)
	if err != nil {
		i.Error("handleAllowSend Unmarshal err", zap.Error(err))
		ctx.WriteErr(err)
		return
	}

	reasonCode, err := i.permissionService().AllowSendForPersonLocal(req.From, req.To)
	if err != nil {
		i.Error("handleAllowSend: allowSend failed", zap.Error(err))
		ctx.WriteErr(err)
		return
	}

	if reasonCode == wkproto.ReasonSuccess {
		ctx.WriteOk()
		return
	}
	ctx.WriteErrorAndStatus(errors.New("not allow send"), proto.Status(reasonCode))
}

func (i *Ingress) handleHasPermissionForChannel(ctx *wkserver.Context) {
	req := &ChannelReq{}
	if err := req.Decode(ctx.Body()); err != nil {
		i.Error("handleHasPermissionForChannel decode err", zap.Error(err))
		ctx.WriteErr(err)
		return
	}
	perm := i.permissionService()
	if perm == nil {
		ctx.WriteOk()
		return
	}
	reasonCode, err := perm.HasPermissionForChannelLocal(req.ChannelId, req.ChannelType)
	if err != nil {
		i.Error("handleHasPermissionForChannel failed", zap.Error(err), zap.String("channelId", req.ChannelId), zap.Uint8("channelType", req.ChannelType))
		ctx.WriteErr(err)
		return
	}
	if reasonCode == wkproto.ReasonSuccess {
		ctx.WriteOk()
		return
	}
	ctx.WriteErrorAndStatus(errors.New("channel permission denied"), proto.Status(reasonCode))
}

func (i *Ingress) handleUpdateTag(c *wkserver.Context) {
	var req = &TagUpdateReq{}
	err := req.Decode(c.Body())
	if err != nil {
		i.Error("handleUpdateTag: decode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	if req.TagKey == "" && req.ChannelId == "" {
		i.Error("tagKey and channelId is nil", zap.Any("req", req))
		c.WriteErr(errors.New("tagKey is nil"))
		return
	}
	if len(req.Uids) == 0 {
		i.Error("uids is nil", zap.Any("req", req))
		c.WriteErr(errors.New("uids is nil"))
		return
	}

	// realFakeChannelId := req.ChannelId
	// if options.G.IsCmdChannel(req.ChannelId) {
	// 	realFakeChannelId = options.G.CmdChannelConvertOrginalChannel(req.ChannelId)
	// }

	tm := i.tagManager()
	tagKey := req.TagKey
	if tagKey == "" {
		if req.ChannelId != "" {
			tagKey = tm.GetChannelTag(req.ChannelId, req.ChannelType)
		}
	}
	if tagKey != "" {
		if tm.Exist(tagKey) {
			if req.Remove {
				err = tm.RemoveUsers(tagKey, req.Uids)
				if err != nil {
					i.Warn("handleUpdateTag: remove users failed", zap.Error(err))
				}
			} else {
				err = tm.AddUsers(tagKey, req.Uids)
				if err != nil {
					i.Warn("handleUpdateTag: add users failed", zap.Error(err))
				}
			}
			if req.ChannelTag {
				newTagKey := wkutil.GenUUID()
				err = tm.RenameTag(tagKey, newTagKey)
				if err != nil {
					i.Warn("handleUpdateTag: rename tag failed", zap.Error(err))
				}
				tm.SetChannelTag(req.ChannelId, req.ChannelType, newTagKey)
			}
		}
	}
	c.WriteOk()

}

func (i *Ingress) handleAddTag(c *wkserver.Context) {
	req := &TagAddReq{}
	err := req.Decode(c.Body())
	if err != nil {
		i.Error("handleAddTag: decode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	if req.TagKey == "" {
		i.Error("tagKey is nil", zap.Any("req", req))
		c.WriteErr(errors.New("tagKey is nil"))
		return
	}
	if len(req.Uids) == 0 {
		i.Error("uids is nil", zap.Any("req", req))
		c.WriteErr(errors.New("uids is nil"))
		return
	}

	_, err = i.tagManager().MakeTagWithTagKey(req.TagKey, req.Uids)
	if err != nil {
		i.Error("handleAddTag: add users failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.WriteOk()
}

func (i *Ingress) handleGetSubscribers(c *wkserver.Context) {
	req := &ChannelReq{}
	err := req.Decode(c.Body())
	if err != nil {
		i.Error("handleGetSubscribers: decode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	if req.ChannelId == "" {
		i.Error("handleGetSubscribers: channelId is nil", zap.Any("req", req))
		c.WriteErr(errors.New("channelId is nil"))
		return
	}

	members, err := i.store().GetSubscribers(req.ChannelId, req.ChannelType)
	if err != nil {
		i.Error("handleGetSubscribers: get subscribers failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	subscribers := make([]string, 0, len(members))
	for _, member := range members {
		subscribers = append(subscribers, member.Uid)
	}

	resp := &SubscribersResp{
		Subscribers: subscribers,
	}
	data, err := resp.Encode()
	if err != nil {
		i.Error("handleGetSubscribers: encode failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.Write(data)
}
