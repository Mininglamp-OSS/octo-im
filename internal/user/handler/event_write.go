package handler

import (
	"github.com/WuKongIM/WuKongIM/pkg/jsonrpc"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) writeFrame(ctx *eventbus.UserContext) {
	type forwardKey struct {
		nodeId uint64
		uid    string
	}
	forwarded := make(map[forwardKey][]*eventbus.Event)
	forwardOrder := make([]forwardKey, 0)
	for _, event := range ctx.Events {
		conn := event.Conn
		frame := event.Frame
		if conn == nil || frame == nil {
			h.Error("write frame conn or frame is nil")
			continue
		}
		if conn.NodeId == 0 {
			h.Error("writeFrame: conn node id is 0")
			continue
		}
		// 如果不是本地节点，则转发写请求
		if !options.G.IsLocalNode(conn.NodeId) {
			// 统计
			h.totalOut(conn, frame)
			key := forwardKey{nodeId: conn.NodeId, uid: conn.Uid}
			if _, ok := forwarded[key]; !ok {
				forwardOrder = append(forwardOrder, key)
			}
			forwarded[key] = append(forwarded[key], event)
			continue
		}

		// 本地节点写请求
		h.writeLocalFrame(event)

	}
	for _, key := range forwardOrder {
		h.forwardsToNode(key.nodeId, key.uid, forwarded[key])
	}
}
func (h *Handler) writeLocalFrame(event *eventbus.Event) {
	conn := event.Conn
	frame := event.Frame
	if recvPacket, ok := frame.(*wkproto.RecvPacket); ok && !options.G.DisableEncryption && !conn.IsJsonRpc && recvPacket.MsgKey == "" {
		realConn, err := common.CheckConnValidAndGetRealConn(conn)
		if err != nil || realConn == nil {
			h.Warn("writeFrame: cannot resolve physical session for deferred encryption",
				zap.Error(err), zap.String("uid", conn.Uid), zap.Int64("connId", conn.ConnId))
			return
		}
		physical, ok := realConn.Context().(*eventbus.Conn)
		if !ok || physical == nil || len(physical.AesKey) == 0 || len(physical.AesIV) == 0 {
			h.Warn("writeFrame: physical session crypto is unavailable",
				zap.String("uid", conn.Uid), zap.Int64("connId", conn.ConnId))
			return
		}
		finalized, err := finalizeRecvPacket(recvPacket, physical)
		if err != nil {
			h.Warn("writeFrame: deferred encryption failed", zap.Error(err), zap.String("uid", conn.Uid))
			return
		}
		frame = finalized
	}

	var (
		data []byte
		err  error
	)

	if conn.IsJsonRpc {
		req, err := jsonrpc.FromFrame(event.ReqId, frame)
		if err != nil {
			h.Error("writeFrame jsonrpc: from frame err", zap.Error(err))
			return
		}
		data, err = jsonrpc.Encode(req)
		if err != nil {
			h.Error("writeFrame jsonrpc: encode err", zap.Error(err))
			return
		}
	} else {
		data, err = eventbus.Proto.EncodeFrame(frame, conn.ProtoVersion)
		if err != nil {
			h.Error("writeFrame: encode frame err", zap.Error(err))
		}
	}

	// 统计
	h.totalOut(conn, frame)
	// 记录消息路径
	event.Track.Record(track.PositionConnWrite)

	// 获取到真实连接
	realConn, err := common.CheckConnValidAndGetRealConn(conn)
	if err != nil {
		h.Warn("writeFrame: conn invalid", zap.Error(err), zap.String("uid", conn.Uid), zap.String("deviceId", conn.DeviceId), zap.Int64("connId", conn.ConnId))
		return
	}
	if realConn == nil {
		h.Info("writeFrame: conn not exist", zap.String("uid", conn.Uid), zap.Uint64("nodeId", conn.NodeId), zap.Int64("connId", conn.ConnId), zap.Uint64("sourceNodeId", event.SourceNodeId))
		// 如果连接不存在了，并且写入事件是其他节点发起的，说明其他节点还不知道连接已经关闭，需要通知其他节点关闭连接
		if event.SourceNodeId != 0 && !options.G.IsLocalNode(event.SourceNodeId) {
			h.forwardToNode(event.SourceNodeId, conn.Uid, &eventbus.Event{
				Type:         eventbus.EventConnRemove,
				Conn:         conn,
				SourceNodeId: options.G.Cluster.NodeId,
			})
		} else if event.SourceNodeId == 0 || options.G.IsLocalNode(event.SourceNodeId) { // 如果是本节点事件，直接删除连接
			eventbus.User.DirectRemoveConn(conn)
		}
		return
	}

	// 开始写入数据
	wsConn, wsok := realConn.(wknet.IWSConn) // websocket连接
	if wsok {
		if conn.IsJsonRpc {
			err = wsConn.WriteServerText(data)
		} else {
			err = wsConn.WriteServerBinary(data)
		}
		if err != nil {
			h.Warn("writeFrame: Failed to ws write the message", zap.Error(err))
		}
	} else {
		_, err := realConn.WriteToOutboundBuffer(data)
		if err != nil {
			h.Warn("writeFrame: Failed to write the message", zap.Error(err))
		}
	}
	_ = realConn.WakeWrite()
}

func finalizeRecvPacket(packet *wkproto.RecvPacket, conn *eventbus.Conn) (*wkproto.RecvPacket, error) {
	clone := *packet
	payload, err := wkutil.AesEncryptPkcs7Base64(packet.Payload, conn.AesKey, conn.AesIV)
	if err != nil {
		return nil, err
	}
	clone.Payload = payload
	msgKeyBytes, err := wkutil.AesEncryptPkcs7Base64([]byte(clone.VerityString()), conn.AesKey, conn.AesIV)
	if err != nil {
		return nil, err
	}
	clone.MsgKey = wkutil.MD5(string(msgKeyBytes))
	return &clone, nil
}
