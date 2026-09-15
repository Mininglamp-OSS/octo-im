package event

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type conns struct {
	conns []*eventbus.Conn // 一个用户有多个连接
	sync.RWMutex
}

func newConns() *conns {
	return &conns{}
}

func (c *conns) add(cn *eventbus.Conn) {
	c.Lock()
	defer c.Unlock()
	c.conns = append(c.conns, cn)
}

// 添加或更新
func (c *conns) addOrUpdateConn(conn *eventbus.Conn) {
	c.Lock()
	defer c.Unlock()
	// 判断连接是否存在
	exist := false
	for i, cn := range c.conns {
		if cn.NodeId == conn.NodeId && cn.ConnId == conn.ConnId {
			exist = true
			preserveRuntimeState(cn, conn)
			c.conns[i] = conn
			break
		}
	}
	if !exist {
		c.conns = append(c.conns, conn)
	}
}

// addOrUpdateRecoveredConn installs a recovery snapshot only when the
// numeric connection ID is still unclaimed or belongs to the same session.
// A normal connection event remains authoritative when an ID is reused.
func (c *conns) addOrUpdateRecoveredConn(conn *eventbus.Conn) {
	c.Lock()
	defer c.Unlock()
	for i, current := range c.conns {
		if current.NodeId != conn.NodeId || current.ConnId != conn.ConnId {
			continue
		}
		if !current.SameSession(conn) {
			return
		}
		preserveRuntimeState(current, conn)
		c.conns[i] = conn
		return
	}
	c.conns = append(c.conns, conn)
}

func (c *conns) remove(cn *eventbus.Conn) {
	c.Lock()
	defer c.Unlock()
	for i, conn := range c.conns {
		matches := conn.SameSession(cn)
		if cn.IsLegacySession() {
			matches = conn.LegacyMatches(cn)
		}
		if matches {
			c.conns = append(c.conns[:i], c.conns[i+1:]...)
			return
		}
	}
}

func preserveRuntimeState(current, incoming *eventbus.Conn) {
	if current == nil || incoming == nil || current == incoming || !current.SameSession(incoming) {
		return
	}
	incoming.InPacketCount.Store(current.InPacketCount.Load())
	incoming.OutPacketCount.Store(current.OutPacketCount.Load())
	incoming.InPacketByteCount.Store(current.InPacketByteCount.Load())
	incoming.OutPacketByteCount.Store(current.OutPacketByteCount.Load())
	incoming.InMsgCount.Store(current.InMsgCount.Load())
	incoming.OutMsgCount.Store(current.OutMsgCount.Load())
	incoming.InMsgByteCount.Store(current.InMsgByteCount.Load())
	incoming.OutMsgByteCount.Store(current.OutMsgByteCount.Load())
	if len(incoming.AesIV) == 0 && len(current.AesIV) > 0 {
		incoming.AesIV = append([]byte(nil), current.AesIV...)
	}
	if len(incoming.AesKey) == 0 && len(current.AesKey) > 0 {
		incoming.AesKey = append([]byte(nil), current.AesKey...)
	}
	if current.LastActive > incoming.LastActive {
		incoming.LastActive = current.LastActive
	}
}

// 移除节点对应的所有连接
func (c *conns) removeConnByNodeId(nodeId uint64) {
	c.Lock()
	defer c.Unlock()
	for i := 0; i < len(c.conns); i++ {
		if c.conns[i].NodeId == nodeId {
			// 删除当前元素并检查下一元素
			c.conns = append(c.conns[:i], c.conns[i+1:]...)
			i-- // 回退索引以便检查新的当前元素
		}
	}
}

func (c *conns) connByConnId(nodeId uint64, connId int64) *eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	for _, conn := range c.conns {
		if conn.ConnId == connId && conn.NodeId == nodeId {
			return conn
		}
	}
	return nil
}

func (c *conns) updateConnAuth(nodeId uint64, connId int64, auth bool) {
	c.Lock()
	defer c.Unlock()
	for i, conn := range c.conns {
		if conn.ConnId == connId && conn.NodeId == nodeId {
			c.conns[i].Auth = auth
			return
		}
	}
}

func (c *conns) updateConn(connId int64, nodeId uint64, newConn *eventbus.Conn) {
	c.Lock()
	defer c.Unlock()
	for i, conn := range c.conns {
		if conn.ConnId == connId && conn.NodeId == nodeId {
			c.conns[i] = newConn
			return
		}
	}
}

func (c *conns) len() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.conns)
}

func (c *conns) allConns() []*eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	return append([]*eventbus.Conn(nil), c.conns...)
}

func (c *conns) authedConns() []*eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	conns := make([]*eventbus.Conn, 0, len(c.conns))
	for _, conn := range c.conns {
		if conn.Auth {
			conns = append(conns, conn)
		}
	}
	return conns
}

func (c *conns) count() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.conns)
}

func (c *conns) connsByDeviceFlag(deviceFlag wkproto.DeviceFlag) []*eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	conns := make([]*eventbus.Conn, 0, len(c.conns))
	for _, conn := range c.conns {
		if conn.DeviceFlag == deviceFlag {
			conns = append(conns, conn)
		}
	}
	return conns
}

func (c *conns) countByDeviceFlag(deviceFlag wkproto.DeviceFlag) int {
	c.RLock()
	defer c.RUnlock()
	count := 0
	for _, conn := range c.conns {
		if conn.DeviceFlag == deviceFlag {
			count++
		}
	}
	return count
}

func (c *conns) connById(nodeId uint64, connId int64) *eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	for _, conn := range c.conns {
		if conn.ConnId == connId && conn.NodeId == nodeId {
			return conn
		}
	}
	return nil
}

func (c *conns) connsByNodeId(nodeId uint64) []*eventbus.Conn {
	c.RLock()
	defer c.RUnlock()
	conns := make([]*eventbus.Conn, 0, len(c.conns))
	for _, conn := range c.conns {
		if conn.NodeId == nodeId {
			conns = append(conns, conn)
		}
	}
	return conns
}

func (c *conns) nodeIds() []uint64 {
	c.RLock()
	defer c.RUnlock()
	nodeIds := make([]uint64, 0, len(c.conns))
	for _, conn := range c.conns {
		for _, nodeId := range nodeIds {
			if nodeId == conn.NodeId {
				continue
			}
		}
		nodeIds = append(nodeIds, conn.NodeId)
	}
	return nodeIds
}

func (c *conns) reset() {
	c.Lock()
	defer c.Unlock()
	c.conns = nil
}
