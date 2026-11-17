package types

import (
	"fmt"
	"time"

	"go.uber.org/atomic"
)

type Tag struct {
	Key string `json:"key"`
	// 关联的频道
	ChannelId   string  `json:"channel_id"`
	ChannelType uint8   `json:"channel_type"`
	Nodes       []*Node `json:"nodes"`
	// 创建时间
	CreatedAt time.Time `json:"created_at"`
	// 最后一次获取时间
	LastGetTime time.Time     `json:"last_get_time"`
	NodeVersion uint64        `json:"node_version"` // 生成tag时的当前节点版本号，如果当前节点版本号大于生成tag时的节点版本号，则tag失效
	GetCount    atomic.Uint64 `json:"get_count"`    // 获取次数
}

func (t *Tag) String() string {

	return fmt.Sprintf("Tag{Key:%s, Nodes:%v, LastGetTime:%v}", t.Key, t.Nodes, t.LastGetTime)
}

type Node struct {
	// 节点id
	LeaderId uint64 `json:"leader_id"`
	// 节点id对应的用户集合
	Uids []string `json:"uids"`
	// 用户涉及到的slot
	SlotIds []uint32 `json:"slot_ids"`
}

func (n *Node) String() string {
	return fmt.Sprintf("Node{LeaderId:%d, Uids:%v, SlotIds:%v}", n.LeaderId, n.Uids, n.SlotIds)
}

func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	var uids []string
	if len(n.Uids) > 0 {
		uids = make([]string, len(n.Uids))
		copy(uids, n.Uids)
	}
	var slotIds []uint32
	if len(n.SlotIds) > 0 {
		slotIds = make([]uint32, len(n.SlotIds))
		copy(slotIds, n.SlotIds)
	}
	return &Node{
		LeaderId: n.LeaderId,
		Uids:     uids,
		SlotIds:  slotIds,
	}
}

func (t *Tag) Clone() *Tag {
	if t == nil {
		return nil
	}
	var nodes []*Node
	if len(t.Nodes) > 0 {
		nodes = make([]*Node, len(t.Nodes))
		for i, n := range t.Nodes {
			nodes[i] = n.Clone()
		}
	}
	cp := &Tag{
		Key:         t.Key,
		ChannelId:   t.ChannelId,
		ChannelType: t.ChannelType,
		Nodes:       nodes,
		CreatedAt:   t.CreatedAt,
		LastGetTime: t.LastGetTime,
		NodeVersion: t.NodeVersion,
	}
	cp.GetCount.Store(t.GetCount.Load())
	return cp
}

func (t *Tag) GetNodeUsers(nodeId uint64) []string {
	for _, node := range t.Nodes {
		if node.LeaderId == nodeId {
			return node.Uids
		}
	}
	return nil
}

func (t *Tag) ExistUserInNode(uid string, nodeId uint64) bool {
	for _, node := range t.Nodes {
		if node.LeaderId == nodeId {
			for _, u := range node.Uids {
				if u == uid {
					return true
				}
			}
		}
	}
	return false
}

func (t *Tag) GetUsers() []string {
	userCount := 0
	for _, node := range t.Nodes {
		userCount += len(node.Uids)
	}
	users := make([]string, 0, userCount)
	for _, node := range t.Nodes {
		users = append(users, node.Uids...)
	}
	return users
}
