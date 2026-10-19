package types

import (
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/atomic"
)

// Tag is immutable once TagManager has published it: fan-out ranges over Nodes
// and Uids without holding a lock, so every change publishes a modified copy.
// Code holding a published tag must not modify it.
type Tag struct {
	Key string `json:"key"`
	// 关联的频道
	ChannelId   string  `json:"channel_id"`
	ChannelType uint8   `json:"channel_type"`
	Nodes       []*Node `json:"nodes"`
	// 创建时间
	CreatedAt time.Time `json:"created_at"`
	// 最后一次获取时间（创建时的值，之后读取请用 LastGet）
	LastGetTime time.Time     `json:"last_get_time"`
	NodeVersion uint64        `json:"node_version"` // 生成tag时的当前节点版本号，如果当前节点版本号大于生成tag时的节点版本号，则tag失效
	GetCount    atomic.Uint64 `json:"get_count"`    // 获取次数

	lastGet atomic.Int64 // 最后一次获取时间（processStart 之后的纳秒数+1，0 表示没有记录），由并发的读取方更新
}

// processStart anchors access times to the monotonic clock, so wall-clock
// steps do not make tags expire early or never.
var processStart = time.Now()

// Touch records an access; concurrent readers call it without a lock.
func (t *Tag) Touch() {
	t.lastGet.Store(int64(time.Since(processStart)) + 1)
}

// InheritAccess copies src's access time and count, so publishing a modified
// copy of src is not recorded as an access.
func (t *Tag) InheritAccess(src *Tag) {
	t.lastGet.Store(src.lastGet.Load())
	t.GetCount.Store(src.GetCount.Load())
}

// LastGet returns the time of the last access.
func (t *Tag) LastGet() time.Time {
	if n := t.lastGet.Load(); n != 0 {
		return processStart.Add(time.Duration(n - 1))
	}
	return t.LastGetTime
}

// MarshalJSON keeps the previous JSON shape with the live access time.
func (t *Tag) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Key         string    `json:"key"`
		ChannelId   string    `json:"channel_id"`
		ChannelType uint8     `json:"channel_type"`
		Nodes       []*Node   `json:"nodes"`
		CreatedAt   time.Time `json:"created_at"`
		LastGetTime time.Time `json:"last_get_time"`
		NodeVersion uint64    `json:"node_version"`
		GetCount    uint64    `json:"get_count"`
	}{t.Key, t.ChannelId, t.ChannelType, t.Nodes, t.CreatedAt, t.LastGet(), t.NodeVersion, t.GetCount.Load()})
}

func (t *Tag) String() string {

	return fmt.Sprintf("Tag{Key:%s, Nodes:%v, LastGetTime:%v}", t.Key, t.Nodes, t.LastGet())
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

// GetNodeUsers returns a copy of the uids on nodeId.
func (t *Tag) GetNodeUsers(nodeId uint64) []string {
	for _, node := range t.Nodes {
		if node.LeaderId == nodeId {
			return append(make([]string, 0, len(node.Uids)), node.Uids...)
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
