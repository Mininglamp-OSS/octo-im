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
	LastGetTime time.Time `json:"last_get_time"`
	NodeVersion uint64    `json:"node_version"` // 生成tag时的当前节点版本号，如果当前节点版本号大于生成tag时的节点版本号，则tag失效

	access atomic.Pointer[tagAccess] // 获取次数和最后一次获取时间，同一个tag发布的各个副本共用
}

// tagAccess is shared by every published copy of a tag, so an access recorded
// on a copy that is being replaced still counts for the tag.
type tagAccess struct {
	lastGet  atomic.Int64  // 最后一次获取时间（processStart 之后的纳秒数+1，0 表示没有记录）
	getCount atomic.Uint64 // 获取次数
}

func (t *Tag) accessState() *tagAccess {
	if a := t.access.Load(); a != nil {
		return a
	}
	t.access.CompareAndSwap(nil, &tagAccess{})
	return t.access.Load()
}

// processStart anchors access times to the monotonic clock, so wall-clock
// steps do not make tags expire early or never.
var processStart = time.Now()

// Touch records an access; concurrent readers call it without a lock.
func (t *Tag) Touch() {
	t.accessState().lastGet.Store(int64(time.Since(processStart)) + 1)
}

// IncGetCount counts a Get; concurrent readers call it without a lock.
func (t *Tag) IncGetCount() {
	t.accessState().getCount.Inc()
}

// GetCount returns how many times the tag was got.
func (t *Tag) GetCount() uint64 {
	return t.accessState().getCount.Load()
}

// ShareAccess makes t record accesses in src's access state, so publishing a
// modified copy of src is not an access and loses none of src's accesses.
func (t *Tag) ShareAccess(src *Tag) {
	t.access.Store(src.accessState())
}

// LastGet returns the time of the last access.
func (t *Tag) LastGet() time.Time {
	if n := t.accessState().lastGet.Load(); n != 0 {
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
	}{t.Key, t.ChannelId, t.ChannelType, t.Nodes, t.CreatedAt, t.LastGet(), t.NodeVersion, t.GetCount()})
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
