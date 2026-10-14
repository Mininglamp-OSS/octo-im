package manager

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/errors"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"go.uber.org/zap"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

type TagManager struct {
	bluckets []*tagBlucket
	// 获取当前节点版本号
	nodeVersion func() uint64
	wklog.Log
	sync.RWMutex
}

func NewTagManager(blucketCount int, nodeVersion func() uint64) *TagManager {
	tg := &TagManager{
		nodeVersion: nodeVersion,
		Log:         wklog.NewWKLog("TagManager"),
	}
	tg.bluckets = make([]*tagBlucket, blucketCount)
	for i := 0; i < blucketCount; i++ {
		tg.bluckets[i] = newTagBlucket(i, options.G.Tag.Expire, tg.existTag)
	}
	return tg
}

func (t *TagManager) Start() error {
	var err error
	for _, b := range t.bluckets {
		err = b.start()
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *TagManager) Stop() {
	for _, b := range t.bluckets {
		b.stop()
	}
}

func (t *TagManager) MakeTag(uids []string) (*types.Tag, error) {
	tagKey := wkutil.GenUUID()
	return t.MakeTagWithTagKey(tagKey, uids)
}

func (t *TagManager) MakeTagWithTagKey(tagKey string, uids []string) (*types.Tag, error) {

	tag, err := t.MakeTagNotCacheWithTagKey(tagKey, uids)
	if err != nil {
		return nil, err
	}
	t.getBlucketByTagKey(tagKey).setTag(tag)
	return tag, nil
}

func (t *TagManager) MakeTagNotCacheWithTagKey(tagKey string, uids []string) (*types.Tag, error) {
	nw := time.Now()
	tag := &types.Tag{
		Key:         tagKey,
		LastGetTime: nw,
		NodeVersion: t.nodeVersion(),
		CreatedAt:   nw,
	}

	nodes, err := t.calcUsersInNode(uids)
	if err != nil {
		return nil, err
	}
	tag.Nodes = nodes

	return tag, nil
}

func (t *TagManager) AddUsers(tagKey string, uids []string) error {

	t.Lock()
	defer t.Unlock()

	tag := t.getTag(tagKey)
	if tag == nil {
		return errors.TagNotExist(tagKey)
	}
	return t.addUsersLocked(tag, uids)
}

// addUsersLocked adds uids to tag; the caller holds the manager lock.
func (t *TagManager) addUsersLocked(tag *types.Tag, uids []string) error {
	// 计算用户所在的节点（先计算，失败时不改动tag）
	nodes, err := t.calcUsersInNode(uids)
	if err != nil {
		return err
	}
	// 去除已经存在的用户
	t.removeExistUidsInTag(tag, uids)
	// 合并节点
	t.mergeNodes(tag, nodes)

	return nil
}

func (t *TagManager) removeExistUidsInTag(tag *types.Tag, uids []string) {
	for _, node := range tag.Nodes {
		for _, uid := range uids {
			for i, nodeUid := range node.Uids {
				if nodeUid == uid {
					node.Uids = append(node.Uids[:i], node.Uids[i+1:]...)
					break
				}
			}
		}
	}
}

func (t *TagManager) RemoveUsers(tagKey string, uids []string) error {

	t.Lock()
	defer t.Unlock()

	tag := t.getTag(tagKey)
	if tag == nil {
		return errors.TagNotExist(tagKey)
	}
	return t.removeUsersLocked(tag, uids)
}

// removeUsersLocked removes uids from tag; the caller holds the manager lock.
func (t *TagManager) removeUsersLocked(tag *types.Tag, uids []string) error {
	// 先确定所有用户所在的节点，失败时不改动tag
	leaderIds := make([]uint64, len(uids))
	for i, uid := range uids {
		leaderIds[i] = service.Cluster.SlotLeaderId(service.Cluster.GetSlotId(uid))
		if leaderIds[i] == 0 {
			return errors.TagSlotLeaderIsZero
		}
	}
	for i, uid := range uids {
		leaderId := leaderIds[i]
		for _, node := range tag.Nodes {
			if node.LeaderId == leaderId {
				for i, nodeUid := range node.Uids {
					if nodeUid == uid {
						node.Uids = append(node.Uids[:i], node.Uids[i+1:]...)
						break
					}
				}
				break
			}
		}
	}
	return nil
}

func (t *TagManager) RemoveTag(tagKey string) {
	t.removeTag(tagKey)
}

func (t *TagManager) GetUsers(tagKey string) []string {
	tag := t.getTag(tagKey)
	if tag == nil {
		return nil
	}
	var uids []string
	for _, node := range tag.Nodes {
		uids = append(uids, node.Uids...)
	}
	return uids
}

func (t *TagManager) Get(tagKey string) *types.Tag {
	tag := t.getTag(tagKey)
	if tag == nil {
		return nil
	}
	if tag.NodeVersion < t.nodeVersion() {
		t.Warn("tag is expired, tagNodeVersion < currentNodeVersion ", zap.String("tagKey", tagKey), zap.Uint64("tagNodeVersion", tag.NodeVersion), zap.Uint64("currentNodeVersion", t.nodeVersion()))
		return nil
	}
	tag.LastGetTime = time.Now()
	tag.GetCount.Inc()
	return tag
}

func (t *TagManager) Exist(tagKey string) bool {
	return t.getTag(tagKey) != nil
}

func (t *TagManager) RenameTag(oldTagKey, newTagKey string) error {
	t.Lock()
	defer t.Unlock()

	tag := t.getTag(oldTagKey)
	if tag == nil {
		return errors.TagNotExist(oldTagKey)
	}
	t.renameTagLocked(tag, newTagKey)
	return nil
}

// renameTagLocked moves tag to newTagKey; the caller holds the manager lock.
func (t *TagManager) renameTagLocked(tag *types.Tag, newTagKey string) {
	oldTagKey := tag.Key
	tag.Key = newTagKey
	tag.LastGetTime = time.Now()
	t.setTag(tag)
	t.removeTag(oldTagKey)
}

// UpdateChannelTagUsers applies a subscriber change to the channel's cached
// tag. Reading the channel's tag key, changing its users, renaming it and
// pointing the channel at the new key happen under one lock: when two
// requests changed the same channel at once, the slower one used to find its
// key renamed away, logged "tag not exist" and dropped its change, leaving a
// joined member without messages or a removed one still receiving them.
//
// Without a cached tag nothing is done: the next message builds the tag from
// the store, which already holds the change. If the cached tag cannot be
// updated it is dropped for the same reason.
func (t *TagManager) UpdateChannelTagUsers(fakeChannelId string, channelType uint8, uids []string, remove bool) error {
	t.Lock()
	defer t.Unlock()

	tagKey := t.GetChannelTag(fakeChannelId, channelType)
	if tagKey == "" {
		return nil
	}
	tag := t.getTag(tagKey)
	if tag == nil {
		t.RemoveChannelTag(fakeChannelId, channelType)
		return nil
	}
	var err error
	if remove {
		err = t.removeUsersLocked(tag, uids)
	} else {
		err = t.addUsersLocked(tag, uids)
	}
	if err != nil {
		t.removeTag(tagKey)
		t.RemoveChannelTag(fakeChannelId, channelType)
		return err
	}
	newTagKey := wkutil.GenUUID()
	t.renameTagLocked(tag, newTagKey)
	t.setChannelTagLocked(fakeChannelId, channelType, newTagKey)
	return nil
}

func (t *TagManager) SetChannelTag(fakeChannelId string, channelType uint8, tagKey string) {
	t.setChannelTagLocked(fakeChannelId, channelType, tagKey)
}

func (t *TagManager) setChannelTagLocked(fakeChannelId string, channelType uint8, tagKey string) {
	blucket := t.getBlucketByChannel(fakeChannelId, channelType)
	blucket.setChannelTag(fakeChannelId, channelType, tagKey)
	tag := t.getTag(tagKey)
	if tag != nil {
		tag.ChannelId = fakeChannelId
		tag.ChannelType = channelType
	}
}

func (t *TagManager) GetChannelTag(fakeChannelId string, channelType uint8) string {
	blucket := t.getBlucketByChannel(fakeChannelId, channelType)
	return blucket.getChannelTag(fakeChannelId, channelType)
}

func (t *TagManager) RemoveChannelTag(fakeChannelId string, channelType uint8) {
	blucket := t.getBlucketByChannel(fakeChannelId, channelType)
	blucket.removeChannelTag(fakeChannelId, channelType)
}

func (t *TagManager) GetAllTags() []*types.Tag {
	var tags []*types.Tag
	for _, blucket := range t.bluckets {
		tags = append(tags, blucket.getAllTags()...)
	}
	return tags
}

func (t *TagManager) GetAllChannelTags() map[string]string {
	channelTags := make(map[string]string)
	for _, blucket := range t.bluckets {
		for k, v := range blucket.getAllChannelTags() {
			channelTags[k] = v
		}
	}
	return channelTags
}

func (t *TagManager) getBlucketByTagKey(tagKey string) *tagBlucket {
	h := fnv.New32a()
	h.Write([]byte(tagKey))
	i := h.Sum32() % uint32(len(t.bluckets))
	return t.bluckets[i]
}

func (t *TagManager) getBlucketByChannel(channelId string, channelType uint8) *tagBlucket {
	h := fnv.New32a()
	h.Write([]byte(wkutil.ChannelToKey(channelId, channelType)))
	i := h.Sum32() % uint32(len(t.bluckets))
	return t.bluckets[i]
}

func (t *TagManager) mergeNodes(tag *types.Tag, nodes []*types.Node) {
	for _, node := range nodes {
		existNode := false
		for _, tagNode := range tag.Nodes {
			if tagNode.LeaderId == node.LeaderId {
				existNode = true

				// 合并用户
				for _, uid := range node.Uids {
					existUser := false
					for _, tagUid := range tagNode.Uids {
						if tagUid == uid {
							existUser = true
							break
						}
					}
					if !existUser {
						tagNode.Uids = append(tagNode.Uids, uid)
					}
				}
				// 合并slot
				for _, slotId := range node.SlotIds {
					existSlot := false
					for _, tagSlotId := range tagNode.SlotIds {
						if tagSlotId == slotId {
							existSlot = true
							break
						}
					}
					if !existSlot {
						tagNode.SlotIds = append(tagNode.SlotIds, slotId)
					}
				}

				break
			}
		}
		if !existNode {
			tag.Nodes = append(tag.Nodes, node)
		}
	}
}

func (t *TagManager) calcUsersInNode(uids []string) ([]*types.Node, error) {

	var nodeMap = make(map[uint64]*types.Node)
	for _, uid := range uids {
		slotId := service.Cluster.GetSlotId(uid)
		leaderId := service.Cluster.SlotLeaderId(slotId)
		if leaderId == 0 {
			return nil, errors.TagSlotLeaderIsZero
		}
		node := nodeMap[leaderId]
		if node == nil {
			node = &types.Node{
				LeaderId: leaderId,
			}
			nodeMap[leaderId] = node
		}
		node.Uids = append(node.Uids, uid)
		existSlot := false
		for _, slot := range node.SlotIds {
			if slot == slotId {
				existSlot = true
				break
			}
		}
		if !existSlot {
			node.SlotIds = append(node.SlotIds, slotId)
		}

	}
	nodes := make([]*types.Node, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}
	return nodes, nil

}

func (t *TagManager) setTag(tag *types.Tag) {
	blucket := t.getBlucketByTagKey(tag.Key)
	blucket.setTag(tag)
}

func (t *TagManager) getTag(tagKey string) *types.Tag {
	blucket := t.getBlucketByTagKey(tagKey)
	return blucket.getTag(tagKey)
}

func (t *TagManager) removeTag(tagKey string) {
	blucket := t.getBlucketByTagKey(tagKey)
	blucket.removeTag(tagKey)
}

func (t *TagManager) existTag(tagKey string) bool {
	blucket := t.getBlucketByTagKey(tagKey)
	return blucket.existTag(tagKey)
}
