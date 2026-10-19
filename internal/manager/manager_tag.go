package manager

import (
	"hash/fnv"
	"slices"
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
	// Publish under the same lock as the copy-and-replace updates.
	t.Lock()
	t.setTag(tag)
	t.Unlock()
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
	tag.Touch()

	return tag, nil
}

func (t *TagManager) AddUsers(tagKey string, uids []string) error {

	t.Lock()
	defer t.Unlock()

	tag := t.getTag(tagKey)
	if tag == nil {
		return errors.TagNotExist(tagKey)
	}
	// 计算用户所在的节点
	nodes, err := t.calcUsersInNode(uids)
	if err != nil {
		return err
	}
	leaders := make(map[string]uint64, len(uids))
	for _, node := range nodes {
		for _, uid := range node.Uids {
			leaders[uid] = node.LeaderId
		}
	}
	// 发布出去的tag可能正在被分发遍历：只替换有变化的节点，没有变化就不发布
	newNodes := make([]*types.Node, 0, len(tag.Nodes)+len(nodes))
	changed := false
	for _, node := range tag.Nodes {
		newNode := addUsersToNode(node, leaders, findNode(nodes, node.LeaderId))
		changed = changed || newNode != node
		newNodes = append(newNodes, newNode)
	}
	for _, node := range nodes {
		if findNode(tag.Nodes, node.LeaderId) == nil {
			newNodes = append(newNodes, node)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	newTag := copyTag(tag)
	newTag.Nodes = newNodes
	t.setTag(newTag)

	return nil
}

// copyTag returns an unpublished copy of tag that keeps its access state. It
// shares the published nodes, so callers that change membership must put new
// nodes in the copy instead of modifying the shared ones.
func copyTag(tag *types.Tag) *types.Tag {
	c := &types.Tag{
		Key:         tag.Key,
		ChannelId:   tag.ChannelId,
		ChannelType: tag.ChannelType,
		Nodes:       tag.Nodes,
		CreatedAt:   tag.CreatedAt,
		LastGetTime: tag.LastGetTime,
		NodeVersion: tag.NodeVersion,
	}
	c.InheritAccess(tag)
	return c
}

// addUsersToNode returns node with the users in add appended and the users
// now led by another node removed. It returns node itself when nothing
// changes, so an unchanged node stays shared with the published tag; a
// changed node gets new lists and the published ones are left alone.
func addUsersToNode(node *types.Node, leaders map[string]uint64, add *types.Node) *types.Node {
	var drop map[int]struct{}
	for uid, leaderId := range leaders {
		if leaderId == node.LeaderId {
			continue
		}
		for i, nodeUid := range node.Uids {
			if nodeUid == uid {
				if drop == nil {
					drop = make(map[int]struct{})
				}
				drop[i] = struct{}{}
				break
			}
		}
	}
	var missing []string
	var newSlotIds []uint32
	if add != nil {
		for _, uid := range add.Uids {
			if !slices.Contains(node.Uids, uid) && !slices.Contains(missing, uid) {
				missing = append(missing, uid)
			}
		}
		for _, slotId := range add.SlotIds {
			if !slices.Contains(node.SlotIds, slotId) {
				newSlotIds = append(newSlotIds, slotId)
			}
		}
	}
	if len(drop) == 0 && len(missing) == 0 && len(newSlotIds) == 0 {
		return node
	}
	uids := node.Uids
	if len(drop) > 0 || len(missing) > 0 {
		uids = withoutIndexes(node.Uids, drop, len(missing))
		uids = append(uids, missing...)
	}
	slotIds := node.SlotIds
	if len(newSlotIds) > 0 {
		slotIds = append(append(make([]uint32, 0, len(node.SlotIds)+len(newSlotIds)), node.SlotIds...), newSlotIds...)
	}
	return &types.Node{LeaderId: node.LeaderId, Uids: uids, SlotIds: slotIds}
}

// withoutIndexes returns a new list of uids without the positions in drop,
// with room for extra more. It is never nil, so /tags keeps reporting an
// emptied node as "uids": [].
func withoutIndexes(uids []string, drop map[int]struct{}, extra int) []string {
	out := make([]string, 0, len(uids)-len(drop)+extra)
	positions := make([]int, 0, len(drop))
	for i := range drop {
		positions = append(positions, i)
	}
	slices.Sort(positions)
	start := 0
	for _, i := range positions {
		out = append(out, uids[start:i]...)
		start = i + 1
	}
	return append(out, uids[start:]...)
}

func findNode(nodes []*types.Node, leaderId uint64) *types.Node {
	for _, node := range nodes {
		if node.LeaderId == leaderId {
			return node
		}
	}
	return nil
}

func (t *TagManager) RemoveUsers(tagKey string, uids []string) error {

	t.Lock()
	defer t.Unlock()

	tag := t.getTag(tagKey)
	if tag == nil {
		return errors.TagNotExist(tagKey)
	}

	// 先算出每个节点要删除的位置。按节点里实际存的用户删除，不依赖槽位领导：
	// 领导选举期间也能删，领导迁移后还留在旧领导节点下的用户也能删掉
	drops := make(map[uint64]map[int]struct{})
	for _, uid := range uids {
		for _, node := range tag.Nodes {
			drop := drops[node.LeaderId]
			for i, nodeUid := range node.Uids {
				if nodeUid != uid {
					continue
				}
				if _, ok := drop[i]; ok {
					continue
				}
				if drop == nil {
					drop = make(map[int]struct{})
					drops[node.LeaderId] = drop
				}
				drop[i] = struct{}{}
				break
			}
		}
	}
	if len(drops) == 0 {
		return nil
	}
	// 发布出去的tag可能正在被分发遍历：只替换有变化的节点
	newTag := copyTag(tag)
	newTag.Nodes = make([]*types.Node, 0, len(tag.Nodes))
	for _, node := range tag.Nodes {
		if drop := drops[node.LeaderId]; len(drop) > 0 {
			node = &types.Node{LeaderId: node.LeaderId, Uids: withoutIndexes(node.Uids, drop, 0), SlotIds: node.SlotIds}
		}
		newTag.Nodes = append(newTag.Nodes, node)
	}
	t.setTag(newTag)
	return nil
}

func (t *TagManager) RemoveTag(tagKey string) {
	// Serialized with copy-and-replace updates so they cannot republish a
	// tag that was just removed.
	t.Lock()
	defer t.Unlock()
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

// Get returns the published tag. It is shared with concurrent fan-out and
// must not be modified.
func (t *TagManager) Get(tagKey string) *types.Tag {
	tag := t.getTag(tagKey)
	if tag == nil {
		return nil
	}
	if tag.NodeVersion < t.nodeVersion() {
		t.Warn("tag is expired, tagNodeVersion < currentNodeVersion ", zap.String("tagKey", tagKey), zap.Uint64("tagNodeVersion", tag.NodeVersion), zap.Uint64("currentNodeVersion", t.nodeVersion()))
		return nil
	}
	tag.Touch()
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
	newTag := copyTag(tag)
	newTag.Key = newTagKey
	newTag.Touch() // a rename has always counted as an access
	t.setTag(newTag)
	t.removeTag(oldTagKey)
	return nil
}

func (t *TagManager) SetChannelTag(fakeChannelId string, channelType uint8, tagKey string) {
	t.Lock()
	defer t.Unlock()
	// Publish the tag with its channel first, so a reader that finds the new
	// mapping also finds the tag labelled with this channel.
	tag := t.getTag(tagKey)
	if tag != nil && (tag.ChannelId != fakeChannelId || tag.ChannelType != channelType) {
		newTag := copyTag(tag)
		newTag.ChannelId = fakeChannelId
		newTag.ChannelType = channelType
		t.setTag(newTag)
	}
	t.getBlucketByChannel(fakeChannelId, channelType).setChannelTag(fakeChannelId, channelType, tagKey)
}

func (t *TagManager) GetChannelTag(fakeChannelId string, channelType uint8) string {
	blucket := t.getBlucketByChannel(fakeChannelId, channelType)
	return blucket.getChannelTag(fakeChannelId, channelType)
}

func (t *TagManager) RemoveChannelTag(fakeChannelId string, channelType uint8) {
	blucket := t.getBlucketByChannel(fakeChannelId, channelType)
	blucket.removeChannelTag(fakeChannelId, channelType)
}

// GetAllTags returns the published tags; they must not be modified.
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
