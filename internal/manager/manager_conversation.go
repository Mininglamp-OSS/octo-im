package manager

import (
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/fasthash"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
)

// ==================== ConversationManager ====================

// ConversationManager 最近会话管理器
// 负责维护用户最近会话的内存缓存，并定时同步到数据库。
// 使用分片机制 (conversationUpdater) 来减少锁竞争。
type ConversationManager struct {
	updaters     []*conversationUpdater
	updaterCount int
	client       *ingress.Client
	stopper      *syncutil.Stopper
	wklog.Log
}

// NewConversationManager 创建最近会话管理器
func NewConversationManager(updaterCount int) *ConversationManager {
	updaters := make([]*conversationUpdater, updaterCount)
	client := ingress.NewClient()
	for i := 0; i < updaterCount; i++ {
		updaters[i] = newConversationUpdater(client)
	}
	return &ConversationManager{
		updaterCount: updaterCount,
		updaters:     updaters,
		client:       client,
		Log:          wklog.NewWKLog("ConversationManager"),
		stopper:      syncutil.NewStopper(),
	}
}

func collectConversationEventSeqs(events []*eventbus.Event) (firstMsgSeq uint64, lastMsgSeq uint64, senderReadSeqs map[string]uint64) {
	senderReadSeqs = make(map[string]uint64)
	for _, event := range events {
		if event == nil || event.Frame == nil || event.Frame.GetNoPersist() {
			continue
		}
		if event.MessageSeq > lastMsgSeq {
			lastMsgSeq = event.MessageSeq
		}
		if firstMsgSeq == 0 || (event.MessageSeq > 0 && event.MessageSeq < firstMsgSeq) {
			firstMsgSeq = event.MessageSeq
		}
		if event.Conn != nil && event.Conn.Uid != "" {
			if event.MessageSeq > senderReadSeqs[event.Conn.Uid] {
				senderReadSeqs[event.Conn.Uid] = event.MessageSeq
			}
		}
	}
	return firstMsgSeq, lastMsgSeq, senderReadSeqs
}

func shouldUpdateConversationForChannel(channelId string, channelType uint8, firstMsgSeq uint64) bool {
	if channelType == wkproto.ChannelTypeLive {
		return false
	}
	if options.G.IsCmdChannel(channelId) {
		return true
	}
	if channelType == wkproto.ChannelTypePerson && !options.G.WhitelistOffOfPerson {
		return false
	}
	if channelType == wkproto.ChannelTypeCustomerService || channelType == wkproto.ChannelTypePerson {
		return firstMsgSeq == 1
	}
	return false
}

// Push 推送消息事件以更新最近会话
func (c *ConversationManager) Push(channelId string, channelType uint8, tagKey string, events []*eventbus.Event) {
	if len(events) == 0 {
		return
	}
	firstMsgSeq, lastMsgSeq, senderReadSeqs := collectConversationEventSeqs(events)
	if lastMsgSeq == 0 || !shouldUpdateConversationForChannel(channelId, channelType, firstMsgSeq) {
		return
	}
	index := c.getShardIndex(channelId)
	c.updaters[index].push(channelId, channelType, tagKey, lastMsgSeq, senderReadSeqs)
}

// PushForTargetUsers updates pending conversations strictly for deliverMask-authorized targetUIDs (closing V11-1),
// merging into existing pending updates without resurrecting deleted conversations on already-delivered slots
// or skipping newly migrated-in slots when LastMsgSeq/TagKey match.
func (c *ConversationManager) PushForTargetUsers(channelId string, channelType uint8, tagKey string, targetUIDs []string, events []*eventbus.Event) {
	if len(events) == 0 || len(targetUIDs) == 0 {
		return
	}
	firstMsgSeq, lastMsgSeq, senderReadSeqs := collectConversationEventSeqs(events)
	if lastMsgSeq == 0 || !shouldUpdateConversationForChannel(channelId, channelType, firstMsgSeq) {
		return
	}
	index := c.getShardIndex(channelId)
	c.updaters[index].pushUsers(channelId, channelType, tagKey, lastMsgSeq, senderReadSeqs, targetUIDs)
}

// Start 启动管理器
func (c *ConversationManager) Start() error {
	c.loadFromFile() // 从本地持久化文件加载未处理的会话
	c.stopper.RunWorker(c.loopStoreConversations)
	return nil
}

// Stop 停止管理器
func (c *ConversationManager) Stop() {
	c.stopper.Stop()
	c.saveToFile() // 将内存中未同步的会话保存到本地文件
}

// DeleteFromCache 从内存缓存中删除指定用户的频道会话
func (c *ConversationManager) DeleteFromCache(uid string, channelId string, channelType uint8) error {
	if channelType == wkproto.ChannelTypeLive {
		return nil
	}
	index := c.getShardIndex(channelId)
	c.updaters[index].removeUserChannelUpdate(channelId, channelType, uid)
	return nil
}

// GetUserChannelsFromCache 获取用户在内存缓存中待同步的会话列表
func (c *ConversationManager) GetUserChannelsFromCache(uid string, conversationType wkdb.ConversationType) ([]wkproto.Channel, error) {
	var allChannels []wkproto.Channel
	for _, updater := range c.updaters {
		channels := updater.getUserChannels(uid, conversationType)
		allChannels = append(allChannels, channels...)
	}
	return allChannels, nil
}

// getShardIndex 根据频道 ID 获取分片索引
func (c *ConversationManager) getShardIndex(channelId string) int {
	return int(fasthash.Hash(channelId) % uint32(c.updaterCount))
}

// loopStoreConversations 循环同步会话到数据库
func (c *ConversationManager) loopStoreConversations() {
	tk := time.NewTicker(options.G.Conversation.SyncInterval)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			c.storeConversations()
		case <-c.stopper.ShouldStop():
			return
		}
	}
}

// storeConversations 执行批量同步会话到数据库的逻辑
func (c *ConversationManager) storeConversations() {
	// 预分配切片容量，减少扩容开销
	conversations := make([]wkdb.Conversation, 0, options.G.Conversation.SyncOnce)
	now := time.Now()

	for _, updater := range c.updaters {
		updates := updater.getChannelUpdates()
		for _, update := range updates {
			conversationType := wkdb.ConversationTypeChat
			if options.G.IsCmdChannel(update.ChannelId) {
				conversationType = wkdb.ConversationTypeCMD
			}
			for uid, readToMsgSeq := range update.UserReadSeqs {
				if uid == "" || update.ChannelId == "" {
					continue
				}
				conversations = append(conversations, wkdb.Conversation{
					ChannelId:    update.ChannelId,
					ChannelType:  update.ChannelType,
					Uid:          uid,
					Type:         conversationType,
					ReadToMsgSeq: readToMsgSeq,
					CreatedAt:    &now,
					UpdatedAt:    &now,
				})
				// 达到单次同步最大数量，触发存储
				if len(conversations) >= options.G.Conversation.SyncOnce {
					goto store
				}
			}
		}
	}
store:
	if len(conversations) > 0 {
		err := service.Store.AddConversationsIfNotExist(conversations)
		if err != nil {
			c.Error("store conversations failed", zap.Error(err), zap.Int("count", len(conversations)))
			return
		}
		// 同步成功后，按已同步的 (channel, uid, readToMsgSeq) 精确清理缓存，避免误删尚未刷新的其它用户或并发新到达的更新
		for _, conversation := range conversations {
			updater := c.updaters[c.getShardIndex(conversation.ChannelId)]
			updater.removeSyncedUserChannelUpdate(conversation.ChannelId, conversation.ChannelType, conversation.Uid, conversation.ReadToMsgSeq)
		}
	}
}

// 持久化文件相关方法
func (c *ConversationManager) saveToFile() {
	conversationDir := path.Join(options.G.DataDir, "conversationv2")
	if err := os.MkdirAll(conversationDir, 0755); err != nil {
		c.Error("mkdir conversation dir failed", zap.Error(err), zap.String("path", conversationDir))
		return
	}

	allUpdates := make([]*channelUpdate, 0)
	for _, updater := range c.updaters {
		allUpdates = append(allUpdates, updater.getChannelUpdates()...)
	}
	data, err := json.Marshal(allUpdates)
	if err != nil {
		c.Error("marshal conversations failed", zap.Error(err))
		return
	}

	filePath := path.Join(conversationDir, "conversations.json")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		c.Error("save conversations to file failed", zap.Error(err), zap.String("path", filePath))
	}
}

func (c *ConversationManager) loadFromFile() {
	filePath := path.Join(options.G.DataDir, "conversationv2", "conversations.json")
	if !wkutil.FileExists(filePath) {
		return
	}
	data, err := wkutil.ReadFile(filePath)
	if err != nil {
		c.Error("load conversations from file failed", zap.Error(err), zap.String("path", filePath))
		return
	}

	if len(data) == 0 {
		return
	}

	var allUpdates []*channelUpdate
	if err := json.Unmarshal(data, &allUpdates); err != nil {
		c.Error("unmarshal conversations from file failed", zap.Error(err))
		return
	}

	for _, update := range allUpdates {
		c.updaters[c.getShardIndex(update.ChannelId)].setChannelUpdate(update)
	}

	// 加载完成后删除文件，避免重复加载
	if err := os.Remove(filePath); err != nil {
		c.Error("remove conversations file failed", zap.Error(err), zap.String("path", filePath))
	}
}

// ==================== conversationUpdater ====================

// conversationUpdater 分片会话处理器
// 负责具体分片内的会话更新维护，通过倒排索引实现 O(1) 的用户会话查找。
type conversationUpdater struct {
	pendingUpdates map[string]*channelUpdate      // channelKey -> channelUpdate (待同步的频道信息)
	userIndex      map[string]map[string]struct{} // uid -> set of channelKeys (倒排索引，用于快速查找用户的所有待同步频道)
	sync.RWMutex
	client *ingress.Client
	wklog.Log
}

func newConversationUpdater(client *ingress.Client) *conversationUpdater {
	return &conversationUpdater{
		pendingUpdates: make(map[string]*channelUpdate),
		userIndex:      make(map[string]map[string]struct{}),
		client:         client,
		Log:            wklog.NewWKLog("conversationUpdater"),
	}
}

// push 添加或更新频道会话信息
func (c *conversationUpdater) push(channelId string, channelType uint8, tagKey string, lastMsgSeq uint64, senderReadSeqs map[string]uint64) {
	key := wkutil.ChannelToKey(channelId, channelType)

	c.Lock()
	defer c.Unlock()

	// 幂等性检查：如果当前待更新项已包含更新的消息，则跳过
	if update := c.pendingUpdates[key]; update != nil {
		if update.LastMsgSeq >= lastMsgSeq || tagKey == update.TagKey {
			return
		}
	}

	tag := service.TagManager.Get(tagKey)
	if tag == nil {
		c.Warn("tag not found, conversation not updated", zap.String("tagKey", tagKey), zap.String("channelId", channelId))
		return
	}

	nodeUsers := tag.GetNodeUsers(options.G.Cluster.NodeId)
	if len(nodeUsers) == 0 {
		return
	}

	// 构建用户已读序号映射：uid -> readToMsgSeq
	userReadSeqs := make(map[string]uint64, len(nodeUsers))
	for _, uid := range nodeUsers {
		if seq, ok := senderReadSeqs[uid]; ok {
			userReadSeqs[uid] = seq // 发送者已读位置
		} else {
			userReadSeqs[uid] = 0 // 接收者默认为 0
		}
	}

	// 更新倒排索引
	c.updateUserIndex(key, userReadSeqs)

	c.pendingUpdates[key] = &channelUpdate{
		ChannelId:    channelId,
		ChannelType:  channelType,
		UserReadSeqs: userReadSeqs,
		TagKey:       tagKey,
		LastMsgSeq:   lastMsgSeq,
	}
}

// updateUserIndex 维护倒排索引，确保 uid -> channelKeys 的映射是最新的
func (c *conversationUpdater) updateUserIndex(channelKey string, newUserReadSeqs map[string]uint64) {
	// 从旧索引中移除当前频道已包含的用户
	if oldUpdate := c.pendingUpdates[channelKey]; oldUpdate != nil {
		for uid := range oldUpdate.UserReadSeqs {
			if keys, ok := c.userIndex[uid]; ok {
				delete(keys, channelKey)
				if len(keys) == 0 {
					delete(c.userIndex, uid)
				}
			}
		}
	}

	// 将新用户添加到索引
	for uid := range newUserReadSeqs {
		if c.userIndex[uid] == nil {
			c.userIndex[uid] = make(map[string]struct{})
		}
		c.userIndex[uid][channelKey] = struct{}{}
	}
}

// setChannelUpdate 直接设置频道更新信息（通常用于文件加载）
func (c *conversationUpdater) setChannelUpdate(update *channelUpdate) {
	if update == nil {
		return
	}
	key := wkutil.ChannelToKey(update.ChannelId, update.ChannelType)
	c.Lock()
	defer c.Unlock()

	c.updateUserIndex(key, update.UserReadSeqs)
	c.pendingUpdates[key] = update
}

// getUserChannels 获取用户在当前分片中待同步的所有频道
func (c *conversationUpdater) getUserChannels(uid string, conversationType wkdb.ConversationType) []wkproto.Channel {
	c.RLock()
	defer c.RUnlock()

	channelKeys := c.userIndex[uid]
	if len(channelKeys) == 0 {
		return nil
	}

	channels := make([]wkproto.Channel, 0, len(channelKeys))
	for channelKey := range channelKeys {
		update := c.pendingUpdates[channelKey]
		if update == nil {
			continue
		}

		isCMD := options.G.IsCmdChannel(update.ChannelId)
		if isCMD && conversationType != wkdb.ConversationTypeCMD {
			continue
		}
		if !isCMD && conversationType != wkdb.ConversationTypeChat {
			continue
		}

		channels = append(channels, wkproto.Channel{
			ChannelID:   update.ChannelId,
			ChannelType: update.ChannelType,
		})
	}
	return channels
}

// pushUsers merges targetUIDs into pendingUpdates[key] monotonically.
func (c *conversationUpdater) pushUsers(channelId string, channelType uint8, tagKey string, lastMsgSeq uint64, senderReadSeqs map[string]uint64, targetUIDs []string) {
	if len(targetUIDs) == 0 {
		return
	}
	key := wkutil.ChannelToKey(channelId, channelType)

	c.Lock()
	defer c.Unlock()

	update := c.pendingUpdates[key]
	if update == nil {
		userReadSeqs := make(map[string]uint64, len(targetUIDs))
		for _, uid := range targetUIDs {
			if uid == "" || options.G.IsSystemUid(uid) {
				continue
			}
			userReadSeqs[uid] = senderReadSeqs[uid]
			if c.userIndex[uid] == nil {
				c.userIndex[uid] = make(map[string]struct{})
			}
			c.userIndex[uid][key] = struct{}{}
		}
		if len(userReadSeqs) == 0 {
			return
		}
		c.pendingUpdates[key] = &channelUpdate{
			ChannelId:    channelId,
			ChannelType:  channelType,
			UserReadSeqs: userReadSeqs,
			TagKey:       tagKey,
			LastMsgSeq:   lastMsgSeq,
		}
		return
	}

	if update.UserReadSeqs == nil {
		update.UserReadSeqs = make(map[string]uint64, len(targetUIDs))
	}
	for _, uid := range targetUIDs {
		if uid == "" || options.G.IsSystemUid(uid) {
			continue
		}
		seq := senderReadSeqs[uid]
		if cur, exists := update.UserReadSeqs[uid]; !exists || seq > cur {
			update.UserReadSeqs[uid] = seq
		}
		if c.userIndex[uid] == nil {
			c.userIndex[uid] = make(map[string]struct{})
		}
		c.userIndex[uid][key] = struct{}{}
	}
	if lastMsgSeq > update.LastMsgSeq {
		update.LastMsgSeq = lastMsgSeq
	}
	if tagKey != "" {
		update.TagKey = tagKey
	}
}

// getChannelUpdates 获取当前分片中所有的待同步频道信息快照（深拷贝 UserReadSeqs 防止与并发更新发生 data race）
func (c *conversationUpdater) getChannelUpdates() []*channelUpdate {
	c.RLock()
	defer c.RUnlock()

	updates := make([]*channelUpdate, 0, len(c.pendingUpdates))
	for _, update := range c.pendingUpdates {
		if update == nil {
			continue
		}
		copiedSeqs := make(map[string]uint64, len(update.UserReadSeqs))
		for uid, seq := range update.UserReadSeqs {
			copiedSeqs[uid] = seq
		}
		updates = append(updates, &channelUpdate{
			ChannelId:    update.ChannelId,
			ChannelType:  update.ChannelType,
			UserReadSeqs: copiedSeqs,
			TagKey:       update.TagKey,
			LastMsgSeq:   update.LastMsgSeq,
		})
	}
	return updates
}

func (c *conversationUpdater) removeSyncedUserChannelUpdate(channelId string, channelType uint8, uid string, syncedReadSeq uint64) {
	key := wkutil.ChannelToKey(channelId, channelType)
	c.Lock()
	defer c.Unlock()

	update := c.pendingUpdates[key]
	if update == nil {
		return
	}
	curSeq, exists := update.UserReadSeqs[uid]
	if !exists || curSeq > syncedReadSeq {
		return
	}
	delete(update.UserReadSeqs, uid)
	if keys, ok := c.userIndex[uid]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(c.userIndex, uid)
		}
	}
	if len(update.UserReadSeqs) == 0 {
		delete(c.pendingUpdates, key)
	}
}

// removeChannelUpdate 同步完成后移除频道信息
func (c *conversationUpdater) removeChannelUpdate(channelId string, channelType uint8) {
	key := wkutil.ChannelToKey(channelId, channelType)
	c.Lock()
	defer c.Unlock()

	if update := c.pendingUpdates[key]; update != nil {
		for uid := range update.UserReadSeqs {
			if keys, ok := c.userIndex[uid]; ok {
				delete(keys, key)
				if len(keys) == 0 {
					delete(c.userIndex, uid)
				}
			}
		}
	}

	delete(c.pendingUpdates, key)
}

// removeUserChannelUpdate 从特定频道的待同步列表中移除特定用户
func (c *conversationUpdater) removeUserChannelUpdate(channelId string, channelType uint8, uid string) {
	key := wkutil.ChannelToKey(channelId, channelType)
	c.Lock()
	defer c.Unlock()

	if update := c.pendingUpdates[key]; update != nil {
		delete(update.UserReadSeqs, uid)
	}

	if keys, ok := c.userIndex[uid]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(c.userIndex, uid)
		}
	}
}

// ==================== 辅助结构体 ====================

// channelUpdate 描述内存中待同步的一个频道的更新详情
type channelUpdate struct {
	ChannelId    string            `json:"channel_id"`     // 频道 ID
	ChannelType  uint8             `json:"channel_type"`   // 频道类型
	UserReadSeqs map[string]uint64 `json:"user_read_seqs"` // 用户 UID 到其在当前更新中已读消息序号的映射
	TagKey       string            `json:"tag_key"`        // 更新时使用的标签
	LastMsgSeq   uint64            `json:"last_msg_seq"`   // 该频道当前的最后消息序号
}
