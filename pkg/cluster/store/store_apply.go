package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"go.uber.org/zap"
)

// ApplyLogs 应用槽日志
// ApplySlotLogs preserves log order. Only contiguous config saves can be
// grouped; delaying conversation writes past removals can resurrect rows.
func (s *Store) ApplySlotLogs(slotId uint32, logs []types.Log) error {
	lock, _ := s.applyLocks.LoadOrStore(slotId, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()
	applied, err := s.wdb.SlotAppliedIndex(slotId)
	if err != nil {
		return err
	}
	for i := 0; i < len(logs); {
		if logs[i].Index <= applied {
			i++
			continue
		}
		cmd := &CMD{}
		if err := cmd.Unmarshal(logs[i].Data); err != nil {
			return &PermanentApplyError{Err: err}
		}
		switch cmd.CmdType {
		case CMDSubscriberOperation, CMDSubscriberCheckpoint:
			cmds, versions, next, err := decodeContiguousSubscriberSourceCommands(logs, i)
			if err != nil {
				return err
			}
			if err = s.applySubscriberRecoveryCommands(slotId, cmds, versions); err != nil {
				return err
			}
			i = next
		case CMDChannelClusterConfigSave, CMDAddOrUpdateUserConversations,
			CMDAddOrUpdateConversationsBatchIfNotExist, CMDConversationEffects:
			cmds, versions, next, err := decodeContiguousCommands(logs, i, cmd.CmdType)
			if err != nil {
				return err
			}
			switch cmd.CmdType {
			case CMDChannelClusterConfigSave:
				err = s.handleChannelClusterConfigSavesForCMDs(cmds, versions)
			case CMDAddOrUpdateUserConversations:
				err = s.handleAddOrUpdateUserConversationsForCMDs(cmds)
			case CMDAddOrUpdateConversationsBatchIfNotExist:
				err = s.handleAddOrUpdateConversationsBatchIfNotExistForCMDs(cmds)
			case CMDConversationEffects:
				err = s.applyConversationEffectCommands(slotId, cmds)
			}
			if err != nil {
				return err
			}
			i = next
		default:
			if err := s.applyCMDForSlot(slotId, cmd, logs[i].Index); err != nil {
				return err
			}
			i++
		}
		applied = logs[i-1].Index
		// Recovery entries have transactional version/progress fences. They can
		// replay a successful prefix, so share its checkpoint at the batch end.
		// Legacy entries must checkpoint individually before any later entry:
		// replaying an older legacy write after a newer write is not generally safe.
		switch cmd.CmdType {
		case CMDSubscriberOperation, CMDConversationEffects, CMDSubscriberCheckpoint:
			if i < len(logs) {
				continue
			}
		}
		if err := s.wdb.SetSlotAppliedIndex(slotId, applied); err != nil {
			return err
		}
	}
	return nil
}

func decodeContiguousSubscriberSourceCommands(logs []types.Log, start int) ([]*CMD, []uint64, int, error) {
	cmds := make([]*CMD, 0, len(logs)-start)
	versions := make([]uint64, 0, len(logs)-start)
	i := start
	for i < len(logs) {
		cmd := &CMD{}
		if err := cmd.Unmarshal(logs[i].Data); err != nil {
			return nil, nil, start, &PermanentApplyError{Err: err}
		}
		if cmd.CmdType != CMDSubscriberOperation && cmd.CmdType != CMDSubscriberCheckpoint {
			break
		}
		cmds = append(cmds, cmd)
		versions = append(versions, logs[i].Index)
		i++
	}
	return cmds, versions, i, nil
}

// decodeContiguousCommands restores batching without the old reordering bug:
// only adjacent commands of the same type may share a database operation.
func decodeContiguousCommands(logs []types.Log, start int, commandType CMDType) ([]*CMD, []uint64, int, error) {
	cmds := make([]*CMD, 0, len(logs)-start)
	versions := make([]uint64, 0, len(logs)-start)
	i := start
	for i < len(logs) {
		cmd := &CMD{}
		if err := cmd.Unmarshal(logs[i].Data); err != nil {
			return nil, nil, start, &PermanentApplyError{Err: err}
		}
		if cmd.CmdType != commandType {
			break
		}
		cmds = append(cmds, cmd)
		versions = append(versions, logs[i].Index)
		i++
	}
	return cmds, versions, i, nil
}

// applyConversationEffectCommands combines adjacent target-slot commands while
// preserving their order. Repeated lifecycle keys split a batch so a later
// churn operation can never be applied before an earlier one.
func (s *Store) applyConversationEffectCommands(slot uint32, cmds []*CMD) error {
	effects := make([]wkdb.ConversationEffect, 0, wkdb.MaxConversationEffects)
	seen := make(map[string]struct{}, wkdb.MaxConversationEffects)
	flush := func() error {
		if len(effects) == 0 {
			return nil
		}
		if err := s.wdb.ApplyConversationEffects(effects); err != nil {
			return err
		}
		effects = effects[:0]
		clear(seen)
		return nil
	}
	for _, cmd := range cmds {
		var page []wkdb.ConversationEffect
		if err := json.Unmarshal(cmd.Data, &page); err != nil {
			return &PermanentApplyError{Err: err}
		}
		if len(page) == 0 || len(page) > wkdb.MaxConversationEffects {
			return &PermanentApplyError{Err: fmt.Errorf("invalid conversation effect page")}
		}
		for _, effect := range page {
			if effect.UID == "" || effect.ChannelID == "" || effect.ChannelType == 0 || effect.Version == 0 || (!effect.Deleted && effect.ConversationID == 0) || effect.CreatedAt <= 0 {
				return &PermanentApplyError{Err: fmt.Errorf("invalid conversation effect")}
			}
			if s.opts.Slot.GetSlotId(effect.UID) != slot {
				return &PermanentApplyError{Err: fmt.Errorf("conversation effect routed to wrong slot")}
			}
			key := fmt.Sprintf("%s\x00%s\x00%d", effect.UID, effect.ChannelID, effect.ChannelType)
			_, duplicate := seen[key]
			if duplicate || len(effects) == wkdb.MaxConversationEffects {
				if err := flush(); err != nil {
					return err
				}
			}
			effects = append(effects, effect)
			seen[key] = struct{}{}
		}
	}
	return flush()
}

func (s *Store) applyCMDForSlot(slot uint32, cmd *CMD, index uint64) error {
	switch cmd.CmdType {
	case CMDAppendMessageEvent:
		event, err := cmd.DecodeCMDMessageEvent(cmd.version)
		if err != nil {
			return &PermanentApplyError{Err: err}
		}
		return s.wdb.AppendMessageEventForSlot(slot, index, event)
	case CMDSubscriberOperation, CMDConversationEffects, CMDSubscriberCheckpoint:
		return s.applySubscriberRecovery(slot, cmd, index)
	default:
		return s.applyCMD(cmd, index)
	}
}

func (s *Store) applyCMD(cmd *CMD, logIndex uint64) error {
	switch cmd.CmdType {
	case _cmdSaveStreamMetaRemoved, _cmdStreamEndRemoved, _cmdAppendStreamItemRemoved,
		_cmdAddStreamMetaRemoved, _cmdAddStreamsRemoved, _cmdSaveStreamV2Removed,
		CMDAppendMessagesOfNotifyQueue, CMDRemoveMessagesOfNotifyQueue,
		CMDDeleteChannelAndClearMessages, CMDChannelClusterConfigDelete,
		CMDAddOrUpdatePlugin, CMDUpdatePluginConfig:
		return nil // known retired commands remain replay-compatible
	case CMDChannelClusterConfigSave: // 保存频道分布式配置
		return s.handleChannelClusterConfigSave(cmd, logIndex)
	case CMDAddOrUpdateConversations: // 添加或更新会话
		return s.handleAddOrUpdateConversations(cmd)
	case CMDAddSubscribers: // 添加订阅者
		return s.handleAddSubscribers(cmd)
	case CMDRemoveSubscribers: // 移除订阅者
		return s.handleRemoveSubscribers(cmd)
	case CMDAddUser: // 添加用户
		return s.handleAddUser(cmd)
	case CMDUpdateUser: // 更新用户
		return s.handleUpdateUser(cmd)
	case CMDAddDevice: // 添加设备信息
		return s.handleAddDevice(cmd)
	case CMDUpdateDevice: // 更新设备信息
		return s.handleUpdateDevice(cmd)
	case CMDAddChannelInfo: // 添加频道信息
		return s.handleAddChannelInfo(cmd)
	case CMDUpdateChannelInfo: // 更新频道信息
		return s.handleUpdateChannel(cmd)
	case CMDRemoveAllSubscriber: // 移除所有订阅者
		return s.handleRemoveAllSubscriber(cmd)
	case CMDDeleteChannel: // 删除频道
		return s.handleDeleteChannel(cmd)
	case CMDAddDenylist: // 添加黑名单
		return s.handleAddDenylist(cmd)
	case CMDRemoveDenylist: // 移除黑名单
		return s.handleRemoveDenylist(cmd)
	case CMDRemoveAllDenylist: // 移除所有黑名单
		return s.handleRemoveAllDenylist(cmd)
	case CMDAddAllowlist: // 添加白名单
		return s.handleAddAllowlist(cmd)
	case CMDRemoveAllowlist: // 移除白名单
		return s.handleRemoveAllowlist(cmd)
	case CMDRemoveAllAllowlist: // 移除所有白名单
		return s.handleRemoveAllAllowlist(cmd)
	case CMDAddOrUpdateUserConversations: // 添加或更新会话
		return s.handleAddOrUpdateUserConversations(cmd)
	case CMDDeleteConversation: // 删除会话
		return s.handleDeleteConversation(cmd)
	case CMDDeleteConversations: // 批量删除某个用户的最近会话
		return s.handleDeleteConversations(cmd)
	// case CMDAppendMessagesOfUser: // 向用户队列里增加消息
	// 	return s.handleAppendMessagesOfUser(cmd)
	case CMDBatchUpdateConversation:
		return s.handleBatchUpdateConversation(cmd)
		// case CMDChannelClusterConfigDelete: // 删除频道分布式配置
		// return s.handleChannelClusterConfigDelete(cmd)
	case CMDSystemUIDsAdd: // 添加系统UID
		return s.handleSystemUIDsAdd(cmd)
	case CMDSystemUIDsRemove: // 移除系统UID
		return s.handleSystemUIDsRemove(cmd)
	case CMDAddOrUpdateTester: // 添加或更新测试机
		return s.handleAddOrUpdateTester(cmd)
	case CMDRemoveTester: // 移除测试机
		return s.handleRemoveTester(cmd)
	case CMDUpdateUserPluginNo: // 更新用户插件编号
		return s.handleUpdateUserPluginNo(cmd)
	case CMDRemovePluginUser:
		return s.handleRemovePluginUser(cmd)
		// case CMDAddOrUpdatePlugin: // 添加或更新插件
		// 	return s.handleAddOrUpdatePlugin(cmd)
		// case CMDUpdatePluginConfig: // 更新插件配置
		// 	return s.handleUpdatePluginConfig(cmd)
	case CMDAddOrUpdateConversationsBatchIfNotExist: // 批量添加或更新最近会话，如果存在则不添加
		return s.handleAddOrUpdateConversationsBatchIfNotExist(cmd)
	case CMDUpdateConversationDeletedAtMsgSeq: // 更新最近会话的已删除的消息序号位置
		return s.handleUpdateConversationDeletedAtMsgSeq(cmd)
	case CMDAppendMessageEvent: // 追加消息事件
		return s.handleAppendMessageEvent(cmd)
	case CMDUpdateConversationIfSeqGreater: // 更新最近会话的已读位置（如果seq更大）
		return s.handleUpdateConversationIfSeqGreater(cmd)
	default:
		s.Error("unknown cmd type", zap.String("cmdType", cmd.CmdType.String()))
		return &PermanentApplyError{Err: fmt.Errorf("unknown slot command %d", cmd.CmdType)}
	}
}

// PermanentApplyError indicates an incompatible or malformed committed entry;
// retrying it cannot repair the state machine. Storage failures remain retryable.
type PermanentApplyError = wkdb.PermanentApplyError

func (s *Store) applyLog(slot uint32, log types.Log) error {
	cmd := &CMD{}
	err := cmd.Unmarshal(log.Data)
	if err != nil {
		s.Error("unmarshal cmd err", zap.Error(err), zap.Uint64("index", log.Index), zap.ByteString("data", log.Data))
		return &PermanentApplyError{Err: err}
	}

	return s.applyCMDForSlot(slot, cmd, log.Index)
}

func (s *Store) loopSaveChannelClusterConfig() {
	maxSizeBatch := 1000
	done := false
	cfgs := make([]*channelCfgReq, 0, maxSizeBatch)
	for {
		select {
		case cfg := <-s.channelCfgCh:
			cfgs = append(cfgs, cfg)
			for !done {
				select {
				case cfg = <-s.channelCfgCh:
					cfgs = append(cfgs, cfg)
				default:
					done = true
				}
				if len(cfgs) >= maxSizeBatch {
					break
				}
			}
			err := s.handleChannelClusterConfigSaves(cfgs)
			if err != nil {
				s.Error("handleChannelClusterConfigSaves err", zap.Error(err))
			}
			cfgs = cfgs[:0]
			done = false
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

func (s *Store) handleChannelClusterConfigSave(cmd *CMD, confVersion uint64) error {
	_, _, configData, err := cmd.DecodeCMDChannelClusterConfigSave()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	channelClusterConfig := wkdb.ChannelClusterConfig{}
	err = channelClusterConfig.Unmarshal(configData)
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	channelClusterConfig.ConfVersion = confVersion

	waitC := make(chan error, 1)

	select {
	case s.channelCfgCh <- &channelCfgReq{
		cfg:   channelClusterConfig,
		errCh: waitC,
	}:
	case <-s.stopper.ShouldStop():
		return ErrStoreStopped
	}

	select {
	case err := <-waitC:
		return err
	case <-s.stopper.ShouldStop():
		return ErrStoreStopped
	}
}

func (s *Store) handleChannelClusterConfigSavesForCMDs(cmds []*CMD, confVersions []uint64) error {

	waits := make([]chan error, 0, len(cmds))
	for i, cmd := range cmds {
		_, _, configData, err := cmd.DecodeCMDChannelClusterConfigSave()
		if err != nil {
			return &PermanentApplyError{Err: err}
		}
		channelClusterConfig := wkdb.ChannelClusterConfig{}
		err = channelClusterConfig.Unmarshal(configData)
		if err != nil {
			return &PermanentApplyError{Err: err}
		}
		channelClusterConfig.ConfVersion = confVersions[i]
		waitC := make(chan error, 1)
		waits = append(waits, waitC)
		select {
		case s.channelCfgCh <- &channelCfgReq{
			cfg:   channelClusterConfig,
			errCh: waitC,
		}:
		case <-s.stopper.ShouldStop():
			return ErrStoreStopped
		}

	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	for _, waitC := range waits {
		select {
		case err := <-waitC:
			if err != nil {
				return err
			}
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case <-s.stopper.ShouldStop():
			return ErrStoreStopped
		}
	}
	return nil
}

func (s *Store) handleAddSubscribers(cmd *CMD) error {
	channelId, channelType, members, err := cmd.DecodeMembers()
	if err != nil {
		s.Error("decode subscribers err", zap.Error(err), zap.String("channelID", channelId), zap.Uint8("channelType", channelType), zap.ByteString("data", cmd.Data))
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddSubscribers(channelId, channelType, members)
}

func (s *Store) handleRemoveSubscribers(cmd *CMD) error {
	channelId, channelType, subscribers, err := cmd.DecodeChannelUids()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveSubscribers(channelId, channelType, subscribers)
}

func (s *Store) handleAddUser(cmd *CMD) error {
	u, err := cmd.DecodeCMDUser()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddUser(u)
}

func (s *Store) handleUpdateUser(cmd *CMD) error {
	u, err := cmd.DecodeCMDUser()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.UpdateUser(u)
}

func (s *Store) handleAddDevice(cmd *CMD) error {
	u, err := cmd.DecodeCMDDevice()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddDevice(u)
}

func (s *Store) handleUpdateDevice(cmd *CMD) error {
	u, err := cmd.DecodeCMDDevice()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.UpdateDevice(u)
}

func (s *Store) handleAddChannelInfo(cmd *CMD) error {
	channelInfo, err := cmd.DecodeChannelInfo()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	_, err = s.wdb.AddChannel(channelInfo)
	return err
}

func (s *Store) handleUpdateChannel(cmd *CMD) error {
	channelInfo, err := cmd.DecodeChannelInfo()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	err = s.wdb.UpdateChannel(channelInfo)
	return err
}

func (s *Store) handleRemoveAllSubscriber(cmd *CMD) error {
	channelId, channelType, err := cmd.DecodeChannel()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveAllSubscriber(channelId, channelType)
}

func (s *Store) handleDeleteChannel(cmd *CMD) error {
	channelId, channelType, err := cmd.DecodeChannel()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.DeleteChannel(channelId, channelType)
}

func (s *Store) handleAddDenylist(cmd *CMD) error {
	channelId, channelType, members, err := cmd.DecodeMembers()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddDenylist(channelId, channelType, members)
}

func (s *Store) handleRemoveDenylist(cmd *CMD) error {
	channelId, channelType, subscribers, err := cmd.DecodeChannelUids()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveDenylist(channelId, channelType, subscribers)
}

func (s *Store) handleRemoveAllDenylist(cmd *CMD) error {
	channelId, channelType, err := cmd.DecodeChannel()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveAllDenylist(channelId, channelType)
}

func (s *Store) handleAddAllowlist(cmd *CMD) error {
	channelId, channelType, subscribers, err := cmd.DecodeMembers()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddAllowlist(channelId, channelType, subscribers)
}

func (s *Store) handleRemoveAllowlist(cmd *CMD) error {
	channelId, channelType, subscribers, err := cmd.DecodeChannelUids()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveAllowlist(channelId, channelType, subscribers)
}

func (s *Store) handleRemoveAllAllowlist(cmd *CMD) error {
	channelId, channelType, err := cmd.DecodeChannel()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveAllAllowlist(channelId, channelType)
}

func (s *Store) handleAddOrUpdateUserConversationsForCMDs(cmds []*CMD) error {
	conversationMap := map[string][]wkdb.Conversation{}
	for _, cmd := range cmds {
		uid, conversations, err := cmd.DecodeCMDAddOrUpdateUserConversations()
		if err != nil {
			return &PermanentApplyError{Err: err}
		}
		conversationMap[uid] = append(conversationMap[uid], conversations...)
	}

	for uid, conversations := range conversationMap {
		err := s.wdb.AddOrUpdateConversationsWithUser(uid, conversations)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) handleAddOrUpdateUserConversations(cmd *CMD) error {
	uid, conversations, err := cmd.DecodeCMDAddOrUpdateUserConversations()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddOrUpdateConversationsWithUser(uid, conversations)
}

func (s *Store) handleDeleteConversation(cmd *CMD) error {
	uid, deleteChannelID, deleteChannelType, err := cmd.DecodeCMDDeleteConversation()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.DeleteConversation(uid, deleteChannelID, deleteChannelType)
}

func (s *Store) handleDeleteConversations(cmd *CMD) error {
	uid, channels, err := cmd.DecodeCMDDeleteConversations()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.DeleteConversations(uid, channels)
}

func (s *Store) handleChannelClusterConfigSaves(reqs []*channelCfgReq) error {
	if len(reqs) > 10 {
		fmt.Println("handleChannelClusterConfigSaves...", len(reqs))
	}
	cfgs := make([]wkdb.ChannelClusterConfig, 0, len(reqs))
	for _, req := range reqs {
		cfgs = append(cfgs, req.cfg)
	}
	err := s.wdb.SaveChannelClusterConfigs(cfgs)
	if err != nil {
		s.Error("save channel cluster config err", zap.Error(err))
	}
	for _, req := range reqs {
		if req.errCh == nil {
			continue
		}
		select {
		case req.errCh <- err:
		default:
		}
	}
	return nil
}

// func (s *Store) handleChannelClusterConfigDelete(cmd *CMD) error {
// 	channelId, channelType, err := cmd.DecodeChannel()
// 	if err != nil {
// 		return err
// 	}
// 	return s.db.DeleteChannelClusterConfig(channelId, channelType)
// }

func (s *Store) handleBatchUpdateConversation(cmd *CMD) error {
	models, err := cmd.DecodeCMDBatchUpdateConversation()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	for _, model := range models {
		var conversationType = wkdb.ConversationTypeChat
		if s.opts.IsCmdChannel(model.ChannelId) {
			conversationType = wkdb.ConversationTypeCMD
		}
		for uid, seq := range model.Uids {
			if s.wdb.SubscriberRecoveryActive() {
				lifecycle, managed, err := s.wdb.ConversationLifecycle(uid, model.ChannelId, model.ChannelType)
				if err != nil {
					return err
				}
				if managed {
					if !lifecycle.Deleted {
						if err := s.wdb.UpdateConversationIfSeqGreater(uid, model.ChannelId, model.ChannelType, seq); err != nil {
							return err
						}
					}
					continue
				}
			}
			conversation := wkdb.Conversation{
				Uid:          uid,
				Type:         conversationType,
				ChannelId:    model.ChannelId,
				ChannelType:  model.ChannelType,
				ReadToMsgSeq: seq,
			}
			err = s.wdb.AddOrUpdateConversationsWithUser(uid, []wkdb.Conversation{conversation})
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (s *Store) handleSystemUIDsAdd(cmd *CMD) error {
	uids, err := cmd.DecodeCMDSystemUIDs()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddSystemUids(uids)
}

func (s *Store) handleSystemUIDsRemove(cmd *CMD) error {
	uids, err := cmd.DecodeCMDSystemUIDs()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveSystemUids(uids)
}

func (s *Store) handleAddOrUpdateConversations(cmd *CMD) error {
	conversations, err := cmd.DecodeCMDAddOrUpdateConversations()
	if err != nil {
		s.Error("handleAddOrUpdateConversations: failed to decode conversations",
			zap.Error(err),
			zap.Int("dataLen", len(cmd.Data)))
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddOrUpdateConversations(conversations)
}

func (s *Store) handleAddOrUpdateTester(cmd *CMD) error {
	tester, err := cmd.DecodeCMDAddOrUpdateTester()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddOrUpdateTester(tester)
}

func (s *Store) handleRemoveTester(cmd *CMD) error {
	no, err := cmd.DecodeCMDRemoveTester()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemoveTester(no)
}

func (s *Store) handleUpdateUserPluginNo(cmd *CMD) error {
	pluginUser, err := cmd.DecodeCMDUserPluginNo()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddOrUpdatePluginUsers([]wkdb.PluginUser{
		pluginUser,
	})
}

func (s *Store) handleRemovePluginUser(cmd *CMD) error {
	pluginNo, uid, err := cmd.DecodeCMDPluginUser()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.RemovePluginUser(pluginNo, uid)
}

func (s *Store) handleAddOrUpdateConversationsBatchIfNotExistForCMDs(cmds []*CMD) error {
	conversations := make([]wkdb.Conversation, 0, len(cmds))
	for _, cmd := range cmds {
		cns, err := cmd.DecodeCMDAddOrUpdateConversations()
		if err != nil {
			s.Error("handleAddOrUpdateConversationsBatchIfNotExistForCMDs: failed to decode conversations",
				zap.Error(err),
				zap.Int("dataLen", len(cmd.Data)))
			return &PermanentApplyError{Err: err}
		}
		conversations = append(conversations, cns...)
	}
	return s.wdb.AddOrUpdateConversationsBatchIfNotExist(conversations)
}

func (s *Store) handleAddOrUpdateConversationsBatchIfNotExist(cmd *CMD) error {
	conversations, err := cmd.DecodeCMDAddOrUpdateConversations()
	if err != nil {
		s.Error("handleAddOrUpdateConversationsBatchIfNotExist: failed to decode conversations",
			zap.Error(err),
			zap.Int("dataLen", len(cmd.Data)))
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.AddOrUpdateConversationsBatchIfNotExist(conversations)
}

func (s *Store) handleUpdateConversationDeletedAtMsgSeq(cmd *CMD) error {
	uid, channelId, channelType, deletedAtMsgSeq, err := cmd.DecodeCMDUpdateConversationDeletedAtMsgSeq()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.UpdateConversationDeletedAtMsgSeq(uid, channelId, channelType, deletedAtMsgSeq)
}

func (s *Store) handleAppendMessageEvent(cmd *CMD) error {
	event, err := cmd.DecodeCMDMessageEvent(cmd.version)
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	_, _, err = s.wdb.AppendMessageEventWithState(event)
	return err
}

// func (s *Store) handleAddOrUpdatePlugin(cmd *CMD) error {
// 	plugin, err := cmd.DecodeCMDPlugin()
// 	if err != nil {
// 		return err
// 	}
// 	return s.wdb.AddOrUpdatePlugin(plugin)
// }

// func (s *Store) handleUpdatePluginConfig(cmd *CMD) error {
// 	pluginNo, config, err := cmd.DecodeCMDPluginConfig()
// 	if err != nil {
// 		return err
// 	}
// 	return s.wdb.UpdatePluginConfig(pluginNo, config)
// }

func (s *Store) handleUpdateConversationIfSeqGreater(cmd *CMD) error {
	uid, channelId, channelType, readToMsgSeq, err := cmd.DecodeCMDUpdateConversationIfSeqGreater()
	if err != nil {
		return &PermanentApplyError{Err: err}
	}
	return s.wdb.UpdateConversationIfSeqGreater(uid, channelId, channelType, readToMsgSeq)
}
