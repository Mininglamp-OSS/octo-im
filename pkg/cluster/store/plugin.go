package store

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// UpdateUserPluginNo 更新用户插件编号
func (s *Store) UpdateUserPluginNo(uid string, pluginNo string) error {
	data := EncodeCMDUserPluginNo(wkdb.PluginUser{
		Uid:       uid,
		PluginNo:  pluginNo,
		CreatedAt: wkutil.TimePtr(time.Now()),
		UpdatedAt: wkutil.TimePtr(time.Now()),
	})
	cmd := NewCMD(CMDUpdateUserPluginNo, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := globalSlot // 默认数据在0槽位上
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

func (s *Store) RemovePluginUser(uid string, pluginNo string) error {
	data := EncodeCMDPluginUser(pluginNo, uid)
	cmd := NewCMD(CMDRemovePluginUser, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := globalSlot // 默认数据在0槽位上
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

// // AddOrUpdatePlugin 添加或更新插件
// func (s *Store) AddOrUpdatePlugin(plugin wkdb.Plugin) error {
// 	data := EncodeCMDPlugin(plugin)
// 	cmd := NewCMD(CMDAddOrUpdatePlugin, data)
// 	cmdData, err := cmd.Marshal()
// 	if err != nil {
// 		return err
// 	}
// 	slotId := globalSlot // 默认数据在0槽位上
// 	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
// 	return err
// }

// // UpdatePluginConfig 更新插件配置
// func (s *Store) UpdatePluginConfig(pluginNo string, config map[string]interface{}) error {
// 	data := EncodeCMDPluginConfig(pluginNo, config)
// 	cmd := NewCMD(CMDUpdatePluginConfig, data)
// 	cmdData, err := cmd.Marshal()
// 	if err != nil {
// 		return err
// 	}
// 	slotId := globalSlot // 默认数据在0槽位上
// 	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
// 	return err
// }

// ExistPluginUser reports whether uid is bound to any plugin. Plugin users are
// slot-0 rows; the read is fenced and an error is never reported as false.
func (s *Store) ExistPluginUser(uid string) (bool, error) {
	return readOwned(s, globalSlot, func() (bool, error) {
		return s.wdb.ExistPluginByUid(uid)
	})
}

// GetHighestPriorityPluginByUid returns the plugin with the highest priority
// bound to uid. The plugin-user rows are slot-0 rows and the read is fenced.
func (s *Store) GetHighestPriorityPluginByUid(uid string) (string, error) {
	return readOwned(s, globalSlot, func() (string, error) {
		return s.wdb.GetHighestPriorityPluginByUid(uid)
	})
}

// SearchPluginUsers searches plugin-user bindings (slot-0 rows), fenced.
func (s *Store) SearchPluginUsers(req wkdb.SearchPluginUserReq) ([]wkdb.PluginUser, error) {
	return readOwned(s, globalSlot, func() ([]wkdb.PluginUser, error) {
		return s.wdb.SearchPluginUsers(req)
	})
}
