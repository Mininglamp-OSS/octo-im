package service

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/types"
)

var RetryManager RetryMgr

type RetryMgr interface {
	// RetryMessageCount 重试消息数量
	RetryMessageCount() int
	// AddRetry 添加重试消息
	AddRetry(msg *types.RetryMessage)
	// RemoveRetry 移除当前物理会话的重试消息
	RemoveRetry(conn *eventbus.Conn, messageId int64) error
	// 获取当前物理会话的重试消息
	RetryMessage(conn *eventbus.Conn, messageId int64) *types.RetryMessage
}
