package manager

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/valyala/fastrand"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// RetryQueue 重试队列
type RetryQueue struct {
	inFlightPQ       inFlightPqueue
	inFlightMessages map[string][]*types.RetryMessage
	inFlightMutex    sync.Mutex
	fakeMessageID    int64
	wklog.Log
	r          *RetryManager
	stopped    atomic.Bool
	retryTimer *timingwheel.Timer
}

// NewRetryQueue NewRetryQueue
func NewRetryQueue(index int, r *RetryManager) *RetryQueue {

	return &RetryQueue{
		r:                r,
		inFlightPQ:       newInFlightPqueue(4056),
		inFlightMessages: make(map[string][]*types.RetryMessage),
		fakeMessageID:    10000,
		Log:              wklog.NewWKLog(fmt.Sprintf("RetryQueue[%d]", index)),
	}
}

func (r *RetryQueue) startInFlightTimeout(msg *types.RetryMessage) {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	key := r.getInFlightKey(msg.FromNode, msg.ConnId, msg.MessageId)
	for _, current := range r.inFlightMessages[key] {
		if retryMessagesSameSession(current, msg) {
			r.Warn("ID already in flight for session", zap.String("key", key), zap.String("uid", msg.Uid), zap.Uint64("fromNode", msg.FromNode), zap.Int64("connId", msg.ConnId), zap.Int64("messageId", msg.MessageId))
			return
		}
	}
	msg.Pri = time.Now().Add(options.G.MessageRetry.Interval).UnixNano()
	r.inFlightMessages[key] = append(r.inFlightMessages[key], msg)
	r.inFlightPQ.Push(msg)
}

func (r *RetryQueue) popInFlightMessage(conn *eventbus.Conn, messageId int64) (*types.RetryMessage, error) {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	if conn == nil {
		return nil, errors.New("connection is nil")
	}
	key := r.getInFlightKey(conn.NodeId, conn.ConnId, messageId)
	msg := r.matchingInFlightMessageLocked(key, conn)
	if msg == nil {
		r.Warn("ID not in flight for session", zap.String("key", key), zap.Uint64("fromNode", conn.NodeId), zap.Int64("connId", conn.ConnId), zap.Int64("messageId", messageId))
		return nil, errors.New("ID not in flight")
	}
	r.removeInFlightMessageLocked(key, msg)
	return msg, nil
}

func (r *RetryQueue) getInFlightMessage(conn *eventbus.Conn, messageId int64) *types.RetryMessage {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	if conn == nil {
		return nil
	}
	key := r.getInFlightKey(conn.NodeId, conn.ConnId, messageId)
	if msg := r.matchingInFlightMessageLocked(key, conn); msg != nil {
		return msg
	}
	// Preserve the caller's ability to reject an ACK from a different physical
	// session when the base message key exists but no session identity matches.
	if messages := r.inFlightMessages[key]; len(messages) > 0 {
		return messages[0]
	}
	return nil
}

func (r *RetryQueue) getInFlightKey(fromNodeId uint64, connId int64, messageId int64) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(fromNodeId, 10))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(connId, 10))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(messageId, 10))
	return b.String()
}
func (r *RetryQueue) finishMessage(conn *eventbus.Conn, messageId int64) error {
	msg, err := r.popInFlightMessage(conn, messageId)
	if err != nil {
		return err
	}
	r.removeFromInFlightPQ(msg)

	return nil
}
func (r *RetryQueue) removeFromInFlightPQ(msg *types.RetryMessage) {
	r.inFlightMutex.Lock()
	if msg.Index == -1 {
		// this item has already been popped off the pqueue
		r.inFlightMutex.Unlock()
		return
	}
	r.inFlightPQ.Remove(msg.Index)
	r.inFlightMutex.Unlock()
}

func (r *RetryQueue) processInFlightQueue(t int64) {
	for !r.stopped.Load() {
		r.inFlightMutex.Lock()
		msg, _ := r.inFlightPQ.PeekAndShift(t)
		if msg == nil {
			r.inFlightMutex.Unlock()
			break
		}
		key := r.getInFlightKey(msg.FromNode, msg.ConnId, msg.MessageId)
		removed := r.removeInFlightMessageLocked(key, msg)
		r.inFlightMutex.Unlock()
		if !removed {
			r.Warn("timed-out retry already removed", zap.String("key", key), zap.String("uid", msg.Uid))
			continue
		}
		r.r.retry(msg) // 重试
	}
}

func (r *RetryQueue) matchingInFlightMessageLocked(key string, conn *eventbus.Conn) *types.RetryMessage {
	for _, msg := range r.inFlightMessages[key] {
		if retrySessionMatches(msg, conn) {
			return msg
		}
	}
	return nil
}

func (r *RetryQueue) removeInFlightMessageLocked(key string, target *types.RetryMessage) bool {
	messages := r.inFlightMessages[key]
	for i, msg := range messages {
		if msg != target {
			continue
		}
		copy(messages[i:], messages[i+1:])
		messages[len(messages)-1] = nil
		messages = messages[:len(messages)-1]
		if len(messages) == 0 {
			delete(r.inFlightMessages, key)
		} else {
			r.inFlightMessages[key] = messages
		}
		return true
	}
	return false
}

func retryMessagesSameSession(left, right *types.RetryMessage) bool {
	if left == nil || right == nil {
		return false
	}
	return retrySessionMatches(left, &eventbus.Conn{
		Uid:         right.Uid,
		NodeId:      right.FromNode,
		ConnId:      right.ConnId,
		Uptime:      right.Uptime,
		OwnerBootID: right.OwnerBootID,
		SessionID:   right.SessionID,
	})
}

// inFlightMessagesCount 返回正在飞行的消息数量
func (r *RetryQueue) inFlightMessagesCount() int {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	count := 0
	for _, messages := range r.inFlightMessages {
		count += len(messages)
	}
	return max(count, len(r.inFlightPQ))
}

// Start 开始运行重试
func (r *RetryQueue) Start() {

	scanInterval := options.G.MessageRetry.ScanInterval

	p := float64(fastrand.Uint32()) / (1 << 32)
	// 以避免系统中因定时器、周期性任务或请求间隔完全一致而导致的同步问题（例如拥堵或资源竞争）。
	jitter := time.Duration(p * float64(scanInterval))
	r.retryTimer = r.r.schedule(scanInterval+jitter, func() {
		now := time.Now().UnixNano()
		r.processInFlightQueue(now)
	})
}

func (r *RetryQueue) Stop() {
	r.stopped.Store(true)
	if r.retryTimer != nil {
		r.retryTimer.Stop()
		r.retryTimer = nil
	}
}
