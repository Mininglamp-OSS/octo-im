package ringlock

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/fasthash"
)

type RingLock struct {
	locker []chan struct{}
	size   uint32 // 环的大小
}

func NewRingLock(size int) *RingLock {
	locker := make([]chan struct{}, size)
	for i := range locker {
		locker[i] = make(chan struct{}, 1)
	}
	return &RingLock{
		locker: locker,
		size:   uint32(size),
	}
}

// 计算哈希值并确定锁的位置
func (r *RingLock) lockPosition(key string) int {
	hash := fasthash.Hash(key)
	position := hash % r.size // 使用哈希值的第一个字节来决定锁的位置
	return int(position)
}

// 获取环型哈希锁
func (r *RingLock) Lock(key string) {
	position := r.lockPosition(key)
	r.locker[position] <- struct{}{}
}

// LockContext bounds waiting for a stripe without spawning a waiter goroutine.
func (r *RingLock) LockContext(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case r.locker[r.lockPosition(key)] <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 释放环型哈希锁
func (r *RingLock) Unlock(key string) {
	position := r.lockPosition(key)
	<-r.locker[position]
}
