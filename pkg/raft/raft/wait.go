package raft

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type wait struct {
	mu sync.RWMutex
	wklog.Log

	progresses   []*progress
	applied      uint64
	stopped      bool
	readLogs     func(uint64, uint64, uint64) ([]types.Log, error)
	verifyStored func(*progress)
}

func newWait(key string) *wait {
	return &wait{
		Log:        wklog.NewWKLog(fmt.Sprintf("applyWait[%s]", key)),
		progresses: make([]*progress, 0),
	}
}

func (m *wait) waitApply(maxIndex uint64) *progress {
	m.mu.Lock()
	defer m.mu.Unlock()
	progress := newApplyProgress(maxIndex)
	m.progresses = append(m.progresses, progress)
	return progress

}

func (m *wait) waitCommit(maxIndex uint64) *progress {
	m.mu.Lock()
	defer m.mu.Unlock()
	progress := newCommitProgress(maxIndex)
	m.progresses = append(m.progresses, progress)
	return progress
}

// didCommit 提交 maxLogIndex已提交的最大日志下标
func (m *wait) didCommit(maxLogIndex uint64) {

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, progress := range m.progresses {

		if progress.done || !progress.waitCommittied {
			continue
		}

		if maxLogIndex >= progress.maxIndex {
			progress.done = true
			progress.waitC <- struct{}{}
			close(progress.waitC)
		}
	}
	// 清理已完成的
	m.clean()
}

func (m *wait) didApply(maxLogIndex uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, progress := range m.progresses {

		if progress.done || !progress.waitApplied || len(progress.logs) > 0 {
			continue
		}

		if maxLogIndex >= progress.maxIndex {
			progress.done = true
			progress.waitC <- struct{}{}
			close(progress.waitC)
		}
	}

	// 清理已完成的
	m.clean()
}

// 清理已完成的
func (m *wait) clean() {
	for i := 0; i < len(m.progresses); {
		if m.progresses[i].done {
			m.progresses = append(m.progresses[:i], m.progresses[i+1:]...)
		} else {
			i++
		}
	}
}

type progress struct {
	maxIndex uint64

	waitC chan struct{}

	// 等待应用
	waitApplied bool
	// 等待提交
	waitCommittied bool

	done      bool
	err       error
	logs      []types.Log
	forwarded bool
}

func newApplyProgress(maxIndex uint64) *progress {
	return &progress{
		maxIndex:    maxIndex,
		waitC:       make(chan struct{}, 1),
		waitApplied: true,
	}
}

func newCommitProgress(maxIndex uint64) *progress {
	return &progress{
		maxIndex:       maxIndex,
		waitC:          make(chan struct{}, 1),
		waitCommittied: true,
	}
}

// registerReceipt and appliedLogs share the frontier and registration lock.
// The frontier is updated by successful Storage.Apply, independently of when
// the loop consumes ApplyResp. An already-applied range is verified on disk.
func (m *wait) registerReceipt(logs []types.Log, forwarded bool) *progress {
	p := newApplyProgress(logs[len(logs)-1].Index)
	p.logs = logs
	p.forwarded = forwarded
	m.mu.Lock()
	if m.stopped {
		m.completeLocked(p, proposalUnknown(types.ErrStopped))
		m.mu.Unlock()
		return p
	}
	m.progresses = append(m.progresses, p)
	alreadyApplied := m.applied >= p.maxIndex
	m.mu.Unlock()
	if alreadyApplied {
		m.verifyStored(p)
	}
	return p
}

// verifyLogs checks every entry in the admitted batch, not just its last index.
func verifyLogs(wantLogs, logs []types.Log) error {
	for _, want := range wantLogs {
		found := false
		for _, got := range logs {
			if got.Index == want.Index {
				found = true
				if got.Id != want.Id || (want.Term != 0 && got.Term != want.Term) || !bytes.Equal(got.Data, want.Data) {
					return types.ErrProposalSuperseded
				}
				break
			}
		}
		if !found {
			return proposalUnknown(fmt.Errorf("raft: applied log %d unavailable", want.Index))
		}
	}
	return nil
}

// readReceipt runs on the bounded storage pool, outside the protocol mutex.
// Cancellation, fencing or stop can detach p while I/O is still in flight.
func (m *wait) readReceipt(p *progress) {
	logs, err := m.readLogs(p.logs[0].Index, p.maxIndex+1, 0)
	if err != nil {
		err = proposalUnknown(err)
	} else {
		err = verifyLogs(p.logs, logs)
	}
	m.complete(p, err)
}
func (m *wait) complete(p *progress, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completeLocked(p, err)
}
func (m *wait) completeLocked(p *progress, err error) {
	if p.done {
		return
	}
	p.done = true
	p.err = err
	close(p.waitC)
	m.clean()
}
func (m *wait) appliedLogs(logs []types.Log) {
	var unverified []*progress
	m.mu.Lock()
	last := logs[len(logs)-1].Index
	if last > m.applied {
		m.applied = last
	}
	// Completion removes from progresses, so iterate a copy.
	for _, p := range append([]*progress(nil), m.progresses...) {
		if !p.done && p.waitApplied && p.maxIndex <= m.applied {
			if len(p.logs) == 0 {
				m.completeLocked(p, nil)
			} else if logs[0].Index <= p.logs[0].Index && last >= p.maxIndex {
				m.completeLocked(p, verifyLogs(p.logs, logs))
			} else {
				unverified = append(unverified, p)
			}
		}
	}
	m.mu.Unlock()
	for _, p := range unverified {
		m.verifyStored(p)
	}
}
func (m *wait) detach(p *progress) (error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.done {
		return p.err, true
	}
	m.completeLocked(p, proposalUnknown(context.Canceled))
	return nil, false
}
func (m *wait) fence(term uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range append([]*progress(nil), m.progresses...) {
		if len(p.logs) > 0 && !p.forwarded && p.logs[0].Term <= term {
			m.completeLocked(p, proposalUnknown(types.ErrNotLeader))
		}
	}
}
func (m *wait) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	for _, p := range append([]*progress(nil), m.progresses...) {
		m.completeLocked(p, proposalUnknown(types.ErrStopped))
	}
}

func (m *wait) fenceForward() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range append([]*progress(nil), m.progresses...) {
		if p.forwarded {
			m.completeLocked(p, proposalUnknown(types.ErrNotLeader))
		}
	}
}
