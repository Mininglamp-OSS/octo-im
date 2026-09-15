// Package presence keeps physical sessions independent of user-slot authority.
package presence

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

var ErrNotReady = errors.New("online session recovery is not ready")

type physicalSession struct {
	raw  wknet.Conn
	conn *eventbus.Conn
}
type readiness struct {
	generation uint64
	version    uint64
	until      time.Time
}

type Manager struct {
	byUID    map[string]map[int64]struct{}
	mu       sync.Mutex
	boot     string
	node     uint64
	physical map[int64]physicalSession
	ready    map[string]*readiness
	// Serializes bounded recovery work. Source snapshots never acquire this gate.
	gate chan struct{}
}

func New(node uint64) *Manager {
	return &Manager{byUID: make(map[string]map[int64]struct{}), boot: wkutil.GenUUID(), node: node, physical: make(map[int64]physicalSession), ready: make(map[string]*readiness), gate: make(chan struct{}, 1)}
}

func (m *Manager) Track(raw wknet.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous, ok := m.physical[raw.ID()]; ok {
		m.unindexLocked(previous.conn)
	}
	m.physical[raw.ID()] = physicalSession{raw: raw}
}

func (m *Manager) Prepare(raw wknet.Conn, conn *eventbus.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.physical[raw.ID()]
	if !ok || entry.raw != raw {
		return
	}
	m.unindexLocked(entry.conn)
	conn.OwnerBootID = m.boot
	conn.SessionID = wkutil.GenUUID()
	entry.conn = copyConn(conn)
	if m.byUID[conn.Uid] == nil {
		m.byUID[conn.Uid] = make(map[int64]struct{})
	}
	m.byUID[conn.Uid][conn.ConnId] = struct{}{}
	m.physical[raw.ID()] = entry
}

func (m *Manager) Authenticate(conn *eventbus.Conn) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.physical[conn.ConnId]
	if !ok || entry.conn == nil || !entry.conn.SameSession(conn) || !conn.Auth || conn.OwnerBootID != m.boot {
		return false
	}
	entry.conn = copyConn(conn)
	m.physical[conn.ConnId] = entry
	// Publish the authenticated context while close and authentication are fenced.
	entry.raw.SetContext(conn)
	m.invalidateLocked(conn.Uid)
	return true
}

func (m *Manager) Close(raw wknet.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.physical[raw.ID()]
	if !ok || entry.raw != raw {
		return
	}
	delete(m.physical, raw.ID())
	m.unindexLocked(entry.conn)
}

func (m *Manager) unindexLocked(conn *eventbus.Conn) {
	if conn == nil {
		return
	}
	delete(m.byUID[conn.Uid], conn.ConnId)
	if len(m.byUID[conn.Uid]) == 0 {
		delete(m.byUID, conn.Uid)
	}
	m.invalidateLocked(conn.Uid)
}

// A close invalidates any snapshot already in flight for this UID. Generation
// checks act as tombstones without retaining every historical session ID.
func (m *Manager) Forget(conn *eventbus.Conn) {
	if conn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invalidateLocked(conn.Uid)
}
func (m *Manager) invalidateLocked(uid string) {
	if r := m.ready[uid]; r != nil {
		r.generation++
		r.until = time.Time{}
	}
}

func copyConn(conn *eventbus.Conn) *eventbus.Conn {
	data, _ := conn.Encode()
	out := &eventbus.Conn{}
	_ = out.Decode(data)
	out.LastActive = uint64(time.Now().Unix())
	return out
}

func (m *Manager) snapshot(uids []string) (snapshotResponse, error) {
	if len(uids) == 0 || len(uids) > 128 {
		return snapshotResponse{}, ErrNotReady
	}
	requested := make(map[string]bool, len(uids))
	for _, uid := range uids {
		requested[uid] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	response := snapshotResponse{Boot: m.boot}
	for uid := range requested {
		for id := range m.byUID[uid] {
			conn := m.physical[id].conn
			if conn == nil || !conn.Auth || conn.Uid != uid {
				continue
			}
			data, err := conn.Encode()
			if err != nil {
				return snapshotResponse{}, err
			}
			response.Sessions = append(response.Sessions, data)
		}
	}
	return response, nil
}

func (m *Manager) liveUIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var uids []string
	for _, entry := range m.physical {
		if entry.conn != nil && entry.conn.Auth && !seen[entry.conn.Uid] {
			seen[entry.conn.Uid] = true
			uids = append(uids, entry.conn.Uid)
		}
	}
	return uids
}

func (m *Manager) lockRecovery(ctx context.Context) error {
	select {
	case m.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
