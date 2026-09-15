// Package presence keeps physical sessions independent of user-slot authority.
package presence

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
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
	online     bool
	lastUsed   time.Time
}

type Manager struct {
	byUID            map[string]map[int64]struct{}
	mu               sync.Mutex
	boot             string
	node             uint64
	physical         map[int64]physicalSession
	ready            map[string]*readiness
	legacyUptimeSeed uint64
	rejected         atomic.Uint64
	// Bounds cold recovery work without serializing unrelated warm deliveries.
	// Source snapshots never acquire this gate.
	gate chan struct{}
}

func New(node uint64) *Manager {
	return &Manager{byUID: make(map[string]map[int64]struct{}), boot: wkutil.GenUUID(), node: node, physical: make(map[int64]physicalSession), ready: make(map[string]*readiness), legacyUptimeSeed: uint64(time.Now().UnixNano()), gate: make(chan struct{}, 8)}
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
	ownerBootID, sessionID := m.boot, ""
	if entry.conn != nil {
		sessionID = entry.conn.SessionID
		conn.Uptime = entry.conn.Uptime
	} else {
		// Uptime is part of the legacy descriptor format, so old nodes preserve
		// it even though they strip the appended boot/session fields. Give every
		// physical socket a high-resolution, monotonically unique value to keep
		// delayed legacy CONNACKs from authenticating a reused numeric ID.
		candidate := uint64(raw.Uptime().UnixNano())
		if candidate <= m.legacyUptimeSeed {
			candidate = m.legacyUptimeSeed + 1
		}
		m.legacyUptimeSeed = candidate
		conn.Uptime = candidate
	}
	m.unindexLocked(entry.conn)
	if sessionID == "" {
		sessionID = wkutil.GenUUID()
	}
	conn.OwnerBootID = ownerBootID
	conn.SessionID = sessionID
	entry.conn = copyConn(conn)
	if m.byUID[conn.Uid] == nil {
		m.byUID[conn.Uid] = make(map[int64]struct{})
	}
	m.byUID[conn.Uid][conn.ConnId] = struct{}{}
	m.physical[raw.ID()] = entry
}

func (m *Manager) Authenticate(conn *eventbus.Conn) bool {
	if conn == nil || !conn.Auth {
		m.rejected.Add(1)
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.physical[conn.ConnId]
	if !ok || entry.conn == nil {
		m.rejected.Add(1)
		return false
	}
	if conn.IsLegacySession() {
		if !entry.conn.LegacyMatches(conn) {
			m.rejected.Add(1)
			return false
		}
		conn.OwnerBootID = entry.conn.OwnerBootID
		conn.SessionID = entry.conn.SessionID
	} else if !conn.HasSessionIdentity() || !entry.conn.SameSession(conn) || conn.OwnerBootID != m.boot {
		m.rejected.Add(1)
		return false
	}
	entry.conn = copyConn(conn)
	m.physical[conn.ConnId] = entry
	// Publish the authenticated context while close and authentication are fenced.
	entry.raw.SetContext(conn)
	m.invalidateLocked(conn.Uid)
	return true
}

func (m *Manager) RejectedCount() uint64 {
	return m.rejected.Load()
}

// LocalSession returns the currently prepared physical session for a
// descriptor that refers to the same socket. It is used to deliver an explicit
// authentication failure when a delayed CONNACK carries a stale identity.
func (m *Manager) LocalSession(claim *eventbus.Conn) *eventbus.Conn {
	if claim == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.physical[claim.ConnId]
	if !ok || entry.conn == nil || !entry.conn.LegacyMatches(claim) {
		return nil
	}
	return copyConn(entry.conn)
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
			safe := copyConn(conn)
			safe.AesIV = nil
			safe.AesKey = nil
			data, err := safe.Encode()
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

func (m *Manager) IsReady(uid string) bool {
	if uid == "" {
		return false
	}
	hasLogicalSession := len(eventbus.User.ConnsByUid(uid)) > 0
	now := time.Now()
	version := service.Cluster.NodeVersion()
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.ready[uid]
	if r == nil || r.version != version || !now.Before(r.until) || (r.online && !hasLogicalSession) {
		return false
	}
	r.lastUsed = now
	return true
}

func (m *Manager) ensureReadyCapacityLocked(add int, now time.Time) {
	const maxReady = 32768
	if len(m.ready)+add <= maxReady {
		return
	}
	for uid, r := range m.ready {
		if !now.Before(r.until) {
			delete(m.ready, uid)
		}
	}
	if len(m.ready)+add <= maxReady {
		return
	}
	type candidate struct {
		uid  string
		used time.Time
	}
	candidates := make([]candidate, 0, len(m.ready))
	for uid, r := range m.ready {
		candidates = append(candidates, candidate{uid: uid, used: r.lastUsed})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].used.Before(candidates[j].used) })
	remove := len(m.ready) + add - maxReady
	for i := 0; i < remove && i < len(candidates); i++ {
		delete(m.ready, candidates[i].uid)
	}
}
